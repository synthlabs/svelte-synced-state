import { getDefaultClient, type SyncedClient, type SyncedClientOptions } from './client.js';
import { indexedAddress, indexedID, indexedWildcard } from './address.js';
import type { StateMessage, SyncResult } from './protocol.js';

let nextSyncID = 1;

export interface SyncedStateOptions extends SyncedClientOptions {
	client?: SyncedClient;
}

export class SyncedState<T> {
	name: string;
	obj: T = $state({} as T);
	ready: boolean = $state(false);
	version: number | undefined = $state(undefined);
	lastSyncError: string | undefined = $state(undefined);
	initialized: Promise<void>;
	#client: SyncedClient;
	#unsubscribe: (() => void) | undefined;
	#pendingSyncs = new Map<string, (result: SyncResult) => void>();

	constructor(name: string, object?: T, options: SyncedStateOptions = {}) {
		this.name = name;
		if (object !== undefined) {
			this.obj = object;
		}

		this.#client = options.client ?? getDefaultClient(options);
		this.initialized = new Promise((resolve) => {
			this.#unsubscribe = this.#client.subscribe<T>(this.name, (message) => {
				this.#handleMessage(message, resolve);
			});
		});
	}

	close() {
		this.#unsubscribe?.();
		this.#unsubscribe = undefined;
	}

	async sync(): Promise<SyncResult> {
		if (this.version === undefined) {
			const error = 'syncedstate: state version is unknown';
			this.lastSyncError = error;
			return { ok: false, error };
		}

		const id = this.#syncID();
		const value = $state.snapshot(this.obj) as T;
		const result = new Promise<SyncResult>((resolve) => {
			this.#pendingSyncs.set(id, resolve);
		});

		const sent = await this.#client.send({
			type: 'set',
			id,
			name: this.name,
			version: this.version + 1,
			value
		});
		if (!sent) {
			this.#pendingSyncs.delete(id);
			const error = 'syncedstate: sync message was not sent';
			this.lastSyncError = error;
			return { ok: false, error };
		}
		return result;
	}

	#handleMessage(message: StateMessage<T>, resolve: () => void) {
		if (message.type === 'error' && message.error) {
			this.#resolveSync(message);
			return;
		}
		if ((message.type === 'snapshot' || message.type === 'update') && message.value !== undefined) {
			this.obj = message.value;
			this.version = message.version;
			this.#resolveSync(message);
			if (!this.ready) {
				this.ready = true;
				resolve();
			}
		}
	}

	#resolveSync(message: StateMessage<T>) {
		if (!message.id) {
			return;
		}

		const resolve = this.#pendingSyncs.get(message.id);
		if (!resolve) {
			return;
		}

		this.#pendingSyncs.delete(message.id);
		if (message.error) {
			this.lastSyncError = message.error;
			resolve(syncResult(false, message.version, message.error));
			return;
		}

		this.lastSyncError = undefined;
		resolve(syncResult(true, message.version));
	}

	#syncID(): string {
		return createSyncID(this.name);
	}
}

export interface SyncedCollectionOptions extends SyncedClientOptions {
	client?: SyncedClient;
}

export class SyncedCollection<T> {
	scope: string;
	wildcard: string;
	entries: Record<string, T> = $state({});
	versions: Record<string, number | undefined> = $state({});
	syncErrors: Record<string, string | undefined> = $state({});
	ready: boolean = $state(false);
	initialized: Promise<void>;
	#client: SyncedClient;
	#unsubscribe: (() => void) | undefined;
	#pendingSyncs = new Map<string, { id: string; resolve: (result: SyncResult) => void }>();

	constructor(scope: string, entries?: Record<string, T>, options: SyncedCollectionOptions = {}) {
		this.scope = scope;
		this.wildcard = indexedWildcard(scope);
		if (entries !== undefined) {
			this.entries = entries;
		}

		this.#client = options.client ?? getDefaultClient(options);
		this.#unsubscribe = this.#client.subscribe<T>(this.wildcard, (message) => {
			this.#handleMessage(message);
		});
		this.initialized = this.#client.connect().then(() => {
			this.ready = true;
		});
	}

	close() {
		this.#unsubscribe?.();
		this.#unsubscribe = undefined;
	}

	address(id: string): string {
		return indexedAddress(this.scope, id);
	}

	async sync(id: string): Promise<SyncResult> {
		if (!(id in this.entries)) {
			const error = 'syncedstate: state entry is missing';
			this.syncErrors[id] = error;
			return { ok: false, error };
		}
		const version = this.versions[id];
		if (version === undefined) {
			const error = 'syncedstate: state version is unknown';
			this.syncErrors[id] = error;
			return { ok: false, error };
		}

		const syncID = this.#syncID(id);
		const value = $state.snapshot(this.entries[id]) as T;
		const result = new Promise<SyncResult>((resolve) => {
			this.#pendingSyncs.set(syncID, { id, resolve });
		});

		const sent = await this.#client.send({
			type: 'set',
			id: syncID,
			name: this.address(id),
			version: version + 1,
			value
		});
		if (!sent) {
			this.#pendingSyncs.delete(syncID);
			const error = 'syncedstate: sync message was not sent';
			this.syncErrors[id] = error;
			return { ok: false, error };
		}
		return result;
	}

	#handleMessage(message: StateMessage<T>) {
		if (message.type === 'error' && message.error) {
			this.#resolveSync(message);
			return;
		}
		if ((message.type !== 'snapshot' && message.type !== 'update') || message.value === undefined || !message.name) {
			return;
		}

		const id = indexedID(this.scope, message.name);
		if (id === undefined) {
			return;
		}

		this.entries[id] = message.value;
		this.versions[id] = message.version;
		this.#resolveSync(message);
	}

	#resolveSync(message: StateMessage<T>) {
		if (!message.id) {
			return;
		}

		const pending = this.#pendingSyncs.get(message.id);
		if (!pending) {
			return;
		}

		this.#pendingSyncs.delete(message.id);
		if (message.error) {
			this.syncErrors[pending.id] = message.error;
			pending.resolve(syncResult(false, message.version, message.error));
			return;
		}

		this.syncErrors[pending.id] = undefined;
		pending.resolve(syncResult(true, message.version));
	}

	#syncID(id: string): string {
		return createSyncID(this.address(id));
	}
}

function createSyncID(name: string): string {
	return `${name}:sync:${nextSyncID++}`;
}

function syncResult(ok: boolean, version?: number, error?: string): SyncResult {
	const result: SyncResult = { ok };
	if (version !== undefined) {
		result.version = version;
	}
	if (error !== undefined) {
		result.error = error;
	}
	return result;
}

export { getDefaultClient, resetDefaultClient, SyncedClient } from './client.js';
export { indexedAddress, indexedWildcard, singletonAddress } from './address.js';
export type { MessageType, StateMessage, SyncResult } from './protocol.js';
