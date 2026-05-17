import { getDefaultClient, type SyncedClient, type SyncedClientOptions } from './client.js';
import { indexedAddress, indexedID, indexedWildcard } from './address.js';
import type { StateMessage } from './protocol.js';

export interface SyncedStateOptions extends SyncedClientOptions {
	client?: SyncedClient;
}

export class SyncedState<T> {
	name: string;
	obj: T = $state({} as T);
	ready: boolean = $state(false);
	version: number | undefined = $state(undefined);
	initialized: Promise<void>;
	#client: SyncedClient;
	#unsubscribe: (() => void) | undefined;

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

	async sync(): Promise<boolean> {
		const value = $state.snapshot(this.obj) as T;
		return this.#client.set(this.name, value);
	}

	#handleMessage(message: StateMessage<T>, resolve: () => void) {
		if ((message.type === 'snapshot' || message.type === 'update') && message.value !== undefined) {
			this.obj = message.value;
			this.version = message.version;
			if (!this.ready) {
				this.ready = true;
				resolve();
			}
		}
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
	ready: boolean = $state(false);
	initialized: Promise<void>;
	#client: SyncedClient;
	#unsubscribe: (() => void) | undefined;

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

	async sync(id: string): Promise<boolean> {
		if (!(id in this.entries)) {
			return false;
		}

		const value = $state.snapshot(this.entries[id]) as T;
		return this.#client.set(this.address(id), value);
	}

	#handleMessage(message: StateMessage<T>) {
		if ((message.type !== 'snapshot' && message.type !== 'update') || message.value === undefined || !message.name) {
			return;
		}

		const id = indexedID(this.scope, message.name);
		if (id === undefined) {
			return;
		}

		this.entries[id] = message.value;
		this.versions[id] = message.version;
	}
}

export { getDefaultClient, resetDefaultClient, SyncedClient } from './client.js';
export { indexedAddress, indexedWildcard, singletonAddress } from './address.js';
export type { StateMessage, MessageType } from './protocol.js';
