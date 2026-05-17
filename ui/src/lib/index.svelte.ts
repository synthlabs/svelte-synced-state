import { getDefaultClient, type SyncedClient, type SyncedClientOptions } from './client.js';
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

export { getDefaultClient, resetDefaultClient, SyncedClient } from './client.js';
export type { StateMessage, MessageType } from './protocol.js';
