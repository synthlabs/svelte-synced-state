import { getDefaultClient } from './client.js';
import { indexedAddress, indexedID, indexedWildcard } from './address.js';
export class SyncedState {
    name;
    obj = $state({});
    ready = $state(false);
    version = $state(undefined);
    initialized;
    #client;
    #unsubscribe;
    constructor(name, object, options = {}) {
        this.name = name;
        if (object !== undefined) {
            this.obj = object;
        }
        this.#client = options.client ?? getDefaultClient(options);
        this.initialized = new Promise((resolve) => {
            this.#unsubscribe = this.#client.subscribe(this.name, (message) => {
                this.#handleMessage(message, resolve);
            });
        });
    }
    close() {
        this.#unsubscribe?.();
        this.#unsubscribe = undefined;
    }
    async sync() {
        const value = $state.snapshot(this.obj);
        return this.#client.set(this.name, value);
    }
    #handleMessage(message, resolve) {
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
export class SyncedCollection {
    scope;
    wildcard;
    entries = $state({});
    versions = $state({});
    ready = $state(false);
    initialized;
    #client;
    #unsubscribe;
    constructor(scope, entries, options = {}) {
        this.scope = scope;
        this.wildcard = indexedWildcard(scope);
        if (entries !== undefined) {
            this.entries = entries;
        }
        this.#client = options.client ?? getDefaultClient(options);
        this.#unsubscribe = this.#client.subscribe(this.wildcard, (message) => {
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
    address(id) {
        return indexedAddress(this.scope, id);
    }
    async sync(id) {
        if (!(id in this.entries)) {
            return false;
        }
        const value = $state.snapshot(this.entries[id]);
        return this.#client.set(this.address(id), value);
    }
    #handleMessage(message) {
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
