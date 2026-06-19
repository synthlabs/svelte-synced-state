import { getDefaultClient } from './client.js';
import { indexedAddress, indexedID, indexedWildcard } from './address.js';
let nextSyncID = 1;
export class SyncedState {
    name;
    obj = $state({});
    ready = $state(false);
    version = $state(undefined);
    lastSyncError = $state(undefined);
    initialized;
    #client;
    #unsubscribe;
    #pendingSyncs = new Map();
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
        if (this.version === undefined) {
            const error = 'syncedstate: state version is unknown';
            this.lastSyncError = error;
            return { ok: false, error };
        }
        const id = this.#syncID();
        const value = $state.snapshot(this.obj);
        const result = new Promise((resolve) => {
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
    #handleMessage(message, resolve) {
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
    #resolveSync(message) {
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
    #syncID() {
        return createSyncID(this.name);
    }
}
export class SyncedCollection {
    scope;
    wildcard;
    entries = $state({});
    versions = $state({});
    syncErrors = $state({});
    ready = $state(false);
    initialized;
    #client;
    #unsubscribe;
    #pendingSyncs = new Map();
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
        const value = $state.snapshot(this.entries[id]);
        const result = new Promise((resolve) => {
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
    #handleMessage(message) {
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
    #resolveSync(message) {
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
    #syncID(id) {
        return createSyncID(this.address(id));
    }
}
function createSyncID(name) {
    return `${name}:sync:${nextSyncID++}`;
}
function syncResult(ok, version, error) {
    const result = { ok };
    if (version !== undefined) {
        result.version = version;
    }
    if (error !== undefined) {
        result.error = error;
    }
    return result;
}
export { createLogger, getDefaultClient, LogLevel, resetDefaultClient, SyncedClient } from './client.js';
export { indexedAddress, indexedWildcard, singletonAddress } from './address.js';
