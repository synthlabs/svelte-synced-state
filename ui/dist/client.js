import { wildcardForAddress } from './address.js';
export class SyncedClient {
    #url;
    #protocols;
    #WebSocketCtor;
    #socket;
    #open;
    #resolveOpen;
    #rejectOpen;
    #subscriptions = new Map();
    #nextID = 1;
    constructor(options = {}) {
        this.#url = resolveURL(options.url);
        this.#protocols = options.protocols;
        const WebSocketCtor = options.WebSocketCtor ?? globalThis.WebSocket;
        if (!WebSocketCtor) {
            throw new Error('SyncedClient requires a WebSocket constructor');
        }
        this.#WebSocketCtor = WebSocketCtor;
    }
    connect() {
        if (this.#socket?.readyState === this.#WebSocketCtor.OPEN) {
            return Promise.resolve();
        }
        if (this.#open) {
            return this.#open;
        }
        this.#socket = new this.#WebSocketCtor(this.#url, this.#protocols);
        this.#open = new Promise((resolve, reject) => {
            this.#resolveOpen = resolve;
            this.#rejectOpen = reject;
        });
        this.#socket.onopen = () => {
            this.#resolveOpen?.();
        };
        this.#socket.onmessage = (event) => {
            this.#handleMessage(event.data);
        };
        this.#socket.onerror = () => {
            this.#rejectOpen?.(new Error('websocket connection failed'));
        };
        this.#socket.onclose = () => {
            this.#open = undefined;
            this.#resolveOpen = undefined;
            this.#rejectOpen = undefined;
            this.#socket = undefined;
        };
        return this.#open;
    }
    subscribe(name, handler) {
        let handlers = this.#subscriptions.get(name);
        if (!handlers) {
            handlers = new Set();
            this.#subscriptions.set(name, handlers);
        }
        handlers.add(handler);
        void this.send({ type: 'subscribe', id: this.#id(), name });
        return () => {
            const current = this.#subscriptions.get(name);
            if (!current) {
                return;
            }
            current.delete(handler);
            if (current.size === 0) {
                this.#subscriptions.delete(name);
                void this.send({ type: 'unsubscribe', id: this.#id(), name });
            }
        };
    }
    async snapshot(name) {
        return this.send({ type: 'snapshot', id: this.#id(), name });
    }
    async set(name, value) {
        return this.send({ type: 'set', id: this.#id(), name, value });
    }
    async send(message) {
        await this.connect();
        if (!this.#socket || this.#socket.readyState !== this.#WebSocketCtor.OPEN) {
            return false;
        }
        this.#socket.send(JSON.stringify(message));
        return true;
    }
    close(code, reason) {
        this.#socket?.close(code, reason);
        this.#socket = undefined;
        this.#open = undefined;
    }
    #handleMessage(data) {
        if (typeof data !== 'string') {
            return;
        }
        const message = JSON.parse(data);
        if (!message.name) {
            return;
        }
        const handlers = new Set();
        const exactHandlers = this.#subscriptions.get(message.name);
        for (const handler of exactHandlers ?? []) {
            handlers.add(handler);
        }
        const wildcard = wildcardForAddress(message.name);
        const wildcardHandlers = wildcard ? this.#subscriptions.get(wildcard) : undefined;
        for (const handler of wildcardHandlers ?? []) {
            handlers.add(handler);
        }
        if (handlers.size === 0) {
            return;
        }
        for (const handler of handlers) {
            handler(message);
        }
    }
    #id() {
        return String(this.#nextID++);
    }
}
let defaultClient;
export function getDefaultClient(options = {}) {
    if (!defaultClient || options.url || options.WebSocketCtor || options.protocols) {
        defaultClient = new SyncedClient(options);
    }
    return defaultClient;
}
export function resetDefaultClient() {
    defaultClient?.close();
    defaultClient = undefined;
}
function resolveURL(url) {
    if (url) {
        return String(url);
    }
    if (typeof globalThis.location === 'undefined') {
        throw new Error('SyncedClient requires a URL outside the browser');
    }
    const protocol = globalThis.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${protocol}//${globalThis.location.host}/synced-state`;
}
