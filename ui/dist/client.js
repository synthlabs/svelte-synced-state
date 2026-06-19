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
    #reconnect;
    #reconnectBaseDelayMs;
    #reconnectMaxDelayMs;
    #reconnectDelayMs;
    #reconnectTimer;
    #intentionalClose = false;
    #status = 'disconnected';
    #statusListeners = new Set();
    constructor(options = {}) {
        this.#url = resolveURL(options.url);
        this.#protocols = options.protocols;
        const WebSocketCtor = options.WebSocketCtor ?? globalThis.WebSocket;
        if (!WebSocketCtor) {
            throw new Error('SyncedClient requires a WebSocket constructor');
        }
        this.#WebSocketCtor = WebSocketCtor;
        this.#reconnect = options.reconnect ?? true;
        this.#reconnectBaseDelayMs = options.reconnectDelayMs ?? 1000;
        this.#reconnectMaxDelayMs = options.reconnectMaxDelayMs ?? 30000;
        this.#reconnectDelayMs = this.#reconnectBaseDelayMs;
        if (options.onStatusChange) {
            this.#statusListeners.add(options.onStatusChange);
        }
    }
    get status() {
        return this.#status;
    }
    onStatusChange(listener) {
        this.#statusListeners.add(listener);
        return () => {
            this.#statusListeners.delete(listener);
        };
    }
    #setStatus(status) {
        if (this.#status === status) {
            return;
        }
        this.#status = status;
        for (const listener of this.#statusListeners) {
            listener(status);
        }
    }
    connect() {
        if (this.#socket?.readyState === this.#WebSocketCtor.OPEN) {
            return Promise.resolve();
        }
        if (this.#open) {
            return this.#open;
        }
        if (this.#reconnectTimer) {
            clearTimeout(this.#reconnectTimer);
            this.#reconnectTimer = undefined;
        }
        this.#intentionalClose = false;
        this.#setStatus('connecting');
        this.#socket = new this.#WebSocketCtor(this.#url, this.#protocols);
        this.#open = new Promise((resolve, reject) => {
            this.#resolveOpen = resolve;
            this.#rejectOpen = reject;
        });
        this.#socket.onopen = () => {
            this.#resolveOpen?.();
            // Reset backoff after a successful open, then re-establish every feed on
            // the new socket — the server only sends snapshots in response to a
            // subscribe, so without this a reopened connection would receive nothing.
            this.#reconnectDelayMs = this.#reconnectBaseDelayMs;
            this.#resubscribe();
            this.#setStatus('connected');
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
            if (this.#reconnect && !this.#intentionalClose) {
                this.#scheduleReconnect();
            }
            else {
                this.#setStatus('disconnected');
            }
        };
        return this.#open;
    }
    #scheduleReconnect() {
        if (this.#reconnectTimer) {
            return;
        }
        const delay = this.#reconnectDelayMs;
        this.#reconnectDelayMs = Math.min(this.#reconnectDelayMs * 2, this.#reconnectMaxDelayMs);
        this.#setStatus('connecting');
        this.#reconnectTimer = setTimeout(() => {
            this.#reconnectTimer = undefined;
            void this.connect().catch(() => {
                // A failed reconnect attempt resolves the open promise with a rejection;
                // the socket's onclose (which always follows in browsers) reschedules.
            });
        }, delay);
    }
    #resubscribe() {
        for (const name of this.#subscriptions.keys()) {
            this.#sendRaw({ type: 'subscribe', id: this.#id(), name });
        }
    }
    #sendRaw(message) {
        if (this.#socket && this.#socket.readyState === this.#WebSocketCtor.OPEN) {
            this.#socket.send(JSON.stringify(message));
        }
    }
    subscribe(name, handler) {
        let handlers = this.#subscriptions.get(name);
        if (!handlers) {
            handlers = new Set();
            this.#subscriptions.set(name, handlers);
        }
        handlers.add(handler);
        if (this.#socket && this.#socket.readyState === this.#WebSocketCtor.OPEN) {
            this.#sendRaw({ type: 'subscribe', id: this.#id(), name });
        }
        else {
            // Not open yet: onopen re-sends every active subscription. Just make sure
            // a connection is opening.
            void this.connect();
        }
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
    async set(name, value, version) {
        return this.send({ type: 'set', id: this.#id(), name, value, version });
    }
    log(payload) {
        if (this.#socket && this.#socket.readyState === this.#WebSocketCtor.OPEN) {
            this.#sendRaw({ type: 'log', value: payload });
            return;
        }
        try {
            void this.connect().catch(() => { });
        }
        catch {
            // Logs are best-effort and should never break application flow.
        }
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
        this.#intentionalClose = true;
        if (this.#reconnectTimer) {
            clearTimeout(this.#reconnectTimer);
            this.#reconnectTimer = undefined;
        }
        this.#socket?.close(code, reason);
        this.#socket = undefined;
        this.#open = undefined;
        this.#setStatus('disconnected');
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
export { createLogger } from './log.js';
export { LogLevel } from './protocol.js';
