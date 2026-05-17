import type { StateMessage } from './protocol.js';
type WebSocketLike = typeof WebSocket;
export interface SyncedClientOptions {
    url?: string | URL;
    protocols?: string | string[];
    WebSocketCtor?: WebSocketLike;
}
export type StateMessageHandler<T = unknown> = (message: StateMessage<T>) => void;
export declare class SyncedClient {
    #private;
    constructor(options?: SyncedClientOptions);
    connect(): Promise<void>;
    subscribe<T>(name: string, handler: StateMessageHandler<T>): () => void;
    snapshot(name: string): Promise<boolean>;
    set<T>(name: string, value: T): Promise<boolean>;
    send(message: StateMessage): Promise<boolean>;
    close(code?: number, reason?: string): void;
}
export declare function getDefaultClient(options?: SyncedClientOptions): SyncedClient;
export declare function resetDefaultClient(): void;
export {};
