import type { StateMessage } from './protocol.js';
type WebSocketLike = typeof WebSocket;
export interface SyncedClientOptions {
    url?: string | URL;
    protocols?: string | string[];
    WebSocketCtor?: WebSocketLike;
    reconnect?: boolean;
    reconnectDelayMs?: number;
    reconnectMaxDelayMs?: number;
    onStatusChange?: (status: ConnectionStatus) => void;
}
export type ConnectionStatus = 'connecting' | 'connected' | 'disconnected';
export type StateMessageHandler<T = unknown> = (message: StateMessage<T>) => void;
export declare class SyncedClient {
    #private;
    constructor(options?: SyncedClientOptions);
    get status(): ConnectionStatus;
    onStatusChange(listener: (status: ConnectionStatus) => void): () => void;
    connect(): Promise<void>;
    subscribe<T>(name: string, handler: StateMessageHandler<T>): () => void;
    snapshot(name: string): Promise<boolean>;
    set<T>(name: string, value: T, version?: number): Promise<boolean>;
    send(message: StateMessage): Promise<boolean>;
    close(code?: number, reason?: string): void;
}
export declare function getDefaultClient(options?: SyncedClientOptions): SyncedClient;
export declare function resetDefaultClient(): void;
export {};
