import { type SyncedClient, type SyncedClientOptions } from './client.js';
import type { SyncResult } from './protocol.js';
export interface SyncedStateOptions extends SyncedClientOptions {
    client?: SyncedClient;
}
export declare class SyncedState<T> {
    #private;
    name: string;
    obj: T;
    ready: boolean;
    version: number | undefined;
    lastSyncError: string | undefined;
    initialized: Promise<void>;
    constructor(name: string, object?: T, options?: SyncedStateOptions);
    close(): void;
    sync(): Promise<SyncResult>;
}
export interface SyncedCollectionOptions extends SyncedClientOptions {
    client?: SyncedClient;
}
export declare class SyncedCollection<T> {
    #private;
    scope: string;
    wildcard: string;
    entries: Record<string, T>;
    versions: Record<string, number | undefined>;
    syncErrors: Record<string, string | undefined>;
    ready: boolean;
    initialized: Promise<void>;
    constructor(scope: string, entries?: Record<string, T>, options?: SyncedCollectionOptions);
    close(): void;
    address(id: string): string;
    sync(id: string): Promise<SyncResult>;
}
export { createLogger, getDefaultClient, LogLevel, resetDefaultClient, SyncedClient } from './client.js';
export type { ConnectionStatus, Logger, LoggerOptions, LogPayload } from './client.js';
export { indexedAddress, indexedWildcard, singletonAddress } from './address.js';
export type { MessageType, StateMessage, SyncResult } from './protocol.js';
