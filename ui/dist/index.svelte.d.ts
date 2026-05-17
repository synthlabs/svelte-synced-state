import { type SyncedClient, type SyncedClientOptions } from './client.js';
export interface SyncedStateOptions extends SyncedClientOptions {
    client?: SyncedClient;
}
export declare class SyncedState<T> {
    #private;
    name: string;
    obj: T;
    ready: boolean;
    version: number | undefined;
    initialized: Promise<void>;
    constructor(name: string, object?: T, options?: SyncedStateOptions);
    close(): void;
    sync(): Promise<boolean>;
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
    ready: boolean;
    initialized: Promise<void>;
    constructor(scope: string, entries?: Record<string, T>, options?: SyncedCollectionOptions);
    close(): void;
    address(id: string): string;
    sync(id: string): Promise<boolean>;
}
export { getDefaultClient, resetDefaultClient, SyncedClient } from './client.js';
export { indexedAddress, indexedWildcard, singletonAddress } from './address.js';
export type { StateMessage, MessageType } from './protocol.js';
