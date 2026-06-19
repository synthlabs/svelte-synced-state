import { type SyncedClient, type SyncedClientOptions } from './client.js';
export interface LoggerOptions extends SyncedClientOptions {
    scope?: string;
    client?: SyncedClient;
}
export interface Logger {
    trace(message: string, scope?: string): void;
    debug(message: string, scope?: string): void;
    info(message: string, scope?: string): void;
    warn(message: string, scope?: string): void;
    error(message: string, scope?: string): void;
    forwardConsole(): () => void;
}
export declare function createLogger(options?: LoggerOptions): Logger;
