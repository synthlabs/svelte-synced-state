export type MessageType = 'subscribe' | 'unsubscribe' | 'set' | 'snapshot' | 'update' | 'log' | 'error';
export declare const LogLevel: {
    readonly Trace: 1;
    readonly Debug: 2;
    readonly Info: 3;
    readonly Warn: 4;
    readonly Error: 5;
};
export type LogLevel = (typeof LogLevel)[keyof typeof LogLevel];
export interface LogPayload {
    level: LogLevel;
    message: string;
    timestamp: string;
    scope: string;
}
export interface StateMessage<T = unknown> {
    type: MessageType;
    id?: string;
    name?: string;
    version?: number;
    value?: T;
    error?: string;
}
export interface SyncResult {
    ok: boolean;
    version?: number;
    error?: string;
}
