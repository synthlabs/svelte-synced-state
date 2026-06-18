export type MessageType = 'subscribe' | 'unsubscribe' | 'set' | 'snapshot' | 'update' | 'error';
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
