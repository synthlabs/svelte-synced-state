export type MessageType = 'subscribe' | 'unsubscribe' | 'set' | 'snapshot' | 'update' | 'log' | 'error';

export const LogLevel = {
	Trace: 1,
	Debug: 2,
	Info: 3,
	Warn: 4,
	Error: 5
} as const;

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
