import { getDefaultClient, type SyncedClient, type SyncedClientOptions } from './client.js';
import { LogLevel, type LogLevel as LogLevelValue } from './protocol.js';

type ConsoleMethod = 'trace' | 'debug' | 'info' | 'log' | 'warn' | 'error';
type LevelName = 'trace' | 'debug' | 'info' | 'warn' | 'error';

type ConsoleFn = (...args: unknown[]) => void;

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

const consoleMethods: ConsoleMethod[] = ['trace', 'debug', 'info', 'log', 'warn', 'error'];
const consoleLevels: Record<ConsoleMethod, LogLevelValue> = {
	trace: LogLevel.Trace,
	debug: LogLevel.Debug,
	info: LogLevel.Info,
	log: LogLevel.Info,
	warn: LogLevel.Warn,
	error: LogLevel.Error
};
const levelMethods: Record<LevelName, ConsoleMethod> = {
	trace: 'trace',
	debug: 'debug',
	info: 'info',
	warn: 'warn',
	error: 'error'
};

let baselineConsole: Record<ConsoleMethod, ConsoleFn> | undefined;
let activeOwner: symbol | undefined;
let activeForwarder: ((method: ConsoleMethod, args: unknown[]) => void) | undefined;
let patched = false;

export function createLogger(options: LoggerOptions = {}): Logger {
	captureBaselineConsole();

	const owner = Symbol('svelte-synced-state logger');
	const scope = options.scope ?? 'app';
	const client = options.client ?? getDefaultClient(options);

	const emit = (level: LogLevelValue, message: string, scopeOverride?: string) => {
		client.log({
			level,
			message,
			timestamp: new Date().toISOString(),
			scope: scopeOverride ?? scope
		});
	};
	const print = (levelName: LevelName, args: unknown[]) => {
		printBaseline(levelMethods[levelName], args);
	};

	const logger: Logger = {
		trace(message: string, scopeOverride?: string) {
			print('trace', [message]);
			emit(LogLevel.Trace, message, scopeOverride);
		},
		debug(message: string, scopeOverride?: string) {
			print('debug', [message]);
			emit(LogLevel.Debug, message, scopeOverride);
		},
		info(message: string, scopeOverride?: string) {
			print('info', [message]);
			emit(LogLevel.Info, message, scopeOverride);
		},
		warn(message: string, scopeOverride?: string) {
			print('warn', [message]);
			emit(LogLevel.Warn, message, scopeOverride);
		},
		error(message: string, scopeOverride?: string) {
			print('error', [message]);
			emit(LogLevel.Error, message, scopeOverride);
		},
		forwardConsole() {
			installConsolePatch();
			activeOwner = owner;
			activeForwarder = (method, args) => {
				emit(consoleLevels[method], stringifyArgs(args));
			};

			return () => {
				if (activeOwner !== owner) {
					return;
				}
				activeOwner = undefined;
				activeForwarder = undefined;
				restoreConsolePatch();
			};
		}
	};

	return logger;
}

function captureBaselineConsole() {
	if (baselineConsole) {
		return;
	}

	const currentConsole = globalThis.console;
	const fallback = currentConsole?.log?.bind(currentConsole) ?? (() => {});
	baselineConsole = Object.fromEntries(
		consoleMethods.map((method) => {
			const fn = currentConsole?.[method]?.bind(currentConsole) ?? fallback;
			return [method, fn];
		})
	) as Record<ConsoleMethod, ConsoleFn>;
}

function installConsolePatch() {
	if (patched) {
		return;
	}
	captureBaselineConsole();

	for (const method of consoleMethods) {
		(globalThis.console as unknown as Record<ConsoleMethod, ConsoleFn>)[method] = (...args: unknown[]) => {
			printBaseline(method, args);
			activeForwarder?.(method, args);
		};
	}
	patched = true;
}

function restoreConsolePatch() {
	if (!patched || activeOwner) {
		return;
	}
	const baseline = baselineConsole;
	if (!baseline) {
		return;
	}
	for (const method of consoleMethods) {
		(globalThis.console as unknown as Record<ConsoleMethod, ConsoleFn>)[method] = baseline[method];
	}
	patched = false;
}

function printBaseline(method: ConsoleMethod, args: unknown[]) {
	captureBaselineConsole();
	baselineConsole?.[method](...args);
}

function stringifyArgs(args: unknown[]): string {
	return args.map(stringifyValue).join(' ');
}

function stringifyValue(value: unknown): string {
	if (typeof value === 'string') {
		return value;
	}
	if (value instanceof Error) {
		return value.stack ?? value.message;
	}
	if (value === undefined) {
		return 'undefined';
	}
	try {
		const json = JSON.stringify(value);
		return json ?? String(value);
	} catch {
		return String(value);
	}
}
