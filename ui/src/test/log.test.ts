import { afterEach, describe, expect, it, vi } from 'vitest';
import type { SyncedClient } from '../lib/client.js';

function stubConsole() {
	const fake = {
		trace: vi.fn(),
		debug: vi.fn(),
		info: vi.fn(),
		log: vi.fn(),
		warn: vi.fn(),
		error: vi.fn()
	};
	vi.stubGlobal('console', fake);
	return fake;
}

async function loadClientEntry() {
	vi.resetModules();
	return import('../lib/client.js');
}

afterEach(() => {
	vi.unstubAllGlobals();
	vi.restoreAllMocks();
});

describe('createLogger', () => {
	it('prints through the baseline console and sends a client log payload', async () => {
		const fakeConsole = stubConsole();
		const { createLogger, LogLevel } = await loadClientEntry();
		const client = { log: vi.fn() };

		const logger = createLogger({
			scope: 'app',
			client: client as unknown as SyncedClient
		});
		logger.info('hello', 'ui');

		expect(fakeConsole.info).toHaveBeenCalledWith('hello');
		expect(client.log).toHaveBeenCalledTimes(1);
		const payload = client.log.mock.calls[0]?.[0];
		expect(payload).toMatchObject({
			level: LogLevel.Info,
			message: 'hello',
			scope: 'ui'
		});
		expect(Number.isNaN(Date.parse(payload.timestamp))).toBe(false);
	});

	it('forwards console calls through one active owner without stacking wrappers', async () => {
		const fakeConsole = stubConsole();
		const logSpy = fakeConsole.log;
		const warnSpy = fakeConsole.warn;
		const { createLogger, LogLevel } = await loadClientEntry();
		const firstClient = { log: vi.fn() };
		const secondClient = { log: vi.fn() };
		const first = createLogger({ scope: 'first', client: firstClient as unknown as SyncedClient });
		const second = createLogger({ scope: 'second', client: secondClient as unknown as SyncedClient });

		const restoreFirst = first.forwardConsole();
		console.log('first', { count: 1 });
		expect(logSpy).toHaveBeenCalledWith('first', { count: 1 });
		expect(firstClient.log).toHaveBeenCalledTimes(1);
		expect(firstClient.log.mock.calls[0]?.[0]).toMatchObject({
			level: LogLevel.Info,
			message: 'first {"count":1}',
			scope: 'first'
		});

		const restoreSecond = second.forwardConsole();
		console.trace('second');
		expect(firstClient.log).toHaveBeenCalledTimes(1);
		expect(secondClient.log).toHaveBeenCalledTimes(1);
		expect(secondClient.log.mock.calls[0]?.[0]).toMatchObject({
			level: LogLevel.Trace,
			message: 'second',
			scope: 'second'
		});

		restoreFirst();
		console.error(new Error('still second'));
		expect(firstClient.log).toHaveBeenCalledTimes(1);
		expect(secondClient.log).toHaveBeenCalledTimes(2);
		expect(secondClient.log.mock.calls[1]?.[0]).toMatchObject({
			level: LogLevel.Error,
			scope: 'second'
		});
		expect(secondClient.log.mock.calls[1]?.[0].message).toContain('still second');

		restoreSecond();
		const callsAfterRestore = secondClient.log.mock.calls.length;
		console.warn('restored');
		expect(warnSpy).toHaveBeenCalledWith('restored');
		expect(secondClient.log).toHaveBeenCalledTimes(callsAfterRestore);
	});

	it('does not recurse when explicit logger calls are made while console is forwarded', async () => {
		stubConsole();
		const { createLogger, LogLevel } = await loadClientEntry();
		const client = { log: vi.fn() };
		const logger = createLogger({ scope: 'ui', client: client as unknown as SyncedClient });

		const restore = logger.forwardConsole();
		logger.warn('explicit');
		restore();

		expect(client.log).toHaveBeenCalledTimes(1);
		expect(client.log.mock.calls[0]?.[0]).toMatchObject({
			level: LogLevel.Warn,
			message: 'explicit',
			scope: 'ui'
		});
	});

	it('exports matching numeric log level values', async () => {
		const { LogLevel } = await loadClientEntry();

		expect(LogLevel).toEqual({
			Trace: 1,
			Debug: 2,
			Info: 3,
			Warn: 4,
			Error: 5
		});
	});
});
