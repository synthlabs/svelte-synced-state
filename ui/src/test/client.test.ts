import { afterEach, describe, expect, it, vi } from 'vitest';
import { LogLevel, SyncedClient, getDefaultClient, resetDefaultClient } from '../lib/client.js';
import type { StateMessage } from '../lib/protocol.js';
import { FakeWebSocket, flushMicrotasks } from './fake-websocket.js';

const WebSocketCtor = FakeWebSocket as unknown as typeof WebSocket;
const defaultURL = 'ws://example.test/synced-state';

function createClient(options: { url?: string | URL; protocols?: string | string[] } = {}) {
	return new SyncedClient({
		url: defaultURL,
		WebSocketCtor,
		...options
	});
}

afterEach(() => {
	resetDefaultClient();
	FakeWebSocket.reset();
	vi.unstubAllGlobals();
});

describe('SyncedClient', () => {
	it('resolves the browser default URL and forwards protocols', async () => {
		vi.stubGlobal('location', { protocol: 'https:', host: 'app.example.test' });
		const client = new SyncedClient({ protocols: ['state.v1'], WebSocketCtor });

		const pending = client.connect();
		const socket = FakeWebSocket.latest();
		expect(socket.url).toBe('wss://app.example.test/synced-state');
		expect(socket.protocols).toEqual(['state.v1']);

		socket.open();
		await expect(pending).resolves.toBeUndefined();
	});

	it('requires an explicit URL outside the browser', () => {
		vi.stubGlobal('location', undefined);

		expect(() => new SyncedClient({ WebSocketCtor })).toThrow(
			'SyncedClient requires a URL outside the browser'
		);
	});

	it('reuses an in-flight or open connection', async () => {
		const client = createClient();

		const first = client.connect();
		const second = client.connect();
		expect(FakeWebSocket.instances).toHaveLength(1);

		FakeWebSocket.latest().open();
		await expect(Promise.all([first, second])).resolves.toEqual([undefined, undefined]);

		await client.connect();
		expect(FakeWebSocket.instances).toHaveLength(1);
	});

	it('rejects the connection promise when the socket errors before opening', async () => {
		const client = createClient();

		const pending = client.connect();
		FakeWebSocket.latest().fail();

		await expect(pending).rejects.toThrow('websocket connection failed');
	});

	it('subscribes once connected and routes messages by state name', async () => {
		const client = createClient();
		const received: StateMessage[] = [];

		client.subscribe('TestState', (message) => received.push(message));

		const socket = FakeWebSocket.latest();
		expect(socket.url).toBe(defaultURL);
		socket.open();
		await flushMicrotasks();

		expect(socket.sentMessages()[0]).toMatchObject({
			type: 'subscribe',
			name: 'TestState'
		});
		expect(socket.sent).toHaveLength(1);

		socket.receive({ type: 'snapshot', name: 'TestState', version: 1, value: { count: 1 } });
		socket.receive({ type: 'snapshot', name: 'OtherState', version: 1, value: { count: 2 } });

		expect(received).toEqual([
			{ type: 'snapshot', name: 'TestState', version: 1, value: { count: 1 } }
		]);
	});

	it('routes indexed messages to exact and wildcard subscriptions', async () => {
		const client = createClient();
		const exactReceived: StateMessage[] = [];
		const wildcardReceived: StateMessage[] = [];

		client.subscribe('customer:123', (message) => exactReceived.push(message));
		client.subscribe('customer:*', (message) => wildcardReceived.push(message));

		const socket = FakeWebSocket.latest();
		socket.open();
		await flushMicrotasks();

		socket.receive({ type: 'update', name: 'customer:123', version: 2, value: { name: 'one' } });
		socket.receive({ type: 'update', name: 'customer:456', version: 2, value: { name: 'two' } });
		socket.receive({ type: 'update', name: 'order:123', version: 2, value: { name: 'order' } });

		expect(exactReceived).toEqual([
			{ type: 'update', name: 'customer:123', version: 2, value: { name: 'one' } }
		]);
		expect(wildcardReceived).toEqual([
			{ type: 'update', name: 'customer:123', version: 2, value: { name: 'one' } },
			{ type: 'update', name: 'customer:456', version: 2, value: { name: 'two' } }
		]);
	});

	it('deduplicates a local handler registered for exact and wildcard routes', async () => {
		const client = createClient();
		const received: StateMessage[] = [];
		const handler = (message: StateMessage) => received.push(message);

		client.subscribe('customer:123', handler);
		client.subscribe('customer:*', handler);

		const socket = FakeWebSocket.latest();
		socket.open();
		await flushMicrotasks();

		socket.receive({ type: 'update', name: 'customer:123', version: 2, value: { name: 'one' } });

		expect(received).toEqual([
			{ type: 'update', name: 'customer:123', version: 2, value: { name: 'one' } }
		]);
	});

	it('sends set and snapshot messages over the open socket', async () => {
		const client = createClient();

		const setPending = client.set('TestState', { count: 3 });
		const socket = FakeWebSocket.latest();
		socket.open();
		expect(await setPending).toBe(true);

		expect(socket.sentMessages()[0]).toMatchObject({
			type: 'set',
			name: 'TestState',
			value: { count: 3 }
		});

		await expect(client.snapshot('TestState')).resolves.toBe(true);
		expect(socket.sentMessages().at(-1)).toMatchObject({
			type: 'snapshot',
			name: 'TestState'
		});
	});

	it('sends log messages only when the socket is open', async () => {
		const client = createClient();
		const pending = client.connect();
		const socket = FakeWebSocket.latest();
		socket.open();
		await pending;

		client.log({
			level: LogLevel.Info,
			message: 'hello',
			timestamp: '2026-06-19T10:00:00.000Z',
			scope: 'ui'
		});

		expect(socket.sentMessages().at(-1)).toMatchObject({
			type: 'log',
			value: {
				level: LogLevel.Info,
				message: 'hello',
				timestamp: '2026-06-19T10:00:00.000Z',
				scope: 'ui'
			}
		});
	});

	it('drops the current log while opening a closed socket', async () => {
		const client = createClient();

		client.log({
			level: LogLevel.Warn,
			message: 'dropped',
			timestamp: '2026-06-19T10:00:00.000Z',
			scope: 'ui'
		});

		const socket = FakeWebSocket.latest();
		expect(socket.readyState).toBe(FakeWebSocket.CONNECTING);
		expect(socket.sent).toHaveLength(0);

		socket.fail();
		await flushMicrotasks();
	});

	it('does not throw when a log-triggered connection cannot be constructed', () => {
		class ThrowingWebSocket {
			static OPEN = 1;

			constructor() {
				throw new Error('boom');
			}
		}
		const client = new SyncedClient({
			url: defaultURL,
			WebSocketCtor: ThrowingWebSocket as unknown as typeof WebSocket
		});

		expect(() =>
			client.log({
				level: LogLevel.Error,
				message: 'ignored',
				timestamp: '2026-06-19T10:00:00.000Z',
				scope: 'ui'
			})
		).not.toThrow();
	});

	it('delivers messages to every local handler and unsubscribes only after the last one is removed', async () => {
		const client = createClient();
		const firstReceived: StateMessage[] = [];
		const secondReceived: StateMessage[] = [];

		const unsubscribeFirst = client.subscribe('TestState', (message) => firstReceived.push(message));
		const unsubscribeSecond = client.subscribe('TestState', (message) => secondReceived.push(message));

		const socket = FakeWebSocket.latest();
		socket.open();
		await flushMicrotasks();

		socket.receive({ type: 'update', name: 'TestState', version: 2, value: { count: 2 } });
		expect(firstReceived).toHaveLength(1);
		expect(secondReceived).toHaveLength(1);

		unsubscribeFirst();
		await flushMicrotasks();
		expect(socket.sentMessages().some((message) => message.type === 'unsubscribe')).toBe(false);

		socket.receive({ type: 'update', name: 'TestState', version: 3, value: { count: 3 } });
		expect(firstReceived).toHaveLength(1);
		expect(secondReceived).toHaveLength(2);

		unsubscribeSecond();
		await flushMicrotasks();
		expect(socket.sentMessages().at(-1)).toMatchObject({
			type: 'unsubscribe',
			name: 'TestState'
		});
	});

	it('ignores messages it cannot route to a subscription', async () => {
		const client = createClient();
		const received: StateMessage[] = [];

		client.subscribe('TestState', (message) => received.push(message));
		const socket = FakeWebSocket.latest();
		socket.open();
		await flushMicrotasks();

		socket.receiveRaw({ type: 'snapshot', name: 'TestState', value: { count: 1 } });
		socket.receive({ type: 'snapshot', version: 1, value: { count: 2 } });
		socket.receive({ type: 'snapshot', name: 'OtherState', version: 1, value: { count: 3 } });

		expect(received).toEqual([]);
	});

	it('can close and reconnect with a new socket', async () => {
		const client = createClient();

		const firstConnect = client.connect();
		const firstSocket = FakeWebSocket.latest();
		firstSocket.open();
		await firstConnect;

		client.close(3000, 'done');
		expect(firstSocket.readyState).toBe(FakeWebSocket.CLOSED);
		expect(firstSocket.closeCode).toBe(3000);
		expect(firstSocket.closeReason).toBe('done');

		const secondConnect = client.connect();
		const secondSocket = FakeWebSocket.latest();
		expect(secondSocket).not.toBe(firstSocket);
		expect(FakeWebSocket.instances).toHaveLength(2);
		secondSocket.open();
		await secondConnect;
	});

	it('reuses the default client until connection options change', () => {
		const first = getDefaultClient({ url: 'ws://one.test/synced-state', WebSocketCtor });
		expect(getDefaultClient()).toBe(first);

		const second = getDefaultClient({ url: 'ws://two.test/synced-state', WebSocketCtor });
		expect(second).not.toBe(first);
	});

	it('reconnects with backoff and re-subscribes after an unexpected close', async () => {
		vi.useFakeTimers();
		try {
			const client = new SyncedClient({
				url: defaultURL,
				WebSocketCtor,
				reconnectDelayMs: 1000,
				reconnectMaxDelayMs: 1000
			});
			const received: StateMessage[] = [];

			client.subscribe('TestState', (message) => received.push(message));

			const first = FakeWebSocket.latest();
			first.open();
			await flushMicrotasks();
			expect(first.sentMessages().filter((message) => message.type === 'subscribe')).toHaveLength(1);

			first.receive({ type: 'snapshot', name: 'TestState', version: 1, value: { count: 1 } });
			expect(received).toEqual([
				{ type: 'snapshot', name: 'TestState', version: 1, value: { count: 1 } }
			]);

			// Simulate the socket dropping while subscribed.
			first.close(1006, 'abnormal');
			await flushMicrotasks();

			// Reconnect is scheduled with backoff; advance past the delay.
			await vi.advanceTimersByTimeAsync(1000);

			const second = FakeWebSocket.latest();
			expect(second).not.toBe(first);
			expect(second.readyState).toBe(FakeWebSocket.CONNECTING);

			second.open();
			await flushMicrotasks();

			// The active subscription was re-sent on the new socket...
			expect(
				second.sentMessages().some((message) => message.type === 'subscribe' && message.name === 'TestState')
			).toBe(true);

			// ...so live updates resume without re-subscribing manually.
			second.receive({ type: 'snapshot', name: 'TestState', version: 2, value: { count: 2 } });
			expect(received).toEqual([
				{ type: 'snapshot', name: 'TestState', version: 1, value: { count: 1 } },
				{ type: 'snapshot', name: 'TestState', version: 2, value: { count: 2 } }
			]);
		} finally {
			vi.useRealTimers();
		}
	});

	it('does not reconnect after an explicit close', async () => {
		const client = new SyncedClient({ url: defaultURL, WebSocketCtor, reconnectDelayMs: 1 });

		const opened = client.connect();
		const first = FakeWebSocket.latest();
		first.open();
		await opened;

		client.close();
		expect(FakeWebSocket.instances).toHaveLength(1);

		// A fast backoff means any incorrectly-scheduled reconnect would fire here.
		await new Promise((resolve) => setTimeout(resolve, 20));
		expect(FakeWebSocket.instances).toHaveLength(1);
	});

	it('reports connection status through onStatusChange', async () => {
		const statuses: string[] = [];
		const client = new SyncedClient({
			url: defaultURL,
			WebSocketCtor,
			onStatusChange: (status) => statuses.push(status)
		});

		client.subscribe('TestState', () => {});
		expect(statuses.at(-1)).toBe('connecting');

		FakeWebSocket.latest().open();
		await flushMicrotasks();
		expect(statuses.at(-1)).toBe('connected');
	});
});
