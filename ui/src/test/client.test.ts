import { afterEach, describe, expect, it } from 'vitest';
import { SyncedClient, resetDefaultClient } from '../lib/client.js';
import type { StateMessage } from '../lib/protocol.js';

class FakeWebSocket {
	static CONNECTING = 0;
	static OPEN = 1;
	static CLOSING = 2;
	static CLOSED = 3;
	static instances: FakeWebSocket[] = [];

	readyState = FakeWebSocket.CONNECTING;
	sent: string[] = [];
	onopen: ((event: Event) => void) | null = null;
	onmessage: ((event: MessageEvent) => void) | null = null;
	onerror: ((event: Event) => void) | null = null;
	onclose: ((event: CloseEvent) => void) | null = null;

	constructor(
		public url: string,
		public protocols?: string | string[]
	) {
		FakeWebSocket.instances.push(this);
	}

	send(data: string) {
		this.sent.push(data);
	}

	close() {
		this.readyState = FakeWebSocket.CLOSED;
		this.onclose?.(new CloseEvent('close'));
	}

	open() {
		this.readyState = FakeWebSocket.OPEN;
		this.onopen?.(new Event('open'));
	}

	receive(message: StateMessage) {
		this.onmessage?.(new MessageEvent('message', { data: JSON.stringify(message) }));
	}
}

afterEach(() => {
	FakeWebSocket.instances = [];
	resetDefaultClient();
});

describe('SyncedClient', () => {
	it('subscribes once connected and routes messages by state name', async () => {
		const client = new SyncedClient({
			url: 'ws://example.test/synced-state',
			WebSocketCtor: FakeWebSocket as unknown as typeof WebSocket
		});
		const received: StateMessage[] = [];

		client.subscribe('TestState', (message) => received.push(message));

		const socket = FakeWebSocket.instances[0];
		expect(socket.url).toBe('ws://example.test/synced-state');
		socket.open();
		await Promise.resolve();

		expect(JSON.parse(socket.sent[0])).toMatchObject({
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

	it('sends full value replacement messages', async () => {
		const client = new SyncedClient({
			url: 'ws://example.test/synced-state',
			WebSocketCtor: FakeWebSocket as unknown as typeof WebSocket
		});

		const pending = client.set('TestState', { count: 3 });
		const socket = FakeWebSocket.instances[0];
		socket.open();
		expect(await pending).toBe(true);

		expect(JSON.parse(socket.sent[0])).toMatchObject({
			type: 'set',
			name: 'TestState',
			value: { count: 3 }
		});
	});

	it('unsubscribes when the last local handler is removed', async () => {
		const client = new SyncedClient({
			url: 'ws://example.test/synced-state',
			WebSocketCtor: FakeWebSocket as unknown as typeof WebSocket
		});

		const unsubscribe = client.subscribe('TestState', () => {});
		const socket = FakeWebSocket.instances[0];
		socket.open();
		await Promise.resolve();

		unsubscribe();
		await Promise.resolve();

		expect(JSON.parse(socket.sent.at(-1) ?? '{}')).toMatchObject({
			type: 'unsubscribe',
			name: 'TestState'
		});
	});
});
