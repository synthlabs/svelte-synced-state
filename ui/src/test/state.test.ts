import { afterEach, describe, expect, it, vi } from 'vitest';
import { SyncedClient, SyncedState, resetDefaultClient } from '../lib/index.svelte.js';
import { FakeWebSocket, flushMicrotasks } from './fake-websocket.js';

const WebSocketCtor = FakeWebSocket as unknown as typeof WebSocket;
const defaultURL = 'ws://example.test/synced-state';

type TestState = {
	count: number;
	label: string;
};

function createState(initial: TestState = { count: 0, label: 'initial' }) {
	const client = new SyncedClient({ url: defaultURL, WebSocketCtor });
	const state = new SyncedState<TestState>('TestState', initial, { client });
	return { state, socket: FakeWebSocket.latest() };
}

afterEach(() => {
	resetDefaultClient();
	FakeWebSocket.reset();
	vi.unstubAllGlobals();
});

describe('SyncedState', () => {
	it('subscribes on construction and keeps the initial object until a snapshot arrives', async () => {
		const { state, socket } = createState({ count: 0, label: 'loading' });

		socket.open();
		await flushMicrotasks();

		expect(socket.sentMessages()[0]).toMatchObject({
			type: 'subscribe',
			name: 'TestState'
		});
		expect(state.ready).toBe(false);
		expect(state.version).toBeUndefined();
		expect(state.obj.count).toBe(0);
		expect(state.obj.label).toBe('loading');

		const initialized = state.initialized.then(() => 'ready');
		socket.receive({ type: 'snapshot', name: 'TestState', version: 1, value: { count: 1, label: 'server' } });

		await expect(initialized).resolves.toBe('ready');
		expect(state.ready).toBe(true);
		expect(state.version).toBe(1);
		expect(state.obj).toEqual({ count: 1, label: 'server' });
	});

	it('applies snapshots and updates with values while ignoring other messages', async () => {
		const { state, socket } = createState();
		socket.open();
		await flushMicrotasks();

		socket.receive({ type: 'snapshot', name: 'TestState', version: 1, value: { count: 1, label: 'one' } });
		await state.initialized;

		socket.receive({ type: 'error', name: 'TestState', error: 'nope' });
		socket.receive({ type: 'update', name: 'TestState', version: 2 });
		expect(state.obj).toEqual({ count: 1, label: 'one' });
		expect(state.version).toBe(1);

		socket.receive({ type: 'update', name: 'TestState', version: 3, value: { count: 3, label: 'three' } });
		expect(state.obj).toEqual({ count: 3, label: 'three' });
		expect(state.version).toBe(3);
	});

	it('syncs the current local state as a full value replacement', async () => {
		const { state, socket } = createState();
		socket.open();
		await flushMicrotasks();

		socket.receive({ type: 'snapshot', name: 'TestState', version: 1, value: { count: 1, label: 'one' } });
		await state.initialized;

		state.obj.count = 5;
		state.obj.label = 'local';

		await expect(state.sync()).resolves.toBe(true);
		expect(socket.sentMessages().at(-1)).toMatchObject({
			type: 'set',
			name: 'TestState',
			value: { count: 5, label: 'local' }
		});
	});

	it('closes the underlying subscription idempotently', async () => {
		const { state, socket } = createState();
		socket.open();
		await flushMicrotasks();

		state.close();
		await flushMicrotasks();
		expect(socket.sentMessages().at(-1)).toMatchObject({
			type: 'unsubscribe',
			name: 'TestState'
		});

		const sentAfterFirstClose = socket.sent.length;
		state.close();
		await flushMicrotasks();
		expect(socket.sent).toHaveLength(sentAfterFirstClose);
	});
});
