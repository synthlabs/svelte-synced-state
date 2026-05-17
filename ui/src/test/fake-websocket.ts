import type { StateMessage } from '../lib/protocol.js';

export class FakeWebSocket {
	static CONNECTING = 0;
	static OPEN = 1;
	static CLOSING = 2;
	static CLOSED = 3;
	static instances: FakeWebSocket[] = [];

	readyState = FakeWebSocket.CONNECTING;
	sent: string[] = [];
	closeCode: number | undefined;
	closeReason: string | undefined;
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

	static reset() {
		FakeWebSocket.instances = [];
	}

	static latest(): FakeWebSocket {
		const socket = FakeWebSocket.instances.at(-1);
		if (!socket) {
			throw new Error('expected a FakeWebSocket instance');
		}
		return socket;
	}

	send(data: string) {
		this.sent.push(data);
	}

	close(code?: number, reason?: string) {
		this.readyState = FakeWebSocket.CLOSED;
		this.closeCode = code;
		this.closeReason = reason;
		this.onclose?.({ code, reason } as CloseEvent);
	}

	open() {
		this.readyState = FakeWebSocket.OPEN;
		this.onopen?.(new Event('open'));
	}

	fail() {
		this.onerror?.(new Event('error'));
	}

	receive(message: StateMessage) {
		this.receiveRaw(JSON.stringify(message));
	}

	receiveRaw(data: unknown) {
		this.onmessage?.({ data } as MessageEvent);
	}

	sentMessages(): StateMessage[] {
		return this.sent.map((message) => JSON.parse(message) as StateMessage);
	}
}

export async function flushMicrotasks() {
	await Promise.resolve();
	await Promise.resolve();
}
