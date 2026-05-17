import { describe, expect, it } from 'vitest';
import { indexedAddress, indexedWildcard, singletonAddress } from '../lib/index.svelte.js';

describe('address helpers', () => {
	it('builds singleton, indexed, and wildcard addresses', () => {
		expect(singletonAddress('appstate')).toBe('appstate');
		expect(indexedAddress('customer', '123')).toBe('customer:123');
		expect(indexedWildcard('customer')).toBe('customer:*');
	});

	it('rejects empty or reserved scope parts', () => {
		expect(() => singletonAddress('')).toThrow("Synced state scope must be non-empty");
		expect(() => indexedAddress('customer:account', '123')).toThrow("Synced state scope must be non-empty");
		expect(() => indexedAddress('customer', '1*2')).toThrow("Synced state id must be non-empty");
	});
});
