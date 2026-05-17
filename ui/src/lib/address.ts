export function singletonAddress(scope: string): string {
	validatePart('scope', scope);
	return scope;
}

export function indexedAddress(scope: string, id: string): string {
	validatePart('scope', scope);
	validatePart('id', id);
	return `${scope}:${id}`;
}

export function indexedWildcard(scope: string): string {
	validatePart('scope', scope);
	return `${scope}:*`;
}

export function indexedID(scope: string, address: string): string | undefined {
	const prefix = `${scope}:`;
	if (!address.startsWith(prefix)) {
		return undefined;
	}

	const id = address.slice(prefix.length);
	if (!isValidPart(id)) {
		return undefined;
	}
	return id;
}

export function wildcardForAddress(address: string): string | undefined {
	const separator = address.indexOf(':');
	if (separator < 0) {
		return undefined;
	}

	const scope = address.slice(0, separator);
	const id = address.slice(separator + 1);
	if (!isValidPart(scope) || !isValidPart(id)) {
		return undefined;
	}
	return `${scope}:*`;
}

function validatePart(label: string, value: string) {
	if (!isValidPart(value)) {
		throw new Error(`Synced state ${label} must be non-empty and cannot contain ':' or '*'`);
	}
}

function isValidPart(value: string): boolean {
	return value !== '' && !value.includes(':') && !value.includes('*');
}
