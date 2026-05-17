<script lang="ts">
	import { onDestroy } from 'svelte';
	import { SyncedState } from 'svelte-synced-state';

	type AppState = {
		authenticated: boolean;
		name: string;
		message: string;
	};

	const appState = new SyncedState<AppState>('AppState', {
		authenticated: false,
		name: '',
		message: ''
	});

	let pendingName = $state('');

	async function login(event: Event) {
		event.preventDefault();

		await fetch('/login', {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify({ name: pendingName })
		});
	}

	async function reset() {
		await fetch('/reset', { method: 'POST' });
	}

	async function syncLocalEdits() {
		// Use this when the UI directly edits appState.obj and should replace the
		// server value. Server-side handlers can also update the same state.
		await appState.sync();
	}

	onDestroy(() => {
		appState.close();
	});
</script>

{#if appState.ready}
	<section>
		<p>{appState.obj.message}</p>

		{#if appState.obj.authenticated}
			<p>Logged in as {appState.obj.name}</p>
			<button type="button" onclick={reset}>Reset</button>
		{:else}
			<form onsubmit={login}>
				<input bind:value={pendingName} placeholder="Name" />
				<button type="submit">Log in</button>
			</form>
		{/if}

		<label>
			Message
			<input bind:value={appState.obj.message} />
		</label>
		<button type="button" onclick={syncLocalEdits}>Sync local edits</button>
	</section>
{:else}
	<p>Loading...</p>
{/if}
