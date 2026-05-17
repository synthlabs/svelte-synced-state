<script lang="ts">
	import { onDestroy } from 'svelte';
	import { SyncedCollection, SyncedState, indexedAddress } from 'svelte-synced-state';

	type Customer = {
		id: string;
		name: string;
		status: string;
	};

	const customers = new SyncedCollection<Customer>('customer');
	let selectedID = $state('100');
	let selected = $state(createSelectedState(selectedID));

	function createSelectedState(id: string) {
		return new SyncedState<Customer>(indexedAddress('customer', id), {
			id,
			name: '',
			status: ''
		});
	}

	function selectCustomer(id: string) {
		selected.close();
		selectedID = id;
		selected = createSelectedState(id);
	}

	async function saveSelected() {
		await fetch(`/customers/${selectedID}`, {
			method: 'PUT',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify({
				name: selected.obj.name,
				status: selected.obj.status
			})
		});
	}

	async function syncCollectionEntry(id: string) {
		await customers.sync(id);
	}

	onDestroy(() => {
		customers.close();
		selected.close();
	});
</script>

<section>
	<h2>Customers</h2>

	{#if customers.ready}
		<ul>
			{#each Object.entries(customers.entries) as [id, customer]}
				<li>
					<button type="button" onclick={() => selectCustomer(id)}>
						{customer.name || id}
					</button>
					<span>{customer.status}</span>
					<button type="button" onclick={() => syncCollectionEntry(id)}>Sync entry</button>
				</li>
			{/each}
		</ul>
	{:else}
		<p>Connecting...</p>
	{/if}
</section>

<section>
	<h2>Selected customer</h2>

	{#if selected.ready}
		<label>
			Name
			<input bind:value={selected.obj.name} />
		</label>
		<label>
			Status
			<input bind:value={selected.obj.status} />
		</label>
		<button type="button" onclick={saveSelected}>Save on server</button>
		<button type="button" onclick={() => selected.sync()}>Sync exact state</button>
	{:else}
		<p>Loading {selectedID}...</p>
	{/if}
</section>
