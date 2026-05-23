# svelte-synced-state

Easily sync Go backend state with Svelte over WebSockets.

This package keeps typed state objects in a Go process and mirrors them into Svelte 5 `$state` objects. The backend exports a standard `net/http` WebSocket handler. The frontend subscribes to named state objects, applies server snapshots and updates, and can sync local changes back as full-value replacements.

This project is based on the same state-syncing model as [tauri-svelte-synced-store](https://github.com/synthlabs/tauri-svelte-synced-store), adapted from Tauri events to a standard Go WebSocket backend.

## Go

```go
package main

import (
	"context"
	"log"
	"net/http"

	syncedstate "github.com/synthlabs/svelte-synced-state"
)

type InternalState struct {
	Authenticated bool   `json:"authenticated"`
	Name          string `json:"name"`
}

func main() {
	manager := syncedstate.NewManager()
	internal, err := syncedstate.Define(manager, "InternalState", InternalState{})
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/synced-state", manager.Handler(
		syncedstate.WithOriginPatterns("http://localhost:*"),
	))

	http.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		err := internal.Update(r.Context(), func(state *InternalState) {
			state.Authenticated = true
			state.Name = r.FormValue("name")
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

Handler options can tune the websocket send buffer. By default, a client is
closed when its send buffer fills. Use `WithBlockOnFullBuffer` to apply
backpressure to the goroutine sending the update instead:

```go
http.Handle("/synced-state", manager.Handler(
	syncedstate.WithSendBuffer(128),
	syncedstate.WithBlockOnFullBuffer(),
))
```

For longer critical sections, use the lower-level lock handle:

```go
locked, err := internal.Lock(context.Background())
if err != nil {
	return err
}
defer locked.Unlock()

locked.Value().Authenticated = true
locked.Value().Name = "Jerod"
return locked.Sync(context.Background())
```

## Svelte

```svelte
<script lang="ts">
	import { SyncedState } from 'svelte-synced-state';

	type InternalState = {
		authenticated: boolean;
		name: string;
	};

	const internal = new SyncedState<InternalState>('InternalState', {
		authenticated: false,
		name: ''
	});

	async function login(event: Event) {
		event.preventDefault();
		await internal.sync();
		await fetch('/login', {
			method: 'POST',
			body: new URLSearchParams({ name: internal.obj.name })
		});
	}
</script>

{#if internal.obj.authenticated}
	<h1>Welcome {internal.obj.name}</h1>
{/if}

<form onsubmit={login}>
	<input bind:value={internal.obj.name} />
	<button type="submit">Log in</button>
</form>
```

## Protocol

The WebSocket transport uses JSON envelopes:

```json
{ "type": "subscribe", "id": "1", "name": "InternalState" }
```

```json
{ "type": "update", "name": "InternalState", "version": 2, "value": { "authenticated": true, "name": "Jerod" } }
```

```json
{ "type": "set", "id": "2", "name": "InternalState", "version": 3, "value": { "authenticated": false, "name": "" } }
```

Supported message types are `subscribe`, `unsubscribe`, `snapshot`, `set`, `update`, and `error`. Snapshots and updates carry the current server-assigned version. Frontend `set` messages carry the next expected version, and stale writes receive a `snapshot` with the latest value/version plus an `error` string. V1 syncs full JSON values, not partial patches.

## Development

```sh
pnpm --dir ui install
sh scripts/check.sh
```
