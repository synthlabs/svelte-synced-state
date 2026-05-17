package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	syncedstate "github.com/synthlabs/svelte-synced-state"
)

type AppState struct {
	Authenticated bool   `json:"authenticated"`
	Name          string `json:"name"`
	Message       string `json:"message"`
}

func main() {
	manager := syncedstate.NewManager()

	appState, err := syncedstate.Define(manager, "AppState", AppState{
		Authenticated: false,
		Name:          "",
		Message:       "Not logged in",
	})
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()

	// Frontend connects to this endpoint through SyncedState/SyncedClient.
	mux.Handle("/synced-state", manager.Handler(
		syncedstate.WithOriginPatterns("http://localhost:*"),
	))

	// A normal HTTP handler can update server state. Connected frontends receive
	// the update over the websocket handler above.
	mux.HandleFunc("POST /login", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err := appState.Update(r.Context(), func(state *AppState) {
			state.Authenticated = true
			state.Name = req.Name
			state.Message = "Welcome " + req.Name
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	// Use Lock when a workflow needs several mutations before one explicit sync.
	mux.HandleFunc("POST /reset", func(w http.ResponseWriter, r *http.Request) {
		locked, err := appState.Lock(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer locked.Unlock()

		state := locked.Value()
		state.Authenticated = false
		state.Name = ""
		state.Message = "Not logged in"

		if err := locked.Sync(context.Background()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	log.Fatal(http.ListenAndServe(":8080", mux))
}
