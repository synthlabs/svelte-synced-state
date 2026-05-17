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
	appStateScope := syncedstate.MustSingletonScope[AppState]("appstate")

	appState, err := appStateScope.Define(manager, AppState{
		Message: "Not logged in",
	})
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/synced-state", manager.Handler(
		syncedstate.WithOriginPatterns("http://localhost:*"),
	))

	mux.HandleFunc("POST /login", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := appState.Update(r.Context(), func(state *AppState) {
			state.Authenticated = true
			state.Name = req.Name
			state.Message = "Welcome " + req.Name
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("POST /reset", func(w http.ResponseWriter, r *http.Request) {
		lookup, err := appStateScope.Lookup(manager)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		locked, err := lookup.Lock(r.Context())
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
