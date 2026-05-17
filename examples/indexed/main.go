package main

import (
	"encoding/json"
	"log"
	"net/http"

	syncedstate "github.com/synthlabs/svelte-synced-state"
)

type Customer struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

func main() {
	manager := syncedstate.NewManager()
	customers := syncedstate.MustIndexedScope[Customer]("customer")

	for _, customer := range []Customer{
		{ID: "100", Name: "Ada Lovelace", Status: "active"},
		{ID: "200", Name: "Grace Hopper", Status: "active"},
	} {
		if _, err := customers.Define(manager, customer.ID, customer); err != nil {
			log.Fatal(err)
		}
	}

	mux := http.NewServeMux()
	mux.Handle("/synced-state", manager.Handler(
		syncedstate.WithOriginPatterns("http://localhost:*"),
	))

	mux.HandleFunc("PUT /customers/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")

		var req struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		customer, err := customers.Lookup(manager, id)
		if err == syncedstate.ErrNotFound {
			customer, err = customers.Define(manager, id, Customer{ID: id})
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := customer.Update(r.Context(), func(customer *Customer) {
			customer.ID = id
			customer.Name = req.Name
			customer.Status = req.Status
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	log.Fatal(http.ListenAndServe(":8080", mux))
}
