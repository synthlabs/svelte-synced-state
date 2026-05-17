package syncedstate

import (
	"errors"
	"testing"
)

func TestScopeAddressValidation(t *testing.T) {
	if _, err := NewSingletonScope[testState](""); !errors.Is(err, ErrInvalidScope) {
		t.Fatalf("empty singleton scope error = %v", err)
	}
	if _, err := NewSingletonScope[testState]("app:state"); !errors.Is(err, ErrInvalidScope) {
		t.Fatalf("singleton scope with colon error = %v", err)
	}
	if _, err := NewIndexedScope[testState]("customer*"); !errors.Is(err, ErrInvalidScope) {
		t.Fatalf("indexed scope with wildcard error = %v", err)
	}

	customers := MustIndexedScope[testState]("customer")
	if _, err := customers.Address(""); !errors.Is(err, ErrInvalidScope) {
		t.Fatalf("empty indexed id error = %v", err)
	}
	if _, err := customers.Address("12:3"); !errors.Is(err, ErrInvalidScope) {
		t.Fatalf("indexed id with colon error = %v", err)
	}
	if address := customers.MustAddress("123"); address != "customer:123" {
		t.Fatalf("address = %q", address)
	}
	if wildcard := customers.Wildcard(); wildcard != "customer:*" {
		t.Fatalf("wildcard = %q", wildcard)
	}
}

func TestSingletonScopeDefineLookup(t *testing.T) {
	manager := NewManager()
	appState := MustSingletonScope[testState]("appstate")

	key, err := appState.Define(manager, testState{Name: "initial"})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	lookup, err := appState.Lookup(manager)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if lookup.Name() != key.Name() || lookup.Name() != "appstate" {
		t.Fatalf("lookup name = %q, key name = %q", lookup.Name(), key.Name())
	}
}

func TestIndexedScopeDefineLookupAndRawCompatibility(t *testing.T) {
	manager := NewManager()
	customers := MustIndexedScope[testState]("customer")

	key, err := customers.Define(manager, "123", testState{Name: "scoped"})
	if err != nil {
		t.Fatalf("define indexed: %v", err)
	}
	if key.Name() != "customer:123" {
		t.Fatalf("key name = %q", key.Name())
	}

	lookup, err := customers.Lookup(manager, "123")
	if err != nil {
		t.Fatalf("lookup indexed: %v", err)
	}
	if lookup.Name() != key.Name() {
		t.Fatalf("lookup name = %q, want %q", lookup.Name(), key.Name())
	}

	raw, err := Define(manager, "customer:456", testState{Name: "raw"})
	if err != nil {
		t.Fatalf("define raw indexed address: %v", err)
	}
	rawLookup, err := customers.Lookup(manager, "456")
	if err != nil {
		t.Fatalf("lookup raw indexed address: %v", err)
	}
	if rawLookup.Name() != raw.Name() {
		t.Fatalf("raw lookup name = %q, want %q", rawLookup.Name(), raw.Name())
	}
}

func TestScopedHelperFunctions(t *testing.T) {
	manager := NewManager()

	if _, err := DefineSingleton(manager, "appstate", testState{}); err != nil {
		t.Fatalf("define singleton: %v", err)
	}
	if _, err := LookupSingleton[testState](manager, "appstate"); err != nil {
		t.Fatalf("lookup singleton: %v", err)
	}
	if _, err := DefineSingleton(manager, "appstate", testState{}); !errors.Is(err, ErrAlreadyDefined) {
		t.Fatalf("duplicate singleton error = %v", err)
	}

	if _, err := DefineIndexed(manager, "customer", "123", testState{}); err != nil {
		t.Fatalf("define indexed: %v", err)
	}
	if _, err := LookupIndexed[testState](manager, "customer", "123"); err != nil {
		t.Fatalf("lookup indexed: %v", err)
	}
	if _, err := DefineIndexed(manager, "customer", "12:3", testState{}); !errors.Is(err, ErrInvalidScope) {
		t.Fatalf("invalid indexed helper error = %v", err)
	}
}
