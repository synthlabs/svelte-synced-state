package syncedstate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type testState struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

func TestDefineLookupSnapshot(t *testing.T) {
	manager := NewManager()
	key, err := Define(manager, "TestState", testState{Name: "initial", Count: 1})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	lookup, err := Lookup[testState](manager, "TestState")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if lookup.Name() != key.Name() {
		t.Fatalf("lookup name = %q, want %q", lookup.Name(), key.Name())
	}

	value, meta, err := lookup.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if value.Name != "initial" || value.Count != 1 {
		t.Fatalf("snapshot = %+v", value)
	}
	if meta.Name != "TestState" || meta.Version != 1 {
		t.Fatalf("meta = %+v", meta)
	}
}

func TestDefineDuplicateAndLookupErrors(t *testing.T) {
	manager := NewManager()
	if _, err := Define(manager, "TestState", testState{}); err != nil {
		t.Fatalf("define: %v", err)
	}
	if _, err := Define(manager, "TestState", testState{}); !errors.Is(err, ErrAlreadyDefined) {
		t.Fatalf("duplicate error = %v", err)
	}
	if _, err := Lookup[string](manager, "TestState"); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("type mismatch error = %v", err)
	}
	if _, err := Lookup[testState](manager, "Missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing error = %v", err)
	}
}

func TestUpdateSetAndLockedSync(t *testing.T) {
	ctx := context.Background()
	manager := NewManager()
	key, err := Define(manager, "TestState", testState{Name: "initial"})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	if err := key.Update(ctx, func(state *testState) {
		state.Count = 2
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	value, meta, err := key.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot after update: %v", err)
	}
	if value.Count != 2 || meta.Version != 2 {
		t.Fatalf("after update value=%+v meta=%+v", value, meta)
	}

	if err := key.Set(ctx, testState{Name: "set", Count: 3}); err != nil {
		t.Fatalf("set: %v", err)
	}

	locked, err := key.Lock(ctx)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	locked.Value().Count = 4
	if err := locked.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	locked.Unlock()

	value, meta, err = key.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot after locked sync: %v", err)
	}
	if value.Name != "set" || value.Count != 4 || meta.Version != 4 {
		t.Fatalf("after locked sync value=%+v meta=%+v", value, meta)
	}
}

func TestSnapshotReturnsCopy(t *testing.T) {
	ctx := context.Background()
	manager := NewManager()
	key, err := Define(manager, "TestState", map[string]int{"count": 1})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	value, _, err := key.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	value["count"] = 2

	next, _, err := key.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot again: %v", err)
	}
	if next["count"] != 1 {
		t.Fatalf("snapshot mutated stored value: %v", next)
	}
}

func TestLockedSyncAfterUnlockReturnsClosed(t *testing.T) {
	ctx := context.Background()
	manager := NewManager()
	key, err := Define(manager, "TestState", testState{Name: "initial"})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	locked, err := key.Lock(ctx)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	locked.Unlock()

	if err := locked.Sync(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("sync after unlock error = %v", err)
	}
	locked.Unlock()
}

func TestKeyOperationsHonorCanceledContextWhileLocked(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *Key[testState]) error
	}{
		{
			name: "Lock",
			run: func(ctx context.Context, key *Key[testState]) error {
				locked, err := key.Lock(ctx)
				if locked != nil {
					locked.Unlock()
				}
				return err
			},
		},
		{
			name: "Snapshot",
			run: func(ctx context.Context, key *Key[testState]) error {
				_, _, err := key.Snapshot(ctx)
				return err
			},
		},
		{
			name: "Update",
			run: func(ctx context.Context, key *Key[testState]) error {
				return key.Update(ctx, func(state *testState) {
					state.Count = 99
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			key, err := Define(manager, "TestState", testState{Name: "initial", Count: 1})
			if err != nil {
				t.Fatalf("define: %v", err)
			}

			locked, err := key.Lock(context.Background())
			if err != nil {
				t.Fatalf("lock: %v", err)
			}
			defer locked.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()

			if err := tt.run(ctx, key); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s error = %v", tt.name, err)
			}
		})
	}
}

func TestConcurrentLookupReferencesShareUpdates(t *testing.T) {
	ctx := context.Background()
	manager := NewManager()
	key, err := Define(manager, "Counter", testState{Name: "counter"})
	if err != nil {
		t.Fatalf("define: %v", err)
	}

	lookup, err := Lookup[testState](manager, "Counter")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}

	const updatesPerRef = 100
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	increment := func(key *Key[testState]) {
		defer wg.Done()
		for range updatesPerRef {
			if err := key.Update(ctx, func(state *testState) {
				state.Count++
			}); err != nil {
				errs <- err
				return
			}
		}
	}

	wg.Add(2)
	go increment(key)
	go increment(lookup)
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("update: %v", err)
	}

	value, meta, err := key.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if value.Count != updatesPerRef*2 {
		t.Fatalf("count = %d, want %d", value.Count, updatesPerRef*2)
	}
	if meta.Version != 1+updatesPerRef*2 {
		t.Fatalf("version = %d, want %d", meta.Version, 1+updatesPerRef*2)
	}

	lookupValue, lookupMeta, err := lookup.Snapshot(ctx)
	if err != nil {
		t.Fatalf("lookup snapshot: %v", err)
	}
	if lookupValue != value || lookupMeta != meta {
		t.Fatalf("lookup snapshot value=%+v meta=%+v, want value=%+v meta=%+v", lookupValue, lookupMeta, value, meta)
	}
}
