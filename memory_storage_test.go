package syncedstate

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestMemoryStorageDefineLookup(t *testing.T) {
	store := newMemoryStorage()
	entry := newEntry("TestState", testState{Name: "initial"})

	if err := store.define("TestState", entry); err != nil {
		t.Fatalf("define: %v", err)
	}

	got, err := store.lookup("TestState")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got != entry {
		t.Fatalf("lookup entry = %p, want %p", got, entry)
	}
}

func TestMemoryStorageDuplicateDefine(t *testing.T) {
	store := newMemoryStorage()
	first := newEntry("TestState", testState{Name: "first"})
	second := newEntry("TestState", testState{Name: "second"})

	if err := store.define("TestState", first); err != nil {
		t.Fatalf("define first: %v", err)
	}
	if err := store.define("TestState", second); !errors.Is(err, ErrAlreadyDefined) {
		t.Fatalf("define duplicate error = %v", err)
	}

	got, err := store.lookup("TestState")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got != first {
		t.Fatalf("duplicate define replaced entry: got %p want %p", got, first)
	}
}

func TestMemoryStorageMissingLookup(t *testing.T) {
	store := newMemoryStorage()

	if _, err := store.lookup("Missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing lookup error = %v", err)
	}
}

func TestMemoryStorageConcurrentAccess(t *testing.T) {
	store := newMemoryStorage()

	const entries = 50
	for i := range entries {
		name := fmt.Sprintf("Preloaded-%d", i)
		if err := store.define(name, newEntry(name, testState{Count: i})); err != nil {
			t.Fatalf("define preloaded %q: %v", name, err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, entries*2)

	for i := range entries {
		i := i
		wg.Add(2)
		go func() {
			defer wg.Done()

			name := fmt.Sprintf("Preloaded-%d", i)
			if _, err := store.lookup(name); err != nil {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()

			name := fmt.Sprintf("Defined-%d", i)
			if err := store.define(name, newEntry(name, testState{Count: i})); err != nil {
				errs <- err
			}
			if _, err := store.lookup(name); err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent access: %v", err)
	}
}
