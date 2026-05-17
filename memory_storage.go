package syncedstate

import "sync"

type memoryStorage struct {
	mu      sync.RWMutex
	entries map[string]*entry
}

func newMemoryStorage() *memoryStorage {
	return &memoryStorage{
		entries: make(map[string]*entry),
	}
}

func (s *memoryStorage) define(name string, entry *entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.entries[name]; ok {
		return ErrAlreadyDefined
	}

	s.entries[name] = entry
	return nil
}

func (s *memoryStorage) lookup(name string) (*entry, error) {
	s.mu.RLock()
	entry, ok := s.entries[name]
	s.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}

	return entry, nil
}
