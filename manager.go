package syncedstate

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
)

type Manager struct {
	mu          sync.RWMutex
	storage     storage
	subscribers map[string]map[*client]struct{}
	nextClient  atomic.Uint64
}

func NewManager(opts ...Option) *Manager {
	m := &Manager{
		storage:     newMemoryStorage(),
		subscribers: make(map[string]map[*client]struct{}),
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}

func Define[T any](manager *Manager, name string, initial T, opts ...KeyOption) (*Key[T], error) {
	cfg := keyConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	e := newEntry(name, initial)
	if err := manager.storage.define(name, e); err != nil {
		return nil, err
	}

	return &Key[T]{manager: manager, entry: e}, nil
}

func Lookup[T any](manager *Manager, name string) (*Key[T], error) {
	e, err := manager.storage.lookup(name)
	if err != nil {
		return nil, err
	}

	if e.typ != reflect.TypeOf((*T)(nil)).Elem() {
		return nil, ErrTypeMismatch
	}

	return &Key[T]{manager: manager, entry: e}, nil
}

func (m *Manager) entry(name string) (*entry, error) {
	return m.storage.lookup(name)
}

func (m *Manager) subscribe(c *client, name string) error {
	if _, wildcard, err := parseWildcardAddress(name); err != nil {
		return err
	} else if !wildcard {
		if _, err := m.entry(name); err != nil {
			return err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.subscribers[name]; !ok {
		m.subscribers[name] = make(map[*client]struct{})
	}
	m.subscribers[name][c] = struct{}{}
	c.subscriptions[name] = struct{}{}
	return nil
}

func (m *Manager) unsubscribe(c *client, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if subscribers, ok := m.subscribers[name]; ok {
		delete(subscribers, c)
		if len(subscribers) == 0 {
			delete(m.subscribers, name)
		}
	}
	delete(c.subscriptions, name)
}

func (m *Manager) removeClient(c *client) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name := range c.subscriptions {
		if subscribers, ok := m.subscribers[name]; ok {
			delete(subscribers, c)
			if len(subscribers) == 0 {
				delete(m.subscribers, name)
			}
		}
	}
}

func (m *Manager) broadcast(ctx context.Context, msg Message) {
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case <-ctx.Done():
		return
	default:
	}

	subscriptionNames := []string{msg.Name}
	if wildcard, ok := wildcardForIndexedAddress(msg.Name); ok {
		subscriptionNames = append(subscriptionNames, wildcard)
	}

	m.mu.RLock()
	clients := make([]*client, 0, len(m.subscribers[msg.Name]))
	seen := make(map[*client]struct{})
	for _, name := range subscriptionNames {
		for c := range m.subscribers[name] {
			if _, ok := seen[c]; ok {
				continue
			}
			seen[c] = struct{}{}
			clients = append(clients, c)
		}
	}
	m.mu.RUnlock()

	for _, c := range clients {
		if !c.enqueue(ctx, msg) {
			return
		}
	}
}
