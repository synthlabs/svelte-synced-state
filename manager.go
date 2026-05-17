package syncedstate

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
)

type Manager struct {
	mu          sync.RWMutex
	entries     map[string]*entry
	subscribers map[string]map[*client]struct{}
	nextClient  atomic.Uint64
}

func NewManager(opts ...Option) *Manager {
	m := &Manager{
		entries:     make(map[string]*entry),
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

	manager.mu.Lock()
	defer manager.mu.Unlock()

	if _, ok := manager.entries[name]; ok {
		return nil, ErrAlreadyDefined
	}

	e := newEntry(name, initial)
	manager.entries[name] = e
	return &Key[T]{manager: manager, entry: e}, nil
}

func Lookup[T any](manager *Manager, name string) (*Key[T], error) {
	manager.mu.RLock()
	e, ok := manager.entries[name]
	manager.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}

	if e.typ != reflect.TypeOf((*T)(nil)).Elem() {
		return nil, ErrTypeMismatch
	}

	return &Key[T]{manager: manager, entry: e}, nil
}

func (m *Manager) entry(name string) (*entry, error) {
	m.mu.RLock()
	e, ok := m.entries[name]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	return e, nil
}

func (m *Manager) subscribe(c *client, name string) error {
	if _, err := m.entry(name); err != nil {
		return err
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

	m.mu.RLock()
	clients := make([]*client, 0, len(m.subscribers[msg.Name]))
	for c := range m.subscribers[msg.Name] {
		clients = append(clients, c)
	}
	m.mu.RUnlock()

	for _, c := range clients {
		c.enqueue(msg)
	}
}
