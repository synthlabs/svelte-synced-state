package syncedstate

type storage interface {
	define(name string, entry *entry) error
	lookup(name string) (*entry, error)
}
