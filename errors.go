package syncedstate

import "errors"

var (
	ErrAlreadyDefined = errors.New("syncedstate: state name already defined")
	ErrClosed         = errors.New("syncedstate: locked state already closed")
	ErrMissingValue   = errors.New("syncedstate: message value is required")
	ErrNotFound       = errors.New("syncedstate: state name not found")
	ErrTypeMismatch   = errors.New("syncedstate: state type mismatch")
)
