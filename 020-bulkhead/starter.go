package bulkhead

import (
	"context"
	"errors"
)

var (
	ErrInvalidConfig = errors.New("invalid bulkhead configuration")
	ErrNilFunc       = errors.New("nil function")
	ErrBulkheadFull  = errors.New("bulkhead full")
	ErrClosed        = errors.New("bulkhead closed")
)

// Limits bounds the compartment of one dependency.
type Limits struct {
	// MaxConcurrent is the number of calls that may run at once. It must be
	// positive.
	MaxConcurrent int
	// MaxWaiting is the number of calls that may wait for a free slot. Zero
	// means that calls never wait.
	MaxWaiting int
}

// Config holds the limits of every dependency.
type Config struct {
	// Default applies to every dependency without an entry in Overrides.
	Default Limits
	// Overrides sets the limits of specific dependencies by name.
	Overrides map[string]Limits
}

// Bulkhead gives every dependency its own concurrency limit and its own
// bounded waiting room. See README.md for the full requirements.
type Bulkhead struct {
	// TODO
}

// New validates the configuration and returns an open bulkhead. See README.md.
func New(cfg Config) (*Bulkhead, error) {
	panic("TODO")
}

// Do runs fn on the caller's goroutine once a slot of dep is free, waits for a
// slot if the waiting room of dep has space, and rejects the call otherwise.
// See README.md for the full requirements.
func (b *Bulkhead) Do(ctx context.Context, dep string, fn func(context.Context) error) error {
	panic("TODO")
}

// Shutdown stops admission, releases waiting calls, and waits for running
// calls to return. It never cancels running calls. See README.md.
func (b *Bulkhead) Shutdown(ctx context.Context) error {
	panic("TODO")
}
