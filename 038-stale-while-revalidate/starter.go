package stalewhilerevalidate

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNilLoader     = errors.New("nil loader")
	ErrInvalidConfig = errors.New("invalid cache configuration")
	ErrClosed        = errors.New("cache is closed")
)

// Value is the cached result of one load. It is immutable once published.
type Value struct {
	Data string
}

// Loader fetches the current value for key from the source of truth.
type Loader interface {
	Load(ctx context.Context, key string) (Value, error)
}

// Cache is a loading cache with two-tier expiry: entries are fresh for
// freshFor, then stale for staleFor, then expired. See README.md.
type Cache struct {
	// TODO
}

// New returns a Cache whose loads run under a context derived from workCtx.
// See README.md for the full requirements.
func New(workCtx context.Context, loader Loader, freshFor, staleFor time.Duration) (*Cache, error) {
	panic("TODO")
}

// Get returns the value for key: a fresh value immediately, a stale value
// immediately while refreshing it in the background, or the result of a
// shared synchronous load when the key is missing or expired. See README.md.
func (c *Cache) Get(ctx context.Context, key string) (Value, error) {
	panic("TODO")
}

// Close stops admission, cancels every in-flight load, and waits for all of
// them to return. See README.md.
func (c *Cache) Close() {
	panic("TODO")
}
