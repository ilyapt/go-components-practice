package writebehind

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidConfig = errors.New("invalid write-behind cache configuration")
	ErrNilStore      = errors.New("nil store")
	ErrFull          = errors.New("too many dirty keys")
	ErrClosed        = errors.New("write-behind cache is closed")
	ErrUnflushed     = errors.New("write-behind cache stopped with unflushed keys")
)

// Entry is one key and the value to persist for it.
type Entry struct {
	Key   string
	Value string
}

// Store is the slow backing store. Write persists a batch of entries: nil
// means every entry in the batch is persisted, an error means none is. Write
// must return when ctx is cancelled. It may keep the slice it receives.
type Store interface {
	Write(ctx context.Context, entries []Entry) error
}

// Cache keeps the latest value of every key in memory and persists changed
// keys to the Store in the background. See README.md for the full
// requirements.
type Cache struct {
	// TODO
}

// New validates the configuration and starts the flusher goroutine. See
// README.md for the full requirements.
func New(store Store, maxDirty int, maxDelay time.Duration, maxPending int) (*Cache, error) {
	panic("TODO")
}

// Put sets the in-memory value of key and marks the key dirty. It never waits
// for the store: it returns ErrFull instead of blocking. See README.md.
func (c *Cache) Put(ctx context.Context, key, value string) error {
	panic("TODO")
}

// Get returns the latest value put for key, whether or not it has been
// persisted yet. It never waits for the store.
func (c *Cache) Get(key string) (string, bool) {
	panic("TODO")
}

// Shutdown stops admission, flushes every dirty key, and waits for the
// flusher to exit. If ctx is done first, it cancels the in-flight Write and
// still waits for the flusher. See README.md.
func (c *Cache) Shutdown(ctx context.Context) error {
	panic("TODO")
}
