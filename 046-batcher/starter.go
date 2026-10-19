package batcher

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNilFlush      = errors.New("nil flush function")
	ErrInvalidConfig = errors.New("invalid batcher configuration")
	ErrFull          = errors.New("batcher queue is full")
	ErrClosed        = errors.New("batcher is closed")
)

type Item struct {
	Value string
}

// FlushFunc receives a batch. The batcher must not reuse the slice afterwards.
type FlushFunc func([]Item)

type Batcher struct {
	// TODO
}

// New validates the configuration and starts the batching goroutine.
func New(maxSize int, maxDelay time.Duration, queueCapacity int, flush FlushFunc) (*Batcher, error) {
	panic("TODO")
}

// Submit admits one item without blocking on queue capacity.
func (b *Batcher) Submit(ctx context.Context, item Item) error {
	panic("TODO")
}

// Shutdown stops admission, flushes every accepted item, and waits for the
// batching goroutine to exit or for ctx to be done.
func (b *Batcher) Shutdown(ctx context.Context) error {
	panic("TODO")
}
