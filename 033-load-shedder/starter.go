package loadshedder

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidConfig = errors.New("invalid load shedder configuration")
	ErrNilFunc       = errors.New("nil function")
	ErrOverloaded    = errors.New("load shedder queue full")
	ErrShed          = errors.New("request shed after waiting too long in the queue")
	ErrStopped       = errors.New("load shedder stopped")
)

// Shedder runs requests on a fixed number of workers and sheds requests that
// waited in its queue longer than maxQueueDelay. See README.md.
type Shedder struct {
	// TODO
}

// New validates the configuration and returns a running Shedder with
// workers workers, room for queueCapacity waiting requests, and the
// queueing-time limit maxQueueDelay. See README.md for the full requirements.
func New(workers, queueCapacity int, maxQueueDelay time.Duration) (*Shedder, error) {
	panic("TODO")
}

// Do admits fn without blocking on queue capacity, waits until a worker runs
// it, and returns the error fn returned. A request that waited in the queue
// longer than maxQueueDelay is not run: Do returns ErrShed. See README.md for
// the full requirements.
func (s *Shedder) Do(ctx context.Context, fn func(context.Context) error) error {
	panic("TODO")
}

// Shutdown stops admission, releases every queued request with ErrStopped,
// and waits for running fn calls to return. If ctx is done first, it cancels
// the running fn calls and still waits for them. See README.md.
func (s *Shedder) Shutdown(ctx context.Context) error {
	panic("TODO")
}
