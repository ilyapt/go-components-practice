package executor

import (
	"context"
	"errors"
)

var (
	ErrInvalidConfig = errors.New("invalid executor configuration")
	ErrNilHandler    = errors.New("nil handler")
	ErrOverloaded    = errors.New("executor overloaded")
	ErrStopped       = errors.New("executor stopped")
)

type Task struct {
	ID      string
	Payload []byte
}

// Handler processes one task. It must return when ctx is cancelled.
type Handler interface {
	Handle(ctx context.Context, task Task)
}

type Executor struct {
	// TODO
}

// New validates the configuration and starts exactly workers worker
// goroutines. See README.md for the full requirements.
func New(workers, queueCapacity int, handler Handler) (*Executor, error) {
	panic("TODO")
}

// Submit admits one task without blocking on queue capacity.
func (e *Executor) Submit(ctx context.Context, task Task) error {
	panic("TODO")
}

// Shutdown stops admission, drains accepted tasks, and waits for the workers
// to exit. If ctx is done first, it cancels the running work and still waits
// for the workers.
func (e *Executor) Shutdown(ctx context.Context) error {
	panic("TODO")
}
