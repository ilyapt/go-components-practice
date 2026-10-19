package refreshcoordinator

import (
	"context"
	"errors"
)

var (
	ErrNilLoader     = errors.New("nil loader")
	ErrInvalidConfig = errors.New("invalid coordinator configuration")
	ErrInvalidID     = errors.New("resource ID is empty")
	ErrFull          = errors.New("refresh queue is full")
	ErrClosed        = errors.New("coordinator is closed")
)

type State struct {
	SourceVersion uint64
	Value         string
}

// Loader fetches the current state of one resource.
type Loader func(context.Context, string) (State, error)

// Snapshot is an immutable view of every resource loaded so far.
type Snapshot struct {
	version uint64
	states  map[string]State
}

func (s *Snapshot) Version() uint64 {
	return s.version
}

func (s *Snapshot) Get(id string) (State, bool) {
	res, ok := s.states[id]
	return res, ok
}

func (s *Snapshot) Len() int {
	return len(s.states)
}

type Coordinator struct {
	// TODO
}

// New validates the configuration and starts the workers. Loads run with
// workCtx. See README.md.
func New(
	workCtx context.Context,
	concurrency int,
	queueCapacity int,
	loader Loader,
) (*Coordinator, error) {
	panic("TODO")
}

// Trigger requests a refresh of resource id. See README.md for coalescing
// and overload rules.
func (c *Coordinator) Trigger(ctx context.Context, id string) error {
	panic("TODO")
}

// Current returns the latest published snapshot without taking a lock.
func (c *Coordinator) Current() *Snapshot {
	panic("TODO")
}

// Shutdown stops admission, finishes accepted work, and waits for the
// workers to exit or for ctx to be done.
func (c *Coordinator) Shutdown(ctx context.Context) error {
	panic("TODO")
}
