package immutablesnapshot

import (
	"context"
	"errors"
)

var ErrNilLoader = errors.New("nil loader")

type Entry struct {
	Block uint64
	Value string
}

// Loader fetches the current value of one resource.
type Loader func(context.Context, string) (Entry, error)

// Snapshot is an immutable, versioned view of all entries.
type Snapshot struct {
	// TODO
}

// Version returns the snapshot's version. The first snapshot is version 1.
func (s *Snapshot) Version() uint64 {
	panic("TODO")
}

// Get returns the entry for id, if present.
func (s *Snapshot) Get(id string) (Entry, bool) {
	panic("TODO")
}

// Len returns the number of entries.
func (s *Snapshot) Len() int {
	panic("TODO")
}

type Store struct {
	// TODO
}

// New publishes a snapshot of initial as version 1. See README.md.
func New(initial map[string]Entry) *Store {
	panic("TODO")
}

// Current returns the latest published snapshot without blocking.
func (s *Store) Current() *Snapshot {
	panic("TODO")
}

// Refresh loads ids and publishes them together as the next version, or
// publishes nothing on failure. See README.md.
func (s *Store) Refresh(ctx context.Context, ids []string, loader Loader) error {
	panic("TODO")
}
