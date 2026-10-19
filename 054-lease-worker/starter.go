package leaseworker

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNoAvailableJob = errors.New("no available job")
	ErrLeaseLost      = errors.New("lease lost")
	ErrInvalidConfig  = errors.New("invalid lease worker configuration")
	ErrNilHandler     = errors.New("nil handler")
)

type Job struct {
	ID      string
	Payload []byte
}

// Store is the job store of 053-lease-fencing, seen from the worker side.
// Claim returns ErrNoAvailableJob when there is nothing to do; Renew and
// Complete return ErrLeaseLost when the caller no longer holds the lease at
// generation.
type Store interface {
	Claim(ctx context.Context, workerID string, lease time.Duration) (Job, uint64, error)
	Renew(ctx context.Context, jobID, workerID string, generation uint64, lease time.Duration) error
	Complete(ctx context.Context, jobID, workerID string, generation uint64, result []byte) error
}

// Handler processes one claimed job and returns its result. It should return
// soon after ctx is done.
type Handler interface {
	Handle(ctx context.Context, job Job) ([]byte, error)
}

// Outcome reports how one claimed job ended. See README.md for the meaning of
// Err.
type Outcome struct {
	JobID      string
	Generation uint64
	Err        error
}

type Worker struct {
	// TODO
}

// New validates the configuration and returns a worker that claims jobs from
// store as workerID. report may be nil. See README.md for the full
// requirements.
func New(store Store, workerID string, lease, renewEvery, pollInterval time.Duration, report func(Outcome)) (*Worker, error) {
	panic("TODO")
}

// Run claims and handles jobs one at a time, renewing each lease in the
// background, until ctx is cancelled. See README.md for the full requirements.
func (w *Worker) Run(ctx context.Context, handler Handler) error {
	panic("TODO")
}
