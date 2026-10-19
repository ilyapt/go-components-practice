package leasefencing

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNoAvailableJob = errors.New("no available job")
	ErrLeaseLost      = errors.New("lease lost")
	ErrInvalidWorker  = errors.New("worker ID is empty")
	ErrInvalidLease   = errors.New("lease duration must be positive")
)

type Job struct {
	ID      string
	Payload []byte
}

type Store struct {
	// TODO
}

// New creates a store for jobs, in claim order, using now as its only clock.
func New(jobs []Job, now func() time.Time) *Store {
	panic("TODO")
}

// Claim leases the first available job to workerID and returns it with its
// new generation. See README.md.
func (s *Store) Claim(ctx context.Context, workerID string, lease time.Duration) (Job, uint64, error) {
	panic("TODO")
}

// Renew extends the lease held by workerID at generation. See README.md.
func (s *Store) Renew(ctx context.Context, jobID, workerID string, generation uint64, lease time.Duration) error {
	panic("TODO")
}

// Complete stores result and permanently completes the job, if workerID still
// holds the lease at generation. See README.md.
func (s *Store) Complete(ctx context.Context, jobID, workerID string, generation uint64, result []byte) error {
	panic("TODO")
}
