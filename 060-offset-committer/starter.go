package offsetcommitter

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidConfig = errors.New("invalid offset committer configuration")
	ErrNilCommitter  = errors.New("nil committer")
	ErrOutOfOrder    = errors.New("offset is not greater than the last tracked offset")
	ErrUnknownOffset = errors.New("offset is not tracked or already done")
	ErrWindowFull    = errors.New("too many uncommitted offsets")
	ErrClosed        = errors.New("offset committer is closed")
)

// Committer durably stores the consumer's position in one partition, for
// example in a Kafka consumer group. offset is the next offset to read. Commit
// must return when ctx is cancelled.
type Committer interface {
	Commit(ctx context.Context, offset int64) error
}

// OffsetCommitter tracks the in-flight offsets of one partition and commits
// the contiguous done prefix. See README.md for the full requirements.
type OffsetCommitter struct {
	// TODO
}

// New validates the configuration and starts the committer-owned goroutine
// that calls committer.Commit. See README.md for the full requirements.
func New(committer Committer, interval time.Duration, maxUncommitted, maxInFlight int) (*OffsetCommitter, error) {
	panic("TODO")
}

// Track registers offset as in flight. Offsets must be strictly increasing.
// Track never blocks: it returns ErrWindowFull when the window is full.
func (c *OffsetCommitter) Track(offset int64) error {
	panic("TODO")
}

// Done marks a tracked offset as processed. It never waits for a commit.
func (c *OffsetCommitter) Done(offset int64) error {
	panic("TODO")
}

// Committed returns the last successfully committed offset (the next offset
// to read), or 0 before the first successful commit.
func (c *OffsetCommitter) Committed() int64 {
	panic("TODO")
}

// Close stops admission, commits the final done prefix, and waits for the
// committer goroutine to exit. If ctx is done first, it cancels the in-flight
// commit and still waits for the goroutine. See README.md.
func (c *OffsetCommitter) Close(ctx context.Context) error {
	panic("TODO")
}
