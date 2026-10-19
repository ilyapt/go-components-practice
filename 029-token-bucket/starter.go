package tokenbucket

import (
	"context"
	"errors"
)

var (
	ErrInvalidConfig = errors.New("invalid token bucket configuration")
	ErrExceedsBurst  = errors.New("request exceeds bucket burst")
)

// Bucket is a token bucket that refills at rate tokens per second up to burst
// tokens. It is safe for concurrent use. See README.md for the full
// requirements.
type Bucket struct {
	// TODO
}

// New returns a full bucket that refills at rate tokens per second and holds
// at most burst tokens. See README.md.
func New(rate float64, burst int) (*Bucket, error) {
	panic("TODO")
}

// Allow is AllowN(1).
func (b *Bucket) Allow() bool {
	panic("TODO")
}

// AllowN takes n tokens if they are available right now and reports whether it
// did. It never blocks. See README.md.
func (b *Bucket) AllowN(n int) bool {
	panic("TODO")
}

// Wait is WaitN(ctx, 1).
func (b *Bucket) Wait(ctx context.Context) error {
	panic("TODO")
}

// WaitN blocks until it can take n tokens or ctx is done. It fails fast when
// the tokens cannot arrive before the ctx deadline. See README.md for the full
// requirements.
func (b *Bucket) WaitN(ctx context.Context, n int) error {
	panic("TODO")
}
