package deadlinebudget

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidConfig   = errors.New("invalid budget configuration")
	ErrBudgetExhausted = errors.New("deadline budget exhausted")
)

// Config describes one request's time budget.
type Config struct {
	// Total bounds the budget, measured from New. Zero means the budget is
	// bounded only by the parent context's deadline.
	Total time.Duration
	// Reserve is kept back at the end of the budget for cleanup and writing
	// the response. Steps can never use it.
	Reserve time.Duration
	// MinStep is the smallest remaining budget worth starting a step with.
	MinStep time.Duration
}

// Budget splits the remaining time of one request across the downstream
// calls it makes. See README.md for the full requirements.
type Budget struct {
	// TODO
}

// New fixes the budget deadline from ctx and cfg. See README.md for the full
// requirements.
func New(ctx context.Context, cfg Config) (*Budget, error) {
	panic("TODO")
}

// Step returns a child of ctx for one downstream call, bounded by limit and
// by the remaining budget, or an error if the step should not start. The
// caller must call the returned CancelFunc. See README.md.
func (b *Budget) Step(ctx context.Context, limit time.Duration) (context.Context, context.CancelFunc, error) {
	panic("TODO")
}

// Remaining returns the time steps may still use, excluding the reserve.
// See README.md.
func (b *Budget) Remaining() time.Duration {
	panic("TODO")
}
