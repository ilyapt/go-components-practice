package retrybudget

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidConfig   = errors.New("invalid retry budget configuration")
	ErrNilOperation    = errors.New("nil operation")
	ErrBudgetExhausted = errors.New("retry budget exhausted")
)

// RetryPolicy classifies operation errors and chooses the wait between
// attempts, as in 017-retry-backoff. It must be safe for concurrent use.
type RetryPolicy interface {
	Retryable(error) bool
	Backoff(attempt int) time.Duration
}

// Operation is one attempt of the work being retried.
type Operation func(context.Context) error

// Config describes the budget shared by every Do call and the retry loop
// each Do call runs. See README.md.
type Config struct {
	// Ratio is the number of tokens each first attempt deposits. 0.1 lets
	// roughly one request in ten be retried once.
	Ratio float64
	// MinRetriesPerSecond is the rate at which tokens accrue with time alone,
	// so that a quiet service can still retry.
	MinRetriesPerSecond float64
	// Burst is both the capacity of the budget and its initial balance.
	Burst int
	// MaxAttempts bounds the attempts of one Do call, including the first.
	MaxAttempts int
	// Policy classifies errors and chooses backoffs.
	Policy RetryPolicy
}

// Budget is a retry loop whose retries draw from one token budget shared by
// all callers.
type Budget struct {
	// TODO
}

// New validates cfg and returns a full budget. See README.md for the full
// requirements.
func New(cfg Config) (*Budget, error) {
	panic("TODO")
}

// Do calls op until it succeeds, fails permanently, runs out of attempts,
// is refused a retry by the budget, or ctx is done. See README.md.
func (b *Budget) Do(ctx context.Context, op Operation) error {
	panic("TODO")
}
