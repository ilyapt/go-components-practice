package retrybackoff

import (
	"context"
	"time"
)

type Result struct {
	Value string
}

type RetryPolicy interface {
	Retryable(error) bool
	Backoff(attempt int) time.Duration
}

type Operation func(context.Context) (Result, error)

// Do calls op until it succeeds, fails permanently, runs out of attempts, or
// ctx is done, waiting policy.Backoff between attempts. See README.md.
func Do(
	ctx context.Context,
	maxAttempts int,
	policy RetryPolicy,
	op Operation,
) (Result, error) {
	panic("TODO")
}
