package circuitbreaker

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNilClient     = errors.New("nil client")
	ErrNilClassifier = errors.New("nil failure classifier")
	ErrInvalidConfig = errors.New("invalid circuit breaker configuration")
	ErrCircuitOpen   = errors.New("circuit breaker is open")
)

type Request struct {
	Value string
}

type Response struct {
	Value string
}

type Client interface {
	Call(context.Context, Request) (Response, error)
}

// FailureClassifier reports whether an error counts against the provider's
// health in the closed state.
type FailureClassifier func(error) bool

type Breaker struct {
	// TODO
}

// New validates the configuration and returns a closed breaker. See README.md.
func New(
	client Client,
	failureThreshold int,
	cooldown time.Duration,
	now func() time.Time,
	classifier FailureClassifier,
) (*Breaker, error) {
	panic("TODO")
}

// Call admits the request according to the breaker state, calls the client
// outside any lock, and records the outcome. See README.md.
func (b *Breaker) Call(ctx context.Context, req Request) (Response, error) {
	panic("TODO")
}
