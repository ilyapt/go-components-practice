package hedgedrequest

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidConfig     = errors.New("invalid hedger configuration")
	ErrNilProvider       = errors.New("nil provider")
	ErrAllAttemptsFailed = errors.New("all hedged attempts failed")
)

type Request struct {
	Key string
}

type Response struct {
	Source  string
	Payload []byte
}

// Provider answers one request. It must return after ctx is cancelled.
type Provider interface {
	Fetch(ctx context.Context, request Request) (Response, error)
}

type Hedger struct {
	// TODO
}

// New validates the configuration. Attempt i of every Do call goes to
// providers[i%len(providers)]. See README.md for the full requirements.
func New(
	providers []Provider,
	hedgeDelay time.Duration,
	maxAttempts int,
) (*Hedger, error) {
	panic("TODO")
}

// Do starts one attempt, starts the next one when hedgeDelay has passed since
// the latest attempt started or when an attempt fails, and returns the first
// successful response. See README.md for the full requirements.
func (h *Hedger) Do(ctx context.Context, request Request) (Response, error) {
	panic("TODO")
}
