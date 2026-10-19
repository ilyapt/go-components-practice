package multiproviderrpc

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidConfig      = errors.New("invalid client configuration")
	ErrNilProvider        = errors.New("nil provider")
	ErrNoFreshResponse    = errors.New("no fresh provider response")
	ErrAllProvidersFailed = errors.New("all providers failed")
)

type Request struct {
	Key        string
	MinVersion uint64
}

type Response struct {
	Version uint64
	Source  string
	Payload []byte
}

type Provider interface {
	Fetch(context.Context, Request) (Response, error)
}

type Client struct {
	// TODO
}

// New validates the configuration. maxConcurrency is shared by all Fetch
// calls on the returned client. See README.md.
func New(
	providers []Provider,
	maxConcurrency int,
	perCallTimeout time.Duration,
) (*Client, error) {
	panic("TODO")
}

// Fetch queries the providers and returns the first fresh response, or the
// freshest stale one with ErrNoFreshResponse. See README.md.
func (c *Client) Fetch(ctx context.Context, req Request) (Response, error) {
	panic("TODO")
}
