package firstsuccess

import (
	"context"
	"errors"
)

var ErrNoProviders = errors.New("no providers")

type Request struct {
	Key string
}

type Response struct {
	Source string
	Value  string
}

type Provider interface {
	Fetch(ctx context.Context, request Request) (Response, error)
}

// FirstSuccess calls every provider concurrently and returns the first
// successful response. See README.md for the full requirements.
func FirstSuccess(
	ctx context.Context,
	providers []Provider,
	request Request,
) (Response, error) {
	panic("TODO")
}
