package timeoutwrapper

import (
	"context"
	"time"
)

type Request struct {
	Key string
}

type Response struct {
	Value string
}

type Client interface {
	Call(ctx context.Context, request Request) (Response, error)
}

// New wraps next so that every call gets its own timeout. See README.md for
// the full requirements.
func New(next Client, timeout time.Duration) (Client, error) {
	panic("TODO")
}
