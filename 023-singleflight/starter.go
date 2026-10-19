package singleflight

import "context"

type Value struct {
	Data string
}

type Loader interface {
	Load(context.Context, string) (Value, error)
}

type Group struct {
	// TODO
}

// New returns a Group whose loads run under workCtx. See README.md.
func New(workCtx context.Context, loader Loader) (*Group, error) {
	panic("TODO")
}

// Get returns the result of the in-flight load for key, starting one if none
// is running. See README.md.
func (g *Group) Get(ctx context.Context, key string) (Value, error) {
	panic("TODO")
}
