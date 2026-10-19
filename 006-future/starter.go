package future

import "context"

type Result struct {
	Value string
}

type Future interface {
	// Await waits for the shared result or for ctx to be done.
	Await(ctx context.Context) (Result, error)
}

// Start begins one execution of fn under workCtx and returns immediately.
// See README.md for the full requirements.
func Start(
	workCtx context.Context,
	fn func(context.Context) (Result, error),
) (Future, error) {
	panic("TODO")
}
