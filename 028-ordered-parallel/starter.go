package orderedparallel

import (
	"context"
	"errors"
)

var (
	ErrInvalidConfig = errors.New("invalid ordered-parallel configuration")
	ErrNilFunc       = errors.New("nil process function")
	ErrNilInput      = errors.New("nil input channel")
)

// Item is one unit of input. The stage never interprets ID; order is the
// order in which items are received from the input channel.
type Item struct {
	ID    int
	Value string
}

// Output is what ProcessFunc produces for one item.
type Output struct {
	Value string
}

// Result is emitted once per processed item, in input order. Err is the error
// ProcessFunc returned for this item, unchanged.
type Result struct {
	Item   Item
	Output Output
	Err    error
}

// ProcessFunc processes one item. It must return soon after ctx is cancelled.
type ProcessFunc func(ctx context.Context, item Item) (Output, error)

// Run validates its arguments and starts the stage: it reads items from in,
// processes them with at most workers concurrent calls of fn, keeps at most
// window items in flight, and emits one Result per item on the returned
// channel in input order. The error is only for validation. See README.md
// for the full requirements.
func Run(ctx context.Context, in <-chan Item, workers, window int, fn ProcessFunc) (<-chan Result, error) {
	panic("TODO")
}
