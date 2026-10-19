package dagexecutor

import (
	"context"
	"errors"
)

var (
	ErrInvalidLimit      = errors.New("invalid concurrency limit")
	ErrNilRun            = errors.New("nil node function")
	ErrDuplicateNode     = errors.New("duplicate node id")
	ErrUnknownDependency = errors.New("unknown dependency")
	ErrSelfDependency    = errors.New("node depends on itself")
	ErrCycle             = errors.New("dependency cycle")
	ErrDependencyFailed  = errors.New("dependency failed")
	ErrGraphFailed       = errors.New("graph failed")
)

// Output is the value a node produces. Dependents receive it unchanged.
type Output any

// Node is one unit of work in the graph. Run is called at most once, after
// every node listed in DependsOn has succeeded. deps maps each dependency ID
// to the Output that dependency returned.
type Node struct {
	ID        string
	DependsOn []string
	Run       func(ctx context.Context, deps map[string]Output) (Output, error)
}

// Result is the outcome of one node: what its Run returned, or why it never
// ran.
type Result struct {
	Output Output
	Err    error
}

// Execute validates the graph, runs every node after its dependencies with at
// most limit Run calls at once, and returns one Result per node once every
// node has finished, been skipped, or been cancelled. See README.md for the
// full requirements.
func Execute(ctx context.Context, nodes []Node, limit int) (map[string]Result, error) {
	panic("TODO")
}
