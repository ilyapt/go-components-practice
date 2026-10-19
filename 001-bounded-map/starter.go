package boundedmap

import "context"

type Input struct {
	Value int
}

type Output struct {
	Value int
}

// Map applies fn to every input with at most limit concurrent calls and
// returns the outputs in input order. See README.md for the full requirements.
func Map(
	ctx context.Context,
	inputs []Input,
	limit int,
	fn func(context.Context, Input) (Output, error),
) ([]Output, error) {
	panic("TODO")
}
