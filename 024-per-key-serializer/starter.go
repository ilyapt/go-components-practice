package perkeyserializer

import "context"

type Operation func(context.Context) error

type Processor struct {
	// TODO
}

// New returns an empty Processor. See README.md.
func New() *Processor {
	panic("TODO")
}

// Process runs op after every earlier operation admitted for key and returns
// its result, or ctx.Err() if the caller stops waiting first. See README.md.
func (p *Processor) Process(ctx context.Context, key string, op Operation) error {
	panic("TODO")
}
