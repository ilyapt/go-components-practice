package idempotentcommands

import (
	"context"
	"errors"
)

var (
	ErrNilHandler      = errors.New("nil handler")
	ErrInvalidCommand  = errors.New("command ID is empty")
	ErrPayloadConflict = errors.New("command ID used with different payload")
)

type Command struct {
	ID      string
	Payload []byte
}

type Result struct {
	Value string
}

// Handler executes a command. It is invoked at most once per command ID.
type Handler func(context.Context, Command) (Result, error)

type Processor struct {
	// TODO
}

// New returns a processor that runs handler under workCtx. See README.md.
func New(workCtx context.Context, handler Handler) (*Processor, error) {
	panic("TODO")
}

// Handle executes cmd once per ID, or waits for and returns the stored
// outcome of an earlier command with the same ID. See README.md.
func (p *Processor) Handle(ctx context.Context, cmd Command) (Result, error) {
	panic("TODO")
}
