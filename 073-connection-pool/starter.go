package connectionpool

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidConfig = errors.New("invalid pool configuration")
	ErrNilDialer     = errors.New("nil dialer")
	ErrClosed        = errors.New("pool closed")
)

// Conn is one pooled connection.
type Conn interface {
	// Healthy reports whether the connection is still usable. It is cheap,
	// does no I/O, and never blocks.
	Healthy() bool
	// Close releases the connection. It may block, for example while it says
	// goodbye to the server.
	Close() error
}

// Dialer opens new connections.
type Dialer interface {
	// Dial opens one connection. ctx bounds the dial only; the returned
	// connection outlives it.
	Dial(ctx context.Context) (Conn, error)
}

// Stats is a snapshot of the pool's counters. See README.md for what each
// field counts.
type Stats struct {
	Open    int
	Idle    int
	Waiting int
}

type Pool struct {
	// TODO
}

// New validates the configuration and returns an empty pool. It does not
// dial. See README.md for the full requirements.
func New(dialer Dialer, maxOpen, maxIdle int, idleTimeout time.Duration) (*Pool, error) {
	panic("TODO")
}

// Get returns a usable idle connection, dials a new one if the pool is below
// maxOpen, or waits for one until ctx is done. See README.md.
func (p *Pool) Get(ctx context.Context) (Conn, error) {
	panic("TODO")
}

// Put returns a connection obtained from Get. A non-nil err marks the
// connection as broken. See README.md.
func (p *Pool) Put(conn Conn, err error) {
	panic("TODO")
}

// Close stops the pool: it rejects new Get calls, wakes every waiter, and
// closes the idle connections. See README.md.
func (p *Pool) Close() error {
	panic("TODO")
}

// Stats returns a snapshot of the pool's counters.
func (p *Pool) Stats() Stats {
	panic("TODO")
}
