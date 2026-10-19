package adaptiveconcurrency

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidConfig = errors.New("invalid adaptive limiter configuration")
	ErrLimitExceeded = errors.New("concurrency limit exceeded")
	ErrClosed        = errors.New("limiter closed")
)

// Outcome tells the limiter what a finished call says about the protected
// dependency's capacity.
type Outcome int

const (
	// Success: the call completed normally. Its latency decides whether the
	// limit grows or shrinks.
	Success Outcome = iota + 1
	// Dropped: the call failed because the dependency is overloaded (timeout,
	// rejection, 503). The limit shrinks whatever the latency was.
	Dropped
	// Ignore: the call says nothing about capacity (for example the caller
	// gave up, or the request was invalid). The limit does not change.
	Ignore
)

// Release ends one admitted call and reports its outcome. The first call
// counts; later calls are no-ops. See README.md.
type Release func(Outcome)

// Config configures a Limiter. See README.md for the validation rules.
type Config struct {
	Initial       int           // starting limit
	Min           int           // lower bound of the limit
	Max           int           // upper bound of the limit
	TargetLatency time.Duration // latency at or below which a Success is "fast"
	BackoffRatio  float64       // multiplicative decrease factor, in (0, 1)
	MaxWaiting    int           // maximum number of queued Acquire calls; 0 means fail fast
}

// Limiter is an adaptive concurrency limiter: it admits at most Limit() calls
// at once and moves the limit with the observed latency and outcomes (AIMD).
type Limiter struct {
	// TODO
}

// New validates cfg and returns a limiter whose limit starts at cfg.Initial.
// See README.md for the full requirements.
func New(cfg Config) (*Limiter, error) {
	panic("TODO")
}

// Acquire admits one call, waits in the bounded queue for room, or rejects
// the call. On success the caller must call the returned Release exactly when
// the call finishes. See README.md for the full requirements.
func (l *Limiter) Acquire(ctx context.Context) (Release, error) {
	panic("TODO")
}

// Limit returns the current concurrency limit.
func (l *Limiter) Limit() int {
	panic("TODO")
}

// InFlight returns the number of admitted calls that have not been released.
func (l *Limiter) InFlight() int {
	panic("TODO")
}

// Close stops admission and fails every queued Acquire with ErrClosed. It does
// not wait for admitted calls. See README.md.
func (l *Limiter) Close() {
	panic("TODO")
}
