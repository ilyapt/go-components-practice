# Future with independent waiter cancellation

**Difficulty:** medium · **Topics:** context ownership, shared result publication, waiter vs work cancellation

Start a piece of work in the background and let any number of callers wait for
its result, each with their own deadline. A waiter that gives up must not
cancel the work for everyone else. This split between "the work" and "the
people waiting for it" is the core of request deduplication, shared cache
loads, async job handles, and workflow step results.

Implement `Start` and `Await` in `starter.go`.

## Requirements

- `Start` begins exactly one execution of `fn` and returns immediately,
  without waiting for `fn`.
- `fn` receives `workCtx`. Cancelling `workCtx` cancels the underlying work.
- `Await(ctx)` waits for the shared result or for `ctx` to be done, whichever
  comes first. A done `ctx` returns `ctx.Err()`.
- Cancelling an `Await` context stops only that caller's wait; it never
  cancels the underlying work.
- Multiple callers may call `Await` concurrently.
- Every waiter that observes completion receives the same `Result` and
  `error`.
- `Await` called after completion returns the stored result immediately, any
  number of times.
- The execution finishes even if nobody is waiting. No goroutine leaks and no
  blocked result publication.
- A nil `fn` is invalid: `Start` returns an error and a nil `Future` without
  starting anything. You choose the error.

## Before coding

1. Who owns cancellation of the underlying work, and who owns cancellation of
   a wait?
2. What does one waiter cancelling mean for the other waiters?
3. Is the result consumed once or published permanently? Which mechanism lets
   one write reach any number of readers?
4. If nobody ever calls `Await`, what happens to the goroutine running `fn`?
