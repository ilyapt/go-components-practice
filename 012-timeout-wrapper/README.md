# Per-call timeout wrapper

**Difficulty:** easy · **Topics:** derived contexts, deadlines, parent vs own cancellation, resource cleanup

Wrap a client so that every call gets its own deadline, while an earlier
deadline or cancellation from the caller still wins. Middleware like this sits
in every RPC client, database driver, and HTTP transport that must not let one
slow dependency hold a request forever.

Implement `New` and `Call` in `starter.go`.

## Requirements

- Every call to the wrapped client receives a child of the caller's context
  with the configured timeout.
- An earlier parent deadline or cancellation still wins.
- If the parent is done, return the parent context's error.
- If the wrapper's timeout expires first, return `context.DeadlineExceeded`,
  whatever the client returned.
- If the client completes before either context is done, return its response
  and error unchanged.
- The client returns after the context passed to it is cancelled.
- Do not start a goroutine per call merely to enforce the timeout: `Call`
  returns only after the wrapped client has returned.
- Release the derived context's timer and resources as soon as each call
  returns.
- `New` rejects a nil client and `timeout <= 0` with an error.

If client completion races with the deadline, either outcome is acceptable as
long as the choice is consistent and documented.

## Before coding

1. Can the wrapper enforce a hard timeout if the client ignores its context?
   What would it cost to try?
2. How will `Call` tell parent cancellation apart from its own timeout?
3. Who releases the resources associated with the derived context, and when?
