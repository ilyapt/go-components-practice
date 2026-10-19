# Fan-out, first success

**Difficulty:** easy · **Topics:** fan-out, first-result aggregation, loser cancellation, result channel ownership

Send the same request to several providers at once and return the first
successful answer, cancelling the rest. This is how clients cut tail latency
across replicated backends, RPC endpoints, mirrors, and redundant data
sources.

Implement `FirstSuccess` in `starter.go`.

## Requirements

- Start one `Fetch` call per provider concurrently.
- Return the first successful response in completion order, not in provider
  slice order.
- After a success, cancel the other in-flight calls. Do not wait for them to
  return.
- If every provider fails, return a non-nil error.
- If the parent context is cancelled first, cancel all calls and return
  `ctx.Err()`.
- An empty provider slice returns `ErrNoProviders`.
- A nil provider is invalid and returns an error without starting any calls.
- A provider returns after the context passed to `Fetch` is cancelled.
- No provider goroutine may remain blocked while publishing its result after
  `FirstSuccess` returns.

You may choose how to aggregate provider errors when all calls fail. Be ready
to explain whether callers can inspect individual errors.

## Before coding

1. Who owns the context passed to providers, and who cancels it?
2. What happens to late results that arrive after `FirstSuccess` has returned?
3. How does the function know that every provider has failed?
4. Who closes the result channel, if anyone, and when is that safe?
