# Stale-while-revalidate cache

**Difficulty:** medium · **Topics:** two-tier expiry, background refresh, per-key load deduplication, work vs caller context, graceful close

A loading cache whose expiry has two tiers. For a while after a load, a value
is *fresh* and served as is. After that it becomes *stale*: still good enough
to answer with, so the cache returns it immediately and refreshes it in the
background. Only when the stale window is over too is the value *expired*, and
callers wait for a new load. Callers almost never pay the load latency, and the
source of truth sees at most one refresh per key no matter how hot the key is.
HTTP caches (`Cache-Control: stale-while-revalidate`), CDNs, service-discovery
and feature-flag clients, token and price caches all work this way.

This task builds on 023-singleflight (one in-flight load per key, work context
vs caller context), 047-immutable-snapshot (a value is published atomically and
never changes afterwards), and 052-refresh-coordinator (coalesced background
refreshes under a component-owned context).

Implement `New`, `Get`, and `Close` in `starter.go`.

```go
func New(workCtx context.Context, loader Loader, freshFor, staleFor time.Duration) (*Cache, error)
func (c *Cache) Get(ctx context.Context, key string) (Value, error)
func (c *Cache) Close()
```

## Requirements

### Freshness

- Every cached value carries the time at which its successful load returned
  (`time.Now()` when the result is stored). Its age is the time elapsed since
  then.
- An entry is **fresh** while `age < freshFor`, **stale** while
  `freshFor <= age < freshFor+staleFor`, and **expired** once
  `age >= freshFor+staleFor`. A key that was never loaded successfully is
  **missing**.
- A successful load publishes the value and its timestamp together, replacing
  the previous entry for that key. A published `Value` is never modified.

### Get

- Fresh: `Get` returns the cached value without calling the loader.
- Stale: `Get` returns the cached value immediately, without waiting for any
  load, and makes sure a background refresh of that key is running: it starts
  one unless a load for that key is already in flight.
- Missing or expired: `Get` waits for a load of that key and returns its
  result. It joins a load already in flight for that key (synchronous or
  background) instead of starting another one. An expired value is never
  returned.
- At most one `Loader.Load` per key runs at any time, however many callers
  ask for it and whichever path started it. Different keys load independently
  and concurrently.
- A waiter whose context is done stops waiting and returns its `ctx.Err()`.
  This never cancels the shared load, even if that waiter started it.
- A load whose waiters have all left still runs to completion and, if it
  succeeds, publishes its value.
- If `ctx` is already done when `Get` is called, `Get` returns `ctx.Err()` and
  starts no load, whatever the state of the entry.
- Do not hold a mutex while calling the loader or while waiting for a load.

### Failures

- A failed synchronous load returns the loader's error to every waiter of
  that load and caches nothing. The next `Get` for that key starts a new load.
- A failed background refresh leaves the entry unchanged: same value, same
  timestamp. Stale callers never see the refresh error. While the entry is
  still stale, the next `Get` starts a new refresh.
- Negative caching and retry backoff are out of scope.

### Context and lifecycle

- `Loader.Load` receives a context derived from `workCtx` and owned by the
  cache, never a caller's context. Background refreshes and synchronous loads
  run under the same work context.
- `Close` stops admission: every `Get` that starts after `Close` has been
  called returns `ErrClosed` without calling the loader, even for a fresh key.
- `Close` cancels the work context and returns only after every in-flight
  load, background or synchronous, has returned. No load starts after `Close`
  returns.
- A `Get` already waiting for a load when `Close` is called receives that
  load's result (typically the loader's context error).
- Repeated and concurrent calls to `Close` are safe; each returns after all
  loads have returned. `Get` and `Close` may be called concurrently; this must
  never panic or leak a goroutine.

Assume `Loader.Load` returns soon after its context is cancelled and does not
panic. Eviction and size limits are out of scope: entries stay in the cache
until `Close`.

## Validation

- A nil loader returns `ErrNilLoader`.
- Non-positive `freshFor` or `staleFor` returns `ErrInvalidConfig`.

## Before coding

1. What per-key state do you keep, and how does `Get` decide between "return
   now", "return now and refresh", and "wait"? Which of these decisions must
   be made under the same lock as starting a load?
2. A background refresh is still running when its entry becomes expired, and
   a new caller arrives. What does that caller do, and why is "at most one
   load per key" also what prevents an older result from overwriting a newer
   one?
3. Which context does a background refresh run under, who cancels it, and
   what happens to it when the `Get` that triggered it has long returned?
4. What does a failed refresh change, and what does a failed synchronous load
   change? What is the linearization point after which `Get` can no longer
   start a load, and how does `Close` know that every load has returned?
