# Immutable snapshot publication

**Difficulty:** medium · **Topics:** copy-on-write, atomic pointer swap, lock-free reads, serialized writers, all-or-nothing updates

Keep a read-mostly view of many resources that readers can query without
locks, while a background refresher builds the next version off to the side
and publishes it in one atomic step. Routing tables, configuration, feature
flags, price and pool caches, and service-discovery views are all served this
way.

Implement `New`, `Current`, `Refresh`, and the `Snapshot` accessors in
`starter.go`.

## Requirements

- `Current` is safe for frequent concurrent reads and never blocks on a slow
  refresh.
- Every returned `Snapshot` is immutable and stays valid and unchanged after
  later refreshes.
- `New` publishes version `1` and does not keep a reference to the caller's
  initial map: later changes to that map are not visible.
- `Refresh` updates only the requested IDs and keeps every other entry from the
  current snapshot. IDs not yet present are added.
- Load the requested entries one by one with `loader`. Assume IDs are
  non-empty and unique.
- If every load succeeds, publish all changes together in one atomic step and
  increment the version by one.
- If a load fails, or `ctx` is cancelled before publication, return the error
  and publish nothing.
- Concurrent calls to `Refresh` never lose successful updates: each refresh
  starts from the latest snapshot published by earlier refreshes.
- An empty ID list is a successful no-op and does not change the version.
- A nil loader returns `ErrNilLoader`.
- Do not hold a mutex that readers need while calling the loader. Serializing
  refresh writers with a dedicated mutex is allowed.
- `Snapshot` does not expose its internal map for mutation.

Assume `Entry` values are immutable and `loader` respects context cancellation.

## Before coding

1. Which objects may still be mutated while a refresh is being built, and
   which must never be mutated again?
2. How do readers avoid seeing a partially updated map?
3. How do two concurrent refreshes avoid overwriting each other's successful
   changes?
4. What happens to a reader that is still using an old snapshot when a new one
   is published?
