# Reviewer notes: immutable-snapshot

> Spoilers. Read after your attempt.

## Test coverage notes

- **`New` keeps a reference to the caller's map**, so the caller can mutate a
  published snapshot. Caught by `TestNewCopiesInitialMapAndPublishesVersionOne`.
- **A refresh mutates the published map in place** rather than publishing a
  new one: old readers see new data, and a failed refresh leaves
  partial changes behind. Caught by
  `TestRefreshPublishesAllChangesTogetherAndKeepsOldSnapshot` and
  `TestRefreshFailurePublishesNothing`; concurrent map access is also flagged by
  the race detector in `TestConcurrentReadersAndRefreshes`.
- **A failed load publishes the entries loaded before it, or bumps the
  version.** Caught by `TestRefreshFailurePublishesNothing`.
- **Two refreshes run their loaders at the same time**, and the later publish
  overwrites the earlier one. Caught by `TestRefreshWritersAreSerialized` and
  `TestConcurrentRefreshesDoNotLoseUpdates`. Note that
  `TestRefreshWritersAreSerialized` fails any design in which loaders of
  different refreshes overlap, including an optimistic retry-on-conflict
  design that would otherwise lose no updates.
- **Writers are serialized, but the second one builds on a base it read before
  it was admitted**, dropping the first one's update. Caught by
  `TestConcurrentRefreshesDoNotLoseUpdates` (entry `a` missing or version 2).
- **A refresh whose context was cancelled before publication still publishes.**
  Caught by `TestCancellationBeforePublicationPublishesNothing` (the loader
  cancels and then succeeds) and `TestRefreshCancelledWhileWaitingPublishesNothing`
  (cancelled while waiting behind another writer, with a loader that ignores
  cancellation).
- **A lock that `Current` needs is held across the loader**, for example one
  mutex for both reads and refreshes. Caught by `TestCurrentDoesNotWaitForLoader`,
  which fails with "is a reader-facing lock held across the loader call?"
  rather than hanging.
- **The version is published separately from the entries**, so a reader can
  see a new version with old entries or the reverse. Caught only sometimes, by
  `TestConcurrentReadersAndRefreshes`, when a reader lands between the two
  writes; look for it in review.
- **An empty ID list bumps the version, or a nil loader is not rejected with
  `ErrNilLoader`.** Caught by `TestEmptyRefreshAndNilLoader`, which also
  covers `New(nil)`.
- **A `Snapshot` accessor or extra method hands out the internal map**, so any
  caller can mutate a published snapshot. Not caught; look for it in review.

## Open choices

- `Current` may be fully lock-free or may take a lock that writers hold only
  briefly at publication; the README forbids only holding a reader-facing lock
  across the loader. Any consistent choice is fine if the user can justify it.
- The exact point that counts as "publication" for the cancellation rule is
  the user's to define; ask them to name it.
- Whether `Refresh` checks the context between loads, or relies on the loader
  and a later check, is open; both satisfy the tests.
- The README does not say what `Refresh` returns for an empty ID list combined
  with a nil loader or an already-cancelled context. The tests do not combine
  those cases.
- How the next map is built (full copy, or a structure that shares unchanged
  parts) is open; the tests only observe the published result.

## Follow-up questions

- The map has 2 million entries and refreshes touch 10 of them every 100 ms.
  What does each refresh cost, and what would you change?
- Refresh has to load its IDs concurrently with a limit. What changes, and does
  all-or-nothing still hold?
- A refresh for a slow resource blocks every other refresh. How would you
  partition the state or the writer side?
- What would an optimistic, retry-on-conflict writer cost you with a slow,
  external loader?
- Readers need "at least version N" after their own refresh. How would you
  offer that without making `Current` block?
- What would you measure in production?
