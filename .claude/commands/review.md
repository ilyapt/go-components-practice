---
description: Run tests and review the current attempt
argument-hint: [task]
---

Review the attempt for `$ARGUMENTS` (if empty, the task whose `starter.go` was most recently modified, see `git status`).

1. Run `go test -race -count=1 -timeout=60s ./<task>/` and report the result briefly. Explain failures in terms of the task requirements.
2. Read the user's code, then `<task>/REVIEW.md`, and go through the review checklist in AGENTS.md.
3. Give feedback in the AGENTS.md feedback format. Only real issues, each with the concrete scenario that breaks it, most severe first.
4. Ask one or two follow-up questions from `REVIEW.md`.
5. Do not fix the code. After the user has improved it and the discussion is done, append an entry to `progress.md`.
