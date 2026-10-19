---
description: Start or resume a practice task
argument-hint: <task>
---

Start the task `$ARGUMENTS` following the "Workflow for one task" in AGENTS.md. The argument may be the full directory (`001-bounded-map`), the name (`bounded-map`), or the number (`001`).

1. If no task was given, read the task list in `README.md` and `progress.md` if it exists, and suggest the next task instead.
2. If the task's `starter.go` already contains more than the `panic("TODO")` stubs, this is a resumed attempt: read the current code and `progress.md`, and continue from where the user stopped.
3. For a new attempt: present the task as described in step 1 of the workflow (what it is, the API, every requirement, where to write the code), then invite clarifying questions. Do not ask the "Before coding" questions until the user has had a chance to clarify.
