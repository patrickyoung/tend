# Development guidance

`tend` is a crash-durable local supervisor for ordinary programs. It is a
sibling of Bench, Agent, and Ply; it never imports or reimplements them.

- One `tend work` claims at most one job, runs one exact argv, records one
  outcome, and exits. A shell or the operating system supplies repetition.
- Job input and output are ordinary files. SQLite owns only transactional
  controller facts: jobs, immutable events, attempts, leases, signals, and
  timers.
- Never hold a database transaction while a child, model, check, network call,
  or person is running.
- A prepared attempt may be safely requeued. A started attempt with no trusted
  result is `unknown` and is never automatically retried.
- Never claim exactly-once execution. Arbitrary programs can perform an effect
  before Tend observes their result.
- Invoke commands directly with exact argv. Never run submitted text through a
  shell. The optional operator-owned completion check is the one explicit
  `/bin/sh -c` seam.
- Keep stdout machine-readable, progress and diagnostics on stderr, and exit
  status meaningful. JSON Lines is the streaming boundary.
- No daemon requirement, socket, RPC server, workflow DSL, provider code,
  built-in tools, plugin system, or second model transcript.
- SQLite is single-host, local-filesystem state. Use WAL, `synchronous=FULL`,
  foreign keys, and a bounded busy timeout.
- Tests are offline and must include crash windows and concurrent claims.
- Run `go test ./...` and `go test -race ./...` before reporting success.
