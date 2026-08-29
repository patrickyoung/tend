# Tend

Tend makes one ordinary command crash-durable without turning Unix programs
into workflow SDK callbacks.

## Contract

An execution is exact argv, a physical working directory, optional stdin, an
optional not-before time, and a serialization key. `tend work` performs at
most one transition: recover one expired lease, fire one due timer, or claim
and run one ready execution. It then exits.

Definitions and evidence are files beneath `jobs/ID/`. SQLite is the sole
authority for state requiring transactions. `tend events` derives JSON Lines
from the immutable event table; there is no second canonical log.

The attempt boundary is deliberately asymmetric:

```text
ready -> running/prepared -> running/started -> done | waiting | failed
              |                    |
              | expired            | result missing, timeout, signal, 125
              v                    v
            ready                unknown
```

The `started` record, including a private launcher PID, is committed before
the submitted program is released through a pipe gate. The launcher gives the
program only stdin, stdout, stderr, its scrubbed environment, and exact argv.
It watches the controller and terminates the submitted process group if the
controller disappears. This can conservatively mark an effect-free crash
unknown, but it never creates an unrecorded-effect window.
`unknown` blocks the same serialization key until a person runs
`tend resolve ID retry|done|fail`. `done` requires the optional submitted
check to exit zero. Resolution will not proceed while a recorded launcher is
still live; partial output is sealed first.

## Files and database

```text
$TEND_ROOT/
  state/tend.db
  jobs/ID/
    definition.json
    input                 optional exact stdin
    attempts/NNN.out      child stdout
    attempts/NNN.err      child stderr
    checks/*.out          unknown-resolution stdout evidence
    checks/*.err          unknown-resolution stderr evidence
```

SQLite contains jobs, immutable events, attempts, artifact references,
signals, and wait requests. Artifact bytes stay in files and are bound by
SHA-256 and size.

## Unix boundary

- Submitted work is executed directly, never through an implicit shell.
- `tend submit` reads piped stdin and prints the job ID.
- `tend work`, `list`, `show`, `events`, `signals`, and `export` print JSON or
  JSON Lines.
- Child output is a named artifact, not mixed into controller stdout.
- Child stdout and stderr remain separate, immutable, digest-bound artifacts.
- Submitted programs do not inherit Tend's gate or controller-watch file
  descriptors.
- A handler requests a durable wait with `tend defer signal NAME`,
  `tend defer until TIME`, or `tend defer manual`, then exits 75.
- A sender uses `tend signal`; arriving-before-wait and arriving-after-wait
  are one transactional protocol, so wakeups are not lost.
- `TEND_ATTEMPT_KEY` identifies one try for fencing and evidence. It changes
  on explicit retry and is not advertised as an external idempotency key.
- A child never opens SQLite. `tend defer` writes a private request file; the
  parent validates that file and performs the waiting transition.
- Completion checks use the same scrubbed base environment as submitted work,
  but receive none of the job/defer capability variables. They use the same
  controller-death launcher. A launcher-held file lock distinguishes an active
  check from orphan evidence; a later resolution removes incomplete orphan
  files before trying again. The resolver retains that lock through the event,
  artifact, and status commit. All resolution choices take the same per-job
  lock and revalidate `unknown`, so none can delete another resolver's
  not-yet-registered evidence or release the serialization key underneath it.

## Non-claims

Tend is not distributed, multi-host, or safe on a network filesystem. It does
not provide exactly-once effects, a workflow language, automatic retry policy,
or automatic resolution of started work. A restartable program may keep its
own checkpoint and receive the same exact argv on explicit retry. Ply and
Agent use that seam to preserve conversation context, but the whole process is
still one Tend attempt and interrupted external effects remain uncertain.

## Check

```sh
go test ./...
go test -race ./...
go vet ./...
```
