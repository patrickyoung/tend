# tend

Crash-durable local execution for ordinary Unix programs.

```sh
go install .
```

```sh
id=$(printf '%s\n' evidence | tend submit -C ./my-agent -- agent run .)

# Run from a shell loop, cron, launchd, systemd, or CI. One call does one thing.
tend work

tend show "$id"
tend events "$id"
```

State defaults to `$HOME/.local/state/tend`; set `TEND_ROOT` to choose an
explicit local directory. Jobs sharing a working directory serialize by
default. `-key` selects another serialization domain.

A command that needs a durable wakeup records it through the ordinary CLI and
then returns `EX_TEMPFAIL`:

```sh
tend defer signal approved
exit 75
```

Another process wakes it with `tend signal JOB approved`. For a timer, use
`tend defer until 15m` before exiting 75.

Scheduled submission uses an absolute time so retrying a stable ID always
means the same request: `tend submit -id daily-20260829 -at 2026-08-29T09:00:00Z -- ...`.

Crashes after a command may have started are not retried. They become
`unknown` and require an explicit resolution:

```sh
tend resolve JOB retry   # accepts possible duplicate effects
tend resolve JOB fail
tend resolve JOB done    # only if submit's -check exits zero
```

The submitted program is released only after `started` is durable. A private
launcher watches the controller and terminates the submitted process group if
the controller disappears. An `unknown` job remains the serialization fence;
resolution waits until that launcher has exited and partial output is sealed.

Each attempt records `NNN.out` and `NNN.err` separately. The database binds
both files by digest and size; `tend check` verifies those bindings and replays
the immutable events against the current projection. Completion checks are
supervised the same way; an interrupted check stays `unknown`, and a later
resolution safely discards its incomplete files before retrying.

Run `tend help` for the full command list.

See [examples/wait-for-input](examples/wait-for-input/README.md) for a complete
shell example that waits indefinitely, accepts user input through stdin,
observes it after a durable wakeup, and continues.

See [examples/may-approval](examples/may-approval/README.md) for the same
durable pause composed with May's exact-action human approval gate.

See [examples/agent-checkpoint](examples/agent-checkpoint/README.md) for a
Tend-managed Agent run that continues the same Ply/Ask conversation after an
explicit retry. Tend still treats a crash after process start as `unknown`;
the checkpoint preserves context but never makes uncertain effects safe.
