# Resume an Agent conversation through Tend

Agent's named checkpoint is a controller-owned pointer to Ply's current Ask
session. Tend does not read it. Tend only reruns the same exact argv after an
operator decides that retrying an `unknown` attempt is acceptable.

From the Tend directory, with `tend`, `agent`, `ply`, `ask`, `brief`, and
`cage` installed on `PATH`:

```sh
job=release-agent
./tend submit -id "$job" -C /path/to/agent-home -- \
  agent run -checkpoint "$job" .
./tend work
```

If the process is interrupted after it starts, Tend records `unknown`. Inspect
the work tree, external effects, Tend's attempt output, and Agent's replay
evidence before choosing what happened:

```sh
./tend show "$job"
./tend events "$job"
./tend resolve "$job" retry
./tend work
```

The new attempt invokes the same `agent run -checkpoint release-agent .`.
Agent maps the name to `.agent/checkpoints/release-agent.current`; Ply locks
that pointer and continues its current Ask session, including across
compaction. If Ply returned a clean not-done status instead of crashing, use
`tend retry "$job"` and `tend work`.

This checkpoint preserves conversation context only. The work tree remains
the state, `bin/check` remains the definition of done, and a command killed
during an external effect may have performed that effect. Tend therefore does
not resolve or retry `unknown` automatically.
