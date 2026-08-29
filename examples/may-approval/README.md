# Tend with May approval

This example composes two independent Unix tools:

- May binds a human decision to one exact action.
- Tend durably parks, wakes, reruns, and records the process.

The worker calls May with Tend's stable job ID. May's exit status is the whole
interface: `0` spends an approval and continues, `3` declines, `75` parks, and
anything else fails closed.

```sh
go build -o ./tend .

demo=$(mktemp -d)
export TEND_ROOT="$demo/tend-state"
export MAY=/absolute/operator-controlled/path/to/may
worker=$PWD/examples/may-approval/worker

printf 'publish release v1.4.2\n' >"$demo/may-action"
job=$(./tend submit -C "$demo" -- "$worker")
./tend work
./tend show "$job"                 # waiting
```

At an operator terminal, inspect and decide the exact pending request:

```sh
"$MAY" pending
"$MAY" decide THE_64_CHARACTER_DIGEST
```

May has now recorded the authority decision, but Tend is deliberately not
polling May. Wake the parked job after either approval or decline:

```sh
./tend signal "$job" may-decision
./tend work
./tend show "$job"
```

On approval, the second attempt atomically spends May's one-use grant, prints
the action it observed, and writes `.tend-may/JOB.completed`. On decline it
exits 3 and Tend records `failed`. A wakeup without a decision merely causes
May to return 75 again, so Tend safely parks again.

`MAY` is one of Tend's deliberately preserved environment variables; use it
for an absolute, operator-controlled executable path. Tend also preserves
`HOME`, which is where standalone May keeps its state.

This demonstration assumes an operator-owned worker and action file. For an
agent-facing production boundary, do not expose May or its state to the model.
Put the agent in Cage or another sandbox and let an operator-controlled
connector construct the exact action and invoke May outside that boundary.
