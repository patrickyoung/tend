# Wait indefinitely for user input

This example runs an ordinary shell script, parks it without a deadline, then
wakes it with user input and lets it continue.

From the Tend source directory:

```sh
go build -o ./tend .

demo=$(mktemp -d)
export TEND_ROOT="$demo/state"
worker=$PWD/examples/wait-for-input/worker

job=$(./tend submit -C "$demo" -- "$worker")
./tend work
./tend show "$job"             # status is waiting
```

Nothing is polling and no Tend daemon is required. The job remains `waiting`
indefinitely. In another shell using the same `TEND_ROOT`, provide any input:

```sh
printf 'please continue\n' |
    ./examples/wait-for-input/answer "$demo" "$job" ./tend
```

The `signal` command performs the durable wakeup transaction. Let one worker
perform the resumed attempt:

```sh
./tend events "$job"            # includes job.woken
./tend work                     # resumed script observes input -> done
./tend show "$job"
cat "$demo/.tend-wait/$job.observed"
```

The response travels through a normal file written atomically from stdin. The
signal is only the durable wakeup. This separation lets the script observe the
input without receiving `TEND_ROOT` or direct access to SQLite. A signal sent
just before the script finishes parking is also safe: Tend consumes it in the
same transaction that records the wait, so it is not lost.
