# Security

Tend is a supervisor, not a sandbox. A submitted command has the authority of
the Tend worker process. Its environment is scrubbed to a small base set plus
names explicitly listed in `TEND_PASS`; this reduces accidental credential
leakage but is not confinement.

Use Cage, a container, a separate user, or another operating-system boundary
when the child is untrusted. Keep `$TEND_ROOT` outside child-writable roots.
Tend rejects a symlinked state database but supports only a local filesystem
and one host.

Children do not receive `$TEND_ROOT` or a database lease token. `tend defer`
writes one bounded request to a private per-attempt temporary file; the parent
validates and commits it only after the child exits 75. `TEND_PASS` is the one
explicit door for additional ambient variables. Operator completion checks
are scrubbed the same way and do not receive job or defer capabilities.

Tend's private launcher does not sandbox or trace the submitted program. It
does keep the submitted process group tied to the controller lifetime. A
program that deliberately escapes its process group is outside this boundary
and belongs in Cage, a container, or another OS-level sandbox.

Arguments and signal payloads are durable state. Do not place secrets in argv;
use protected input files or explicitly passed environment variables.
