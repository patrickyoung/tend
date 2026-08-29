package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const version = "0.1.1"

type app struct {
	root        string
	store       *Store
	in          io.Reader
	out, errout io.Writer
}

func main() { os.Exit(realMain(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }
func realMain(args []string, in io.Reader, out, errout io.Writer) int {
	if len(args) == 0 {
		usage(errout)
		return 2
	}
	if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		usage(out)
		return 0
	}
	if args[0] == "version" || args[0] == "--version" {
		fmt.Fprintln(out, "tend "+version)
		return 0
	}
	if args[0] == "_exec" {
		return internalExec(args[1:], errout)
	}
	if args[0] == "defer" && os.Getenv("TEND_DEFER_PATH") != "" {
		if err := deferToFile(args[1:]); err != nil {
			fmt.Fprintln(errout, "tend defer:", err)
			return 2
		}
		return 0
	}
	root, err := tendRoot()
	if err != nil {
		fmt.Fprintln(errout, "tend:", err)
		return 2
	}
	s, err := openStore(root)
	if err != nil {
		fmt.Fprintln(errout, "tend:", err)
		return 2
	}
	defer s.Close()
	a := &app{root: root, store: s, in: in, out: out, errout: errout}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	code, err := a.dispatch(ctx, args[0], args[1:])
	if err != nil {
		fmt.Fprintf(errout, "tend %s: %v\n", args[0], err)
	}
	return code
}

// internalExec is the private pre-exec gate and controller-death watcher. The
// submitted program receives only ordinary stdin/stdout/stderr and exact argv;
// the two control descriptors are retained by this launcher, never inherited.
func internalExec(args []string, errout io.Writer) int {
	if len(args) < 2 || args[0] != "--" {
		fmt.Fprintln(errout, "tend: invalid internal exec request")
		return 125
	}
	gate := os.NewFile(3, "tend-exec-gate")
	control := os.NewFile(4, "tend-controller-watch")
	if gate == nil || control == nil {
		fmt.Fprintln(errout, "tend: internal exec descriptors are missing")
		return 125
	}
	var permit [1]byte
	n, err := gate.Read(permit[:])
	_ = gate.Close()
	if err != nil || n != 1 || permit[0] != 1 {
		return 125
	}
	argv := args[1:]
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.Env = os.Environ()
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(errout, "tend: exec:", err)
		return 127
	}
	wait := make(chan error, 1)
	parentGone := make(chan struct{}, 1)
	go func() { wait <- cmd.Wait() }()
	go func() {
		var b [1]byte
		_, _ = control.Read(b[:])
		parentGone <- struct{}{}
	}()
	select {
	case err := <-wait:
		exit, sig := processStatus(err)
		if sig != 0 {
			return 125
		}
		return exit
	case <-parentGone:
		// The launcher is the process-group leader. TERM includes this process
		// and every submitted descendant. The launcher begins ignoring TERM
		// only after the target has started, then escalates the whole group so
		// a target that traps TERM cannot outlive controller death.
		signal.Ignore(syscall.SIGTERM)
		_ = syscall.Kill(-os.Getpid(), syscall.SIGTERM)
		time.Sleep(150 * time.Millisecond)
		_ = syscall.Kill(-os.Getpid(), syscall.SIGKILL)
		return 125
	}
}
func usage(w io.Writer) {
	fmt.Fprint(w, `usage: tend COMMAND [arguments]

  submit [-id ID] [-key KEY] [-C DIR] [-at TIME] [-check SH] -- CMD [ARG...]
  work                         perform one durable transition, then exit
  list                         print current jobs as JSON Lines
  show ID                      print one job as JSON
  events [ID]                  print immutable history as JSON Lines
  signal [-id ID] JOB NAME [PAYLOAD...]
  signals [JOB]                print durable signals as JSON Lines
  defer signal NAME            request a signal wait from inside a job
  defer until TIME             request a timer wait from inside a job
  defer manual                 request an explicit wake from inside a job
  retry ID                     requeue an observed failed job
  resolve ID retry|done|fail   resolve an effect-unknown attempt
  cancel ID                    cancel queued work or request a running stop
  export                       export every event as JSON Lines
  check                        verify database and artifact invariants
  help
  version

Submit -at is an absolute RFC3339 time. Defer-until accepts RFC3339 or a
duration such as 15m. TEND_ROOT selects the state directory; the default is
$HOME/.local/state/tend. TEND_LEASE, TEND_JOB_MAX, and TEND_CHECK_MAX set
positive durations. TEND_PASS is a space-separated list of extra environment
variable names passed to work and checks.

Work exits 0 after a durable action, 1 when idle, 2 on controller failure, and
130 when interrupted after execution started. Other commands use 0 for
success, 1 for a negative result, and 2 for usage or controller failure.
`)
}

func (a *app) dispatch(ctx context.Context, verb string, args []string) (int, error) {
	switch verb {
	case "submit":
		return a.cmdSubmit(ctx, args)
	case "work":
		if len(args) != 0 {
			return 2, errors.New("usage: tend work")
		}
		return a.cmdWork(ctx)
	case "list", "ls":
		if len(args) != 0 {
			return 2, errors.New("usage: tend list")
		}
		return a.cmdList(ctx)
	case "show":
		if len(args) != 1 {
			return 2, errors.New("usage: tend show ID")
		}
		return a.cmdShow(ctx, args[0])
	case "events":
		if len(args) > 1 {
			return 2, errors.New("usage: tend events [ID]")
		}
		id := ""
		if len(args) == 1 {
			id = args[0]
		}
		return a.cmdEvents(ctx, id)
	case "export":
		if len(args) != 0 {
			return 2, errors.New("usage: tend export")
		}
		return a.cmdEvents(ctx, "")
	case "signal":
		return a.cmdSignal(ctx, args)
	case "signals":
		if len(args) > 1 {
			return 2, errors.New("usage: tend signals [JOB]")
		}
		id := ""
		if len(args) == 1 {
			id = args[0]
		}
		return a.cmdSignals(ctx, id)
	case "defer":
		return a.cmdDefer(ctx, args)
	case "retry":
		if len(args) != 1 {
			return 2, errors.New("usage: tend retry ID")
		}
		return a.cmdRetry(ctx, args[0])
	case "resolve":
		if len(args) != 2 {
			return 2, errors.New("usage: tend resolve ID retry|done|fail")
		}
		return a.cmdResolve(ctx, args[0], args[1])
	case "cancel":
		if len(args) != 1 {
			return 2, errors.New("usage: tend cancel ID")
		}
		return a.cmdCancel(ctx, args[0])
	case "check":
		if len(args) != 0 {
			return 2, errors.New("usage: tend check")
		}
		return a.cmdCheck(ctx)
	default:
		return 2, fmt.Errorf("unknown command %q (try tend help)", verb)
	}
}
func tendRoot() (string, error) {
	root := os.Getenv("TEND_ROOT")
	if root == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		root = filepath.Join(home, ".local", "state", "tend")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if err = os.MkdirAll(abs, 0o700); err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(abs)
}
func newFlagSet(name string, w io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(w)
	return fs
}
func parseTime(s string, now time.Time) (time.Time, error) {
	if s == "" || s == "now" {
		return now, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		if d < 0 {
			return time.Time{}, errors.New("duration must not be negative")
		}
		return now.Add(d), nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q is neither RFC3339 nor a duration", s)
	}
	return t, nil
}
func cleanID(s string) bool {
	if len(s) == 0 || len(s) > 64 || s[0] < 'a' || s[0] > 'z' || s[len(s)-1] == '-' {
		return false
	}
	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
			return false
		}
	}
	return true
}
func validSignalName(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' && r != '.' && r != '_' {
			return false
		}
	}
	return true
}
func joinPayload(a []string) string { return strings.Join(a, " ") }
