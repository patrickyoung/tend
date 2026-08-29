package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func (a *app) cmdSignal(ctx context.Context, args []string) (int, error) {
	fs := newFlagSet("signal", a.errout)
	sid := fs.String("id", "", "idempotency key")
	if err := fs.Parse(args); err != nil {
		return 2, err
	}
	rest := fs.Args()
	if len(rest) < 2 {
		return 2, errors.New("usage: tend signal [-id ID] JOB NAME [PAYLOAD...]")
	}
	job, name := rest[0], rest[1]
	if !validSignalName(name) {
		return 2, fmt.Errorf("invalid signal name %q", name)
	}
	payload := joinPayload(rest[2:])
	if len(rest) == 2 {
		read := true
		if f, ok := a.in.(*os.File); ok {
			if fi, statErr := f.Stat(); statErr == nil && fi.Mode()&os.ModeCharDevice != 0 {
				read = false
			}
		}
		if read {
			b, err := io.ReadAll(io.LimitReader(a.in, (1<<20)+1))
			if err != nil {
				return 2, err
			}
			if len(b) > 1<<20 {
				return 2, errors.New("payload exceeds 1 MiB")
			}
			payload = string(b)
		}
	}
	var err error
	if *sid == "" {
		*sid, err = makeID("signal")
		if err != nil {
			return 2, err
		}
	}
	if !cleanID(*sid) {
		return 2, fmt.Errorf("invalid signal id %q", *sid)
	}
	woke, duplicate, err := a.store.signal(ctx, *sid, job, name, payload)
	if err != nil {
		return 2, err
	}
	if err := writeJSON(a.out, map[string]any{"id": *sid, "job": job, "name": name, "accepted": true, "duplicate": duplicate, "woke": woke}); err != nil {
		return 2, err
	}
	return 0, nil
}
func (s *Store) signal(ctx context.Context, sid, job, name, payload string) (bool, bool, error) {
	digest := sumBytes([]byte(payload))
	now := nowUS()
	c, err := beginImmediate(ctx, s.db)
	if err != nil {
		return false, false, err
	}
	defer rollback(c)
	var oj, on, od string
	var oldWoke int
	err = c.QueryRowContext(ctx, "SELECT job_id,name,payload_digest,woke FROM signals WHERE id=?", sid).Scan(&oj, &on, &od, &oldWoke)
	if err == nil {
		if oj != job || on != name || od != digest {
			return false, false, fmt.Errorf("signal id %s already names different bytes", sid)
		}
		return oldWoke != 0, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, false, err
	}
	var status string
	var wk, wkey sql.NullString
	if err = c.QueryRowContext(ctx, "SELECT status,wait_kind,wait_key FROM jobs WHERE id=?", job).Scan(&status, &wk, &wkey); err != nil {
		return false, false, err
	}
	wake := status == "waiting" && (wk.String == "manual" || (wk.String == "signal" && wkey.String == name))
	if _, err = c.ExecContext(ctx, "INSERT INTO signals(id,job_id,name,payload,payload_digest,received_us,woke)VALUES(?,?,?,?,?,?,?)", sid, job, name, payload, digest, now, boolInt(wake)); err != nil {
		return false, false, err
	}
	if _, err = appendEvent(ctx, c, job, "signal.received", map[string]any{"id": sid, "name": name, "payload_digest": digest, "woke": wake}, now); err != nil {
		return false, false, err
	}
	if wake {
		if _, err = c.ExecContext(ctx, `UPDATE jobs SET status='ready',wait_kind=NULL,wait_key=NULL,wake_at_us=NULL,not_before_us=?,updated_us=? WHERE id=? AND status='waiting'`, now, now, job); err != nil {
			return false, false, err
		}
		seq, e := appendEvent(ctx, c, job, "job.woken", map[string]any{"signal": sid, "name": name}, now)
		if e != nil {
			return false, false, e
		}
		if _, e = c.ExecContext(ctx, "UPDATE signals SET consumed_us=?,consumed_seq=? WHERE id=?", now, seq, sid); e != nil {
			return false, false, e
		}
	}
	return wake, false, commit(c)
}
func (a *app) cmdSignals(ctx context.Context, job string) (int, error) {
	q := "SELECT id,job_id,name,payload,payload_digest,received_us,consumed_us,consumed_seq FROM signals"
	var rows *sql.Rows
	var err error
	if job == "" {
		rows, err = a.store.db.QueryContext(ctx, q+" ORDER BY received_us,id")
	} else {
		rows, err = a.store.db.QueryContext(ctx, q+" WHERE job_id=? ORDER BY received_us,id", job)
	}
	if err != nil {
		return 2, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, j, n, p, d string
		var received int64
		var consumed, seq sql.NullInt64
		if err = rows.Scan(&id, &j, &n, &p, &d, &received, &consumed, &seq); err != nil {
			return 2, err
		}
		v := map[string]any{"id": id, "job": j, "name": n, "payload": p, "payload_digest": d, "received_us": received, "consumed": consumed.Valid}
		if consumed.Valid {
			v["consumed_us"] = consumed.Int64
			v["consumed_seq"] = seq.Int64
		}
		if err = writeJSON(a.out, v); err != nil {
			return 2, err
		}
	}
	return 0, rows.Err()
}

type deferRequest struct {
	Version  int    `json:"version"`
	Kind     string `json:"kind"`
	Key      string `json:"key,omitempty"`
	WakeAtUS int64  `json:"wake_at_us,omitempty"`
}

func deferToFile(args []string) error {
	if len(args) == 0 || len(args) > 2 {
		return errors.New("usage: tend defer signal NAME | until TIME | manual")
	}
	r := deferRequest{Version: 1, Kind: args[0]}
	switch r.Kind {
	case "signal":
		if len(args) != 2 || !validSignalName(args[1]) {
			return errors.New("usage: tend defer signal NAME")
		}
		r.Key = args[1]
	case "until":
		if len(args) != 2 {
			return errors.New("usage: tend defer until TIME")
		}
		t, err := parseTime(args[1], time.Now().UTC())
		if err != nil {
			return err
		}
		r.Kind, r.WakeAtUS = "timer", t.UnixMicro()
	case "manual":
		if len(args) != 1 {
			return errors.New("usage: tend defer manual")
		}
	default:
		return errors.New("usage: tend defer signal NAME | until TIME | manual")
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return writeDurable(os.Getenv("TEND_DEFER_PATH"), append(b, '\n'), 0o600)
}

func (a *app) cmdDefer(_ context.Context, _ []string) (int, error) {
	return 2, errors.New("defer is available only inside a running Tend job")
}

func (s *Store) recordWaitRequest(ctx context.Context, cl claim, path string) error {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(b) > 4096 {
		return errors.New("defer request is too large")
	}
	var r deferRequest
	if err := json.Unmarshal(b, &r); err != nil {
		return err
	}
	if r.Version != 1 || (r.Kind != "manual" && r.Kind != "signal" && r.Kind != "timer") {
		return errors.New("invalid defer request")
	}
	if r.Kind == "signal" && !validSignalName(r.Key) {
		return errors.New("invalid defer signal")
	}
	if r.Kind == "timer" && r.WakeAtUS == 0 {
		return errors.New("invalid defer timer")
	}
	c, err := beginImmediate(ctx, s.db)
	if err != nil {
		return err
	}
	defer rollback(c)
	var n int
	if err := c.QueryRowContext(ctx, `SELECT count(*) FROM jobs j JOIN attempts a ON a.job_id=j.id
WHERE j.id=? AND j.status='running' AND j.lease_token=? AND a.id=? AND a.state='started'`,
		cl.Job.ID, cl.Token, cl.Attempt.ID).Scan(&n); err != nil {
		return err
	}
	if n != 1 {
		return errors.New("attempt is no longer current")
	}
	_, err = c.ExecContext(ctx, `INSERT INTO wait_requests(attempt_id,kind,key,wake_at_us)
VALUES(?,?,?,?) ON CONFLICT(attempt_id) DO UPDATE SET kind=excluded.kind,
key=excluded.key,wake_at_us=excluded.wake_at_us`, cl.Attempt.ID, r.Kind,
		nullableString(r.Key), nullableInt64(r.WakeAtUS))
	if err != nil {
		return err
	}
	return commit(c)
}

func (s *Store) transition(ctx context.Context, id string, from []string, to, kind string, payload any) error {
	c, err := beginImmediate(ctx, s.db)
	if err != nil {
		return err
	}
	defer rollback(c)
	var status string
	if err = c.QueryRowContext(ctx, "SELECT status FROM jobs WHERE id=?", id).Scan(&status); err != nil {
		return err
	}
	ok := false
	for _, v := range from {
		if status == v {
			ok = true
		}
	}
	if !ok {
		return fmt.Errorf("job %s is %s", id, status)
	}
	now := nowUS()
	if _, err = c.ExecContext(ctx, `UPDATE jobs SET status=?,wait_kind=NULL,wait_key=NULL,wake_at_us=NULL,lease_owner=NULL,lease_token=NULL,lease_expires_us=NULL,cancel_requested=0,not_before_us=?,updated_us=? WHERE id=?`, to, now, now, id); err != nil {
		return err
	}
	if _, err = appendEvent(ctx, c, id, kind, payload, now); err != nil {
		return err
	}
	return commit(c)
}
func (a *app) cmdRetry(ctx context.Context, id string) (int, error) {
	if err := a.store.transition(ctx, id, []string{"failed"}, "ready", "job.retried", map[string]any{"by": "operator"}); err != nil {
		return 1, err
	}
	fmt.Fprintln(a.out, id)
	return 0, nil
}
func (a *app) cmdCancel(ctx context.Context, id string) (int, error) {
	c, err := beginImmediate(ctx, a.store.db)
	if err != nil {
		return 2, err
	}
	defer rollback(c)
	var status string
	if err = c.QueryRowContext(ctx, "SELECT status FROM jobs WHERE id=?", id).Scan(&status); err != nil {
		return 1, err
	}
	now := nowUS()
	switch status {
	case "ready", "waiting", "failed":
		_, err = c.ExecContext(ctx, `UPDATE jobs SET status='cancelled',wait_kind=NULL,wait_key=NULL,wake_at_us=NULL,updated_us=? WHERE id=?`, now, id)
	case "running":
		_, err = c.ExecContext(ctx, "UPDATE jobs SET cancel_requested=1,updated_us=? WHERE id=?", now, id)
	case "unknown":
		return 1, errors.New("effect is unknown; use tend resolve")
	default:
		return 1, fmt.Errorf("job %s is already %s", id, status)
	}
	if err != nil {
		return 2, err
	}
	if _, err = appendEvent(ctx, c, id, "job.cancel-requested", map[string]any{"from": status}, now); err != nil {
		return 2, err
	}
	if err = commit(c); err != nil {
		return 2, err
	}
	fmt.Fprintln(a.out, id)
	return 0, nil
}
func (a *app) cmdResolve(ctx context.Context, id, resolution string) (int, error) {
	if resolution != "retry" && resolution != "fail" && resolution != "done" {
		return 2, errors.New("resolution is retry, done, or fail")
	}
	j, err := a.store.job(ctx, id)
	if err != nil {
		return 1, err
	}
	if j.Status != "unknown" {
		return 1, fmt.Errorf("job %s is %s, not unknown", id, j.Status)
	}
	dir := filepath.Join(j.RunDir, "checks")
	if err = ensureDir(dir, 0o700); err != nil {
		return 1, err
	}
	if err = syncDir(j.RunDir); err != nil {
		return 1, err
	}
	lock, err := acquireFileLock(ctx, filepath.Join(dir, "active.lock"))
	if err != nil {
		return 1, err
	}
	defer func() {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
	}()
	if err = a.store.db.QueryRowContext(ctx, "SELECT status FROM jobs WHERE id=?", id).Scan(&j.Status); err != nil {
		return 1, err
	}
	if j.Status != "unknown" {
		return 1, fmt.Errorf("job %s is %s, not unknown", id, j.Status)
	}
	if err = a.removeUnregisteredCheckFiles(ctx, j.ID, dir); err != nil {
		return 1, err
	}
	if err = a.sealUnknownEvidence(ctx, id); err != nil {
		return 1, err
	}
	switch resolution {
	case "retry":
		if err := a.store.transition(ctx, id, []string{"unknown"}, "ready", "job.resolved", map[string]any{"resolution": "retry", "duplicate_effect_risk": "accepted"}); err != nil {
			return 1, err
		}
	case "fail":
		if err := a.store.transition(ctx, id, []string{"unknown"}, "failed", "job.resolved", map[string]any{"resolution": "fail"}); err != nil {
			return 1, err
		}
	case "done":
		if err := a.resolveDone(ctx, id, lock); err != nil {
			var broken *checkBrokenError
			if errors.As(err, &broken) {
				return 2, err
			}
			return 1, err
		}
	}
	fmt.Fprintln(a.out, id)
	return 0, nil
}

func (a *app) sealUnknownEvidence(ctx context.Context, id string) error {
	var attempt string
	var number, pid int
	var runDir string
	var digest sql.NullString
	err := a.store.db.QueryRowContext(ctx, `SELECT a.id,a.number,COALESCE(a.process_pid,0),
j.run_dir,a.output_digest FROM jobs j JOIN attempts a ON a.job_id=j.id
WHERE j.id=? AND j.status='unknown' AND a.state='unknown'
ORDER BY a.number DESC LIMIT 1`, id).Scan(&attempt, &number, &pid, &runDir, &digest)
	if err != nil {
		return err
	}
	if digest.Valid {
		return nil
	}
	if processGroupExists(pid) {
		return fmt.Errorf("attempt %s launcher is still live; retry resolution after it exits", attempt)
	}
	outPath := filepath.Join(runDir, "attempts", fmt.Sprintf("%03d.out", number))
	errPath := filepath.Join(runDir, "attempts", fmt.Sprintf("%03d.err", number))
	for _, path := range []string{outPath, errPath} {
		f, openErr := os.OpenFile(path, os.O_RDWR, 0)
		if openErr != nil {
			return openErr
		}
		if openErr = f.Sync(); openErr != nil {
			_ = f.Close()
			return openErr
		}
		_ = f.Close()
	}
	if err = syncDir(filepath.Dir(outPath)); err != nil {
		return err
	}
	outDigest, outSize, err := sumFile(outPath)
	if err != nil {
		return err
	}
	errDigest, errSize, err := sumFile(errPath)
	if err != nil {
		return err
	}
	outRel, _ := filepath.Rel(a.root, outPath)
	errRel, _ := filepath.Rel(a.root, errPath)
	now := nowUS()
	c, err := beginImmediate(ctx, a.store.db)
	if err != nil {
		return err
	}
	defer rollback(c)
	res, err := c.ExecContext(ctx, `UPDATE attempts SET output_path=?,output_digest=?,
output_size=?,stderr_path=?,stderr_digest=?,stderr_size=? WHERE id=? AND state='unknown'
AND output_digest IS NULL AND EXISTS(SELECT 1 FROM jobs WHERE id=? AND status='unknown')`,
		outRel, outDigest, outSize, errRel, errDigest, errSize, attempt, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return errors.New("unknown attempt changed while evidence was sealed")
	}
	seq, err := appendEvent(ctx, c, id, "attempt.output-sealed", map[string]any{
		"attempt": attempt, "output_digest": outDigest, "output_size": outSize,
		"stderr_digest": errDigest, "stderr_size": errSize,
	}, now)
	if err != nil {
		return err
	}
	for _, item := range []struct {
		name, digest, rel string
		size              int64
	}{{fmt.Sprintf("attempt-%d-output", number), outDigest, outRel, outSize},
		{fmt.Sprintf("attempt-%d-stderr", number), errDigest, errRel, errSize}} {
		if _, err = c.ExecContext(ctx, `INSERT INTO artifacts(job_id,event_seq,name,digest,
size,relpath,created_us)VALUES(?,?,?,?,?,?,?)`, id, seq, item.name, item.digest,
			item.size, item.rel, now); err != nil {
			return err
		}
	}
	return commit(c)
}

var errCheckRejected = errors.New("completion check rejected")

type checkBrokenError struct{ why string }

func (e *checkBrokenError) Error() string { return "completion check is broken: " + e.why }

type checkResult struct {
	name                     string
	outPath, errPath         string
	outDigest, errDigest     string
	outSize, errSize         int64
	exit, signal             int
	passed, rejected, broken bool
	note                     string
}

func (a *app) resolveDone(ctx context.Context, id string, lock *os.File) error {
	j, err := a.store.job(ctx, id)
	if err != nil {
		return err
	}
	if j.Status != "unknown" {
		return fmt.Errorf("job %s is %s, not unknown", id, j.Status)
	}
	if !j.CheckArgvJSON.Valid {
		return errors.New("job has no submitted -check")
	}
	var argv []string
	if err = json.Unmarshal([]byte(j.CheckArgvJSON.String), &argv); err != nil || len(argv) == 0 {
		return errors.New("stored check is invalid")
	}
	if err = a.store.db.QueryRowContext(ctx, "SELECT status FROM jobs WHERE id=?", id).Scan(&j.Status); err != nil {
		return err
	}
	if j.Status != "unknown" {
		return fmt.Errorf("job %s is %s, not unknown", id, j.Status)
	}
	result, err := a.runResolutionCheck(ctx, j, argv, lock)
	if err != nil {
		return err
	}
	if err = testBarrier("_TEND_TEST_RESOLVE_AFTER_CHECK_READY", "_TEND_TEST_RESOLVE_AFTER_CHECK_RELEASE"); err != nil {
		return err
	}
	outRel, _ := filepath.Rel(a.root, result.outPath)
	errRel, _ := filepath.Rel(a.root, result.errPath)
	now := nowUS()
	txCtx := context.Background()
	c, err := beginImmediate(txCtx, a.store.db)
	if err != nil {
		return err
	}
	defer rollback(c)
	var status string
	if err = c.QueryRowContext(txCtx, "SELECT status FROM jobs WHERE id=?", id).Scan(&status); err != nil {
		return err
	}
	if status != "unknown" {
		return fmt.Errorf("job changed to %s while check ran", status)
	}
	kind := "resolution.check-broken"
	if result.rejected {
		kind = "resolution.check-rejected"
	}
	if result.passed {
		kind = "job.resolved"
		if _, err = c.ExecContext(txCtx, "UPDATE jobs SET status='done',updated_us=? WHERE id=? AND status='unknown'", now, id); err != nil {
			return err
		}
	}
	seq, err := appendEvent(txCtx, c, id, kind, map[string]any{"resolution": "done",
		"passed": result.passed, "rejected": result.rejected, "broken": result.broken,
		"check_argv": argv, "exit": result.exit, "signal": result.signal, "note": result.note,
		"output_digest": result.outDigest, "output_size": result.outSize,
		"stderr_digest": result.errDigest, "stderr_size": result.errSize}, now)
	if err != nil {
		return err
	}
	if _, err = c.ExecContext(txCtx, `INSERT INTO artifacts(job_id,event_seq,name,digest,size,relpath,created_us)VALUES(?,?,?,?,?,?,?)`, id, seq, result.name+"-stdout", result.outDigest, result.outSize, outRel, now); err != nil {
		return err
	}
	if _, err = c.ExecContext(txCtx, `INSERT INTO artifacts(job_id,event_seq,name,digest,size,relpath,created_us)VALUES(?,?,?,?,?,?,?)`, id, seq, result.name+"-stderr", result.errDigest, result.errSize, errRel, now); err != nil {
		return err
	}
	if err = commit(c); err != nil {
		return err
	}
	if result.rejected {
		return fmt.Errorf("%w; see %s and %s", errCheckRejected, result.outPath, result.errPath)
	}
	if result.broken {
		return &checkBrokenError{why: result.note}
	}
	return nil
}

func (a *app) runResolutionCheck(ctx context.Context, j Job, argv []string, lock *os.File) (checkResult, error) {
	max, err := durationEnv("TEND_CHECK_MAX", 2*time.Minute)
	if err != nil {
		return checkResult{}, err
	}
	dir := filepath.Join(j.RunDir, "checks")
	if err = ensureDir(dir, 0o700); err != nil {
		return checkResult{}, err
	}
	if err = syncDir(j.RunDir); err != nil {
		return checkResult{}, err
	}
	name, _ := makeID("resolve")
	r := checkResult{name: name, outPath: filepath.Join(dir, name+".out"), errPath: filepath.Join(dir, name+".err")}
	out, err := os.OpenFile(r.outPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return r, err
	}
	errfile, err := os.OpenFile(r.errPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = out.Close()
		return r, err
	}
	p := &preparedExec{output: out, stderr: errfile, outputPath: r.outPath, stderrPath: r.errPath, outputDir: dir}
	defer p.close()
	p.gateRead, p.gateWrite, err = os.Pipe()
	if err != nil {
		p.close()
		return r, err
	}
	p.controlRead, p.controlWrite, err = os.Pipe()
	if err != nil {
		p.close()
		return r, err
	}
	exe, err := os.Executable()
	if err != nil {
		p.close()
		return r, err
	}
	cmd := exec.Command(exe, append([]string{"_exec", "--"}, argv...)...)
	cmd.Dir = j.CWD
	cmd.Stdin = strings.NewReader("")
	cmd.Stdout = out
	cmd.Stderr = errfile
	cmd.Env, err = scrubbedEnv()
	if err != nil {
		_ = out.Close()
		_ = errfile.Close()
		return r, err
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.ExtraFiles = []*os.File{p.gateRead, p.controlRead, lock}
	if err = cmd.Start(); err != nil {
		_, _ = fmt.Fprintln(errfile, "tend: check exec:", err)
		r.exit = 127
		r.broken = true
		r.note = "start failed: " + err.Error()
		if err = sealOutput(p); err != nil {
			return r, err
		}
		return finishCheckFiles(r)
	}
	_ = p.gateRead.Close()
	p.gateRead = nil
	_ = p.controlRead.Close()
	p.controlRead = nil
	if _, err = p.gateWrite.Write([]byte{1}); err != nil {
		_ = p.gateWrite.Close()
		_ = p.controlWrite.Close()
		p.gateWrite, p.controlWrite = nil, nil
		_ = cmd.Wait()
		return r, err
	}
	_ = p.gateWrite.Close()
	p.gateWrite = nil
	runCtx, cancel := context.WithTimeout(ctx, max)
	defer cancel()
	ch := make(chan error, 1)
	go func() { ch <- cmd.Wait() }()
	var runErr error
	timedOut, interrupted := false, false
	select {
	case runErr = <-ch:
	case <-runCtx.Done():
		_ = p.controlWrite.Close()
		p.controlWrite = nil
		runErr = <-ch
		timedOut = errors.Is(runCtx.Err(), context.DeadlineExceeded)
		interrupted = !timedOut
	}
	if p.controlWrite != nil {
		_ = p.controlWrite.Close()
		p.controlWrite = nil
	}
	lingering := quiesceGroup(cmd.Process.Pid)
	if err = sealOutput(p); err != nil {
		return r, err
	}
	r.exit, r.signal = processStatus(runErr)
	switch {
	case timedOut:
		r.broken = true
		r.note = "timed out"
	case interrupted:
		r.broken = true
		r.note = "interrupted"
	case lingering || errors.Is(runErr, exec.ErrWaitDelay):
		r.broken = true
		r.note = "background descendants outlived the check"
	case r.signal != 0:
		r.broken = true
		r.note = "ended by signal"
	case r.exit == 0:
		r.passed = true
		r.note = "accepted"
	case r.exit == 1:
		r.rejected = true
		r.note = "rejected"
	default:
		r.broken = true
		r.note = fmt.Sprintf("exited %d", r.exit)
	}
	return finishCheckFiles(r)
}

func acquireFileLock(ctx context.Context, path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			_ = f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (a *app) removeUnregisteredCheckFiles(ctx context.Context, job, dir string) error {
	rows, err := a.store.db.QueryContext(ctx, "SELECT relpath FROM artifacts WHERE job_id=?", job)
	if err != nil {
		return err
	}
	registered := map[string]bool{}
	for rows.Next() {
		var rel string
		if err = rows.Scan(&rel); err != nil {
			_ = rows.Close()
			return err
		}
		registered[filepath.Join(a.root, rel)] = true
	}
	_ = rows.Close()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		ext := filepath.Ext(entry.Name())
		if ext != ".out" && ext != ".err" {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if !registered[path] {
			if err = os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return syncDir(dir)
}

func finishCheckFiles(r checkResult) (checkResult, error) {
	var err error
	r.outDigest, r.outSize, err = sumFile(r.outPath)
	if err != nil {
		return r, err
	}
	r.errDigest, r.errSize, err = sumFile(r.errPath)
	return r, err
}
