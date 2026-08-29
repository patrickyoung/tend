package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type workRecord struct {
	Job     string `json:"job,omitempty"`
	Attempt string `json:"attempt,omitempty"`
	Event   string `json:"event"`
	Status  string `json:"status,omitempty"`
	Exit    *int   `json:"exit,omitempty"`
	Output  string `json:"output,omitempty"`
	Error   string `json:"error,omitempty"`
}
type claim struct {
	Job          Job
	Attempt      Attempt
	Token, Owner string
}
type preparedExec struct {
	cmd                               *exec.Cmd
	input, output, stderr             *os.File
	gateRead, gateWrite, controlRead  *os.File
	controlWrite                      *os.File
	activeLock                        *os.File
	outputPath, stderrPath, outputDir string
	deferPath                         string
}

func (p *preparedExec) close() {
	if p.input != nil {
		_ = p.input.Close()
	}
	if p.output != nil {
		_ = p.output.Close()
	}
	if p.stderr != nil {
		_ = p.stderr.Close()
	}
	if p.gateRead != nil {
		_ = p.gateRead.Close()
	}
	if p.gateWrite != nil {
		_ = p.gateWrite.Close()
	}
	if p.controlRead != nil {
		_ = p.controlRead.Close()
	}
	if p.controlWrite != nil {
		_ = p.controlWrite.Close()
	}
	if p.activeLock != nil {
		_ = p.activeLock.Close()
		p.activeLock = nil
	}
	if p.deferPath != "" {
		_ = os.Remove(p.deferPath)
	}
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	s := os.Getenv(name)
	if s == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return d, nil
}
func (a *app) cmdWork(ctx context.Context) (int, error) {
	lease, err := durationEnv("TEND_LEASE", 30*time.Second)
	if err != nil {
		return 2, err
	}
	if lease < 300*time.Millisecond {
		return 2, errors.New("TEND_LEASE must be at least 300ms")
	}
	maxRun, err := durationEnv("TEND_JOB_MAX", 30*time.Minute)
	if err != nil {
		return 2, err
	}
	if rec, ok, e := a.store.recoverOne(ctx); e != nil {
		return 2, e
	} else if ok {
		if err := writeJSON(a.out, rec); err != nil {
			return 2, err
		}
		return 0, nil
	}
	if rec, ok, e := a.store.wakeOneTimer(ctx); e != nil {
		return 2, e
	} else if ok {
		if err := writeJSON(a.out, rec); err != nil {
			return 2, err
		}
		return 0, nil
	}
	cl, err := a.store.claim(ctx, lease)
	if errors.Is(err, sql.ErrNoRows) {
		return 1, nil
	}
	if err != nil {
		return 2, err
	}
	if os.Getenv("_TEND_TEST_CRASH") == "after-prepare" {
		os.Exit(91)
	}
	prepared, err := a.prepareExecution(ctx, cl)
	if err != nil {
		_ = a.store.preflightFailed(context.Background(), cl, err.Error())
		return 2, fmt.Errorf("execution preflight: %w", err)
	}
	defer prepared.close()
	rec, interrupted, err := a.execute(ctx, cl, prepared, lease, maxRun)
	if err != nil {
		return 2, err
	}
	if err := writeJSON(a.out, rec); err != nil {
		return 2, err
	}
	if interrupted {
		return 130, nil
	}
	return 0, nil
}

func (a *app) prepareExecution(ctx context.Context, cl claim) (*preparedExec, error) {
	var argv []string
	if err := json.Unmarshal([]byte(cl.Job.ArgvJSON), &argv); err != nil || len(argv) == 0 {
		return nil, errors.New("stored argv is invalid")
	}
	physicalCWD, err := filepath.EvalSymlinks(cl.Job.CWD)
	if err != nil || physicalCWD != cl.Job.CWD {
		return nil, fmt.Errorf("working directory is unavailable: %s", cl.Job.CWD)
	}
	input, err := a.preflightInput(ctx, cl)
	if err != nil {
		return nil, err
	}
	ok := false
	p := &preparedExec{input: input}
	defer func() {
		if !ok {
			if p.outputDir != "" {
				_ = removePreparedFiles(p)
			}
			p.close()
		}
	}()
	deferFile, err := os.CreateTemp("", "tend-defer-"+cl.Job.ID+"-")
	if err != nil {
		return nil, err
	}
	p.deferPath = deferFile.Name()
	if err := deferFile.Close(); err != nil {
		return nil, err
	}
	if err := os.Remove(p.deferPath); err != nil {
		return nil, err
	}
	env, err := workerEnv(cl, p.deferPath)
	if err != nil {
		return nil, err
	}
	if st, statErr := os.Lstat(cl.Job.RunDir); statErr != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("job directory is missing or symlinked")
	}
	p.outputDir = filepath.Join(cl.Job.RunDir, "attempts")
	if err := ensureDir(p.outputDir, 0o700); err != nil {
		return nil, err
	}
	if err := syncDir(cl.Job.RunDir); err != nil {
		return nil, err
	}
	p.activeLock, err = acquireFileLock(ctx, filepath.Join(p.outputDir,
		fmt.Sprintf("%03d.active.lock", cl.Attempt.Number)))
	if err != nil {
		return nil, err
	}
	p.outputPath = filepath.Join(p.outputDir, fmt.Sprintf("%03d.out", cl.Attempt.Number))
	p.stderrPath = filepath.Join(p.outputDir, fmt.Sprintf("%03d.err", cl.Attempt.Number))
	p.output, err = os.OpenFile(p.outputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	p.stderr, err = os.OpenFile(p.stderrPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if err = syncDir(p.outputDir); err != nil {
		return nil, err
	}
	p.gateRead, p.gateWrite, err = os.Pipe()
	if err != nil {
		return nil, err
	}
	p.controlRead, p.controlWrite, err = os.Pipe()
	if err != nil {
		return nil, err
	}
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	internalArgv := append([]string{"_exec", "--"}, argv...)
	p.cmd = exec.Command(exe, internalArgv...)
	p.cmd.Dir = cl.Job.CWD
	p.cmd.Stdin = input
	// The private launcher quiesces its submitted process group before it
	// exits, so direct file descriptors are safe and do not leave controller
	// copy buffers that could arrive after unknown evidence is sealed.
	p.cmd.Stdout = p.output
	p.cmd.Stderr = p.stderr
	p.cmd.Env = env
	p.cmd.ExtraFiles = []*os.File{p.gateRead, p.controlRead}
	p.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	p.cmd.WaitDelay = 50 * time.Millisecond
	ok = true
	return p, nil
}

func (a *app) preflightInput(ctx context.Context, cl claim) (*os.File, error) {
	var digest, rel string
	var size int64
	err := a.store.db.QueryRowContext(ctx, `SELECT digest,size,relpath FROM artifacts
WHERE job_id=? AND name='input'`, cl.Job.ID).Scan(&digest, &size, &rel)
	if errors.Is(err, sql.ErrNoRows) {
		if _, statErr := os.Stat(filepath.Join(cl.Job.RunDir, "input")); statErr == nil {
			return nil, errors.New("unregistered input file is present")
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return nil, statErr
		}
		return os.Open(os.DevNull)
	}
	if err != nil {
		return nil, err
	}
	expected := filepath.Join("jobs", cl.Job.ID, "input")
	if rel != expected {
		return nil, fmt.Errorf("registered input path is %q, want %q", rel, expected)
	}
	f, err := os.Open(filepath.Join(a.root, rel))
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = f.Close()
		}
	}()
	lst, err := os.Lstat(filepath.Join(a.root, rel))
	if err != nil || lst.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("registered input is missing or symlinked")
	}
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		return nil, errors.New("registered input is not a regular file")
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return nil, err
	}
	got := hex.EncodeToString(h.Sum(nil))
	if n != size || got != digest {
		return nil, fmt.Errorf("digest mismatch: got %s/%d, want %s/%d", got, n, digest, size)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	ok = true
	return f, nil
}

func (s *Store) preflightFailed(ctx context.Context, cl claim, note string) error {
	now := nowUS()
	c, err := beginImmediate(ctx, s.db)
	if err != nil {
		return err
	}
	defer rollback(c)
	var cancel int
	if err = c.QueryRowContext(ctx, `SELECT cancel_requested FROM jobs WHERE id=?
AND status='running' AND lease_token=?`, cl.Job.ID, cl.Token).Scan(&cancel); err != nil {
		return err
	}
	outcome := "preflight-failed"
	status, event := "failed", "attempt.preflight-failed"
	if cancel != 0 {
		outcome, status, event = "cancelled-before-start", "cancelled", "attempt.cancelled"
	}
	res, err := c.ExecContext(ctx, `UPDATE attempts SET state='abandoned',finished_us=?,
outcome=? WHERE id=? AND state='prepared'`, now, outcome, cl.Attempt.ID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return errors.New("prepared attempt changed during preflight")
	}
	res, err = c.ExecContext(ctx, `UPDATE jobs SET status=?,lease_owner=NULL,
lease_token=NULL,lease_expires_us=NULL,cancel_requested=0,updated_us=? WHERE id=?
AND status='running' AND lease_token=?`, status, now, cl.Job.ID, cl.Token)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return errors.New("job changed during preflight")
	}
	if _, err := appendEvent(ctx, c, cl.Job.ID, event, map[string]any{
		"attempt": cl.Attempt.ID, "error": note, "executed": false,
	}, now); err != nil {
		return err
	}
	return commit(c)
}

type recoveryCandidate struct {
	jobID, attemptID, state, runDir string
	number, pid, cancel             int
}

func (s *Store) recoverOne(ctx context.Context) (workRecord, bool, error) {
	now := nowUS()
	var r recoveryCandidate
	err := s.db.QueryRowContext(ctx, `SELECT j.id,a.id,a.state,j.run_dir,a.number,
COALESCE(a.process_pid,0),j.cancel_requested FROM jobs j JOIN attempts a ON a.job_id=j.id
WHERE j.status='running' AND j.lease_expires_us<? AND a.state IN('prepared','started')
ORDER BY j.lease_expires_us,j.id LIMIT 1`, now).Scan(&r.jobID, &r.attemptID,
		&r.state, &r.runDir, &r.number, &r.pid, &r.cancel)
	if errors.Is(err, sql.ErrNoRows) {
		return workRecord{}, false, nil
	}
	if err != nil {
		return workRecord{}, false, err
	}
	if r.state == "prepared" {
		return s.recoverPrepared(ctx, r, now)
	}
	if processGroupExists(r.pid) {
		return s.recoverLiveStarted(ctx, r, now)
	}
	return s.recoverStarted(ctx, r, now)
}

func processGroupExists(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(-pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func (s *Store) recoverPrepared(ctx context.Context, r recoveryCandidate, now int64) (workRecord, bool, error) {
	for _, ext := range []string{"out", "err"} {
		if err := os.Remove(filepath.Join(r.runDir, "attempts", fmt.Sprintf("%03d.%s", r.number, ext))); err != nil && !errors.Is(err, os.ErrNotExist) {
			return workRecord{}, false, err
		}
	}
	_ = syncDir(filepath.Join(r.runDir, "attempts"))
	if err := testBarrier("_TEND_TEST_RECOVER_PREPARED_READY", "_TEND_TEST_RECOVER_PREPARED_RELEASE"); err != nil {
		return workRecord{}, false, err
	}
	c, err := beginImmediate(ctx, s.db)
	if err != nil {
		return workRecord{}, false, err
	}
	defer rollback(c)
	var cancel int
	if err = c.QueryRowContext(ctx, `SELECT cancel_requested FROM jobs WHERE id=?
AND status='running' AND lease_expires_us<?`, r.jobID, now).Scan(&cancel); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workRecord{}, false, nil
		}
		return workRecord{}, false, err
	}
	status, event, outcome := "ready", "attempt.abandoned", "ready"
	if cancel != 0 {
		status, event, outcome = "cancelled", "attempt.cancelled", "cancelled-before-start"
	}
	res, err := c.ExecContext(ctx, `UPDATE attempts SET state='abandoned',finished_us=?,
outcome=? WHERE id=? AND state='prepared' AND EXISTS(SELECT 1 FROM jobs WHERE id=?
AND status='running' AND lease_expires_us<?)`, now, outcome, r.attemptID, r.jobID, now)
	if err != nil {
		return workRecord{}, false, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return workRecord{}, false, nil
	}
	if _, err = c.ExecContext(ctx, `UPDATE jobs SET status=?,lease_owner=NULL,
lease_token=NULL,lease_expires_us=NULL,cancel_requested=0,updated_us=? WHERE id=?`,
		status, now, r.jobID); err != nil {
		return workRecord{}, false, err
	}
	if _, err = appendEvent(ctx, c, r.jobID, event, map[string]any{
		"attempt": r.attemptID, "from": "prepared", "to": status, "executed": false,
	}, now); err != nil {
		return workRecord{}, false, err
	}
	if err = commit(c); err != nil {
		return workRecord{}, false, err
	}
	return workRecord{Job: r.jobID, Attempt: r.attemptID, Event: event, Status: status}, true, nil
}

func (s *Store) recoverLiveStarted(ctx context.Context, r recoveryCandidate, now int64) (workRecord, bool, error) {
	c, err := beginImmediate(ctx, s.db)
	if err != nil {
		return workRecord{}, false, err
	}
	defer rollback(c)
	res, err := c.ExecContext(ctx, `UPDATE attempts SET state='unknown',finished_us=?,
outcome='unknown' WHERE id=? AND state='started' AND EXISTS(SELECT 1 FROM jobs
WHERE id=? AND status='running' AND lease_expires_us<?)`, now, r.attemptID, r.jobID, now)
	if err != nil {
		return workRecord{}, false, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return workRecord{}, false, nil
	}
	if _, err = c.ExecContext(ctx, `UPDATE jobs SET status='unknown',lease_owner=NULL,
lease_token=NULL,lease_expires_us=NULL,cancel_requested=0,updated_us=? WHERE id=?`,
		now, r.jobID); err != nil {
		return workRecord{}, false, err
	}
	if _, err = appendEvent(ctx, c, r.jobID, "attempt.effect-unknown", map[string]any{
		"attempt": r.attemptID, "from": "started", "to": "unknown",
		"process_pid": r.pid, "process_may_be_live": true,
	}, now); err != nil {
		return workRecord{}, false, err
	}
	if err = commit(c); err != nil {
		return workRecord{}, false, err
	}
	return workRecord{Job: r.jobID, Attempt: r.attemptID,
		Event: "attempt.effect-unknown", Status: "unknown"}, true, nil
}

func (s *Store) recoverStarted(ctx context.Context, r recoveryCandidate, now int64) (workRecord, bool, error) {
	outPath := filepath.Join(r.runDir, "attempts", fmt.Sprintf("%03d.out", r.number))
	errPath := filepath.Join(r.runDir, "attempts", fmt.Sprintf("%03d.err", r.number))
	for _, path := range []string{outPath, errPath} {
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return workRecord{}, false, err
		}
		if err = f.Sync(); err != nil {
			_ = f.Close()
			return workRecord{}, false, err
		}
		_ = f.Close()
	}
	if err := syncDir(filepath.Dir(outPath)); err != nil {
		return workRecord{}, false, err
	}
	outDigest, outSize, err := sumFile(outPath)
	if err != nil {
		return workRecord{}, false, err
	}
	errDigest, errSize, err := sumFile(errPath)
	if err != nil {
		return workRecord{}, false, err
	}
	outRel, _ := filepath.Rel(s.root, outPath)
	errRel, _ := filepath.Rel(s.root, errPath)
	c, err := beginImmediate(ctx, s.db)
	if err != nil {
		return workRecord{}, false, err
	}
	defer rollback(c)
	res, err := c.ExecContext(ctx, `UPDATE attempts SET state='unknown',finished_us=?,
outcome='unknown',output_path=?,output_digest=?,output_size=?,stderr_path=?,
stderr_digest=?,stderr_size=? WHERE id=? AND state='started' AND EXISTS(SELECT 1
FROM jobs WHERE id=? AND status='running' AND lease_expires_us<?)`, now, outRel,
		outDigest, outSize, errRel, errDigest, errSize, r.attemptID, r.jobID, now)
	if err != nil {
		return workRecord{}, false, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return workRecord{}, false, nil
	}
	if _, err = c.ExecContext(ctx, `UPDATE jobs SET status='unknown',lease_owner=NULL,
lease_token=NULL,lease_expires_us=NULL,cancel_requested=0,updated_us=? WHERE id=?`,
		now, r.jobID); err != nil {
		return workRecord{}, false, err
	}
	seq, err := appendEvent(ctx, c, r.jobID, "attempt.effect-unknown", map[string]any{
		"attempt": r.attemptID, "from": "started", "to": "unknown",
		"process_pid": r.pid, "process_may_be_live": false,
		"output_digest": outDigest, "output_size": outSize,
		"stderr_digest": errDigest, "stderr_size": errSize,
	}, now)
	if err != nil {
		return workRecord{}, false, err
	}
	for _, item := range []struct {
		name, digest, rel string
		size              int64
	}{{fmt.Sprintf("attempt-%d-output", r.number), outDigest, outRel, outSize},
		{fmt.Sprintf("attempt-%d-stderr", r.number), errDigest, errRel, errSize}} {
		if _, err = c.ExecContext(ctx, `INSERT INTO artifacts(job_id,event_seq,name,digest,
size,relpath,created_us)VALUES(?,?,?,?,?,?,?)`, r.jobID, seq, item.name,
			item.digest, item.size, item.rel, now); err != nil {
			return workRecord{}, false, err
		}
	}
	if err = commit(c); err != nil {
		return workRecord{}, false, err
	}
	return workRecord{Job: r.jobID, Attempt: r.attemptID,
		Event: "attempt.effect-unknown", Status: "unknown",
		Output: outRel, Error: errRel}, true, nil
}
func (s *Store) wakeOneTimer(ctx context.Context) (workRecord, bool, error) {
	now := nowUS()
	c, err := beginImmediate(ctx, s.db)
	if err != nil {
		return workRecord{}, false, err
	}
	defer rollback(c)
	var id string
	err = c.QueryRowContext(ctx, "SELECT id FROM jobs WHERE status='waiting' AND wait_kind='timer' AND wake_at_us<=? ORDER BY wake_at_us,id LIMIT 1", now).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return workRecord{}, false, nil
	}
	if err != nil {
		return workRecord{}, false, err
	}
	if _, err = c.ExecContext(ctx, `UPDATE jobs SET status='ready',wait_kind=NULL,wait_key=NULL,wake_at_us=NULL,not_before_us=?,updated_us=? WHERE id=?`, now, now, id); err != nil {
		return workRecord{}, false, err
	}
	if _, err = appendEvent(ctx, c, id, "timer.fired", map[string]any{"at_us": now}, now); err != nil {
		return workRecord{}, false, err
	}
	if err = commit(c); err != nil {
		return workRecord{}, false, err
	}
	return workRecord{Job: id, Event: "timer.fired", Status: "ready"}, true, nil
}
func (s *Store) claim(ctx context.Context, lease time.Duration) (claim, error) {
	now := nowUS()
	token, err := makeID("lease")
	if err != nil {
		return claim{}, err
	}
	aid, err := makeID("attempt")
	if err != nil {
		return claim{}, err
	}
	host, _ := os.Hostname()
	owner := fmt.Sprintf("%s/%d/%s", host, os.Getpid(), token)
	c, err := beginImmediate(ctx, s.db)
	if err != nil {
		return claim{}, err
	}
	defer rollback(c)
	j, err := scanJob(c.QueryRowContext(ctx, "SELECT "+jobColumns+` FROM jobs j WHERE j.status='ready' AND j.not_before_us<=? AND NOT EXISTS(SELECT 1 FROM jobs x WHERE x.serial_key=j.serial_key AND x.status IN('running','unknown')) ORDER BY j.not_before_us,j.created_us,j.id LIMIT 1`, now))
	if err != nil {
		return claim{}, err
	}
	var number int
	if err = c.QueryRowContext(ctx, "SELECT COALESCE(MAX(number),0)+1 FROM attempts WHERE job_id=?", j.ID).Scan(&number); err != nil {
		return claim{}, err
	}
	effect := sumBytes([]byte(j.ID + "\x00" + strconv.Itoa(number) + "\x00" + j.DefinitionDigest))
	if _, err = c.ExecContext(ctx, `INSERT INTO attempts(id,job_id,number,state,prepared_us,effect_key)VALUES(?,?,?,'prepared',?,?)`, aid, j.ID, number, now, effect); err != nil {
		return claim{}, err
	}
	expires := time.Now().Add(lease).UnixMicro()
	res, err := c.ExecContext(ctx, `UPDATE jobs SET status='running',lease_owner=?,lease_token=?,lease_expires_us=?,updated_us=? WHERE id=? AND status='ready'`, owner, token, expires, now, j.ID)
	if err != nil {
		return claim{}, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return claim{}, errors.New("claim lost")
	}
	if _, err = appendEvent(ctx, c, j.ID, "attempt.prepared", map[string]any{"attempt": aid, "number": number, "owner": owner, "lease_expires_us": expires, "effect_key": effect}, now); err != nil {
		return claim{}, err
	}
	if err = commit(c); err != nil {
		return claim{}, err
	}
	return claim{Job: j, Attempt: Attempt{ID: aid, JobID: j.ID, Number: number, State: "prepared", EffectKey: effect}, Token: token, Owner: owner}, nil
}
func (s *Store) start(ctx context.Context, cl claim, lease time.Duration, pid int) (bool, error) {
	now := nowUS()
	c, err := beginImmediate(ctx, s.db)
	if err != nil {
		return false, err
	}
	defer rollback(c)
	var status, token string
	var cancel int
	if err = c.QueryRowContext(ctx, `SELECT status,COALESCE(lease_token,''),cancel_requested
FROM jobs WHERE id=?`, cl.Job.ID).Scan(&status, &token, &cancel); err != nil {
		return false, err
	}
	if status != "running" || token != cl.Token {
		return false, errors.New("attempt is no longer current")
	}
	if cancel != 0 {
		res, e := c.ExecContext(ctx, `UPDATE attempts SET state='abandoned',finished_us=?,
outcome='cancelled-before-start' WHERE id=? AND job_id=? AND state='prepared'`,
			now, cl.Attempt.ID, cl.Job.ID)
		if e != nil {
			return false, e
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return false, errors.New("attempt is no longer prepared")
		}
		if _, e = c.ExecContext(ctx, `UPDATE jobs SET status='cancelled',lease_owner=NULL,
lease_token=NULL,lease_expires_us=NULL,cancel_requested=0,updated_us=? WHERE id=?`,
			now, cl.Job.ID); e != nil {
			return false, e
		}
		if _, e = appendEvent(ctx, c, cl.Job.ID, "attempt.cancelled",
			map[string]any{"attempt": cl.Attempt.ID, "from": "prepared", "executed": false}, now); e != nil {
			return false, e
		}
		return true, commit(c)
	}
	res, err := c.ExecContext(ctx, `UPDATE attempts SET state='started',started_us=?,
process_pid=? WHERE id=? AND job_id=? AND state='prepared'`,
		now, pid, cl.Attempt.ID, cl.Job.ID)
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return false, errors.New("attempt is no longer prepared")
	}
	expires := time.Now().Add(lease).UnixMicro()
	res, err = c.ExecContext(ctx, "UPDATE jobs SET lease_expires_us=?,updated_us=? WHERE id=? AND status='running' AND lease_token=? AND lease_expires_us>=?", expires, now, cl.Job.ID, cl.Token, now)
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return false, errors.New("lease expired before start; executed nothing")
	}
	_, err = appendEvent(ctx, c, cl.Job.ID, "attempt.started", map[string]any{
		"attempt": cl.Attempt.ID, "effect_key": cl.Attempt.EffectKey,
		"process_pid": pid,
	}, now)
	if err != nil {
		return false, err
	}
	return false, commit(c)
}

type heartbeatResult struct{ lost, cancel bool }

func (s *Store) renew(ctx context.Context, job, token string, lease time.Duration) (heartbeatResult, error) {
	now := nowUS()
	expires := time.Now().Add(lease).UnixMicro()
	var cancel int
	err := s.db.QueryRowContext(ctx, "UPDATE jobs SET lease_expires_us=?,updated_us=? WHERE id=? AND status='running' AND lease_token=? AND lease_expires_us>=? RETURNING cancel_requested", expires, now, job, token, now).Scan(&cancel)
	if errors.Is(err, sql.ErrNoRows) {
		return heartbeatResult{lost: true}, nil
	}
	return heartbeatResult{cancel: cancel != 0}, err
}

func terminateGroup(p *os.Process) func() {
	if p == nil {
		return func() {}
	}
	_ = syscall.Kill(-p.Pid, syscall.SIGTERM)
	timer := time.AfterFunc(2*time.Second, func() { _ = syscall.Kill(-p.Pid, syscall.SIGKILL) })
	return func() {
		// If the leader exited on TERM, kill any remaining members now. Stopping
		// the timer also prevents a delayed signal from reaching a reused pgid.
		if timer.Stop() {
			_ = syscall.Kill(-p.Pid, syscall.SIGKILL)
		}
	}
}

type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}

// quiesceGroup runs after the submitted leader has exited. A surviving
// process in its group means the attempt is not actually quiescent yet. Kill
// it before sealing output and report the attempt unknown: it may already have
// performed an effect.
func quiesceGroup(pid int) bool {
	if err := syscall.Kill(-pid, 0); err != nil {
		return false
	}
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(-pid, 0); err != nil {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	return true
}
func (a *app) execute(parent context.Context, cl claim, p *preparedExec, lease, maxRun time.Duration) (workRecord, bool, error) {
	cmd := p.cmd
	if err := cmd.Start(); err != nil {
		_ = a.store.preflightFailed(context.Background(), cl, "internal exec start failed: "+err.Error())
		_ = removePreparedFiles(p)
		return workRecord{}, false, fmt.Errorf("internal exec start: %w", err)
	}
	_ = p.gateRead.Close()
	p.gateRead = nil
	_ = p.controlRead.Close()
	p.controlRead = nil
	if err := testBarrier("_TEND_TEST_BEFORE_START_READY", "_TEND_TEST_BEFORE_START_RELEASE"); err != nil {
		_ = p.gateWrite.Close()
		_ = p.controlWrite.Close()
		p.gateWrite, p.controlWrite = nil, nil
		_ = cmd.Wait()
		return workRecord{}, false, err
	}
	cancelled, err := a.store.start(parent, cl, lease, cmd.Process.Pid)
	if err != nil {
		_ = p.gateWrite.Close()
		p.gateWrite = nil
		_ = p.controlWrite.Close()
		p.controlWrite = nil
		_ = cmd.Wait()
		return workRecord{}, false, err
	}
	if cancelled {
		_ = p.gateWrite.Close()
		p.gateWrite = nil
		_ = p.controlWrite.Close()
		p.controlWrite = nil
		_ = cmd.Wait()
		if err = removePreparedFiles(p); err != nil {
			return workRecord{}, false, err
		}
		return workRecord{Job: cl.Job.ID, Attempt: cl.Attempt.ID,
			Event: "attempt.cancelled", Status: "cancelled"}, false, nil
	}
	fmt.Fprintf(a.errout, "tend: %s attempt %d started\n", cl.Job.ID, cl.Attempt.Number)
	if os.Getenv("_TEND_TEST_CRASH") == "after-start" {
		os.Exit(92)
	}
	if _, err = p.gateWrite.Write([]byte{1}); err != nil {
		_ = p.gateWrite.Close()
		p.gateWrite = nil
		_ = p.controlWrite.Close()
		p.controlWrite = nil
		_ = cmd.Wait()
		if sealErr := sealOutput(p); sealErr != nil {
			return workRecord{}, false, sealErr
		}
		exit := 125
		return a.finish(cl, "unknown", &exit, 0, p.outputPath, p.stderrPath,
			"failed to release durable exec gate")
	}
	_ = p.gateWrite.Close()
	p.gateWrite = nil
	runCtx, cancel := context.WithTimeout(parent, maxRun)
	defer cancel()
	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()
	tick := time.NewTicker(lease / 3)
	defer tick.Stop()
	var waitErr error
	done := false
	timedOut, interrupted, lost, cancelled := false, false, false, false
	for !done {
		select {
		case waitErr = <-waitCh:
			done = true
		case <-tick.C:
			hr, e := a.store.renew(context.Background(), cl.Job.ID, cl.Token, lease)
			if e != nil || hr.lost || hr.cancel {
				lost = e != nil || hr.lost
				cancelled = hr.cancel
				_ = p.controlWrite.Close()
				p.controlWrite = nil
				waitErr = <-waitCh
				done = true
			}
		case <-runCtx.Done():
			_ = p.controlWrite.Close()
			p.controlWrite = nil
			waitErr = <-waitCh
			timedOut = errors.Is(runCtx.Err(), context.DeadlineExceeded)
			interrupted = !timedOut
			done = true
		}
	}
	if p.controlWrite != nil {
		_ = p.controlWrite.Close()
		p.controlWrite = nil
	}
	lingering := quiesceGroup(cmd.Process.Pid)
	if err := sealOutput(p); err != nil {
		return workRecord{}, interrupted, err
	}
	exit, sig := processStatus(waitErr)
	status, note := "failed", "observed nonzero exit"
	var deferErr error
	if exit == 75 && sig == 0 {
		deferErr = a.store.recordWaitRequest(context.Background(), cl, p.deferPath)
	}
	switch {
	case lost:
		status, note = "unknown", "lease ownership was lost"
	case cancelled:
		status, note = "unknown", "cancelled after execution started"
	case timedOut:
		status, note = "unknown", "timeout after execution started"
	case interrupted:
		status, note = "unknown", "controller interrupted after execution started"
	case lingering || errors.Is(waitErr, exec.ErrWaitDelay):
		status, note = "unknown", "background descendants outlived the submitted command"
	case deferErr != nil:
		status, note = "failed", "invalid defer request: "+deferErr.Error()
	case sig != 0:
		status, note = "unknown", "process ended by signal"
	case exit == 0:
		status, note = "done", "observed exit 0"
	case exit == 75:
		status, note = "waiting", "observed exit 75"
	case exit == 125:
		status, note = "unknown", "exit 125: effects may exist"
	case exit == 2:
		status, note = "failed", "exit 2: not done"
	case exit == 3:
		status, note = "failed", "exit 3: declined"
	}
	rec, _, err := a.finish(cl, status, &exit, sig, p.outputPath, p.stderrPath, note)
	return rec, interrupted, err
}

func removePreparedFiles(p *preparedExec) error {
	for _, f := range []*os.File{p.output, p.stderr, p.gateRead, p.gateWrite,
		p.controlRead, p.controlWrite} {
		if f != nil {
			_ = f.Close()
		}
	}
	p.output, p.stderr = nil, nil
	p.gateRead, p.gateWrite = nil, nil
	p.controlRead, p.controlWrite = nil, nil
	for _, path := range []string{p.outputPath, p.stderrPath} {
		if path != "" {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return syncDir(p.outputDir)
}

func sealOutput(p *preparedExec) error {
	if err := p.output.Sync(); err != nil {
		return err
	}
	if err := p.output.Close(); err != nil {
		return err
	}
	p.output = nil
	if err := p.stderr.Sync(); err != nil {
		return err
	}
	if err := p.stderr.Close(); err != nil {
		return err
	}
	p.stderr = nil
	return syncDir(p.outputDir)
}
func processStatus(err error) (int, int) {
	if err == nil {
		return 0, 0
	}
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		return 127, 0
	}
	if ws, ok := ee.Sys().(syscall.WaitStatus); ok {
		if ws.Signaled() {
			return 128 + int(ws.Signal()), int(ws.Signal())
		}
		return ws.ExitStatus(), 0
	}
	return ee.ExitCode(), 0
}
func workerEnv(cl claim, deferPath string) ([]string, error) {
	env, err := scrubbedEnv()
	if err != nil {
		return nil, err
	}
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	return append(env, "TEND="+exe, "TEND_JOB_ID="+cl.Job.ID,
		"TEND_ATTEMPT_KEY="+cl.Attempt.EffectKey, "TEND_DEFER_PATH="+deferPath), nil
}

func scrubbedEnv() ([]string, error) {
	keep := []string{"PATH", "HOME", "LANG", "LC_ALL", "TMPDIR", "ASK", "PLY", "MAY", "CAGE"}
	if extra := os.Getenv("TEND_PASS"); extra != "" {
		for _, n := range strings.Fields(extra) {
			if !validEnvName(n) {
				return nil, fmt.Errorf("TEND_PASS: invalid variable %q", n)
			}
			keep = append(keep, n)
		}
	}
	seen := map[string]bool{}
	env := []string{}
	for _, n := range keep {
		if seen[n] {
			continue
		}
		seen[n] = true
		if v, ok := os.LookupEnv(n); ok {
			env = append(env, n+"="+v)
		}
	}
	return env, nil
}
func validEnvName(s string) bool {
	if s == "" || !((s[0] >= 'A' && s[0] <= 'Z') || (s[0] >= 'a' && s[0] <= 'z') || s[0] == '_') {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
			return false
		}
	}
	return true
}

func (a *app) finish(cl claim, status string, exit *int, termSignal int, outPath, stderrPath, note string) (workRecord, bool, error) {
	digest, size, err := sumFile(outPath)
	if err != nil {
		return workRecord{}, false, err
	}
	rel, err := filepath.Rel(a.root, outPath)
	if err != nil {
		return workRecord{}, false, err
	}
	stderrDigest, stderrSize, err := sumFile(stderrPath)
	if err != nil {
		return workRecord{}, false, err
	}
	stderrRel, err := filepath.Rel(a.root, stderrPath)
	if err != nil {
		return workRecord{}, false, err
	}
	now := nowUS()
	c, err := beginImmediate(context.Background(), a.store.db)
	if err != nil {
		return workRecord{}, false, err
	}
	defer rollback(c)
	var ast, jst, token string
	if err = c.QueryRowContext(context.Background(), `SELECT a.state,j.status,COALESCE(j.lease_token,'') FROM attempts a JOIN jobs j ON j.id=a.job_id WHERE a.id=? AND a.job_id=?`, cl.Attempt.ID, cl.Job.ID).Scan(&ast, &jst, &token); err != nil {
		return workRecord{}, false, err
	}
	if ast != "started" || jst != "running" || token != cl.Token {
		if ast == "unknown" {
			var adopted bool
			if jst == "unknown" {
				res, updateErr := c.ExecContext(context.Background(), `UPDATE attempts SET
output_path=?,output_digest=?,output_size=?,stderr_path=?,stderr_digest=?,stderr_size=?
WHERE id=? AND state='unknown' AND output_digest IS NULL`, rel, digest, size,
					stderrRel, stderrDigest, stderrSize, cl.Attempt.ID)
				if updateErr != nil {
					return workRecord{}, false, updateErr
				}
				if n, _ := res.RowsAffected(); n == 1 {
					adopted = true
				}
			}
			if adopted {
				seq, e := appendEvent(context.Background(), c, cl.Job.ID,
					"attempt.output-sealed", map[string]any{
						"attempt": cl.Attempt.ID, "late": true,
						"observed_status": status, "output_digest": digest,
						"output_size": size, "stderr_digest": stderrDigest,
						"stderr_size": stderrSize,
					}, now)
				if e != nil {
					return workRecord{}, false, e
				}
				for _, item := range []struct {
					name, digest, rel string
					size              int64
				}{{fmt.Sprintf("attempt-%d-output", cl.Attempt.Number), digest, rel, size},
					{fmt.Sprintf("attempt-%d-stderr", cl.Attempt.Number), stderrDigest, stderrRel, stderrSize}} {
					if _, e = c.ExecContext(context.Background(), `INSERT INTO artifacts
(job_id,event_seq,name,digest,size,relpath,created_us)VALUES(?,?,?,?,?,?,?)`,
						cl.Job.ID, seq, item.name, item.digest, item.size, item.rel, now); e != nil {
						return workRecord{}, false, e
					}
				}
				if e = commit(c); e != nil {
					return workRecord{}, false, e
				}
				return workRecord{Job: cl.Job.ID, Attempt: cl.Attempt.ID,
					Event: "attempt.output-sealed", Status: jst, Exit: exit,
					Output: rel, Error: stderrRel}, true, nil
			}
			var oldOut, oldErr string
			var oldOutSize, oldErrSize int64
			if e := c.QueryRowContext(context.Background(), `SELECT output_digest,
output_size,stderr_digest,stderr_size FROM attempts WHERE id=?`, cl.Attempt.ID).
				Scan(&oldOut, &oldOutSize, &oldErr, &oldErrSize); e != nil {
				return workRecord{}, false, e
			}
			if oldOut == digest && oldOutSize == size && oldErr == stderrDigest && oldErrSize == stderrSize {
				if _, e := appendEvent(context.Background(), c, cl.Job.ID,
					"attempt.late-result", map[string]any{"attempt": cl.Attempt.ID,
						"observed_status": status, "already_sealed": true}, now); e != nil {
					return workRecord{}, false, e
				}
				if e := commit(c); e != nil {
					return workRecord{}, false, e
				}
				return workRecord{Job: cl.Job.ID, Attempt: cl.Attempt.ID,
					Event: "attempt.late-result", Status: jst, Exit: exit,
					Output: rel, Error: stderrRel}, true, nil
			}
			return workRecord{}, false, errors.New("late result differs from sealed unknown evidence")
		}
		seq, e := appendEvent(context.Background(), c, cl.Job.ID, "attempt.late-result", map[string]any{"attempt": cl.Attempt.ID, "observed_status": status, "output_digest": digest, "output_size": size, "stderr_digest": stderrDigest, "stderr_size": stderrSize}, now)
		if e != nil {
			return workRecord{}, false, e
		}
		if _, e = c.ExecContext(context.Background(), `INSERT INTO artifacts(job_id,event_seq,name,digest,size,relpath,created_us)VALUES(?,?,?,?,?,?,?)`, cl.Job.ID, seq, "late-output", digest, size, rel, now); e != nil {
			return workRecord{}, false, e
		}
		if _, e = c.ExecContext(context.Background(), `INSERT INTO artifacts(job_id,event_seq,name,digest,size,relpath,created_us)VALUES(?,?,?,?,?,?,?)`, cl.Job.ID, seq, "late-stderr", stderrDigest, stderrSize, stderrRel, now); e != nil {
			return workRecord{}, false, e
		}
		if e = commit(c); e != nil {
			return workRecord{}, false, e
		}
		return workRecord{Job: cl.Job.ID, Attempt: cl.Attempt.ID, Event: "attempt.late-result", Status: jst, Exit: exit, Output: rel, Error: stderrRel}, true, nil
	}
	final, waitKind, waitKey := status, "", ""
	var wake sql.NullInt64
	var consumeSignal string
	if status == "waiting" {
		var kind string
		var key sql.NullString
		err = c.QueryRowContext(context.Background(), "SELECT kind,key,wake_at_us FROM wait_requests WHERE attempt_id=?", cl.Attempt.ID).Scan(&kind, &key, &wake)
		if errors.Is(err, sql.ErrNoRows) {
			kind = "manual"
			err = nil
		}
		if err != nil {
			return workRecord{}, false, err
		}
		waitKind, waitKey = kind, key.String
		if kind == "signal" {
			err = c.QueryRowContext(context.Background(), "SELECT id FROM signals WHERE job_id=? AND name=? AND consumed_us IS NULL ORDER BY received_us,id LIMIT 1", cl.Job.ID, waitKey).Scan(&consumeSignal)
		} else if kind == "manual" {
			err = c.QueryRowContext(context.Background(), "SELECT id FROM signals WHERE job_id=? AND consumed_us IS NULL ORDER BY received_us,id LIMIT 1", cl.Job.ID).Scan(&consumeSignal)
		} else if kind == "timer" && wake.Valid && wake.Int64 <= now {
			final, waitKind, waitKey, wake = "ready", "", "", sql.NullInt64{}
		}
		if consumeSignal != "" {
			final, waitKind, waitKey, wake = "ready", "", "", sql.NullInt64{}
		} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return workRecord{}, false, err
		}
	}
	var exitValue any
	if exit != nil {
		exitValue = *exit
	}
	attemptState := "finished"
	if final == "unknown" {
		attemptState = "unknown"
	}
	if _, err = c.ExecContext(context.Background(), `UPDATE attempts SET state=?,finished_us=?,outcome=?,exit_code=?,term_signal=?,output_path=?,output_digest=?,output_size=?,stderr_path=?,stderr_digest=?,stderr_size=? WHERE id=? AND state='started'`, attemptState, now, final, exitValue, nullableInt(termSignal), rel, digest, size, stderrRel, stderrDigest, stderrSize, cl.Attempt.ID); err != nil {
		return workRecord{}, false, err
	}
	res, err := c.ExecContext(context.Background(), `UPDATE jobs SET status=?,wait_kind=?,wait_key=?,wake_at_us=?,lease_owner=NULL,lease_token=NULL,lease_expires_us=NULL,cancel_requested=0,updated_us=? WHERE id=? AND lease_token=?`, final, nullableString(waitKind), nullableString(waitKey), nullableNullInt(wake), now, cl.Job.ID, cl.Token)
	if err != nil {
		return workRecord{}, false, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return workRecord{}, false, errors.New("completion lost its lease")
	}
	var consumedSignals []string
	if consumeSignal != "" {
		consumedSignals = append(consumedSignals, consumeSignal)
	}
	if final == "done" || final == "failed" || final == "cancelled" {
		rows, queryErr := c.QueryContext(context.Background(), "SELECT id FROM signals WHERE job_id=? AND consumed_us IS NULL ORDER BY received_us,id", cl.Job.ID)
		if queryErr != nil {
			return workRecord{}, false, queryErr
		}
		for rows.Next() {
			var id string
			if queryErr = rows.Scan(&id); queryErr != nil {
				_ = rows.Close()
				return workRecord{}, false, queryErr
			}
			if id != consumeSignal {
				consumedSignals = append(consumedSignals, id)
			}
		}
		_ = rows.Close()
	}
	seq, err := appendEvent(context.Background(), c, cl.Job.ID, "attempt.finished", map[string]any{"attempt": cl.Attempt.ID, "status": final, "exit": exitValue, "signal": termSignal, "note": note, "output_digest": digest, "output_size": size, "stderr_digest": stderrDigest, "stderr_size": stderrSize, "wait_kind": nullableString(waitKind), "wait_key": nullableString(waitKey), "wake_at_us": nullableNullInt(wake), "consumed_signals": consumedSignals}, now)
	if err != nil {
		return workRecord{}, false, err
	}
	if _, err = c.ExecContext(context.Background(), `INSERT INTO artifacts(job_id,event_seq,name,digest,size,relpath,created_us)VALUES(?,?,?,?,?,?,?)`, cl.Job.ID, seq, fmt.Sprintf("attempt-%d-output", cl.Attempt.Number), digest, size, rel, now); err != nil {
		return workRecord{}, false, err
	}
	if _, err = c.ExecContext(context.Background(), `INSERT INTO artifacts(job_id,event_seq,name,digest,size,relpath,created_us)VALUES(?,?,?,?,?,?,?)`, cl.Job.ID, seq, fmt.Sprintf("attempt-%d-stderr", cl.Attempt.Number), stderrDigest, stderrSize, stderrRel, now); err != nil {
		return workRecord{}, false, err
	}
	if consumeSignal != "" {
		if _, err = c.ExecContext(context.Background(), "UPDATE signals SET consumed_us=?,consumed_seq=? WHERE id=?", now, seq, consumeSignal); err != nil {
			return workRecord{}, false, err
		}
	}
	if final == "done" || final == "failed" || final == "cancelled" {
		_, _ = c.ExecContext(context.Background(), "UPDATE signals SET consumed_us=?,consumed_seq=? WHERE job_id=? AND consumed_us IS NULL", now, seq, cl.Job.ID)
	}
	if err = commit(c); err != nil {
		return workRecord{}, false, err
	}
	return workRecord{Job: cl.Job.ID, Attempt: cl.Attempt.ID, Event: "attempt.finished", Status: final, Exit: exit, Output: rel, Error: stderrRel}, false, nil
}
func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func nullableInt(n int) any {
	if n == 0 {
		return nil
	}
	return n
}
func nullableInt64(n int64) any {
	if n == 0 {
		return nil
	}
	return n
}
func nullableNullInt(n sql.NullInt64) any {
	if !n.Valid {
		return nil
	}
	return n.Int64
}
