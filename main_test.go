package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

var testBinary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "tend-test-bin-")
	if err != nil {
		panic(err)
	}
	testBinary = filepath.Join(dir, "tend")
	cmd := exec.Command("go", "build", "-o", testBinary, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		panic(fmt.Sprintf("build test binary: %v\n%s", err, out))
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type result struct {
	code     int
	out, err string
}

func runTend(t *testing.T, root string, input *string, extraEnv []string, args ...string) result {
	t.Helper()
	cmd := exec.Command(testBinary, args...)
	cmd.Env = append(os.Environ(), "TEND_ROOT="+root)
	cmd.Env = append(cmd.Env, extraEnv...)
	if input != nil {
		cmd.Stdin = strings.NewReader(*input)
	}
	var out, errout bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errout
	err := cmd.Run()
	code := 0
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			code = ee.ExitCode()
		} else {
			t.Fatalf("run tend: %v", err)
		}
	}
	return result{code: code, out: out.String(), err: errout.String()}
}

func mustRun(t *testing.T, root string, input *string, args ...string) result {
	t.Helper()
	r := runTend(t, root, input, nil, args...)
	if r.code != 0 {
		t.Fatalf("tend %v exited %d\nstdout: %s\nstderr: %s", args, r.code, r.out, r.err)
	}
	return r
}

func jobStatus(t *testing.T, root, id string) string {
	t.Helper()
	r := mustRun(t, root, nil, "show", id)
	var v jobView
	if err := json.Unmarshal([]byte(r.out), &v); err != nil {
		t.Fatal(err)
	}
	return v.Status
}

func waitForPath(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSubmitWorkEventsAndCheck(t *testing.T) {
	root := t.TempDir()
	work := t.TempDir()
	input := "hello\n"
	mustRun(t, root, &input, "submit", "-id", "basic", "-C", work, "--", "/bin/sh", "-c", "cat; printf err >&2")
	r := mustRun(t, root, nil, "work")
	if !strings.Contains(r.out, `"status":"done"`) {
		t.Fatalf("work: %s", r.out)
	}
	if got := jobStatus(t, root, "basic"); got != "done" {
		t.Fatalf("status %s", got)
	}
	out, err := os.ReadFile(filepath.Join(root, "jobs", "basic", "attempts", "001.out"))
	if err != nil || string(out) != "hello\n" {
		t.Fatalf("artifact %q, %v", out, err)
	}
	errBytes, err := os.ReadFile(filepath.Join(root, "jobs", "basic", "attempts", "001.err"))
	if err != nil || string(errBytes) != "err" {
		t.Fatalf("stderr artifact %q, %v", errBytes, err)
	}
	events := mustRun(t, root, nil, "events", "basic").out
	for _, kind := range []string{"job.submitted", "attempt.prepared", "attempt.started", "attempt.finished"} {
		if !strings.Contains(events, kind) {
			t.Fatalf("missing %s in %s", kind, events)
		}
	}
	if r := mustRun(t, root, nil, "check"); strings.TrimSpace(r.out) != "ok" {
		t.Fatalf("check: %s", r.out)
	}
}

func TestSubmitGeneratesValidID(t *testing.T) {
	root := t.TempDir()
	r := mustRun(t, root, nil, "submit", "--", "true")
	id := strings.TrimSpace(r.out)
	if !cleanID(id) || !strings.HasPrefix(id, "job-") {
		t.Fatalf("generated invalid job id %q", id)
	}
	mustRun(t, root, nil, "work")
	if got := jobStatus(t, root, id); got != "done" {
		t.Fatalf("status %s", got)
	}
}

func TestStatePathMayContainURLCharacters(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state ? hash#")
	mustRun(t, root, nil, "submit", "-id", "oddpath", "--", "true")
	mustRun(t, root, nil, "work")
	if got := jobStatus(t, root, "oddpath"); got != "done" {
		t.Fatalf("status %s", got)
	}
}

func TestConcurrentFreshRootInitialization(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fresh")
	const n = 24
	var wg sync.WaitGroup
	codes := make(chan int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes <- runTend(t, root, nil, nil, "list").code
		}()
	}
	wg.Wait()
	close(codes)
	for code := range codes {
		if code != 0 {
			t.Fatalf("fresh concurrent open exited %d", code)
		}
	}
}

func TestConcurrentIdenticalSubmitConverges(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fresh")
	input := "identical"
	var wg sync.WaitGroup
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- runTend(t, root, &input, nil, "submit", "-id", "converge", "--", "cat")
		}()
	}
	wg.Wait()
	close(results)
	for r := range results {
		if r.code != 0 {
			t.Fatalf("submit exited %d: %s", r.code, r.err)
		}
	}
	mustRun(t, root, nil, "check")
}

func TestFilesystemInstallerAndDatabaseAdopterConverge(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fresh")
	barriers := t.TempDir()
	ready := filepath.Join(barriers, "ready")
	release := filepath.Join(barriers, "release")
	input := "identical"

	installer := exec.Command(testBinary, "submit", "-id", "converge-barrier", "--", "cat")
	installer.Env = append(os.Environ(),
		"TEND_ROOT="+root,
		"_TEND_TEST_AFTER_FILES_READY="+ready,
		"_TEND_TEST_AFTER_FILES_RELEASE="+release,
	)
	installer.Stdin = strings.NewReader(input)
	var installerOut, installerErr bytes.Buffer
	installer.Stdout, installer.Stderr = &installerOut, &installerErr
	if err := installer.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.WriteFile(release, []byte("release\n"), 0o600)
		if installer.ProcessState == nil {
			_ = installer.Process.Kill()
			_ = installer.Wait()
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("installer did not reach after-files barrier")
		}
		time.Sleep(5 * time.Millisecond)
	}

	adopter := runTend(t, root, &input, nil, "submit", "-id", "converge-barrier", "--", "cat")
	if adopter.code != 0 {
		t.Fatalf("adopter exited %d: %s", adopter.code, adopter.err)
	}
	if err := os.WriteFile(release, []byte("release\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := installer.Wait(); err != nil {
		t.Fatalf("installer: %v\nstdout: %s\nstderr: %s", err, installerOut.String(), installerErr.String())
	}
	mustRun(t, root, nil, "check")
}

func TestSubmitResolvesRelativeProgramAgainstCWD(t *testing.T) {
	root, work := t.TempDir(), t.TempDir()
	program := filepath.Join(work, "worker")
	if err := os.WriteFile(program, []byte("#!/bin/sh\nprintf relative\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	mustRun(t, root, nil, "submit", "-id", "relative", "-C", work, "--", "./worker")
	mustRun(t, root, nil, "work")
	b, err := os.ReadFile(filepath.Join(root, "jobs", "relative", "attempts", "001.out"))
	if err != nil || string(b) != "relative" {
		t.Fatalf("output %q, %v", b, err)
	}
}

func TestStableIDBindsPipedInput(t *testing.T) {
	root := t.TempDir()
	one, two := "one", "two"
	mustRun(t, root, &one, "submit", "-id", "same", "--", "cat")
	mustRun(t, root, &one, "submit", "-id", "same", "--", "cat")
	r := runTend(t, root, &two, nil, "submit", "-id", "same", "--", "cat")
	if r.code != 2 || !strings.Contains(r.err, "different bytes") {
		t.Fatalf("different input was accepted: %d %s %s", r.code, r.out, r.err)
	}
}

func TestModifiedSubmittedInputExecutesNothing(t *testing.T) {
	root, dir := t.TempDir(), t.TempDir()
	effect := filepath.Join(dir, "effect")
	input := "safe"
	mustRun(t, root, &input, "submit", "-id", "input-tamper", "--", "/bin/sh", "-c", "cat >'"+effect+"'")
	if err := os.WriteFile(filepath.Join(root, "jobs", "input-tamper", "input"), []byte("evil"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := runTend(t, root, nil, nil, "work")
	if r.code != 2 || !strings.Contains(r.err, "digest mismatch") {
		t.Fatalf("work %d: %s %s", r.code, r.out, r.err)
	}
	if _, err := os.Stat(effect); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tampered input executed: %v", err)
	}
	if got := jobStatus(t, root, "input-tamper"); got != "failed" {
		t.Fatalf("status %s", got)
	}
}

func TestUnexpectedInputExecutesNothing(t *testing.T) {
	root, dir := t.TempDir(), t.TempDir()
	effect := filepath.Join(dir, "effect")
	mustRun(t, root, nil, "submit", "-id", "unexpected-input", "--", "/bin/sh", "-c", "cat >'"+effect+"'")
	if err := os.WriteFile(filepath.Join(root, "jobs", "unexpected-input", "input"), []byte("added"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := runTend(t, root, nil, nil, "work")
	if r.code != 2 || !strings.Contains(r.err, "unregistered input") {
		t.Fatalf("work %d: %s %s", r.code, r.out, r.err)
	}
	if _, err := os.Stat(effect); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected input executed: %v", err)
	}
}

func TestCrashAfterFilesIsAdoptedByRetry(t *testing.T) {
	root := t.TempDir()
	input := "same bytes"
	r := runTend(t, root, &input, []string{"_TEND_TEST_CRASH=after-files"}, "submit", "-id", "adopt", "--", "cat")
	if r.code != 90 {
		t.Fatalf("crash exit %d: %s", r.code, r.err)
	}
	mustRun(t, root, &input, "submit", "-id", "adopt", "--", "cat")
	mustRun(t, root, nil, "work")
	if got := jobStatus(t, root, "adopt"); got != "done" {
		t.Fatalf("status %s", got)
	}
}

func TestPreparedCrashIsSafeToRequeue(t *testing.T) {
	root := t.TempDir()
	effect := filepath.Join(t.TempDir(), "effect")
	mustRun(t, root, nil, "submit", "-id", "prepared", "--", "/bin/sh", "-c", "echo x >> '"+effect+"'")
	r := runTend(t, root, nil, []string{"TEND_LEASE=300ms", "_TEND_TEST_CRASH=after-prepare"}, "work")
	if r.code != 91 {
		t.Fatalf("crash code %d: %s", r.code, r.err)
	}
	time.Sleep(400 * time.Millisecond)
	r = mustRun(t, root, nil, "work")
	if !strings.Contains(r.out, "attempt.abandoned") {
		t.Fatalf("recovery: %s", r.out)
	}
	if _, err := os.Stat(effect); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("prepared attempt executed: %v", err)
	}
	mustRun(t, root, nil, "work")
	b, _ := os.ReadFile(effect)
	if string(b) != "x\n" {
		t.Fatalf("effect %q", b)
	}
}

func TestStartedCrashBecomesUnknownAndDoesNotRepeat(t *testing.T) {
	root := t.TempDir()
	dir := t.TempDir()
	effect := filepath.Join(dir, "effect")
	mustRun(t, root, nil, "submit", "-id", "opaque", "-key", "one", "--", "/bin/sh", "-c", "echo x >> '"+effect+"'; sleep 1")
	cmd := exec.Command(testBinary, "work")
	cmd.Env = append(os.Environ(), "TEND_ROOT="+root, "TEND_LEASE=400ms")
	var out, errout bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(effect); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("effect never appeared: %s", errout.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	time.Sleep(1200 * time.Millisecond)
	r := mustRun(t, root, nil, "work")
	if !strings.Contains(r.out, "attempt.effect-unknown") {
		t.Fatalf("recovery: %s", r.out)
	}
	if got := jobStatus(t, root, "opaque"); got != "unknown" {
		t.Fatalf("status %s", got)
	}
	r = runTend(t, root, nil, nil, "work")
	if r.code != 1 {
		t.Fatalf("blocked key work exit %d: %s %s", r.code, r.out, r.err)
	}
	b, _ := os.ReadFile(effect)
	if string(b) != "x\n" {
		t.Fatalf("effect repeated: %q", b)
	}
	mustRun(t, root, nil, "resolve", "opaque", "fail")
	if got := jobStatus(t, root, "opaque"); got != "failed" {
		t.Fatalf("resolved status %s", got)
	}
	mustRun(t, root, nil, "check")
}

func TestControllerCrashKillsLaunchedProcessGroup(t *testing.T) {
	root, dir := t.TempDir(), t.TempDir()
	late := filepath.Join(dir, "late")
	mustRun(t, root, nil, "submit", "-id", "watchdog", "-key", "watchdog-key",
		"--", "/bin/sh", "-c", "trap '' TERM; sleep .7; touch '"+late+"'")
	cmd := exec.Command(testBinary, "work")
	cmd.Env = append(os.Environ(), "TEND_ROOT="+root, "TEND_LEASE=300ms")
	var errout bytes.Buffer
	cmd.Stderr = &errout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for !strings.Contains(mustRun(t, root, nil, "events", "watchdog").out, "attempt.started") {
		if time.Now().After(deadline) {
			t.Fatalf("attempt did not start: %s", errout.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	time.Sleep(800 * time.Millisecond)
	if _, err := os.Stat(late); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target survived controller death: %v", err)
	}
	mustRun(t, root, nil, "work")
	mustRun(t, root, nil, "resolve", "watchdog", "fail")
	mustRun(t, root, nil, "submit", "-id", "successor", "-key", "watchdog-key", "--", "true")
	mustRun(t, root, nil, "work")
	mustRun(t, root, nil, "check")
}

func TestIsolatedLauncherDeathCannotReleaseSerialization(t *testing.T) {
	root, dir := t.TempDir(), t.TempDir()
	late := filepath.Join(dir, "late")
	mustRun(t, root, nil, "submit", "-id", "launcher-death", "-key", "launcher-key",
		"--", "/bin/sh", "-c", "sleep .7; touch '"+late+"'")
	worker := exec.Command(testBinary, "work")
	worker.Env = append(os.Environ(), "TEND_ROOT="+root)
	var out, errout bytes.Buffer
	worker.Stdout, worker.Stderr = &out, &errout
	if err := worker.Start(); err != nil {
		t.Fatal(err)
	}
	var launcher int
	deadline := time.Now().Add(3 * time.Second)
	for launcher == 0 {
		db, err := sql.Open("sqlite", "file:"+filepath.Join(root, "state", "tend.db")+"?_pragma=busy_timeout(5000)")
		if err == nil {
			_ = db.QueryRow(`SELECT COALESCE(process_pid,0) FROM attempts
WHERE job_id='launcher-death' AND state='started'`).Scan(&launcher)
			_ = db.Close()
		}
		if time.Now().After(deadline) {
			t.Fatal("launcher pid was not recorded")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := syscall.Kill(launcher, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := worker.Wait(); err != nil {
		t.Fatalf("worker: %v\nstdout: %s\nstderr: %s", err, out.String(), errout.String())
	}
	time.Sleep(800 * time.Millisecond)
	if _, err := os.Stat(late); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target survived isolated launcher death: %v", err)
	}
	if got := jobStatus(t, root, "launcher-death"); got != "unknown" {
		t.Fatalf("status %s", got)
	}
	mustRun(t, root, nil, "resolve", "launcher-death", "fail")
	mustRun(t, root, nil, "submit", "-id", "launcher-successor", "-key", "launcher-key", "--", "true")
	mustRun(t, root, nil, "work")
}

func TestLateControllerEvidenceConvergesWithUnknown(t *testing.T) {
	root := t.TempDir()
	mustRun(t, root, nil, "submit", "-id", "late-evidence", "--", "/bin/sh", "-c",
		"sleep 1; printf late")
	cmd := exec.Command(testBinary, "work")
	cmd.Env = append(os.Environ(), "TEND_ROOT="+root, "TEND_LEASE=300ms")
	var out, errout bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = syscall.Kill(cmd.Process.Pid, syscall.SIGCONT)
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	deadline := time.Now().Add(3 * time.Second)
	for !strings.Contains(mustRun(t, root, nil, "events", "late-evidence").out, "attempt.started") {
		if time.Now().After(deadline) {
			t.Fatal("attempt did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := syscall.Kill(cmd.Process.Pid, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	r := mustRun(t, root, nil, "work")
	if !strings.Contains(r.out, "attempt.effect-unknown") {
		t.Fatalf("recovery: %s", r.out)
	}
	// Resolve after the private launcher exits, but before the stopped original
	// controller can submit its late result.
	resolveDeadline := time.Now().Add(4 * time.Second)
	for {
		r = runTend(t, root, nil, nil, "resolve", "late-evidence", "fail")
		if r.code == 0 {
			break
		}
		if r.code != 1 || !strings.Contains(r.err, "launcher is still live") || time.Now().After(resolveDeadline) {
			t.Fatalf("resolve %d: %s %s", r.code, r.out, r.err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := syscall.Kill(cmd.Process.Pid, syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("original worker: %v\nstdout: %s\nstderr: %s", err, out.String(), errout.String())
	}
	mustRun(t, root, nil, "check")
}

func TestPreparedCancellationExecutesNothing(t *testing.T) {
	root, dir, barriers := t.TempDir(), t.TempDir(), t.TempDir()
	effect := filepath.Join(dir, "effect")
	ready := filepath.Join(barriers, "ready")
	release := filepath.Join(barriers, "release")
	mustRun(t, root, nil, "submit", "-id", "cancel-prepared", "--", "touch", effect)
	cmd := exec.Command(testBinary, "work")
	cmd.Env = append(os.Environ(), "TEND_ROOT="+root,
		"_TEND_TEST_BEFORE_START_READY="+ready,
		"_TEND_TEST_BEFORE_START_RELEASE="+release)
	var out, errout bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.WriteFile(release, []byte("release\n"), 0o600)
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	waitForPath(t, ready, 5*time.Second)
	mustRun(t, root, nil, "cancel", "cancel-prepared")
	if err := os.WriteFile(release, []byte("release\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("worker: %v\nstdout: %s\nstderr: %s", err, out.String(), errout.String())
	}
	if _, err := os.Stat(effect); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("prepared cancellation executed: %v", err)
	}
	if got := jobStatus(t, root, "cancel-prepared"); got != "cancelled" {
		t.Fatalf("status %s", got)
	}
	mustRun(t, root, nil, "check")
}

func TestCancellationWinsDuringPreparedRecovery(t *testing.T) {
	root, barriers := t.TempDir(), t.TempDir()
	ready := filepath.Join(barriers, "ready")
	release := filepath.Join(barriers, "release")
	mustRun(t, root, nil, "submit", "-id", "cancel-recovery", "--", "true")
	r := runTend(t, root, nil, []string{"TEND_LEASE=300ms", "_TEND_TEST_CRASH=after-prepare"}, "work")
	if r.code != 91 {
		t.Fatalf("crash %d: %s", r.code, r.err)
	}
	time.Sleep(400 * time.Millisecond)
	cmd := exec.Command(testBinary, "work")
	cmd.Env = append(os.Environ(), "TEND_ROOT="+root,
		"_TEND_TEST_RECOVER_PREPARED_READY="+ready,
		"_TEND_TEST_RECOVER_PREPARED_RELEASE="+release)
	var out, errout bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.WriteFile(release, []byte("release\n"), 0o600)
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	waitForPath(t, ready, 5*time.Second)
	mustRun(t, root, nil, "cancel", "cancel-recovery")
	if err := os.WriteFile(release, []byte("release\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("recovery: %v\nstdout: %s\nstderr: %s", err, out.String(), errout.String())
	}
	if !strings.Contains(out.String(), "attempt.cancelled") {
		t.Fatalf("recovery did not consume cancellation: %s", out.String())
	}
	if got := jobStatus(t, root, "cancel-recovery"); got != "cancelled" {
		t.Fatalf("status %s", got)
	}
}

func TestJobTimeoutKillsDelayedEffect(t *testing.T) {
	root, dir := t.TempDir(), t.TempDir()
	late := filepath.Join(dir, "late")
	mustRun(t, root, nil, "submit", "-id", "timeout", "--", "/bin/sh", "-c",
		"sleep .7; touch '"+late+"'")
	r := runTend(t, root, nil, []string{"TEND_JOB_MAX=150ms"}, "work")
	if r.code != 0 || !strings.Contains(r.out, `"status":"unknown"`) {
		t.Fatalf("work %d: %s %s", r.code, r.out, r.err)
	}
	time.Sleep(800 * time.Millisecond)
	if _, err := os.Stat(late); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target survived timeout: %v", err)
	}
}

func TestSameKeySerializesConcurrentWorkers(t *testing.T) {
	root := t.TempDir()
	dir := t.TempDir()
	effect := filepath.Join(dir, "effect")
	for _, id := range []string{"one", "two"} {
		mustRun(t, root, nil, "submit", "-id", id, "-key", "serial", "--", "/bin/sh", "-c", "sleep .3; echo "+id+" >> '"+effect+"'")
	}
	first := exec.Command(testBinary, "work")
	first.Env = append(os.Environ(), "TEND_ROOT="+root)
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for jobStatus(t, root, "one") != "running" {
		if time.Now().After(deadline) {
			t.Fatal("first never ran")
		}
		time.Sleep(10 * time.Millisecond)
	}
	r := runTend(t, root, nil, nil, "work")
	if r.code != 1 {
		t.Fatalf("second worker exit %d: %s %s", r.code, r.out, r.err)
	}
	if err := first.Wait(); err != nil {
		t.Fatal(err)
	}
	mustRun(t, root, nil, "work")
	b, _ := os.ReadFile(effect)
	lines := strings.Fields(string(b))
	if strings.Join(lines, " ") != "one two" {
		t.Fatalf("effects %q", b)
	}
}

func TestDifferentKeysCanRunConcurrently(t *testing.T) {
	root := t.TempDir()
	dir := t.TempDir()
	for _, pair := range [][2]string{{"a", "ka"}, {"b", "kb"}} {
		mustRun(t, root, nil, "submit", "-id", pair[0], "-key", pair[1], "--", "/bin/sh", "-c", "sleep .2")
	}
	var wg sync.WaitGroup
	codes := make(chan int, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); codes <- runTend(t, root, nil, nil, "work").code }()
	}
	wg.Wait()
	close(codes)
	for c := range codes {
		if c != 0 {
			t.Fatalf("work exit %d", c)
		}
	}
	if jobStatus(t, root, "a") != "done" || jobStatus(t, root, "b") != "done" {
		t.Fatal("jobs did not both finish")
	}
	_ = dir
}

func TestBackgroundDescendantCannotMutateSealedOutput(t *testing.T) {
	root, dir := t.TempDir(), t.TempDir()
	effect := filepath.Join(dir, "late-effect")
	script := `(sleep .3; echo late >"` + effect + `"; echo late-output) & echo leader`
	mustRun(t, root, nil, "submit", "-id", "background", "--", "/bin/sh", "-c", script)
	r := mustRun(t, root, nil, "work")
	if !strings.Contains(r.out, `"status":"unknown"`) {
		t.Fatalf("background attempt was trusted: %s", r.out)
	}
	time.Sleep(400 * time.Millisecond)
	if _, err := os.Stat(effect); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("background effect survived: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(root, "jobs", "background", "attempts", "001.out"))
	if err != nil || string(b) != "leader\n" {
		t.Fatalf("sealed output changed: %q, %v", b, err)
	}
	mustRun(t, root, nil, "check")
}

func TestSignalBeforeWaitIsNotLost(t *testing.T) {
	root := t.TempDir()
	dir := t.TempDir()
	marker := filepath.Join(dir, "marker")
	script := `if test -f "` + marker + `"; then exit 0; fi; : >"` + marker + `"; "$TEND" defer signal go; exit 75`
	mustRun(t, root, nil, "submit", "-id", "sig", "--", "/bin/sh", "-c", script)
	mustRun(t, root, nil, "signal", "-id", "signal-one", "sig", "go", "yes")
	r := mustRun(t, root, nil, "work")
	if !strings.Contains(r.out, `"status":"ready"`) {
		t.Fatalf("lost early signal: %s", r.out)
	}
	mustRun(t, root, nil, "work")
	if jobStatus(t, root, "sig") != "done" {
		t.Fatal("signal job not done")
	}
}

func TestSignalAfterWaitWakes(t *testing.T) {
	root := t.TempDir()
	dir := t.TempDir()
	marker := filepath.Join(dir, "marker")
	script := `if test -f "` + marker + `"; then exit 0; fi; : >"` + marker + `"; "$TEND" defer signal go; exit 75`
	mustRun(t, root, nil, "submit", "-id", "late", "--", "/bin/sh", "-c", script)
	mustRun(t, root, nil, "work")
	if jobStatus(t, root, "late") != "waiting" {
		t.Fatal("not waiting")
	}
	r := mustRun(t, root, nil, "signal", "late", "go")
	if !strings.Contains(r.out, `"woke":true`) {
		t.Fatalf("did not wake: %s", r.out)
	}
	var signalResponse map[string]any
	if err := json.Unmarshal([]byte(r.out), &signalResponse); err != nil {
		t.Fatal(err)
	}
	sid, _ := signalResponse["id"].(string)
	duplicate := mustRun(t, root, nil, "signal", "-id", sid, "late", "go")
	if !strings.Contains(duplicate.out, `"duplicate":true`) || !strings.Contains(duplicate.out, `"woke":true`) {
		t.Fatalf("duplicate response changed: %s", duplicate.out)
	}
	mustRun(t, root, nil, "work")
	if jobStatus(t, root, "late") != "done" {
		t.Fatal("not done")
	}
}

func TestExampleWaitForUserInput(t *testing.T) {
	root, work := t.TempDir(), t.TempDir()
	source, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	worker := filepath.Join(source, "examples", "wait-for-input", "worker")
	answer := filepath.Join(source, "examples", "wait-for-input", "answer")
	mustRun(t, root, nil, "submit", "-id", "user-input-example", "-C", work,
		"--", worker)
	mustRun(t, root, nil, "work")
	if got := jobStatus(t, root, "user-input-example"); got != "waiting" {
		t.Fatalf("status %s", got)
	}
	cmd := exec.Command(answer, work, "user-input-example", testBinary)
	cmd.Env = append(os.Environ(), "TEND_ROOT="+root)
	cmd.Stdin = strings.NewReader("please continue\n")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("answer: %v\n%s", err, output)
	}
	if got := jobStatus(t, root, "user-input-example"); got != "ready" {
		t.Fatalf("status after answer %s", got)
	}
	mustRun(t, root, nil, "work")
	if got := jobStatus(t, root, "user-input-example"); got != "done" {
		t.Fatalf("final status %s", got)
	}
	out, err := os.ReadFile(filepath.Join(root, "jobs", "user-input-example",
		"attempts", "002.out"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "observed user input:\nplease continue\n") ||
		!strings.Contains(string(out), "continuing after user input") {
		t.Fatalf("resumed output:\n%s", out)
	}
	observed, err := os.ReadFile(filepath.Join(work, ".tend-wait",
		"user-input-example.observed"))
	if err != nil || string(observed) != "please continue\n" {
		t.Fatalf("observed response %q, %v", observed, err)
	}
	mustRun(t, root, nil, "check")
}

func TestExampleMayApprovalComposition(t *testing.T) {
	root, work := t.TempDir(), t.TempDir()
	source, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	worker := filepath.Join(source, "examples", "may-approval", "worker")
	fakeMay := filepath.Join(work, "fake-may")
	fake := `#!/bin/sh
set -eu
printf '%s\n' "$1" >.may-seen-job
cat >.may-seen-action
if test -f .may-grant; then
    rm .may-grant
    exit 0
fi
exit 75
`
	if err := os.WriteFile(fakeMay, []byte(fake), 0o700); err != nil {
		t.Fatal(err)
	}
	action := "publish release v1.4.2\n"
	if err := os.WriteFile(filepath.Join(work, "may-action"), []byte(action), 0o600); err != nil {
		t.Fatal(err)
	}
	mustRun(t, root, nil, "submit", "-id", "may-example", "-C", work, "--", worker)
	r := runTend(t, root, nil, []string{"MAY=" + fakeMay}, "work")
	if r.code != 0 {
		t.Fatalf("first work %d: %s %s", r.code, r.out, r.err)
	}
	if got := jobStatus(t, root, "may-example"); got != "waiting" {
		t.Fatalf("status %s", got)
	}
	seen, err := os.ReadFile(filepath.Join(work, ".may-seen-action"))
	if err != nil || string(seen) != action {
		t.Fatalf("May saw action %q, %v", seen, err)
	}
	if err := os.WriteFile(filepath.Join(work, ".may-grant"), []byte("approved\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mustRun(t, root, nil, "signal", "may-example", "may-decision")
	r = runTend(t, root, nil, []string{"MAY=" + fakeMay}, "work")
	if r.code != 0 || !strings.Contains(r.out, `"status":"done"`) {
		t.Fatalf("resumed work %d: %s %s", r.code, r.out, r.err)
	}
	completed, err := os.ReadFile(filepath.Join(work, ".tend-may", "may-example.completed"))
	if err != nil || string(completed) != action {
		t.Fatalf("completed action %q, %v", completed, err)
	}
	if _, err := os.Stat(filepath.Join(work, ".may-grant")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("grant was not spent: %v", err)
	}
	mustRun(t, root, nil, "check")
}

func TestTimerFiresAsItsOwnTransition(t *testing.T) {
	root := t.TempDir()
	dir := t.TempDir()
	marker := filepath.Join(dir, "marker")
	script := `if test -f "` + marker + `"; then exit 0; fi; : >"` + marker + `"; "$TEND" defer until 100ms; exit 75`
	mustRun(t, root, nil, "submit", "-id", "timer", "--", "/bin/sh", "-c", script)
	mustRun(t, root, nil, "work")
	if jobStatus(t, root, "timer") != "waiting" {
		t.Fatal("not waiting")
	}
	time.Sleep(150 * time.Millisecond)
	r := mustRun(t, root, nil, "work")
	if !strings.Contains(r.out, "timer.fired") {
		t.Fatalf("timer transition: %s", r.out)
	}
	mustRun(t, root, nil, "work")
	if jobStatus(t, root, "timer") != "done" {
		t.Fatal("not done")
	}
}

func TestNotBeforePreventsEarlyClaim(t *testing.T) {
	root := t.TempDir()
	at := time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	mustRun(t, root, nil, "submit", "-id", "later", "-at", at, "--", "true")
	if r := runTend(t, root, nil, nil, "work"); r.code != 1 {
		t.Fatalf("early work exit %d: %s %s", r.code, r.out, r.err)
	}
	// Advance the stored eligibility point instead of depending on wall-clock
	// sleeps, which makes the early assertion stable under race-enabled CI.
	db, err := sql.Open("sqlite", "file:"+filepath.Join(root, "state", "tend.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("UPDATE jobs SET not_before_us=? WHERE id='later'", nowUS()); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	_ = db.Close()
	mustRun(t, root, nil, "work")
	if got := jobStatus(t, root, "later"); got != "done" {
		t.Fatalf("status %s", got)
	}
}

func TestResolveDoneRequiresPassingCheck(t *testing.T) {
	root := t.TempDir()
	dir := t.TempDir()
	proof := filepath.Join(dir, "proof")
	mustRun(t, root, nil, "submit", "-id", "resolve", "-C", dir, "-check", "test -f proof", "--", "/bin/sh", "-c", "touch proof; exit 125")
	mustRun(t, root, nil, "work")
	if jobStatus(t, root, "resolve") != "unknown" {
		t.Fatal("not unknown")
	}
	mustRun(t, root, nil, "resolve", "resolve", "done")
	if jobStatus(t, root, "resolve") != "done" {
		t.Fatal("not done")
	}
	if _, err := os.Stat(proof); err != nil {
		t.Fatal(err)
	}
	mustRun(t, root, nil, "check")
}

func TestResolveCheckRejectsAndBrokenIsDistinct(t *testing.T) {
	for _, tc := range []struct {
		id, check string
		want      int
		event     string
	}{{"reject", "exit 1", 1, "resolution.check-rejected"}, {"broken", "exit 2", 2, "resolution.check-broken"}} {
		root := t.TempDir()
		mustRun(t, root, nil, "submit", "-id", tc.id, "-check", tc.check, "--", "/bin/sh", "-c", "exit 125")
		mustRun(t, root, nil, "work")
		r := runTend(t, root, nil, nil, "resolve", tc.id, "done")
		if r.code != tc.want {
			t.Fatalf("%s exit %d: %s", tc.id, r.code, r.err)
		}
		if jobStatus(t, root, tc.id) != "unknown" {
			t.Fatalf("%s left unknown", tc.id)
		}
		if events := mustRun(t, root, nil, "events", tc.id).out; !strings.Contains(events, tc.event) {
			t.Fatalf("missing %s: %s", tc.event, events)
		}
		mustRun(t, root, nil, "check")
	}
}

func TestResolutionCheckTimeoutKillsGroup(t *testing.T) {
	root, dir := t.TempDir(), t.TempDir()
	late := filepath.Join(dir, "late")
	check := `(sleep 1; touch "` + late + `") & wait`
	mustRun(t, root, nil, "submit", "-id", "check-timeout", "-check", check, "--", "/bin/sh", "-c", "exit 125")
	mustRun(t, root, nil, "work")
	r := runTend(t, root, nil, []string{"TEND_CHECK_MAX=300ms"}, "resolve", "check-timeout", "done")
	if r.code != 2 || !strings.Contains(r.err, "broken") {
		t.Fatalf("resolve %d: %s", r.code, r.err)
	}
	time.Sleep(1100 * time.Millisecond)
	if _, err := os.Stat(late); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("check descendant survived: %v", err)
	}
	if jobStatus(t, root, "check-timeout") != "unknown" {
		t.Fatal("broken check changed status")
	}
}

func TestResolutionCheckUsesScrubbedEnvironment(t *testing.T) {
	root := t.TempDir()
	mustRun(t, root, nil, "submit", "-id", "check-env", "-check", "env", "--",
		"/bin/sh", "-c", "exit 125")
	r := runTend(t, root, nil, []string{"TEND_ROOT=" + root, "PRIVATE_CHECK_SECRET=hidden"}, "work")
	if r.code != 0 {
		t.Fatalf("work %d: %s", r.code, r.err)
	}
	r = runTend(t, root, nil, []string{"PRIVATE_CHECK_SECRET=hidden"}, "resolve", "check-env", "done")
	if r.code != 0 {
		t.Fatalf("resolve %d: %s", r.code, r.err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "jobs", "check-env", "checks"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".out" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(root, "jobs", "check-env", "checks", entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "TEND_ROOT=") || strings.Contains(string(b), "PRIVATE_CHECK_SECRET=") {
			t.Fatalf("ambient authority leaked to check:\n%s", b)
		}
	}
}

func TestInterruptedResolutionCheckIsQuiescedAndReconciled(t *testing.T) {
	root, dir := t.TempDir(), t.TempDir()
	late := filepath.Join(dir, "late")
	check := "sleep .7; touch '" + late + "'"
	mustRun(t, root, nil, "submit", "-id", "check-crash", "-check", check,
		"--", "/bin/sh", "-c", "exit 125")
	mustRun(t, root, nil, "work")
	resolver := exec.Command(testBinary, "resolve", "check-crash", "done")
	resolver.Env = append(os.Environ(), "TEND_ROOT="+root)
	var out, errout bytes.Buffer
	resolver.Stdout, resolver.Stderr = &out, &errout
	if err := resolver.Start(); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(root, "jobs", "check-crash", "checks", "active.lock")
	waitForPath(t, lockPath, 3*time.Second)
	deadline := time.Now().Add(3 * time.Second)
	for !fileLockHeld(lockPath) {
		if time.Now().After(deadline) {
			t.Fatal("resolution check lock was not held")
		}
		time.Sleep(10 * time.Millisecond)
	}
	checkDir := filepath.Dir(lockPath)
	for {
		entries, _ := os.ReadDir(checkDir)
		found := false
		for _, entry := range entries {
			found = found || filepath.Ext(entry.Name()) == ".out"
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("resolution evidence file was not created")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := resolver.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = resolver.Wait()
	for fileLockHeld(lockPath) {
		if time.Now().After(deadline) {
			t.Fatal("resolution launcher did not release lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(800 * time.Millisecond)
	if _, err := os.Stat(late); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("resolution check survived controller death: %v", err)
	}
	r := runTend(t, root, nil, nil, "check")
	if r.code != 1 || !strings.Contains(r.err, "unsealed resolution evidence") {
		t.Fatalf("check %d: %s %s", r.code, r.out, r.err)
	}
	mustRun(t, root, nil, "resolve", "check-crash", "done")
	mustRun(t, root, nil, "check")
}

func TestConcurrentResolutionKeepsEvidenceUntilCommit(t *testing.T) {
	root, barriers := t.TempDir(), t.TempDir()
	ready := filepath.Join(barriers, "ready")
	release := filepath.Join(barriers, "release")
	mustRun(t, root, nil, "submit", "-id", "resolve-race", "-check", "exit 0",
		"--", "/bin/sh", "-c", "exit 125")
	mustRun(t, root, nil, "work")
	first := exec.Command(testBinary, "resolve", "resolve-race", "done")
	first.Env = append(os.Environ(), "TEND_ROOT="+root,
		"_TEND_TEST_RESOLVE_AFTER_CHECK_READY="+ready,
		"_TEND_TEST_RESOLVE_AFTER_CHECK_RELEASE="+release)
	var firstOut, firstErr bytes.Buffer
	first.Stdout, first.Stderr = &firstOut, &firstErr
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.WriteFile(release, []byte("release\n"), 0o600)
		if first.ProcessState == nil {
			_ = first.Process.Kill()
			_ = first.Wait()
		}
	}()
	waitForPath(t, ready, 5*time.Second)
	second := exec.Command(testBinary, "resolve", "resolve-race", "fail")
	second.Env = append(os.Environ(), "TEND_ROOT="+root)
	var secondOut, secondErr bytes.Buffer
	second.Stdout, second.Stderr = &secondOut, &secondErr
	if err := second.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if second.ProcessState != nil {
		t.Fatalf("second resolver did not wait for evidence lock: %s", secondErr.String())
	}
	if err := os.WriteFile(release, []byte("release\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := first.Wait(); err != nil {
		t.Fatalf("first resolver: %v\nstdout: %s\nstderr: %s", err, firstOut.String(), firstErr.String())
	}
	err := second.Wait()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 1 || !strings.Contains(secondErr.String(), "not unknown") {
		t.Fatalf("second resolver: %v\nstdout: %s\nstderr: %s", err, secondOut.String(), secondErr.String())
	}
	mustRun(t, root, nil, "check")
	entries, err := os.ReadDir(filepath.Join(root, "jobs", "resolve-race", "checks"))
	if err != nil {
		t.Fatal(err)
	}
	var evidence int
	for _, entry := range entries {
		if ext := filepath.Ext(entry.Name()); ext == ".out" || ext == ".err" {
			evidence++
		}
	}
	if evidence != 2 {
		t.Fatalf("got %d resolution evidence files, want 2", evidence)
	}
}

func TestCheckDetectsWaitProjectionCorruption(t *testing.T) {
	root := t.TempDir()
	mustRun(t, root, nil, "submit", "-id", "wait-corrupt", "--", "/bin/sh", "-c",
		`"$TEND" defer signal go; exit 75`)
	mustRun(t, root, nil, "work")
	db, err := sql.Open("sqlite", "file:"+filepath.Join(root, "state", "tend.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("UPDATE jobs SET wait_key='other' WHERE id='wait-corrupt'"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	_ = db.Close()
	r := runTend(t, root, nil, nil, "check")
	if r.code != 1 || !strings.Contains(r.err, "event replay wait-corrupt") {
		t.Fatalf("check %d: %s %s", r.code, r.out, r.err)
	}
}

func TestCheckUsesConsistentSnapshotDuringWork(t *testing.T) {
	root := t.TempDir()
	const jobs = 40
	for i := 0; i < jobs; i++ {
		mustRun(t, root, nil, "submit", "-id", fmt.Sprintf("snapshot-%02d", i),
			"-key", fmt.Sprintf("key-%02d", i), "--", "true")
	}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				r := runTend(t, root, nil, nil, "work")
				if r.code == 1 {
					return
				}
				if r.code != 0 {
					t.Errorf("worker %d: %s", r.code, r.err)
					return
				}
			}
		}()
	}
	for i := 0; i < 50; i++ {
		r := runTend(t, root, nil, nil, "check")
		if r.code != 0 {
			t.Fatalf("concurrent check %d: %s %s", r.code, r.out, r.err)
		}
	}
	wg.Wait()
	mustRun(t, root, nil, "check")
}

func TestCheckDoesNotRejectConcurrentSubmitInstallation(t *testing.T) {
	root := t.TempDir()
	const jobs = 30
	results := make(chan result, jobs)
	var wg sync.WaitGroup
	for i := 0; i < jobs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results <- runTend(t, root, nil, nil, "submit", "-id",
				fmt.Sprintf("install-%02d", i), "--", "true")
		}(i)
	}
	for i := 0; i < 30; i++ {
		r := runTend(t, root, nil, nil, "check")
		if r.code != 0 {
			t.Fatalf("concurrent submit check %d: %s %s", r.code, r.out, r.err)
		}
	}
	wg.Wait()
	close(results)
	for r := range results {
		if r.code != 0 {
			t.Fatalf("submit %d: %s", r.code, r.err)
		}
	}
	mustRun(t, root, nil, "check")
}

func TestArtifactTamperFailsCheck(t *testing.T) {
	root := t.TempDir()
	mustRun(t, root, nil, "submit", "-id", "tamper", "--", "true")
	mustRun(t, root, nil, "work")
	path := filepath.Join(root, "jobs", "tamper", "attempts", "001.out")
	if err := os.WriteFile(path, []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := runTend(t, root, nil, nil, "check")
	if r.code != 1 || !strings.Contains(r.err, "digest mismatch") {
		t.Fatalf("check %d: %s %s", r.code, r.out, r.err)
	}
}

func TestUnexpectedTerminalAttemptEvidenceFailsCheck(t *testing.T) {
	root := t.TempDir()
	mustRun(t, root, nil, "submit", "-id", "extra-evidence", "--", "true")
	mustRun(t, root, nil, "work")
	path := filepath.Join(root, "jobs", "extra-evidence", "attempts", "002.out")
	if err := os.WriteFile(path, []byte("unexpected"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := runTend(t, root, nil, nil, "check")
	if r.code != 1 || !strings.Contains(r.err, "unsealed attempt evidence") {
		t.Fatalf("check %d: %s %s", r.code, r.out, r.err)
	}
}

func TestEventsAreImmutable(t *testing.T) {
	root := t.TempDir()
	mustRun(t, root, nil, "submit", "-id", "immutable", "--", "true")
	db, err := sql.Open("sqlite", "file:"+filepath.Join(root, "state", "tend.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("UPDATE events SET kind='x'"); err == nil {
		t.Fatal("event update succeeded")
	}
	if _, err = db.Exec("DELETE FROM events"); err == nil {
		t.Fatal("event delete succeeded")
	}
}

func TestArtifactMetadataCannotBeRewritten(t *testing.T) {
	root := t.TempDir()
	mustRun(t, root, nil, "submit", "-id", "sealed", "--", "true")
	mustRun(t, root, nil, "work")
	db, err := sql.Open("sqlite", "file:"+filepath.Join(root, "state", "tend.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("UPDATE artifacts SET digest=?", strings.Repeat("0", 64)); err == nil {
		t.Fatal("artifact update succeeded")
	}
	if _, err = db.Exec("UPDATE attempts SET output_digest=?", strings.Repeat("0", 64)); err == nil {
		t.Fatal("attempt result update succeeded")
	}
}

func TestNewerSchemaIsRejectedBeforeDDL(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(state, "tend.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL); INSERT INTO meta VALUES('schema_version','99')"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	r := runTend(t, root, nil, nil, "list")
	if r.code != 2 || !strings.Contains(r.err, "unsupported schema version") {
		t.Fatalf("list %d: %s", r.code, r.err)
	}
	db, err = sql.Open("sqlite", "file:"+filepath.Join(state, "tend.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='jobs'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("old binary applied DDL to newer schema")
	}
}
