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
	"time"
)

const maxInput = 64 << 20

type definition struct {
	Version     int      `json:"version"`
	ID          string   `json:"id"`
	SerialKey   string   `json:"serial_key"`
	CWD         string   `json:"cwd"`
	Argv        []string `json:"argv"`
	CheckArgv   []string `json:"check_argv,omitempty"`
	NotBeforeUS int64    `json:"not_before_us"`
	InputDigest string   `json:"input_digest,omitempty"`
	InputSize   int      `json:"input_size,omitempty"`
}
type jobView struct {
	ID              string   `json:"id"`
	Status          string   `json:"status"`
	SerialKey       string   `json:"serial_key"`
	CWD             string   `json:"cwd"`
	Argv            []string `json:"argv"`
	CheckArgv       []string `json:"check_argv,omitempty"`
	RunDir          string   `json:"run_dir"`
	CreatedUS       int64    `json:"created_us"`
	UpdatedUS       int64    `json:"updated_us"`
	NotBeforeUS     int64    `json:"not_before_us"`
	WaitKind        string   `json:"wait_kind,omitempty"`
	WaitKey         string   `json:"wait_key,omitempty"`
	WakeAtUS        int64    `json:"wake_at_us,omitempty"`
	LeaseExpiresUS  int64    `json:"lease_expires_us,omitempty"`
	CancelRequested bool     `json:"cancel_requested"`
}

func (a *app) cmdSubmit(ctx context.Context, args []string) (int, error) {
	fs := newFlagSet("submit", a.errout)
	id := fs.String("id", "", "stable job id")
	key := fs.String("key", "", "serialization key")
	cwd := fs.String("C", ".", "working directory")
	at := fs.String("at", "", "not before")
	check := fs.String("check", "", "operator shell check for resolving unknown")
	if err := fs.Parse(args); err != nil {
		return 2, err
	}
	argv := fs.Args()
	if len(argv) == 0 {
		return 2, errors.New("usage: tend submit [flags] -- CMD [ARG...]")
	}
	now := time.Now().UTC()
	notBeforeUS := int64(0)
	if *at != "" && *at != "now" {
		when, err := time.Parse(time.RFC3339, *at)
		if err != nil {
			return 2, errors.New("submit -at requires an absolute RFC3339 time")
		}
		notBeforeUS = when.UnixMicro()
	}
	physical, err := filepath.Abs(*cwd)
	if err != nil {
		return 2, err
	}
	physical, err = filepath.EvalSymlinks(physical)
	if err != nil {
		return 2, fmt.Errorf("resolve working directory: %w", err)
	}
	st, err := os.Stat(physical)
	if err != nil || !st.IsDir() {
		return 2, fmt.Errorf("not a directory: %s", physical)
	}
	resolved, err := resolveProgram(physical, argv[0])
	if err != nil {
		return 2, err
	}
	argv = append([]string{resolved}, argv[1:]...)
	if *key == "" {
		*key = physical
	}
	if *id == "" {
		*id, err = makeID("job-" + now.Format("20060102t150405"))
		if err != nil {
			return 2, err
		}
	}
	if !cleanID(*id) {
		return 2, fmt.Errorf("invalid id %q", *id)
	}
	var checkArgv []string
	if *check != "" {
		checkArgv = []string{"/bin/sh", "-c", *check}
	}
	var input []byte
	read := true
	if f, ok := a.in.(*os.File); ok {
		if fi, e := f.Stat(); e == nil && fi.Mode()&os.ModeCharDevice != 0 {
			read = false
		}
	}
	if read {
		input, err = io.ReadAll(io.LimitReader(a.in, maxInput+1))
		if err != nil {
			return 2, err
		}
		if len(input) > maxInput {
			return 2, fmt.Errorf("input exceeds %d bytes", maxInput)
		}
	}
	inputDigest := ""
	if len(input) > 0 {
		inputDigest = sumBytes(input)
	}
	d := definition{Version: 1, ID: *id, SerialKey: *key, CWD: physical,
		Argv: argv, CheckArgv: checkArgv, NotBeforeUS: notBeforeUS,
		InputDigest: inputDigest, InputSize: len(input)}
	def, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return 2, err
	}
	def = append(def, '\n')
	digest := sumBytes(def)
	if old, err := a.store.job(ctx, *id); err == nil {
		if old.DefinitionDigest != digest {
			return 2, fmt.Errorf("job %s already exists with different bytes", *id)
		}
		fmt.Fprintln(a.out, *id)
		return 0, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 2, err
	}
	jobs := filepath.Join(a.root, "jobs")
	if err = os.MkdirAll(jobs, 0o700); err != nil {
		return 2, err
	}
	tmpID, _ := makeID("tmp")
	tmp := filepath.Join(jobs, "."+tmpID)
	runDir := filepath.Join(jobs, *id)
	if _, statErr := os.Stat(runDir); statErr == nil {
		if err := a.adoptRunDir(ctx, d, runDir, digest, input); err != nil {
			return 2, err
		}
		fmt.Fprintln(a.out, *id)
		return 0, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return 2, statErr
	}
	if err = os.Mkdir(tmp, 0o700); err != nil {
		return 2, err
	}
	installed := false
	defer func() {
		if !installed {
			_ = os.RemoveAll(tmp)
		}
	}()
	if err = writeDurable(filepath.Join(tmp, "definition.json"), def, 0o600); err != nil {
		return 2, err
	}
	if len(input) > 0 {
		if err = writeDurable(filepath.Join(tmp, "input"), input, 0o600); err != nil {
			return 2, err
		}
	}
	if err = syncDir(tmp); err != nil {
		return 2, err
	}
	if err = os.Rename(tmp, runDir); err != nil {
		if _, statErr := os.Stat(runDir); statErr == nil {
			if adoptErr := a.adoptRunDir(ctx, d, runDir, digest, input); adoptErr != nil {
				return 2, adoptErr
			}
			fmt.Fprintln(a.out, *id)
			return 0, nil
		}
		return 2, err
	}
	installed = true
	if err = syncDir(jobs); err != nil {
		return 2, err
	}
	if err = testAfterFilesBarrier(); err != nil {
		return 2, err
	}
	if os.Getenv("_TEND_TEST_CRASH") == "after-files" {
		os.Exit(90)
	}
	if err = a.store.submit(ctx, d, runDir, digest); err != nil {
		return 2, err
	}
	fmt.Fprintln(a.out, *id)
	return 0, nil
}

func (a *app) adoptRunDir(ctx context.Context, d definition, runDir, digest string, input []byte) error {
	definitionPath := filepath.Join(runDir, "definition.json")
	b, err := os.ReadFile(definitionPath)
	if err != nil {
		return fmt.Errorf("job directory exists without a readable definition: %w", err)
	}
	if sumBytes(b) != digest {
		return fmt.Errorf("job %s directory exists with different bytes", d.ID)
	}
	inputPath := filepath.Join(runDir, "input")
	if len(input) == 0 {
		if _, err := os.Stat(inputPath); err == nil {
			return fmt.Errorf("job %s directory has unexpected input", d.ID)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else {
		got, size, err := sumFile(inputPath)
		if err != nil {
			return fmt.Errorf("job %s input is missing: %w", d.ID, err)
		}
		if got != d.InputDigest || size != int64(d.InputSize) {
			return fmt.Errorf("job %s directory has different input", d.ID)
		}
	}
	if err := a.store.submit(ctx, d, runDir, digest); err != nil {
		if old, readErr := a.store.job(ctx, d.ID); readErr == nil && old.DefinitionDigest == digest {
			return nil
		}
		return err
	}
	return nil
}

func resolveProgram(cwd, name string) (string, error) {
	var path string
	var err error
	if filepath.IsAbs(name) {
		path = name
	} else if filepath.Dir(name) != "." {
		path = filepath.Join(cwd, name)
	} else if len(name) >= 2 && name[:2] == "./" {
		path = filepath.Join(cwd, name)
	} else {
		path, err = exec.LookPath(name)
		if err != nil {
			return "", fmt.Errorf("resolve command %q: %w", name, err)
		}
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", err
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve command %q: %w", name, err)
	}
	st, err := os.Stat(path)
	if err != nil || st.IsDir() || st.Mode()&0o111 == 0 {
		return "", fmt.Errorf("command is not executable: %s", path)
	}
	return path, nil
}
func (s *Store) submit(ctx context.Context, d definition, runDir, digest string) error {
	now := nowUS()
	notBeforeUS := d.NotBeforeUS
	if notBeforeUS == 0 {
		notBeforeUS = now
	}
	argv, _ := json.Marshal(d.Argv)
	var check any
	if len(d.CheckArgv) > 0 {
		b, _ := json.Marshal(d.CheckArgv)
		check = string(b)
	}
	c, err := beginImmediate(ctx, s.db)
	if err != nil {
		return err
	}
	defer rollback(c)
	_, err = c.ExecContext(ctx, `INSERT INTO jobs(id,serial_key,cwd,argv_json,check_argv_json,run_dir,definition_digest,status,created_us,updated_us,last_seq,not_before_us) VALUES(?,?,?,?,?,?,?,'ready',?,?,0,?)`, d.ID, d.SerialKey, d.CWD, string(argv), check, runDir, digest, now, now, notBeforeUS)
	if err != nil {
		var oldDigest, oldRunDir string
		readErr := c.QueryRowContext(ctx,
			`SELECT definition_digest,run_dir FROM jobs WHERE id=?`, d.ID,
		).Scan(&oldDigest, &oldRunDir)
		if readErr == nil && oldDigest == digest && oldRunDir == runDir {
			// Another identical submit may install the directory while this
			// submit is waiting for SQLite, or vice versa. The durable row is
			// already complete, so the requests converge.
			return nil
		}
		return err
	}
	seq, err := appendEvent(ctx, c, d.ID, "job.submitted", map[string]any{"argv": d.Argv, "cwd": d.CWD, "serial_key": d.SerialKey, "not_before_us": notBeforeUS, "definition_digest": digest, "input_digest": d.InputDigest, "input_size": d.InputSize}, now)
	if err != nil {
		return err
	}
	defPath := filepath.Join(runDir, "definition.json")
	rel, _ := filepath.Rel(s.root, defPath)
	_, size, err := sumFile(defPath)
	if err != nil {
		return err
	}
	if _, err = c.ExecContext(ctx, `INSERT INTO artifacts(job_id,event_seq,name,digest,size,relpath,created_us)VALUES(?,?,?,?,?,?,?)`, d.ID, seq, "definition", digest, size, rel, now); err != nil {
		return err
	}
	inputPath := filepath.Join(runDir, "input")
	if _, err = os.Stat(inputPath); err == nil {
		dig, n, e := sumFile(inputPath)
		if e != nil {
			return e
		}
		rel, _ = filepath.Rel(s.root, inputPath)
		if _, e = c.ExecContext(ctx, `INSERT INTO artifacts(job_id,event_seq,name,digest,size,relpath,created_us)VALUES(?,?,?,?,?,?,?)`, d.ID, seq, "input", dig, n, rel, now); e != nil {
			return e
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return commit(c)
}

// testAfterFilesBarrier lets a subprocess test pause the filesystem installer
// after rename but before the database transaction. It is inert unless both
// private test variables are set.
func testAfterFilesBarrier() error {
	return testBarrier("_TEND_TEST_AFTER_FILES_READY", "_TEND_TEST_AFTER_FILES_RELEASE")
}

func testBarrier(readyName, releaseName string) error {
	ready := os.Getenv(readyName)
	release := os.Getenv(releaseName)
	if ready == "" || release == "" {
		return nil
	}
	if err := os.WriteFile(ready, []byte("ready\n"), 0o600); err != nil {
		return err
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(release); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if time.Now().After(deadline) {
			return errors.New("test after-files barrier timed out")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
func viewOf(j Job) (jobView, error) {
	var argv, check []string
	if err := json.Unmarshal([]byte(j.ArgvJSON), &argv); err != nil {
		return jobView{}, err
	}
	if j.CheckArgvJSON.Valid {
		if err := json.Unmarshal([]byte(j.CheckArgvJSON.String), &check); err != nil {
			return jobView{}, err
		}
	}
	v := jobView{ID: j.ID, Status: j.Status, SerialKey: j.SerialKey, CWD: j.CWD, Argv: argv, CheckArgv: check, RunDir: j.RunDir, CreatedUS: j.CreatedUS, UpdatedUS: j.UpdatedUS, NotBeforeUS: j.NotBeforeUS, CancelRequested: j.CancelRequested}
	if j.WaitKind.Valid {
		v.WaitKind = j.WaitKind.String
	}
	if j.WaitKey.Valid {
		v.WaitKey = j.WaitKey.String
	}
	if j.WakeAtUS.Valid {
		v.WakeAtUS = j.WakeAtUS.Int64
	}
	if j.LeaseExpiresUS.Valid {
		v.LeaseExpiresUS = j.LeaseExpiresUS.Int64
	}
	return v, nil
}

func (a *app) cmdShow(ctx context.Context, id string) (int, error) {
	j, err := a.store.job(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return 1, fmt.Errorf("no job %s", id)
	}
	if err != nil {
		return 2, err
	}
	v, err := viewOf(j)
	if err != nil {
		return 2, err
	}
	if err = writeJSON(a.out, v); err != nil {
		return 2, err
	}
	return 0, nil
}
func (a *app) cmdList(ctx context.Context) (int, error) {
	rows, err := a.store.db.QueryContext(ctx, "SELECT "+jobColumns+" FROM jobs ORDER BY created_us,id")
	if err != nil {
		return 2, err
	}
	defer rows.Close()
	for rows.Next() {
		j, e := scanJob(rows)
		if e != nil {
			return 2, e
		}
		v, e := viewOf(j)
		if e != nil {
			return 2, e
		}
		if e = writeJSON(a.out, v); e != nil {
			return 2, e
		}
	}
	return 0, rows.Err()
}
func (a *app) cmdEvents(ctx context.Context, id string) (int, error) {
	q := `SELECT j.id,e.seq,e.kind,e.created_us,e.payload FROM events e JOIN jobs j ON j.id=e.job_id`
	var rows *sql.Rows
	var err error
	if id == "" {
		rows, err = a.store.db.QueryContext(ctx, q+" ORDER BY e.created_us,j.id,e.seq")
	} else {
		rows, err = a.store.db.QueryContext(ctx, q+" WHERE j.id=? ORDER BY e.seq", id)
	}
	if err != nil {
		return 2, err
	}
	defer rows.Close()
	for rows.Next() {
		var e Event
		var payload string
		if err = rows.Scan(&e.Job, &e.Seq, &e.Kind, &e.CreatedUS, &payload); err != nil {
			return 2, err
		}
		e.Payload = json.RawMessage(payload)
		if err = writeJSON(a.out, e); err != nil {
			return 2, err
		}
	}
	return 0, rows.Err()
}
