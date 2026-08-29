package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

const schemaVersion = 1

const schema = `
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  serial_key TEXT NOT NULL,
  cwd TEXT NOT NULL,
  argv_json TEXT NOT NULL CHECK(json_valid(argv_json)),
  check_argv_json TEXT CHECK(check_argv_json IS NULL OR json_valid(check_argv_json)),
  run_dir TEXT NOT NULL UNIQUE,
  definition_digest TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN
    ('ready','running','waiting','unknown','done','failed','cancelled')),
  created_us INTEGER NOT NULL,
  updated_us INTEGER NOT NULL,
  last_seq INTEGER NOT NULL DEFAULT 0 CHECK(last_seq >= 0),
  not_before_us INTEGER NOT NULL,
  wait_kind TEXT,
  wait_key TEXT,
  wake_at_us INTEGER,
  lease_owner TEXT,
  lease_token TEXT,
  lease_expires_us INTEGER,
  cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK(cancel_requested IN (0,1)),
  CHECK((status='running' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL
         AND lease_expires_us IS NOT NULL)
     OR (status<>'running' AND lease_owner IS NULL AND lease_token IS NULL
         AND lease_expires_us IS NULL)),
  CHECK((status='waiting' AND wait_kind IS NOT NULL)
     OR (status<>'waiting' AND wait_kind IS NULL AND wait_key IS NULL
         AND wake_at_us IS NULL))
);
CREATE TRIGGER IF NOT EXISTS jobs_definition_no_update
BEFORE UPDATE OF serial_key,cwd,argv_json,check_argv_json,run_dir,definition_digest,created_us
ON jobs BEGIN SELECT RAISE(ABORT,'job definition is immutable'); END;
CREATE UNIQUE INDEX IF NOT EXISTS one_active_per_key
  ON jobs(serial_key) WHERE status IN ('running','unknown');
CREATE INDEX IF NOT EXISTS ready_jobs
  ON jobs(not_before_us,created_us,id) WHERE status='ready';
CREATE INDEX IF NOT EXISTS due_jobs
  ON jobs(wake_at_us,id) WHERE status='waiting' AND wake_at_us IS NOT NULL;

CREATE TABLE IF NOT EXISTS attempts (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id),
  number INTEGER NOT NULL CHECK(number > 0),
  state TEXT NOT NULL CHECK(state IN
    ('prepared','started','finished','abandoned','unknown')),
  prepared_us INTEGER NOT NULL,
  started_us INTEGER,
  finished_us INTEGER,
  outcome TEXT,
  exit_code INTEGER,
  term_signal INTEGER,
  process_pid INTEGER,
  effect_key TEXT NOT NULL,
  output_path TEXT,
  output_digest TEXT,
  output_size INTEGER,
  stderr_path TEXT,
  stderr_digest TEXT,
  stderr_size INTEGER,
  UNIQUE(job_id,number)
);
CREATE TRIGGER IF NOT EXISTS attempt_result_no_rewrite
BEFORE UPDATE OF outcome,exit_code,term_signal,output_path,output_digest,output_size,
stderr_path,stderr_digest,stderr_size ON attempts
WHEN OLD.output_digest IS NOT NULL OR OLD.stderr_digest IS NOT NULL
BEGIN SELECT RAISE(ABORT,'attempt result is immutable'); END;
CREATE TRIGGER IF NOT EXISTS attempt_process_no_rewrite
BEFORE UPDATE OF process_pid ON attempts WHEN OLD.process_pid IS NOT NULL
BEGIN SELECT RAISE(ABORT,'attempt process identity is immutable'); END;
CREATE UNIQUE INDEX IF NOT EXISTS one_open_attempt
  ON attempts(job_id) WHERE state IN ('prepared','started');

CREATE TABLE IF NOT EXISTS events (
  job_id TEXT NOT NULL REFERENCES jobs(id),
  seq INTEGER NOT NULL CHECK(seq > 0),
  kind TEXT NOT NULL,
  created_us INTEGER NOT NULL,
  payload TEXT NOT NULL CHECK(json_valid(payload)),
  PRIMARY KEY(job_id,seq)
);
CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events BEGIN
  SELECT RAISE(ABORT,'events are immutable');
END;
CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events BEGIN
  SELECT RAISE(ABORT,'events are immutable');
END;

CREATE TABLE IF NOT EXISTS artifacts (
  id INTEGER PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id),
  event_seq INTEGER NOT NULL,
  name TEXT NOT NULL,
  digest TEXT NOT NULL CHECK(length(digest)=64 AND digest NOT GLOB '*[^0-9a-f]*'),
  size INTEGER NOT NULL CHECK(size >= 0),
  relpath TEXT NOT NULL UNIQUE,
  created_us INTEGER NOT NULL,
  FOREIGN KEY(job_id,event_seq) REFERENCES events(job_id,seq)
);
CREATE TRIGGER IF NOT EXISTS artifacts_no_update BEFORE UPDATE ON artifacts BEGIN
  SELECT RAISE(ABORT,'artifacts are immutable');
END;
CREATE TRIGGER IF NOT EXISTS artifacts_no_delete BEFORE DELETE ON artifacts BEGIN
  SELECT RAISE(ABORT,'artifacts are immutable');
END;

CREATE TABLE IF NOT EXISTS signals (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id),
  name TEXT NOT NULL,
  payload TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  received_us INTEGER NOT NULL,
  woke INTEGER NOT NULL DEFAULT 0 CHECK(woke IN (0,1)),
  consumed_us INTEGER,
  consumed_seq INTEGER,
  CHECK((consumed_us IS NULL AND consumed_seq IS NULL)
     OR (consumed_us IS NOT NULL AND consumed_seq IS NOT NULL)),
  FOREIGN KEY(job_id,consumed_seq) REFERENCES events(job_id,seq)
);
CREATE TRIGGER IF NOT EXISTS signal_bytes_no_update
BEFORE UPDATE OF job_id,name,payload,payload_digest,received_us,woke ON signals BEGIN
  SELECT RAISE(ABORT,'signal bytes are immutable');
END;
CREATE INDEX IF NOT EXISTS pending_signals
  ON signals(job_id,name,received_us,id) WHERE consumed_us IS NULL;

CREATE TABLE IF NOT EXISTS wait_requests (
  attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),
  kind TEXT NOT NULL CHECK(kind IN ('signal','timer','manual')),
  key TEXT,
  wake_at_us INTEGER,
  CHECK((kind='signal' AND key IS NOT NULL AND wake_at_us IS NULL)
     OR (kind='timer' AND key IS NULL AND wake_at_us IS NOT NULL)
     OR (kind='manual' AND key IS NULL AND wake_at_us IS NULL))
);
`

type Store struct {
	db   *sql.DB
	root string
}

type Job struct {
	ID, SerialKey, CWD, ArgvJSON, RunDir, DefinitionDigest, Status string
	CheckArgvJSON                                                  sql.NullString
	CreatedUS, UpdatedUS, LastSeq, NotBeforeUS                     int64
	WaitKind, WaitKey, LeaseOwner, LeaseToken                      sql.NullString
	WakeAtUS, LeaseExpiresUS                                       sql.NullInt64
	CancelRequested                                                bool
}

type Attempt struct {
	ID, JobID, State, EffectKey string
	Number                      int
	ProcessPID                  sql.NullInt64
}

type Event struct {
	Job       string          `json:"job"`
	Seq       int64           `json:"seq"`
	Kind      string          `json:"kind"`
	CreatedUS int64           `json:"created_us"`
	Payload   json.RawMessage `json:"payload"`
}

func openStore(root string) (*Store, error) {
	state := filepath.Join(root, "state")
	if err := os.MkdirAll(state, 0o700); err != nil {
		return nil, err
	}
	// Every process may be the first process after installation. Serialize
	// WAL/schema initialization with an OS lock that is released on crash.
	lock, err := os.OpenFile(filepath.Join(state, "init.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return nil, err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	path := filepath.Join(state, "tend.db")
	if fi, err := os.Lstat(path); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("state/tend.db must not be a symlink")
	}
	u := &url.URL{Scheme: "file", Path: path}
	query := u.Query()
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "synchronous(FULL)")
	u.RawQuery = query.Encode()
	dsn := u.String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, q := range []string{
		"PRAGMA synchronous=FULL", "PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000", "PRAGMA fullfsync=ON", "PRAGMA checkpoint_fullfsync=ON",
	} {
		if _, err = db.ExecContext(ctx, q); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %w", q, err)
		}
	}
	var hasMeta int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master
WHERE type='table' AND name='meta'`).Scan(&hasMeta); err != nil {
		db.Close()
		return nil, err
	}
	var version string
	newSchema := hasMeta == 0
	if !newSchema {
		err = db.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='schema_version'").Scan(&version)
		if errors.Is(err, sql.ErrNoRows) {
			newSchema, err = true, nil
		} else if err == nil && version != fmt.Sprint(schemaVersion) {
			db.Close()
			return nil, fmt.Errorf("unsupported schema version %s", version)
		} else if err != nil {
			db.Close()
			return nil, err
		}
	}
	if _, err = db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("PRAGMA journal_mode=WAL: %w", err)
	}
	if newSchema {
		c, beginErr := beginImmediate(ctx, db)
		if beginErr != nil {
			db.Close()
			return nil, beginErr
		}
		if _, err = c.ExecContext(ctx, schema); err == nil {
			_, err = c.ExecContext(ctx, "INSERT INTO meta(key,value) VALUES('schema_version',?)", fmt.Sprint(schemaVersion))
		}
		if err == nil {
			err = commit(c)
		} else {
			rollback(c)
		}
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	if err = os.Chmod(path, 0o600); err != nil {
		db.Close()
		return nil, err
	}
	if err = syncDir(state); err != nil {
		db.Close()
		return nil, err
	}
	if err = syncDir(root); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, root: root}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func beginImmediate(ctx context.Context, db *sql.DB) (*sql.Conn, error) {
	c, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err = c.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}
func commit(c *sql.Conn) error {
	_, err := c.ExecContext(context.Background(), "COMMIT")
	_ = c.Close()
	return err
}
func rollback(c *sql.Conn) { _, _ = c.ExecContext(context.Background(), "ROLLBACK"); _ = c.Close() }

func appendEvent(ctx context.Context, c *sql.Conn, id, kind string, payload any, at int64) (int64, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	res, err := c.ExecContext(ctx, "UPDATE jobs SET last_seq=last_seq+1,updated_us=? WHERE id=?", at, id)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return 0, sql.ErrNoRows
	}
	var seq int64
	if err = c.QueryRowContext(ctx, "SELECT last_seq FROM jobs WHERE id=?", id).Scan(&seq); err != nil {
		return 0, err
	}
	_, err = c.ExecContext(ctx, "INSERT INTO events(job_id,seq,kind,created_us,payload) VALUES(?,?,?,?,?)", id, seq, kind, at, string(b))
	return seq, err
}

const jobColumns = `id,serial_key,cwd,argv_json,check_argv_json,run_dir,
definition_digest,status,created_us,updated_us,last_seq,not_before_us,
wait_kind,wait_key,wake_at_us,lease_owner,lease_token,lease_expires_us,cancel_requested`

type scanner interface{ Scan(...any) error }

func scanJob(row scanner) (Job, error) {
	var j Job
	var cancel int
	err := row.Scan(&j.ID, &j.SerialKey, &j.CWD, &j.ArgvJSON, &j.CheckArgvJSON, &j.RunDir,
		&j.DefinitionDigest, &j.Status, &j.CreatedUS, &j.UpdatedUS, &j.LastSeq, &j.NotBeforeUS,
		&j.WaitKind, &j.WaitKey, &j.WakeAtUS, &j.LeaseOwner, &j.LeaseToken, &j.LeaseExpiresUS, &cancel)
	j.CancelRequested = cancel != 0
	return j, err
}
func (s *Store) job(ctx context.Context, id string) (Job, error) {
	return scanJob(s.db.QueryRowContext(ctx, "SELECT "+jobColumns+" FROM jobs WHERE id=?", id))
}
func nowUS() int64 { return time.Now().UTC().UnixMicro() }
