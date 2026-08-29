package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type dbReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type artifactCheck struct {
	rel, digest string
	size        int64
}

type attemptEvidence struct {
	job, runDir, state string
	number, pid        int
	sealed             bool
}

func (a *app) cmdCheck(ctx context.Context) (int, error) {
	problems := []string{}
	conn, err := a.store.db.Conn(ctx)
	if err != nil {
		return 2, err
	}
	if _, err = conn.ExecContext(ctx, "BEGIN"); err != nil {
		_ = conn.Close()
		return 2, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
		_ = conn.Close()
	}()
	var q dbReader = conn
	var integrity string
	if err := q.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil {
		problems = append(problems, "integrity_check: "+err.Error())
	} else if integrity != "ok" {
		problems = append(problems, "integrity_check: "+integrity)
	}
	rows, err := q.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		problems = append(problems, "foreign_key_check: "+err.Error())
	} else {
		if rows.Next() {
			problems = append(problems, "foreign_key_check returned rows")
		}
		_ = rows.Close()
	}
	checks := []struct{ name, q string }{{"event sequence", `SELECT count(*) FROM jobs j WHERE j.last_seq!=COALESCE((SELECT MAX(seq)FROM events e WHERE e.job_id=j.id),0) OR j.last_seq!=(SELECT count(*)FROM events e WHERE e.job_id=j.id)`}, {"running attempt", `SELECT count(*) FROM jobs j WHERE (j.status='running' AND (SELECT count(*) FROM attempts a WHERE a.job_id=j.id AND a.state IN('prepared','started'))!=1) OR (j.status!='running' AND EXISTS(SELECT 1 FROM attempts a WHERE a.job_id=j.id AND a.state IN('prepared','started')))`}, {"unknown attempt", `SELECT count(*) FROM jobs j WHERE j.status='unknown' AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.job_id=j.id AND a.state='unknown')`}, {"active serialization key", `SELECT count(*) FROM(SELECT serial_key FROM jobs WHERE status IN('running','unknown')GROUP BY serial_key HAVING count(*)>1)`}, {"done evidence", `SELECT count(*) FROM jobs j WHERE j.status='done' AND NOT EXISTS(SELECT 1 FROM events e WHERE e.job_id=j.id AND (e.kind='attempt.finished' AND json_extract(e.payload,'$.status')='done' OR e.kind='job.resolved' AND json_extract(e.payload,'$.passed')=1))`}}
	for _, ck := range checks {
		var n int
		if err := q.QueryRowContext(ctx, ck.q).Scan(&n); err != nil {
			problems = append(problems, ck.name+": "+err.Error())
		} else if n != 0 {
			problems = append(problems, fmt.Sprintf("%s: %d violation(s)", ck.name, n))
		}
	}
	problems = append(problems, a.checkDefinitionBindings(ctx, q)...)
	problems = append(problems, a.checkAttemptBindings(ctx, q)...)
	problems = append(problems, a.checkSignalBindings(ctx, q)...)
	statusRows, err := q.QueryContext(ctx, `SELECT id,status,not_before_us,wait_kind,
wait_key,wake_at_us,cancel_requested FROM jobs ORDER BY id`)
	if err != nil {
		problems = append(problems, "event replay: "+err.Error())
	} else {
		type projectedJob struct {
			id, status, waitKind, waitKey string
			notBefore, wakeAt             int64
			cancel                        bool
		}
		var statuses []projectedJob
		for statusRows.Next() {
			var j projectedJob
			var wk, wkey sql.NullString
			var wake sql.NullInt64
			var cancel int
			if err := statusRows.Scan(&j.id, &j.status, &j.notBefore, &wk, &wkey,
				&wake, &cancel); err != nil {
				problems = append(problems, "event replay: "+err.Error())
				break
			}
			j.waitKind, j.waitKey, j.wakeAt, j.cancel = wk.String, wkey.String,
				wake.Int64, cancel != 0
			statuses = append(statuses, j)
		}
		_ = statusRows.Close()
		for _, projected := range statuses {
			replayed, err := replayProjection(ctx, q, projected.id)
			if err != nil {
				problems = append(problems, "event replay "+projected.id+": "+err.Error())
			} else if replayed.status != projected.status ||
				replayed.notBefore != projected.notBefore ||
				replayed.waitKind != projected.waitKind ||
				replayed.waitKey != projected.waitKey ||
				replayed.wakeAt != projected.wakeAt ||
				replayed.cancel != projected.cancel {
				problems = append(problems, fmt.Sprintf(
					"event replay %s: got %+v, projection is status=%s not_before=%d wait=%s/%s/%d cancel=%t",
					projected.id, replayed, projected.status, projected.notBefore,
					projected.waitKind, projected.waitKey, projected.wakeAt, projected.cancel))
			}
		}
	}
	var artifacts []artifactCheck
	expectedArtifact := map[string]bool{}
	art, err := q.QueryContext(ctx, "SELECT relpath,digest,size FROM artifacts ORDER BY id")
	if err != nil {
		problems = append(problems, "artifacts: "+err.Error())
	} else {
		for art.Next() {
			var item artifactCheck
			if err = art.Scan(&item.rel, &item.digest, &item.size); err != nil {
				problems = append(problems, "artifacts: "+err.Error())
				break
			}
			artifacts = append(artifacts, item)
			expectedArtifact[item.rel] = true
		}
		_ = art.Close()
	}
	var attempts []attemptEvidence
	arows, attemptErr := q.QueryContext(ctx, `SELECT j.id,j.run_dir,a.number,a.state,
COALESCE(a.process_pid,0),a.output_digest IS NOT NULL FROM attempts a
JOIN jobs j ON j.id=a.job_id ORDER BY j.id,a.number`)
	if attemptErr != nil {
		problems = append(problems, "attempt evidence: "+attemptErr.Error())
	} else {
		for arows.Next() {
			var item attemptEvidence
			var sealed int
			if err = arows.Scan(&item.job, &item.runDir, &item.number, &item.state,
				&item.pid, &sealed); err != nil {
				problems = append(problems, "attempt evidence: "+err.Error())
				break
			}
			item.sealed = sealed != 0
			attempts = append(attempts, item)
		}
		_ = arows.Close()
	}
	jobDirs := map[string]bool{}
	jr, e := q.QueryContext(ctx, "SELECT run_dir FROM jobs")
	if e == nil {
		for jr.Next() {
			var p string
			_ = jr.Scan(&p)
			jobDirs[p] = true
		}
		_ = jr.Close()
	} else {
		problems = append(problems, "job directories: "+e.Error())
	}
	if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
		return 2, err
	}
	committed = true
	_ = conn.Close()

	for _, item := range artifacts {
		if filepath.IsAbs(item.rel) || item.rel == ".." || strings.HasPrefix(item.rel, ".."+string(os.PathSeparator)) {
			problems = append(problems, "artifact path escapes root: "+item.rel)
			continue
		}
		got, n, e := sumFile(filepath.Join(a.root, item.rel))
		if e != nil {
			problems = append(problems, "artifact missing: "+item.rel)
		} else if got != item.digest || n != item.size {
			problems = append(problems, "artifact digest mismatch: "+item.rel)
		}
	}
	dbpath := filepath.Join(a.root, "state", "tend.db")
	if fi, e := os.Lstat(dbpath); e != nil || fi.Mode()&os.ModeSymlink != 0 {
		problems = append(problems, "database is missing or symlinked")
	}
	knownAttemptFiles := map[string]bool{}
	for _, attempt := range attempts {
		for _, ext := range []string{"out", "err"} {
			path := filepath.Join(attempt.runDir, "attempts",
				fmt.Sprintf("%03d.%s", attempt.number, ext))
			knownAttemptFiles[path] = true
			rel, _ := filepath.Rel(a.root, path)
			_, statErr := os.Lstat(path)
			if errors.Is(statErr, os.ErrNotExist) {
				if attempt.sealed {
					problems = append(problems, "sealed attempt evidence missing: "+rel)
				}
				continue
			}
			if statErr != nil {
				problems = append(problems, "attempt evidence: "+statErr.Error())
				continue
			}
			if expectedArtifact[rel] {
				continue
			}
			live := (attempt.state == "prepared" || attempt.state == "started") ||
				(attempt.state == "unknown" && processGroupExists(attempt.pid))
			if !live && !a.artifactNowRegistered(ctx, rel) {
				problems = append(problems, "unsealed attempt evidence: "+rel)
			}
		}
	}
	for runDir := range jobDirs {
		dir := filepath.Join(runDir, "attempts")
		entries, readErr := os.ReadDir(dir)
		if errors.Is(readErr, os.ErrNotExist) {
			continue
		}
		if readErr != nil {
			problems = append(problems, "attempt directory: "+readErr.Error())
			continue
		}
		for _, entry := range entries {
			ext := filepath.Ext(entry.Name())
			if ext != ".out" && ext != ".err" {
				problems = append(problems, "unexpected attempt file: "+filepath.Join(dir, entry.Name()))
				continue
			}
			path := filepath.Join(dir, entry.Name())
			if !knownAttemptFiles[path] && !a.attemptPathNowActive(ctx, path) {
				problems = append(problems, "unsealed attempt evidence: "+path)
			}
		}
		checkDir := filepath.Join(runDir, "checks")
		checkEntries, checkErr := os.ReadDir(checkDir)
		if errors.Is(checkErr, os.ErrNotExist) {
			continue
		}
		if checkErr != nil {
			problems = append(problems, "check directory: "+checkErr.Error())
			continue
		}
		active := fileLockHeld(filepath.Join(checkDir, "active.lock"))
		for _, entry := range checkEntries {
			ext := filepath.Ext(entry.Name())
			if ext != ".out" && ext != ".err" {
				continue
			}
			path := filepath.Join(checkDir, entry.Name())
			rel, _ := filepath.Rel(a.root, path)
			if expectedArtifact[rel] || a.artifactNowRegistered(ctx, rel) || active {
				continue
			}
			problems = append(problems, "unsealed resolution evidence: "+path)
		}
	}
	entries, _ := os.ReadDir(filepath.Join(a.root, "jobs"))
	for _, entry := range entries {
		if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
			p := filepath.Join(a.root, "jobs", entry.Name())
			if !jobDirs[p] && !a.jobDirNowReferenced(ctx, p) && !adoptableJobDir(p, entry.Name()) {
				problems = append(problems, "unreferenced job directory: "+p)
			}
		}
	}
	if len(problems) > 0 {
		for _, p := range problems {
			fmt.Fprintln(a.errout, "not ok", p)
		}
		return 1, nil
	}
	fmt.Fprintln(a.out, "ok")
	return 0, nil
}

func fileLockHeld(path string) bool {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return false
	}
	defer f.Close()
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		return false
	}
	return errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN)
}

func (a *app) jobDirNowReferenced(ctx context.Context, path string) bool {
	var n int
	return a.store.db.QueryRowContext(ctx,
		"SELECT count(*) FROM jobs WHERE run_dir=?", path).Scan(&n) == nil && n != 0
}

func adoptableJobDir(path, name string) bool {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.Name() != "definition.json" && entry.Name() != "input" {
			return false
		}
	}
	b, err := os.ReadFile(filepath.Join(path, "definition.json"))
	if err != nil {
		return false
	}
	var d definition
	if json.Unmarshal(b, &d) != nil || d.Version != 1 || d.ID != name {
		return false
	}
	inputPath := filepath.Join(path, "input")
	if d.InputDigest == "" {
		_, err = os.Stat(inputPath)
		return errors.Is(err, os.ErrNotExist)
	}
	digest, size, err := sumFile(inputPath)
	return err == nil && digest == d.InputDigest && size == int64(d.InputSize)
}

func (a *app) artifactNowRegistered(ctx context.Context, rel string) bool {
	var n int
	return a.store.db.QueryRowContext(ctx,
		"SELECT count(*) FROM artifacts WHERE relpath=?", rel).Scan(&n) == nil && n != 0
}

func (a *app) attemptPathNowActive(ctx context.Context, path string) bool {
	rows, err := a.store.db.QueryContext(ctx, `SELECT j.run_dir,a.number,a.state
FROM attempts a JOIN jobs j ON j.id=a.job_id WHERE a.state IN('prepared','started')`)
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var runDir, state string
		var number int
		if rows.Scan(&runDir, &number, &state) != nil {
			return false
		}
		for _, ext := range []string{"out", "err"} {
			if path == filepath.Join(runDir, "attempts", fmt.Sprintf("%03d.%s", number, ext)) {
				return true
			}
		}
	}
	return false
}

func (a *app) checkDefinitionBindings(ctx context.Context, q dbReader) []string {
	type binding struct {
		id, jobDigest, name, digest, payload string
		size                                 int64
	}
	rows, err := q.QueryContext(ctx, `SELECT j.id,j.definition_digest,
a.name,a.digest,a.size,e.payload FROM jobs j JOIN artifacts a ON a.job_id=j.id
JOIN events e ON e.job_id=a.job_id AND e.seq=a.event_seq
WHERE e.kind='job.submitted' AND a.name IN('definition','input') ORDER BY j.id,a.name`)
	if err != nil {
		return []string{"definition binding: " + err.Error()}
	}
	var all []binding
	for rows.Next() {
		var b binding
		if err := rows.Scan(&b.id, &b.jobDigest, &b.name, &b.digest, &b.size, &b.payload); err != nil {
			_ = rows.Close()
			return []string{"definition binding: " + err.Error()}
		}
		all = append(all, b)
	}
	_ = rows.Close()
	var problems []string
	for _, b := range all {
		var p map[string]any
		if err := json.Unmarshal([]byte(b.payload), &p); err != nil {
			problems = append(problems, "definition binding "+b.id+": "+err.Error())
			continue
		}
		if b.name == "definition" {
			if b.digest != b.jobDigest || p["definition_digest"] != b.digest {
				problems = append(problems, "definition binding mismatch: "+b.id)
			}
		} else if p["input_digest"] != b.digest || jsonNumber(p["input_size"]) != b.size {
			problems = append(problems, "input binding mismatch: "+b.id)
		}
	}
	return problems
}

func (a *app) checkAttemptBindings(ctx context.Context, q dbReader) []string {
	type attemptBinding struct {
		job, id, outDigest, errDigest string
		number                        int
		outSize, errSize              int64
	}
	rows, err := q.QueryContext(ctx, `SELECT job_id,id,number,output_digest,output_size,
stderr_digest,stderr_size FROM attempts WHERE output_digest IS NOT NULL ORDER BY job_id,number`)
	if err != nil {
		return []string{"attempt binding: " + err.Error()}
	}
	var all []attemptBinding
	for rows.Next() {
		var b attemptBinding
		if err := rows.Scan(&b.job, &b.id, &b.number, &b.outDigest, &b.outSize, &b.errDigest, &b.errSize); err != nil {
			_ = rows.Close()
			return []string{"attempt binding: " + err.Error()}
		}
		all = append(all, b)
	}
	_ = rows.Close()
	var problems []string
	for _, b := range all {
		var raw string
		err := q.QueryRowContext(ctx, `SELECT payload FROM events WHERE job_id=?
AND kind IN('attempt.finished','attempt.effect-unknown','attempt.output-sealed')
AND json_extract(payload,'$.attempt')=? ORDER BY seq DESC LIMIT 1`, b.job, b.id).Scan(&raw)
		if err != nil {
			problems = append(problems, "attempt event missing: "+b.id)
			continue
		}
		var p map[string]any
		_ = json.Unmarshal([]byte(raw), &p)
		if p["output_digest"] != b.outDigest || jsonNumber(p["output_size"]) != b.outSize ||
			p["stderr_digest"] != b.errDigest || jsonNumber(p["stderr_size"]) != b.errSize {
			problems = append(problems, "attempt event mismatch: "+b.id)
		}
		items := []struct {
			name, digest string
			size         int64
		}{{fmt.Sprintf("attempt-%d-output", b.number), b.outDigest, b.outSize},
			{fmt.Sprintf("attempt-%d-stderr", b.number), b.errDigest, b.errSize}}
		for _, item := range items {
			var digest string
			var size int64
			if err := q.QueryRowContext(ctx, "SELECT digest,size FROM artifacts WHERE job_id=? AND name=?", b.job, item.name).Scan(&digest, &size); err != nil || digest != item.digest || size != item.size {
				problems = append(problems, "attempt artifact mismatch: "+b.id+"/"+item.name)
			}
		}
	}
	return problems
}

func (a *app) checkSignalBindings(ctx context.Context, q dbReader) []string {
	type signalBinding struct {
		id, job, name, payload, digest string
		received                       int64
		woke                           bool
		consumedUS, consumedSeq        sql.NullInt64
	}
	rows, err := q.QueryContext(ctx, `SELECT id,job_id,name,payload,payload_digest,
received_us,woke,consumed_us,consumed_seq FROM signals ORDER BY id`)
	if err != nil {
		return []string{"signal binding: " + err.Error()}
	}
	var all []signalBinding
	for rows.Next() {
		var b signalBinding
		var woke int
		if err := rows.Scan(&b.id, &b.job, &b.name, &b.payload, &b.digest,
			&b.received, &woke, &b.consumedUS, &b.consumedSeq); err != nil {
			_ = rows.Close()
			return []string{"signal binding: " + err.Error()}
		}
		b.woke = woke != 0
		all = append(all, b)
	}
	_ = rows.Close()
	var problems []string
	for _, b := range all {
		if sumBytes([]byte(b.payload)) != b.digest {
			problems = append(problems, "signal digest mismatch: "+b.id)
		}
		var eventDigest, eventName string
		var eventWoke int
		var eventCreated int64
		if err := q.QueryRowContext(ctx, `SELECT json_extract(payload,'$.payload_digest'),
json_extract(payload,'$.name'),json_extract(payload,'$.woke'),created_us FROM events
WHERE job_id=? AND kind='signal.received' AND json_extract(payload,'$.id')=?`,
			b.job, b.id).Scan(&eventDigest, &eventName, &eventWoke, &eventCreated); err != nil || eventDigest != b.digest || eventName != b.name ||
			eventWoke != boolInt(b.woke) || eventCreated != b.received {
			problems = append(problems, "signal event mismatch: "+b.id)
		}
		if b.consumedSeq.Valid {
			var kind, raw string
			var created int64
			if err := q.QueryRowContext(ctx, `SELECT kind,payload,created_us FROM events
WHERE job_id=? AND seq=?`, b.job, b.consumedSeq.Int64).Scan(&kind, &raw, &created); err != nil {
				problems = append(problems, "signal consumption event missing: "+b.id)
				continue
			}
			var p map[string]any
			_ = json.Unmarshal([]byte(raw), &p)
			bound := kind == "job.woken" && p["signal"] == b.id
			if kind == "attempt.finished" {
				if ids, ok := p["consumed_signals"].([]any); ok {
					for _, id := range ids {
						bound = bound || id == b.id
					}
				}
			}
			if !bound || !b.consumedUS.Valid || b.consumedUS.Int64 != created {
				problems = append(problems, "signal consumption mismatch: "+b.id)
			}
		} else if b.consumedUS.Valid || b.woke {
			problems = append(problems, "signal consumption projection mismatch: "+b.id)
		}
	}
	return problems
}

func jsonNumber(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return -1
}

type replayedProjection struct {
	status, waitKind, waitKey string
	notBefore, wakeAt         int64
	cancel                    bool
}

func replayProjection(ctx context.Context, q dbReader, id string) (replayedProjection, error) {
	rows, err := q.QueryContext(ctx, `SELECT kind,created_us,payload FROM events
WHERE job_id=? ORDER BY seq`, id)
	if err != nil {
		return replayedProjection{}, err
	}
	defer rows.Close()
	var r replayedProjection
	for rows.Next() {
		var kind, raw string
		var created int64
		if err := rows.Scan(&kind, &created, &raw); err != nil {
			return replayedProjection{}, err
		}
		var p map[string]any
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			return replayedProjection{}, err
		}
		switch kind {
		case "job.submitted":
			r.status = "ready"
			r.notBefore = jsonNumber(p["not_before_us"])
		case "attempt.abandoned":
			r.status, r.cancel = "ready", false
		case "attempt.cancelled":
			r.status, r.cancel = "cancelled", false
		case "attempt.prepared":
			r.status, r.cancel = "running", false
		case "attempt.effect-unknown":
			r.status, r.cancel = "unknown", false
		case "attempt.preflight-failed":
			r.status, r.cancel = "failed", false
		case "attempt.finished":
			if v, ok := p["status"].(string); ok {
				r.status = v
			}
			r.waitKind, _ = p["wait_kind"].(string)
			r.waitKey, _ = p["wait_key"].(string)
			if p["wake_at_us"] != nil {
				r.wakeAt = jsonNumber(p["wake_at_us"])
			} else {
				r.wakeAt = 0
			}
			r.cancel = false
		case "timer.fired":
			r.status, r.waitKind, r.waitKey, r.wakeAt, r.cancel = "ready", "", "", 0, false
			r.notBefore = jsonNumber(p["at_us"])
		case "job.woken", "job.retried":
			r.status, r.waitKind, r.waitKey, r.wakeAt, r.cancel = "ready", "", "", 0, false
			r.notBefore = created
		case "job.cancel-requested":
			if p["from"] != "running" {
				r.status, r.waitKind, r.waitKey, r.wakeAt = "cancelled", "", "", 0
			} else {
				r.cancel = true
			}
		case "job.resolved":
			switch p["resolution"] {
			case "retry":
				r.status, r.notBefore = "ready", created
			case "fail":
				r.status, r.notBefore = "failed", created
			case "done":
				if p["passed"] == true {
					r.status = "done"
				}
			}
			r.waitKind, r.waitKey, r.wakeAt, r.cancel = "", "", 0, false
		}
	}
	if err := rows.Err(); err != nil {
		return replayedProjection{}, err
	}
	if r.status == "" {
		return replayedProjection{}, fmt.Errorf("no state-bearing event")
	}
	return r, nil
}
