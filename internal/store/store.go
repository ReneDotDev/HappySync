// Package store provides the transactional, private recovery state. Immutable
// records include tombstones indefinitely; the network uses ID-keyset pagination.
package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/happysync/happysync/internal/config"
	"github.com/happysync/happysync/internal/model"
	_ "modernc.org/sqlite"
)

const SchemaVersion = 2

type Store struct {
	db       *sql.DB
	recordMu sync.Mutex
	digest   string
}
type Journal struct {
	ID     string       `json:"id"`
	Path   string       `json:"path"`
	Stage  string       `json:"stage"`
	Backup string       `json:"backup,omitempty"`
	Record model.Record `json:"record"`
}
type Checkpoint struct {
	Hash string `json:"hash"`
	Path string `json:"path"`
	Size int64  `json:"size"`
}

func Open(root string) (*Store, error) {
	if err := config.EnsureStateDir(root); err != nil {
		return nil, err
	}
	path := filepath.Join(config.StateDir(root), "state.db")
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return nil, errors.New("unsafe database path")
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	// Create the database before SQLite opens it so permissions are private even
	// on a system with a permissive umask. SQLite inherits them for WAL files.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	err = f.Chmod(0600)
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if err = config.ProtectPrivatePath(path, false); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	fail := func(err error) (*Store, error) { _ = db.Close(); return nil, err }
	for _, pragma := range []string{"PRAGMA busy_timeout=5000", "PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL", "PRAGMA foreign_keys=ON"} {
		if _, err = db.Exec(pragma); err != nil {
			return fail(fmt.Errorf("open state database: %w", err))
		}
	}
	var check string
	if err = db.QueryRow("PRAGMA quick_check").Scan(&check); err != nil {
		return fail(err)
	}
	if check != "ok" {
		return fail(errors.New("state database corruption detected"))
	}
	if err = s.migrate(); err != nil {
		return fail(err)
	}
	// quick_check verifies SQLite pages, not that a claimed schema has all of
	// our tables. Fail at startup rather than accepting incomplete state.
	for _, q := range []string{
		"SELECT name,value FROM counters LIMIT 0",
		"SELECT id,path,payload FROM records LIMIT 0",
		"SELECT path,payload FROM baselines LIMIT 0",
		"SELECT id,payload FROM journals LIMIT 0",
		"SELECT hash,payload FROM checkpoints LIMIT 0",
		"SELECT key,value FROM metadata LIMIT 0",
	} {
		rows, err := db.Query(q)
		if err != nil {
			return fail(errors.New("invalid state database schema"))
		}
		if err = rows.Close(); err != nil {
			return fail(err)
		}
	}
	var counter int64
	if err = db.QueryRow("SELECT value FROM counters WHERE name='local'").Scan(&counter); err != nil || counter < 0 {
		return fail(errors.New("invalid persisted local revision counter"))
	}
	return s, nil
}

func (s *Store) migrate() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err = tx.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version > SchemaVersion {
		return errors.New("state database uses a newer unsupported schema")
	}
	if version == 0 {
		statements := []string{
			"CREATE TABLE counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL CHECK(value>=0))",
			"INSERT INTO counters VALUES ('local',0)",
			"CREATE TABLE records (id TEXT PRIMARY KEY, path TEXT NOT NULL, payload BLOB NOT NULL)",
			"CREATE INDEX records_path ON records(path)",
			"CREATE TABLE baselines (path TEXT PRIMARY KEY, payload BLOB NOT NULL)",
			"CREATE TABLE journals (id TEXT PRIMARY KEY, payload BLOB NOT NULL)",
			"CREATE TABLE checkpoints (hash TEXT PRIMARY KEY, payload BLOB NOT NULL)",
			"PRAGMA user_version=1",
		}
		for _, q := range statements {
			if _, err = tx.Exec(q); err != nil {
				return err
			}
		}
		version = 1
	}
	if version == 1 {
		if _, err = tx.Exec("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"); err != nil {
			return err
		}
		if _, err = tx.Exec("PRAGMA user_version=2"); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) Close() error { return s.db.Close() }

// Metadata values are local control state, never replicated. The counter table
// is separate so caller preferences cannot reset causal revision numbers.
func (s *Store) GetMeta(key string) (string, error) {
	var value string
	err := s.db.QueryRow("SELECT value FROM metadata WHERE key=?", key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

func (s *Store) SetMeta(key, value string) error {
	if key == "" {
		return errors.New("metadata key is required")
	}
	_, err := s.db.Exec("INSERT INTO metadata(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, value)
	return err
}

// ReserveCounters durably allocates a contiguous batch. Gaps after crashes are safe;
// counters are never reused, and one scan does not fsync once per discovered file.
func (s *Store) ReserveCounters(count int) (uint64, error) {
	if count < 1 {
		return 0, errors.New("counter reservation must be positive")
	}
	var n int64
	err := s.db.QueryRow("UPDATE counters SET value=value+? WHERE name='local' AND value<=? RETURNING value", count, int64(math.MaxInt64)-int64(count)).Scan(&n)
	if err == sql.ErrNoRows {
		return 0, errors.New("local revision counter exhausted")
	}
	return uint64(n) - uint64(count) + 1, err
}
func (s *Store) NextCounter() (uint64, error) { return s.ReserveCounters(1) }

// RecordDigest summarizes immutable IDs in canonical order. Merge invalidates it
// only after a successful insert; duplicate pages keep the cached summary valid.
func (s *Store) RecordDigest(ctx context.Context) (string, error) {
	s.recordMu.Lock()
	defer s.recordMu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if s.digest != "" {
		return s.digest, nil
	}
	rows, err := s.db.QueryContext(ctx, "SELECT id FROM records ORDER BY id")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	h := sha256.New()
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return "", err
		}
		h.Write([]byte(id))
		h.Write([]byte{'\n'})
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	s.digest = hex.EncodeToString(h.Sum(nil))
	return s.digest, nil
}
func (s *Store) HasRecords() (bool, error) {
	var exists bool
	err := s.db.QueryRow("SELECT EXISTS(SELECT 1 FROM records LIMIT 1)").Scan(&exists)
	return exists, err
}

func (s *Store) Merge(records []model.Record) error {
	if len(records) == 0 {
		return nil
	}
	s.recordMu.Lock()
	defer s.recordMu.Unlock()
	inserted := false
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	insert, err := tx.Prepare("INSERT INTO records(id,path,payload) VALUES (?,?,?) ON CONFLICT(id) DO NOTHING")
	if err != nil {
		return err
	}
	defer insert.Close()
	lookup, err := tx.Prepare("SELECT payload FROM records WHERE id=?")
	if err != nil {
		return err
	}
	defer lookup.Close()
	for _, r := range records {
		if r.ID == "" || r.Path == "" {
			return errors.New("record needs an ID and path")
		}
		raw, err := json.Marshal(r)
		if err != nil {
			return err
		}
		result, err := insert.Exec(r.ID, r.Path, raw)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 0 {
			inserted = true
		}
		if changed == 0 {
			var old []byte
			if err = lookup.QueryRow(r.ID).Scan(&old); err != nil {
				return err
			}
			if !bytes.Equal(old, raw) {
				return errors.New("immutable record ID collision")
			}
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if inserted {
		s.digest = ""
	}
	return nil
}

func readRecords(rows *sql.Rows) ([]model.Record, error) {
	defer rows.Close()
	out := make([]model.Record, 0)
	for rows.Next() {
		var raw []byte
		var r model.Record
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, errors.New("invalid persisted record")
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (s *Store) Records() ([]model.Record, error) {
	rows, err := s.db.Query("SELECT payload FROM records ORDER BY id")
	if err != nil {
		return nil, err
	}
	return readRecords(rows)
}
func (s *Store) RecordPage(afterID string, limit int) ([]model.Record, error) {
	if limit < 1 || limit > 4096 {
		return nil, errors.New("record page limit must be 1..4096")
	}
	rows, err := s.db.Query("SELECT payload FROM records WHERE id>? ORDER BY id LIMIT ?", afterID, limit)
	if err != nil {
		return nil, err
	}
	return readRecords(rows)
}
func (s *Store) RecordsForPath(path string) ([]model.Record, error) {
	rows, err := s.db.Query("SELECT payload FROM records WHERE path=? ORDER BY id", path)
	if err != nil {
		return nil, err
	}
	return readRecords(rows)
}

func (s *Store) Baselines() (map[string]model.Baseline, error) {
	rows, err := s.db.Query("SELECT path,payload FROM baselines")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]model.Baseline)
	for rows.Next() {
		var path string
		var raw []byte
		var b model.Baseline
		if err = rows.Scan(&path, &raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &b); err != nil {
			return nil, errors.New("invalid persisted baseline")
		}
		out[path] = b
	}
	return out, rows.Err()
}
func (s *Store) SetBaselines(baselines map[string]model.Baseline) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec("DELETE FROM baselines"); err != nil {
		return err
	}
	stmt, err := tx.Prepare("INSERT INTO baselines(path,payload) VALUES (?,?)")
	if err != nil {
		return err
	}
	defer stmt.Close()
	for path, b := range baselines {
		raw, err := json.Marshal(b)
		if err != nil {
			return err
		}
		if _, err = stmt.Exec(path, raw); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// PatchBaselines persists changed scan observations in one transaction.
func (s *Store) PatchBaselines(updates map[string]model.Baseline, deleted []string) error {
	if len(updates) == 0 && len(deleted) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare("INSERT INTO baselines(path,payload) VALUES (?,?) ON CONFLICT(path) DO UPDATE SET payload=excluded.payload")
	if err != nil {
		return err
	}
	defer stmt.Close()
	for p, b := range updates {
		raw, err := json.Marshal(b)
		if err != nil {
			return err
		}
		if _, err = stmt.Exec(p, raw); err != nil {
			return err
		}
	}
	for _, p := range deleted {
		if _, err = tx.Exec("DELETE FROM baselines WHERE path=?", p); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// PutBaseline commits one observation without rewriting the full scan index.
func (s *Store) PutBaseline(path string, baseline model.Baseline) error {
	if path == "" {
		return errors.New("baseline requires a path")
	}
	raw, err := json.Marshal(baseline)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("INSERT INTO baselines(path,payload) VALUES (?,?) ON CONFLICT(path) DO UPDATE SET payload=excluded.payload", path, raw)
	return err
}

func (s *Store) DeleteBaseline(path string) error {
	_, err := s.db.Exec("DELETE FROM baselines WHERE path=?", path)
	return err
}
func (s *Store) PutJournal(j Journal) error {
	if j.ID == "" {
		return errors.New("journal requires an ID")
	}
	raw, err := json.Marshal(j)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("INSERT INTO journals(id,payload) VALUES (?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", j.ID, raw)
	return err
}
func (s *Store) DeleteJournal(id string) error {
	_, err := s.db.Exec("DELETE FROM journals WHERE id=?", id)
	return err
}
func (s *Store) Journals() ([]Journal, error) {
	rows, err := s.db.Query("SELECT payload FROM journals ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Journal, 0)
	for rows.Next() {
		var raw []byte
		var j Journal
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &j); err != nil {
			return nil, errors.New("invalid persisted recovery journal")
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
func (s *Store) PutCheckpoint(c Checkpoint) error {
	if c.Hash == "" || c.Size < 0 {
		return errors.New("invalid block checkpoint")
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("INSERT INTO checkpoints(hash,payload) VALUES (?,?) ON CONFLICT(hash) DO UPDATE SET payload=excluded.payload", c.Hash, raw)
	return err
}
func (s *Store) DeleteCheckpoint(hash string) error {
	_, err := s.db.Exec("DELETE FROM checkpoints WHERE hash=?", hash)
	return err
}
func (s *Store) Checkpoints() ([]Checkpoint, error) {
	rows, err := s.db.Query("SELECT payload FROM checkpoints ORDER BY hash")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Checkpoint, 0)
	for rows.Next() {
		var raw []byte
		var c Checkpoint
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &c); err != nil {
			return nil, errors.New("invalid persisted block checkpoint")
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
