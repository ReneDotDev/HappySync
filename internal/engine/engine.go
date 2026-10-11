// Package engine coordinates local observations, causal reconciliation and verified transfers.
package engine

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/happysync/happysync/internal/config"
	"github.com/happysync/happysync/internal/index"
	"github.com/happysync/happysync/internal/model"
	"github.com/happysync/happysync/internal/network"
	"github.com/happysync/happysync/internal/reconcile"
	logstatus "github.com/happysync/happysync/internal/status"
	"github.com/happysync/happysync/internal/store"
	"github.com/happysync/happysync/internal/transfer"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/zeebo/blake3"
)

type Options struct {
	Log          func(Status)
	Fault        func(string) error
	Network      network.Config
	Workers      int
	StagingLimit int64
	ScanInterval time.Duration
	Ignore       []string
	Executable   string
}
type Status struct {
	Version            int               `json:"version"`
	Folder             string            `json:"folder"`
	SyncID             string            `json:"sync_id"`
	PeerID             string            `json:"peer_id"`
	PID                int               `json:"pid"`
	State              string            `json:"state"`
	Addresses          []string          `json:"addresses"`
	Peers              network.Stats     `json:"peers"`
	LocalFiles         int               `json:"local_files"`
	PendingFiles       int               `json:"pending_files"`
	OutstandingBlocks  int               `json:"outstanding_blocks"`
	Conflicts          int               `json:"conflicts"`
	ConflictPaths      []string          `json:"recent_conflicts"`
	Warnings           []string          `json:"warnings"`
	Errors             []string          `json:"errors"`
	Received           uint64            `json:"received_bytes"`
	Sent               uint64            `json:"sent_bytes"`
	LastReconciliation time.Time         `json:"last_reconciliation,omitempty"`
	Download           float64           `json:"download_bytes_per_second"`
	Upload             float64           `json:"upload_bytes_per_second"`
	SourceBytes        map[string]uint64 `json:"source_bytes"`
}
type blockSource struct {
	Path   string
	Offset int64
	Size   int64
}

var errPending = errors.New("transfer pending")

type download struct {
	done     chan struct{}
	err      error
	manifest model.Manifest
	cancel   context.CancelFunc
}
type Engine struct {
	downloads   map[string]*download
	downloadWG  sync.WaitGroup
	sourceBytes sync.Map
	sourceMu    sync.RWMutex
	sources     map[string]map[string]blockSource
	root        string
	cfg         *config.Config
	opts        Options
	db          *store.Store
	cache       *transfer.Cache
	node        *network.Node
	fs          *os.Root
	mu          sync.RWMutex
	status      Status
	baselines   map[string]model.Baseline
	manifests   map[string]model.Manifest
	wake        chan struct{}
	watcher     *fsnotify.Watcher
	done        chan struct{}
	lastBytes   uint64
	lastSent    uint64
	lastRate    time.Time
}

func New(ctx context.Context, cfg *config.Config, opts Options) (*Engine, error) {
	if opts.Workers == 0 {
		opts.Workers = 8
	}
	if opts.StagingLimit == 0 {
		opts.StagingLimit = 8 << 30
	}
	if opts.ScanInterval == 0 {
		opts.ScanInterval = 10 * time.Minute
	}
	db, err := store.Open(cfg.Root)
	if err != nil {
		return nil, err
	}
	cache, err := transfer.New(cfg.Root, opts.Workers, opts.StagingLimit)
	if err != nil {
		db.Close()
		return nil, err
	}
	fs, err := os.OpenRoot(cfg.Root)
	if err != nil {
		db.Close()
		return nil, err
	}
	e := &Engine{sources: map[string]map[string]blockSource{}, downloads: map[string]*download{}, root: cfg.Root, cfg: cfg, opts: opts, db: db, cache: cache, fs: fs, wake: make(chan struct{}, 1), done: make(chan struct{}), manifests: map[string]model.Manifest{}, lastRate: time.Now()}
	e.status = Status{Version: 1, Folder: cfg.Root, SyncID: hex.EncodeToString(cfg.SyncID[:6]), PID: os.Getpid(), State: "discovering", Warnings: []string{}, Errors: []string{}}
	if e.baselines, err = db.Baselines(); err != nil {
		e.Close()
		return nil, err
	}
	if err = e.recover(); err != nil {
		e.Close()
		return nil, err
	}
	if err = e.cleanupStages(); err != nil {
		e.Close()
		return nil, err
	}
	if err = e.scan(true); err != nil {
		e.Close()
		return nil, err
	}
	opts.Network.Identity = cfg.Identity
	opts.Network.SyncID = cfg.SyncID
	opts.Network.Secret = cfg.Secret
	if e.node, err = network.New(ctx, opts.Network); err != nil {
		e.Close()
		return nil, err
	}
	e.node.SetBackend(e)
	e.status.PeerID = e.node.PeerID()
	e.status.Addresses = e.node.Addresses()
	if e.watcher, err = fsnotify.NewWatcher(); err == nil {
		e.watchDirectories()
		go e.watch(ctx)
	} else {
		e.status.Warnings = append(e.status.Warnings, "filesystem notifications unavailable; scans remain enabled")
	}
	go e.run(ctx)
	return e, nil
}
func (e *Engine) Close() error {
	if e.watcher != nil {
		e.watcher.Close()
	}
	if e.node != nil {
		e.node.Close()
	}
	if e.fs != nil {
		e.fs.Close()
	}
	if e.db != nil {
		return e.db.Close()
	}
	return nil
}
func (e *Engine) Wait() { <-e.done }
func (e *Engine) Status() Status {
	e.mu.RLock()
	s := e.status
	s.Errors = append([]string{}, s.Errors...)
	s.Warnings = append([]string{}, s.Warnings...)
	s.ConflictPaths = append([]string{}, s.ConflictPaths...)
	e.mu.RUnlock()
	if e.node != nil {
		s.Peers = e.node.Stats()
		s.Sent = s.Peers.SentBytes
	}
	for i, message := range s.Errors {
		s.Errors[i] = logstatus.Redact(message)
	}
	for i, message := range s.Warnings {
		s.Warnings[i] = logstatus.Redact(message)
	}
	s.Peers.LastError = logstatus.Redact(s.Peers.LastError)
	s.Received = e.cache.Received.Load()
	s.SourceBytes = map[string]uint64{}
	e.sourceBytes.Range(func(key, value any) bool {
		s.SourceBytes[key.(string)] = value.(*atomic.Uint64).Load()
		return true
	})
	return s
}
func (e *Engine) signal() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}
func (e *Engine) watch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-e.watcher.Events:
			if !ok {
				return
			}
			relative, _ := filepath.Rel(e.root, event.Name)
			if relative != ".HappySync" && !strings.HasPrefix(relative, ".HappySync"+string(filepath.Separator)) {
				e.signal()
			}
		case _, ok := <-e.watcher.Errors:
			if !ok {
				return
			}
			e.signal()
		}
	}
}
func (e *Engine) watchDirectories() {
	filepath.WalkDir(e.root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if p != e.root && (d.Name() == ".HappySync" || d.Name() == ".git" || d.Name() == "node_modules") {
				return filepath.SkipDir
			}
			e.watcher.Add(p)
		}
		return nil
	})
}
func (e *Engine) run(ctx context.Context) {
	defer close(e.done)
	defer e.downloadWG.Wait()
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()
	full := time.NewTicker(e.opts.ScanInterval)
	defer full.Stop()
	for {
		force := false
		select {
		case <-ctx.Done():
			return
		case <-full.C:
			force = true
		case <-tick.C:
		case <-e.wake:
		}
		if ctx.Err() != nil {
			return
		}
		err := e.step(ctx, force)
		e.mu.Lock()
		if err != nil {
			e.status.Errors = []string{err.Error()}
			e.status.State = "error"
		} else {
			e.status.Errors = []string{}
			e.status.State = "idle"
			if e.status.PendingFiles > 0 {
				e.status.State = "syncing"
			}
			if len(e.node.Peers()) == 0 {
				e.status.State = "discovering"
				if e.status.PendingFiles > 0 {
					e.status.State = "offline"
				}
			}
			e.status.LastReconciliation = time.Now()
		}
		now := time.Now()
		bytes := e.cache.Received.Load()
		sent := e.node.Stats().SentBytes
		elapsed := now.Sub(e.lastRate).Seconds()
		e.status.Download = float64(bytes-e.lastBytes) / elapsed
		e.status.Upload = float64(sent-e.lastSent) / elapsed
		e.lastBytes = bytes
		e.lastSent = sent
		e.lastRate = now
		e.mu.Unlock()
		if e.opts.Log != nil {
			e.opts.Log(e.Status())
		}
	}
}
func (e *Engine) scan(force bool) error {
	hasRecords, err := e.db.HasRecords()
	if err != nil {
		return err
	}
	fresh := !hasRecords && len(e.baselines) == 0
	current, manifests, warnings, err := index.Scan(e.root, index.ScanOptions{Ignore: append(append([]string{}, e.cfg.Ignore...), e.opts.Ignore...), Executable: e.opts.Executable, Previous: e.baselines, PreviousManifests: e.manifests, Force: force})
	if err != nil {
		return err
	}
	var changed, removed []string
	for p, b := range current {
		old, ok := e.baselines[p]
		if ok && !old.Deleted && old.Hash == b.Hash && old.Directory == b.Directory {
			b.Clock, b.SourceID, b.SourcePath = old.Clock, old.SourceID, old.SourcePath
			current[p] = b
		} else {
			changed = append(changed, p)
		}
	}
	for p, old := range e.baselines {
		if _, ok := current[p]; ok {
			continue
		}
		if old.Deleted {
			current[p] = old
			continue
		}
		ignored, err := e.ignored(p, old.Directory)
		if err != nil {
			return err
		}
		if !ignored {
			removed = append(removed, p)
		}
	}
	count := len(changed) + len(removed)
	var counter uint64
	if count > 0 {
		counter, err = e.db.ReserveCounters(count)
		if err != nil {
			return err
		}
	}
	added := make([]model.Record, 0, count)
	sort.Strings(changed)
	sort.Strings(removed)
	for _, p := range changed {
		b := current[p]
		r := reconcile.NewRecord(p, peerID(e.cfg), counter, e.baselines[p].Clock, manifests[p], false, b.Directory, fresh, b.Mtime)
		counter++
		added = append(added, r)
		b.Clock = r.Clock
		current[p] = b
	}
	for _, p := range removed {
		r := reconcile.NewRecord(p, peerID(e.cfg), counter, e.baselines[p].Clock, model.Manifest{}, true, false, false)
		counter++
		added = append(added, r)
		current[p] = model.Baseline{Deleted: true, Clock: r.Clock}
	}
	if err = e.db.Merge(added); err != nil {
		return err
	}
	updates := map[string]model.Baseline{}
	var deleted []string
	for p, b := range current {
		old, ok := e.baselines[p]
		if !ok || !equalBaseline(old, b) {
			updates[p] = b
		}
	}
	for p := range e.baselines {
		if _, ok := current[p]; !ok {
			deleted = append(deleted, p)
		}
	}
	if err = e.db.PatchBaselines(updates, deleted); err != nil {
		return err
	}
	e.baselines = current
	e.manifests = manifests
	e.refreshSources()
	e.mu.Lock()
	e.status.Warnings = warnings
	e.status.LocalFiles = 0
	for _, b := range current {
		if !b.Directory && !b.Deleted {
			e.status.LocalFiles++
		}
	}
	e.mu.Unlock()
	if e.watcher != nil {
		e.watchDirectories()
	}
	return nil
}
func (e *Engine) step(ctx context.Context, force bool) error {
	if err := e.recover(); err != nil {
		return err
	}
	if err := e.scan(force); err != nil {
		return err
	}
	peers := e.node.Peers()
	for _, p := range peers {
		localDigest, err := e.Digest(ctx)
		if err != nil {
			return err
		}
		if remoteDigest, err := e.node.StateDigest(ctx, p); err == nil && remoteDigest == localDigest {
			continue
		}
		cursor := ""
		for page := 0; page < 10000; page++ {
			records, next, err := e.node.FetchRecords(ctx, p, cursor, 128)
			if err != nil {
				break
			}
			if err = e.Receive(ctx, records); err != nil {
				return err
			}
			if next == "" {
				break
			}
			if next <= cursor {
				return errors.New("peer did not advance snapshot cursor")
			}
			cursor = next
			if page == 9999 {
				return errors.New("peer snapshot exceeds record page limit")
			}
		}
	}
	records, err := e.db.Records()
	if err != nil {
		return err
	}
	entries, err := reconcile.Resolve(records)
	if err != nil {
		return err
	}
	// Obsolete requests must not retain chunks or occupy the file queue indefinitely.
	needed := map[string]bool{}
	for _, entry := range entries {
		if !entry.Record.Deleted && !entry.Record.Directory {
			needed[entry.Record.Manifest.Hash] = true
		}
	}
	for hash, task := range e.downloads {
		if needed[hash] {
			continue
		}
		task.cancel()
		select {
		case <-task.done:
			e.cache.Unretain(task.manifest)
			e.cache.Release(task.manifest)
			delete(e.downloads, hash)
		default:
		}
	}
	// Preserve conflict content before replacing or removing its original source path.
	sort.SliceStable(entries, func(i, j int) bool {
		a := entries[i].Path != entries[i].Record.Path
		b := entries[j].Path != entries[j].Record.Path
		if a != b {
			return a
		}
		if entries[i].Record.Deleted != entries[j].Record.Deleted {
			return entries[i].Record.Deleted
		}
		if entries[i].Record.Deleted {
			return len(entries[i].Path) > len(entries[j].Path)
		}
		return entries[i].Path < entries[j].Path
	})
	pending := 0
	conflicts := 0
	conflictPaths := []string{}
	outstanding := map[string]bool{}
	for _, entry := range entries {
		if entry.Path != entry.Record.Path || e.baselines[entry.Path].SourceID != "" {
			conflicts++
			if len(conflictPaths) < 32 {
				conflictPaths = append(conflictPaths, entry.Path)
			}
		}
		base, ok := e.baselines[entry.Path]
		if entry.Record.Deleted {
			if ok && !base.Deleted {
				pending++
			}
		} else if !ok || base.Deleted || base.Hash != entry.Record.Manifest.Hash || base.Directory != entry.Record.Directory {
			pending++
			for _, b := range entry.Record.Manifest.Blocks {
				if !e.HasBlock(ctx, b.Hash, b.Size) {
					outstanding[b.Hash] = true
				}
			}
		}
	}
	e.mu.Lock()
	e.status.PendingFiles = pending
	e.status.Conflicts = conflicts
	e.status.ConflictPaths = conflictPaths
	e.status.OutstandingBlocks = len(outstanding)
	if pending > 0 {
		e.status.State = "syncing"
	}
	e.mu.Unlock()
	remaining := 0
	blocked := map[string]int{}
	pendingConflict := map[string]bool{}
	for _, entry := range entries {
		if entry.Path != entry.Record.Path {
			b, ok := e.baselines[entry.Path]
			if !ok || b.Deleted || b.Hash != entry.Record.Manifest.Hash || b.Directory != entry.Record.Directory {
				blocked[entry.Record.Path]++
				pendingConflict[entry.Path] = true
			}
		}
	}
	// Observation is per path. An unrelated offline download must not prevent
	// a completed path from recording every causally observed identical version.
	clocks := map[string]model.Vector{}
	left := map[string]int{}
	incomplete := map[string]bool{}
	for _, r := range records {
		clocks[r.Path] = joinClock(clocks[r.Path], r.Clock)
	}
	for _, entry := range entries {
		left[entry.Record.Path]++
	}
	settle := func(entry model.Entry, complete bool) error {
		p := entry.Record.Path
		if !complete {
			incomplete[p] = true
		}
		left[p]--
		if left[p] != 0 || incomplete[p] {
			return nil
		}
		if b, ok := e.baselines[p]; ok && !clockContains(b.Clock, clocks[p]) {
			b.Clock = joinClock(b.Clock, clocks[p])
			if err := e.db.PutBaseline(p, b); err != nil {
				return err
			}
			e.baselines[p] = b
		}
		return nil
	}
	for _, entry := range entries {
		if entry.Path == entry.Record.Path && blocked[entry.Path] > 0 {
			remaining++
			if err = settle(entry, false); err != nil {
				return err
			}
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if ignored, err := e.ignored(entry.Path, entry.Record.Directory); err != nil {
			return err
		} else if ignored {
			if err = settle(entry, false); err != nil {
				return err
			}
			continue
		}
		if err = e.apply(ctx, entry); err != nil {
			if errors.Is(err, errPending) {
				remaining++
				if err = settle(entry, false); err != nil {
					return err
				}
				continue
			}
			return err
		}
		if pendingConflict[entry.Path] {
			blocked[entry.Record.Path]--
		}
		if err = settle(entry, true); err != nil {
			return err
		}
	}
	if remaining > 0 {
		e.mu.Lock()
		e.status.PendingFiles = remaining
		e.mu.Unlock()
		return nil
	}
	e.mu.Lock()
	e.status.PendingFiles = 0
	e.status.OutstandingBlocks = 0
	e.mu.Unlock()
	return nil
}
func (e *Engine) apply(ctx context.Context, entry model.Entry) error {
	r := entry.Record
	base, exists := e.baselines[entry.Path]
	if r.Deleted && (!exists || base.Deleted) {
		if exists && base.Deleted && clockContains(base.Clock, r.Clock) {
			return nil
		}
		b := model.Baseline{Deleted: true, Clock: joinClock(base.Clock, r.Clock)}
		e.baselines[entry.Path] = b
		return e.db.PutBaseline(entry.Path, b)
	}
	if !r.Deleted && exists && !base.Deleted && base.Hash == r.Manifest.Hash && base.Directory == r.Directory {
		if clockContains(base.Clock, r.Clock) {
			return nil
		}
		base.Clock = joinClock(base.Clock, r.Clock)
		e.baselines[entry.Path] = base
		return e.db.PutBaseline(entry.Path, base)
	}
	target, err := index.SafePath(e.root, entry.Path)
	if err != nil {
		return err
	}
	relative, _ := filepath.Rel(e.root, target)
	if err = e.checkUnchanged(entry.Path); err != nil {
		return err
	}

	stage := ""
	if !r.Deleted && !r.Directory {
		if err = e.localBlocks(r); err != nil && !os.IsNotExist(err) { /* Other peers may have the version. */
		}
		peers := e.node.Peers()
		if err = e.download(ctx, r.Manifest, peers); err != nil {
			return err
		}

		if stage, err = e.cache.Assemble(r.Manifest); err != nil {
			return err
		}
	}
	if err = e.checkUnchanged(entry.Path); err != nil {
		if stage != "" {
			os.Remove(stage)
		}
		return err
	}
	journal := store.Journal{ID: r.ID + ":" + entry.Path, Path: entry.Path, Stage: stage, Record: r}
	// A file-to-directory change requires two filesystem operations. Retain the
	// original in a journaled private backup across the gap between them.
	if exists && !base.Deleted && !base.Directory && r.Directory && !r.Deleted {
		backup, err := os.CreateTemp(filepath.Join(config.StateDir(e.root), "tmp"), "backup-*")
		if err != nil {
			return err
		}
		journal.Backup = backup.Name()
		if err = backup.Close(); err != nil {
			return err
		}
		if err = os.Remove(journal.Backup); err != nil {
			return err
		}
	}
	if err = e.db.PutJournal(journal); err != nil {
		if stage != "" {
			_ = os.Remove(stage)
		}
		return err
	}
	if err = e.fault("after-journal"); err != nil {
		return err
	}
	// Journal persistence may block on IO. Recheck the user's file afterwards,
	// before modifying it, so an edit during that wait remains a local revision.
	if err = e.checkUnchanged(entry.Path); err != nil {
		if stage != "" {
			_ = os.Remove(stage)
		}
		if clearErr := e.db.DeleteJournal(journal.ID); clearErr != nil {
			return clearErr
		}
		return err
	}
	if exists && !base.Deleted && base.Directory != r.Directory && !r.Deleted {
		if journal.Backup != "" {
			backupRel, _ := filepath.Rel(e.root, journal.Backup)
			err = e.fs.Rename(relative, backupRel)
		} else {
			err = e.fs.Remove(relative)
		}
		if err != nil {
			return fmt.Errorf("cannot change file type at %q: %w", entry.Path, err)
		}
		if err = e.fault("after-type-backup"); err != nil {
			return err
		}
	}
	if r.Deleted {
		err = e.fs.Remove(relative)
		if os.IsNotExist(err) {
			err = nil
		}
	} else if r.Directory {
		err = e.fs.MkdirAll(relative, 0755)
	} else {
		if err = e.fs.MkdirAll(filepath.Dir(relative), 0755); err == nil {
			stageRel, _ := filepath.Rel(e.root, stage)
			err = e.fs.Rename(stageRel, relative)
		}
	}
	if err != nil {
		return fmt.Errorf("cannot apply %q: %w", entry.Path, err)
	}
	if err = e.fault("after-install"); err != nil {
		return err
	}
	if err = e.updateBaseline(entry.Path, r); err != nil {
		return err
	}
	if err = e.db.PutBaseline(entry.Path, e.baselines[entry.Path]); err != nil {
		return err
	}
	if !r.Directory && !r.Deleted {
		if task := e.downloads[r.Manifest.Hash]; task != nil {
			e.cache.Unretain(task.manifest)
		}
		e.cache.Release(r.Manifest)
		delete(e.downloads, r.Manifest.Hash)
	}
	if err = e.fault("after-baseline"); err != nil {
		return err
	}
	if journal.Backup != "" {
		backupRel, _ := filepath.Rel(e.root, journal.Backup)
		if err = e.fs.Remove(backupRel); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return e.db.DeleteJournal(journal.ID)
}
func (e *Engine) checkUnchanged(path string) error {
	p, err := index.SafePath(e.root, path)
	if err != nil {
		return err
	}
	info, err := os.Lstat(p)
	old, known := e.baselines[path]
	if os.IsNotExist(err) {
		if known && !old.Deleted {
			return errors.New("local file changed during synchronization; retrying")
		}
		return nil
	}
	if err != nil {
		return err
	}
	if !known || old.Deleted {
		return errors.New("unindexed local file appeared during synchronization; retrying")
	}
	if info.IsDir() {
		if !old.Directory {
			return errors.New("local path changed type; retrying")
		}
		return nil
	}
	m, err := index.BuildManifest(p)
	if err != nil {
		return err
	}
	if old.Hash != m.Hash {
		return errors.New("local file changed during synchronization; retrying")
	}
	return nil
}
func (e *Engine) updateBaseline(path string, r model.Record) error {
	previous := e.manifests[path]
	if r.Deleted {
		e.baselines[path] = model.Baseline{Deleted: true, Clock: r.Clock}
		delete(e.manifests, path)
		e.updateSources(path, previous, model.Manifest{})
		return nil
	}
	p, err := index.SafePath(e.root, path)
	if err != nil {
		return err
	}
	if !r.Directory && r.Mtime != 0 {
		relative, _ := filepath.Rel(e.root, p)
		stamp := time.Unix(0, r.Mtime)
		if err := e.fs.Chtimes(relative, stamp, stamp); err != nil {
			e.mu.Lock()
			e.status.Warnings = append(e.status.Warnings, fmt.Sprintf("cannot preserve modification time for %q: %v", path, err))
			e.mu.Unlock()
		}
	}
	info, err := os.Lstat(p)
	if err != nil {
		return err
	}
	e.baselines[path] = model.Baseline{Hash: r.Manifest.Hash, Size: r.Manifest.Size, Directory: r.Directory, Mtime: info.ModTime().UnixNano(), Clock: r.Clock}
	if path != r.Path {
		derived := reconcile.NewRecord(path, r.Origin, r.Counter, r.Clock, r.Manifest, false, r.Directory, false, r.Mtime)
		if err = e.db.Merge([]model.Record{derived}); err != nil {
			return err
		}
		b := e.baselines[path]
		b.SourceID = r.ID
		b.SourcePath = r.Path
		e.baselines[path] = b
	}
	e.manifests[path] = r.Manifest
	e.updateSources(path, previous, r.Manifest)
	return nil
}
func (e *Engine) recover() error {
	journals, err := e.db.Journals()
	if err != nil {
		return err
	}
	for _, j := range journals {
		p, err := index.SafePath(e.root, j.Path)
		if err != nil {
			return err
		}
		matches := false
		if j.Record.Deleted {
			_, err = os.Lstat(p)
			matches = os.IsNotExist(err)
		} else {
			if j.Record.Directory {
				info, err := os.Stat(p)
				matches = err == nil && info.IsDir()
			} else {
				m, err := index.BuildManifest(p)
				matches = err == nil && m.Hash == j.Record.Manifest.Hash
			}
		}
		if matches {
			if err = e.updateBaseline(j.Path, j.Record); err != nil {
				return err
			}
			if err = e.db.PutBaseline(j.Path, e.baselines[j.Path]); err != nil {
				return err
			}
		}
		if j.Backup != "" {
			backupRel, err := filepath.Rel(e.root, j.Backup)
			if err != nil || !strings.HasPrefix(filepath.ToSlash(backupRel), ".HappySync/tmp/backup-") {
				return errors.New("unsafe type-change recovery backup")
			}
			if _, err = e.fs.Stat(backupRel); err == nil {
				if matches {
					if err = e.fs.Remove(backupRel); err != nil {
						return err
					}
				} else {
					targetRel, _ := filepath.Rel(e.root, p)
					if _, err = e.fs.Lstat(targetRel); os.IsNotExist(err) {
						if err = e.fs.Rename(backupRel, targetRel); err != nil {
							return err
						}
					} else {
						// A post-crash local file also survives. Do not overwrite it during recovery.
						recovered := reconcile.ConflictPath(j.Path, j.Record.ID+":type-recovery")
						safe, err := index.SafePath(e.root, recovered)
						if err != nil {
							return err
						}
						recoveredRel, _ := filepath.Rel(e.root, safe)
						if _, err = e.fs.Lstat(recoveredRel); !os.IsNotExist(err) {
							return errors.New("type-change recovery copy already exists; preserve private backup and inspect destination")
						}
						if err = e.fs.Rename(backupRel, recoveredRel); err != nil {
							return err
						}
					}
				}
			} else if !os.IsNotExist(err) {
				return err
			}
		}
		if j.Stage != "" {
			rel, err := filepath.Rel(e.root, j.Stage)
			if err == nil && strings.HasPrefix(filepath.ToSlash(rel), ".HappySync/tmp/") {
				e.fs.Remove(rel)
			}
		}
		if err = e.db.DeleteJournal(j.ID); err != nil {
			return err
		}
	}
	return nil
}
func (e *Engine) localBlocks(r model.Record) error {
	for _, b := range r.Manifest.Blocks {
		if _, err := e.cache.Read(b.Hash, b.Size); err == nil {
			continue
		}
		data, err := e.ReadBlock(context.Background(), b.Hash, b.Size)
		if err != nil {
			return err
		}
		if err = e.cache.Put(b.Hash, data); err != nil {
			return err
		}
	}
	return nil
}

// An assembly can be interrupted before a journal exists. Only after journals
// have recovered can unreferenced install files be safely reclaimed. Backups
// are deliberately retained: they may be the only remaining original content.
func (e *Engine) cleanupStages() error {
	entries, err := os.ReadDir(filepath.Join(config.StateDir(e.root), "tmp"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "install-") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("unsafe orphan installation stage")
		}
		if err = e.fs.Remove(filepath.Join(".HappySync", "tmp", entry.Name())); err != nil {
			return err
		}
	}
	return nil
}
func (e *Engine) Digest(ctx context.Context) (string, error) { return e.db.RecordDigest(ctx) }
func (e *Engine) Snapshot(ctx context.Context, cursor string, limit int) ([]model.Record, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if limit < 1 || limit > 128 {
		limit = 128
	}
	records, err := e.db.RecordPage(cursor, limit+1)
	if err != nil {
		return nil, "", err
	}
	// Keep pages below the wire bound even when several large manifests share a page.
	// One record may use up to 12MiB; ordinary pages are limited to 4MiB.
	size, count := 0, 0
	for count < len(records) && count < limit {
		raw, err := json.Marshal(records[count])
		if err != nil {
			return nil, "", err
		}
		if len(raw) > 12<<20 {
			return nil, "", errors.New("file manifest exceeds protocol record size limit")
		}
		if count > 0 && size+len(raw) > 4<<20 {
			break
		}
		size += len(raw)
		count++
	}
	next := ""
	if count < len(records) {
		next = records[count-1].ID
	}
	return records[:count], next, nil
}
func (e *Engine) Receive(ctx context.Context, records []model.Record) error {
	for _, r := range records {
		if err := reconcile.ValidateRecord(r); err != nil {
			return err
		}
	}
	return e.db.Merge(records)
}
func (e *Engine) ReadBlock(ctx context.Context, hash string, size int64) ([]byte, error) {
	if size < 0 || size > model.BlockSize {
		return nil, errors.New("invalid block size")
	}
	if data, err := e.cache.Read(hash, size); err == nil {
		return data, nil
	}
	e.sourceMu.RLock()
	var sources []blockSource
	for _, source := range e.sources[hash] {
		sources = append(sources, source)
		if len(sources) == 8 {
			break
		}
	}
	e.sourceMu.RUnlock()
	for _, source := range sources {
		if source.Size != size {
			continue
		}
		p, err := index.SafePath(e.root, source.Path)
		if err != nil {
			continue
		}
		relative, err := filepath.Rel(e.root, p)
		if err != nil {
			continue
		}
		f, err := e.fs.Open(relative)
		if err != nil {
			continue
		}
		data := make([]byte, size)
		_, err = f.ReadAt(data, source.Offset)
		f.Close()
		if err == nil {
			h := blake3.Sum256(data)
			if hex.EncodeToString(h[:]) == hash {
				return data, nil
			}
		}
	}
	return nil, errors.New("verified block unavailable")
}
func (e *Engine) HasBlock(ctx context.Context, hash string, size int64) bool {
	if !validBlockRequest(hash, size) {
		return false
	}
	if info, err := os.Stat(filepath.Join(e.cache.Dir, hash)); err == nil && info.Size() == size {
		return true
	}
	e.sourceMu.RLock()
	defer e.sourceMu.RUnlock()
	for _, source := range e.sources[hash] {
		if source.Size == size {
			return true
		}
	}
	return false
}
func validBlockRequest(hash string, size int64) bool {
	b, err := hex.DecodeString(hash)
	return err == nil && len(b) == 32 && size >= 0 && size <= model.BlockSize
}
func (e *Engine) refreshSources() {
	out := map[string]map[string]blockSource{}
	for p, m := range e.manifests {
		var offset int64
		for _, b := range m.Blocks {
			if out[b.Hash] == nil {
				out[b.Hash] = map[string]blockSource{}
			}
			out[b.Hash][p] = blockSource{p, offset, b.Size}
			offset += b.Size
		}
	}
	e.sourceMu.Lock()
	e.sources = out
	e.sourceMu.Unlock()
}
func (e *Engine) updateSources(p string, previous, current model.Manifest) {
	e.sourceMu.Lock()
	defer e.sourceMu.Unlock()
	for _, b := range previous.Blocks {
		delete(e.sources[b.Hash], p)
		if len(e.sources[b.Hash]) == 0 {
			delete(e.sources, b.Hash)
		}
	}
	var offset int64
	for _, b := range current.Blocks {
		if e.sources[b.Hash] == nil {
			e.sources[b.Hash] = map[string]blockSource{}
		}
		e.sources[b.Hash][p] = blockSource{p, offset, b.Size}
		offset += b.Size
	}
}
func joinClock(a, b model.Vector) model.Vector {
	out := model.Vector{}
	for p, n := range a {
		out[p] = n
	}
	for p, n := range b {
		if n > out[p] {
			out[p] = n
		}
	}
	return out
}

func (e *Engine) ignored(p string, dir bool) (bool, error) {
	return index.Ignored(e.root, p, dir, index.ScanOptions{Ignore: append(append([]string{}, e.cfg.Ignore...), e.opts.Ignore...), Executable: e.opts.Executable})
}

func (e *Engine) fault(stage string) error {
	if e.opts.Fault != nil {
		return e.opts.Fault(stage)
	}
	return nil
}
func (e *Engine) download(ctx context.Context, m model.Manifest, peers []peer.ID) error {
	if err := e.cache.Ensure(ctx, m, 0, nil); err == nil {
		return nil
	}
	if task, ok := e.downloads[m.Hash]; ok {
		select {
		case <-task.done:
			if task.err != nil {
				e.cache.Unretain(task.manifest)
				delete(e.downloads, m.Hash)
			}
			return task.err
		default:
			return errPending
		}
	}
	active := 0
	for _, task := range e.downloads {
		select {
		case <-task.done:
		default:
			active++
		}
	}
	if active >= 2 {
		return errPending
	}
	if len(peers) == 0 {
		return errPending
	}
	downloadCtx, cancel := context.WithCancel(ctx)
	task := &download{done: make(chan struct{}), manifest: m, cancel: cancel}
	e.cache.Retain(m)
	e.downloads[m.Hash] = task
	e.downloadWG.Add(1)
	go func() {
		ctx := downloadCtx
		defer cancel()
		defer e.downloadWG.Done()
		defer e.signal()
		defer close(task.done)
		availability := make([][]bool, len(peers))
		for i, p := range peers {
			for offset := 0; offset < len(m.Blocks); offset += 256 {
				end := offset + 256
				if end > len(m.Blocks) {
					end = len(m.Blocks)
				}
				bits, err := e.node.BlockAvailability(ctx, p, m.Blocks[offset:end])
				if err != nil {
					availability[i] = nil
					break
				}
				availability[i] = append(availability[i], bits...)
			}
		}
		task.err = e.cache.EnsureAvailable(ctx, m, len(peers), availability, func(ctx context.Context, i int, h string, n int64) ([]byte, error) {
			data, err := e.node.FetchBlock(ctx, peers[i], h, n)
			if err == nil {
				counter, _ := e.sourceBytes.LoadOrStore(peers[i].String(), new(atomic.Uint64))
				counter.(*atomic.Uint64).Add(uint64(len(data)))
			}
			return data, err
		})
	}()
	return errPending
}

func clockContains(a, b model.Vector) bool {
	for p, n := range b {
		if a[p] < n {
			return false
		}
	}
	return true
}
func equalBaseline(a, b model.Baseline) bool {
	return a.Deleted == b.Deleted && a.SourceID == b.SourceID && a.SourcePath == b.SourcePath && a.Hash == b.Hash && a.Directory == b.Directory && a.Size == b.Size && a.Mtime == b.Mtime && maps.Equal(a.Clock, b.Clock)
}
