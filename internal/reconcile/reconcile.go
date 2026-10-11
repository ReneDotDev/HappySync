// Package reconcile computes a deterministic, loss-preserving projection of
// replicated causal records. It performs no filesystem operations.
package reconcile

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/happysync/happysync/internal/index"
	"github.com/happysync/happysync/internal/model"
	"github.com/zeebo/blake3"
)

// ClockFor joins all observed history for a path, including retained tombstones.
func ClockFor(records []model.Record, p string) model.Vector {
	v := model.Vector{}
	for _, r := range records {
		if r.Path == p || ConflictPath(r.Path, r.ID) == p {
			join(v, r.Clock)
		}
	}
	return v
}
func join(a, b model.Vector) {
	for k, v := range b {
		if v > a[k] {
			a[k] = v
		}
	}
}
func clone(v model.Vector) model.Vector { r := model.Vector{}; join(r, v); return r }

// Dominates reports a strict causal ordering, independent of wall clocks.
func Dominates(a, b model.Vector) bool {
	strict := false
	for k, v := range b {
		if a[k] < v {
			return false
		}
		if a[k] > v {
			strict = true
		}
	}
	for k, v := range a {
		if v > b[k] {
			strict = true
		}
	}
	return strict
}

func NewRecord(p, origin string, counter uint64, clock model.Vector, manifest model.Manifest, deleted, directory, imported bool, mtime ...int64) model.Record {
	clock = clone(clock)
	clock[origin] = counter
	if manifest.Blocks != nil {
		manifest.Blocks = append([]model.Block{}, manifest.Blocks...)
	}
	r := model.Record{Path: p, Origin: origin, Counter: counter, Clock: clock, Manifest: manifest, Deleted: deleted, Directory: directory, Imported: imported}
	if len(mtime) > 0 && !deleted && !directory {
		r.Mtime = mtime[0]
	}
	r.ID = recordID(r)
	return r
}

// IDs bind every immutable operation field, including the causal vector.
func recordID(r model.Record) string {
	r.ID = ""
	b, _ := json.Marshal(r)
	h := blake3.Sum256(b)
	return hex.EncodeToString(h[:])
}
func validHash(s string) bool {
	if len(s) != 64 {
		return false
	}
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && strings.ToLower(s) == s
}

// ValidateManifest bounds all lengths before transfer allocation.
func ValidateManifest(m model.Manifest) error {
	if !validHash(m.Hash) || m.Size < 0 || m.Size > 1<<50 {
		return errors.New("invalid manifest digest or length")
	}
	if len(m.Blocks) > 1<<20 {
		return errors.New("manifest block count exceeds limit")
	}
	expected := m.Size / int64(model.BlockSize)
	if m.Size%int64(model.BlockSize) != 0 {
		expected++
	}
	if int64(len(m.Blocks)) != expected {
		return errors.New("manifest block count disagrees with length")
	}
	var total int64
	for i, b := range m.Blocks {
		if !validHash(b.Hash) || b.Size <= 0 || b.Size > model.BlockSize {
			return errors.New("invalid block")
		}
		if i < len(m.Blocks)-1 && b.Size != model.BlockSize {
			return errors.New("short nonfinal block")
		}
		total += b.Size
	}
	if total != m.Size {
		return errors.New("manifest block lengths disagree")
	}
	if m.Size == 0 {
		h := blake3.Sum256(nil)
		if m.Hash != hex.EncodeToString(h[:]) {
			return errors.New("invalid empty content hash")
		}
	}
	return nil
}
func ValidateRecord(r model.Record) error {
	if err := index.ValidatePath(r.Path); err != nil {
		return err
	}
	if r.Origin == "" || len(r.Origin) > 256 || r.Counter == 0 || r.Counter == ^uint64(0) || len(r.Clock) == 0 || len(r.Clock) > 1024 || r.Clock[r.Origin] != r.Counter {
		return errors.New("invalid operation origin or causal vector")
	}
	for k, v := range r.Clock {
		if k == "" || len(k) > 256 || v == 0 || v == ^uint64(0) {
			return errors.New("invalid causal vector component")
		}
	}
	if r.Deleted || r.Directory {
		if r.Manifest.Hash != "" || r.Manifest.Size != 0 || len(r.Manifest.Blocks) != 0 {
			return errors.New("nonfile record carries a manifest")
		}
	} else if err := ValidateManifest(r.Manifest); err != nil {
		return err
	}
	if r.ID != recordID(r) {
		return errors.New("operation ID disagrees with content")
	}
	return nil
}
func sameOperation(a, b model.Record) bool {
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	return string(aj) == string(bj)
}
func signature(r model.Record) string {
	if r.Deleted {
		return "deleted"
	}
	if r.Directory {
		return "directory"
	}
	return "file:" + r.Manifest.Hash
}

// ConflictPath is bounded and derived from an immutable event, so retransmission
// and merge order cannot produce new conflict copies.
func ConflictPath(p, id string) string {
	if !validHash(id) {
		sum := blake3.Sum256([]byte(id))
		id = hex.EncodeToString(sum[:])
	}
	dir, base := path.Split(p)
	suffix := ".happysync-conflict-" + id
	for len(base)+len(suffix) > 255 {
		_, size := lastRune(base)
		base = base[:len(base)-size]
	}
	return dir + base + suffix
}
func lastRune(s string) (rune, int) {
	r := []rune(s)
	if len(r) == 0 {
		return 0, 0
	}
	last := r[len(r)-1]
	return last, len(string(last))
}

// Resolve includes deleted originals alongside preserved conflict content.
// Input records are never mutated and full history must be retained by storage.
func Resolve(records []model.Record) ([]model.Entry, error) {
	groups := map[string][]model.Record{}
	ids := map[string]model.Record{}
	for _, r := range records {
		if err := ValidateRecord(r); err != nil {
			return nil, fmt.Errorf("invalid record: %w", err)
		}
		if previous, ok := ids[r.ID]; ok {
			if !sameOperation(previous, r) {
				return nil, errors.New("operation ID collision")
			}
			continue
		}
		r.Clock = clone(r.Clock)
		ids[r.ID] = r
	}
	for _, r := range ids {
		groups[r.Path] = append(groups[r.Path], r)
	}
	var output []model.Entry
	for p, history := range groups {
		output = append(output, project(p, history)...)
	}
	// Conflict copies are editable/deletable regular replicated paths. Explicit
	// operations on those paths causally supersede their generated source copy.
	var err error
	output, err = mergeProjected(output)
	if err != nil {
		return nil, err
	}
	// A live descendant requires directory ancestors. These indexes keep this
	// pass linear in total path depth rather than quadratic in the file count.
	liveAncestors := map[string]bool{}
	for _, e := range output {
		if !e.Record.Deleted {
			for p := path.Dir(e.Path); p != "."; p = path.Dir(p) {
				liveAncestors[p] = true
			}
		}
	}
	moves := map[string]string{}
	for _, e := range output {
		if liveAncestors[e.Path] && e.Record.Deleted {
			moves[e.Path] = ConflictPath(e.Path, e.Record.ID)
		}
	}
	movePath := func(p string) string {
		parts := strings.Split(p, "/")
		destination := ""
		for n, part := range parts {
			original := strings.Join(parts[:n+1], "/")
			if moved, ok := moves[original]; ok && n < len(parts)-1 {
				_, base := path.Split(moved)
				part = base
			}
			if n > 0 {
				destination += "/"
			}
			destination += part
		}
		return destination
	}
	for i := range output {
		original := output[i].Path
		// Original child tombstones must still remove old physical descendants
		// before the original directory is cleaned up. Only preserved live bytes
		// move into the conflict subtree.
		if !output[i].Record.Deleted {
			output[i].Path = movePath(original)
		}
		if liveAncestors[original] && !output[i].Record.Deleted && !output[i].Record.Directory {
			output[i].Path = ConflictPath(output[i].Path, output[i].Record.ID)
		}
	}
	output, err = mergeProjected(output)
	if err != nil {
		return nil, err
	}
	// Explicit paths may occupy the conflict namespace. Never silently overwrite
	// one: fail visibly while keeping every source event and original byte intact.
	seen := map[string]string{}
	for _, entry := range output {
		if err := index.ValidatePath(entry.Path); err != nil {
			return nil, err
		}
		key := index.FoldPath(entry.Path)
		if old, ok := seen[key]; ok {
			return nil, fmt.Errorf("portable projected path collision: %q and %q", old, entry.Path)
		}
		seen[key] = entry.Path
	}
	sort.Slice(output, func(i, j int) bool { return output[i].Path < output[j].Path })
	return output, nil
}

func project(p string, history []model.Record) []model.Entry {
	var frontier []model.Record
	for i, r := range history {
		suppressed := false
		for j, newer := range history {
			if i != j && (!newer.Imported || r.Imported) && Dominates(newer.Clock, r.Clock) {
				suppressed = true
				break
			}
		}
		if !suppressed {
			frontier = append(frontier, r)
		}
	}
	sort.Slice(frontier, func(i, j int) bool {
		// A materialized copy is an ordinary replicated object. Prefer its
		// explicit path over the equal-content generated projection it records.
		if (frontier[i].Path == p) != (frontier[j].Path == p) {
			return frontier[i].Path == p
		}
		if frontier[i].Imported != frontier[j].Imported {
			return !frontier[i].Imported
		}
		return frontier[i].ID < frontier[j].ID
	})
	coalesced := map[string]model.Record{}
	var keys []string
	for _, r := range frontier {
		key := signature(r)
		if _, exists := coalesced[key]; !exists {
			keys = append(keys, key)
			coalesced[key] = r
		}
	}
	sort.Slice(keys, func(i, j int) bool { return coalesced[keys[i]].ID < coalesced[keys[j]].ID })
	winner := ""
	if _, exists := coalesced["deleted"]; exists {
		winner = "deleted"
	} else if len(keys) > 0 {
		winner = keys[0]
	}
	var output []model.Entry
	for _, key := range keys {
		r := coalesced[key]
		destination := p
		if key != winner {
			destination = ConflictPath(p, r.ID)
		}
		output = append(output, model.Entry{Path: destination, Record: r})
	}
	return output
}
func mergeProjected(input []model.Entry) ([]model.Entry, error) {
	// Bounded repeated passes also handle a user occupying a generated conflict
	// name. A hostile chain is refused visibly instead of risking an overwrite.
	for attempt := 0; attempt < 16; attempt++ {
		groups := map[string][]model.Record{}
		duplicate := false
		for _, e := range input {
			if len(groups[e.Path]) > 0 {
				duplicate = true
			}
			groups[e.Path] = append(groups[e.Path], e.Record)
		}
		if !duplicate {
			return input, nil
		}
		var next []model.Entry
		for p, records := range groups {
			next = append(next, project(p, records)...)
		}
		input = next
	}
	return nil, errors.New("conflict path collision chain exceeds limit")
}
