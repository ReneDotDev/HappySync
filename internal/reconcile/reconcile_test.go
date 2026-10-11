package reconcile

import (
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/happysync/happysync/internal/model"
	"github.com/zeebo/blake3"
)

func manifest(content string) model.Manifest {
	hash := blake3.Sum256([]byte(content))
	m := model.Manifest{Hash: hex.EncodeToString(hash[:]), Size: int64(len(content))}
	if len(content) > 0 {
		m.Blocks = []model.Block{{Hash: m.Hash, Size: m.Size}}
	}
	return m
}
func file(p, peer, content string, counter uint64, clock model.Vector) model.Record {
	return NewRecord(p, peer, counter, clock, manifest(content), false, false, false)
}
func deletion(p, peer string, counter uint64, clock model.Vector) model.Record {
	return NewRecord(p, peer, counter, clock, model.Manifest{}, true, false, false)
}
func resolve(t *testing.T, records ...model.Record) []model.Entry {
	t.Helper()
	entries, err := Resolve(records)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}
func assertContents(t *testing.T, entries []model.Entry, contents ...string) {
	t.Helper()
	have := map[string]bool{}
	for _, e := range entries {
		if !e.Record.Deleted && !e.Record.Directory {
			have[e.Record.Manifest.Hash] = true
		}
	}
	if len(have) != len(contents) {
		t.Fatalf("content count %d != %d: %+v", len(have), len(contents), entries)
	}
	for _, s := range contents {
		if !have[manifest(s).Hash] {
			t.Fatalf("content lost: %q", s)
		}
	}
}
func TestCausalEditAndDeletion(t *testing.T) {
	initial := file("file", "a", "old", 1, nil)
	edit := file("file", "b", "new", 1, initial.Clock)
	entries := resolve(t, initial, edit)
	if len(entries) != 1 || entries[0].Record.ID != edit.ID {
		t.Fatalf("newer not selected %+v", entries)
	}
	tombstone := deletion("file", "a", 2, edit.Clock)
	entries = resolve(t, initial, edit, tombstone)
	if len(entries) != 1 || !entries[0].Record.Deleted {
		t.Fatal("causal deletion not honored")
	}
	recreated := file("file", "c", "recreated", 1, tombstone.Clock)
	entries = resolve(t, initial, edit, tombstone, recreated)
	assertContents(t, entries, "recreated")
	if entries[0].Path != "file" {
		t.Fatal("recreation misplaced")
	}
}
func TestConcurrentEditsAndIdempotence(t *testing.T) {
	a := file("file.txt", "a", "alpha", 1, nil)
	b := file("file.txt", "b", "beta", 1, nil)
	entries := resolve(t, a, b, a, b)
	assertContents(t, entries, "alpha", "beta")
	if len(entries) != 2 {
		t.Fatal("duplicate copies")
	}
	for _, e := range entries {
		if e.Path != "file.txt" && e.Path != ConflictPath("file.txt", e.Record.ID) {
			t.Fatal("unstable copy path")
		}
	}
	reverse := resolve(t, b, a)
	if !reflect.DeepEqual(entries, reverse) {
		t.Fatal("merge order matters")
	}
}
func TestIdenticalConcurrentContentCoalescesClockWithoutMutation(t *testing.T) {
	a := file("f", "a", "same", 1, nil)
	b := file("f", "b", "same", 2, nil)
	before, _ := json.Marshal([]model.Record{a, b})
	entries := resolve(t, a, b)
	after, _ := json.Marshal([]model.Record{a, b})
	if string(before) != string(after) {
		t.Fatal("input mutated")
	}
	if len(entries) != 1 || ClockFor([]model.Record{a, b}, "f")["a"] != 1 || ClockFor([]model.Record{a, b}, "f")["b"] != 2 {
		t.Fatalf("not coalesced %+v", entries)
	}
	if err := ValidateRecord(entries[0].Record); err != nil {
		t.Fatal(err)
	}
	newer := file("f", "c", "next", 1, ClockFor([]model.Record{a, b}, "f"))
	assertContents(t, resolve(t, a, b, newer), "next")
}
func TestConcurrentDeletePreservesEditAndImportedBytes(t *testing.T) {
	initial := file("f", "a", "old", 1, nil)
	gone := deletion("f", "a", 2, initial.Clock)
	edited := file("f", "b", "edited", 1, initial.Clock)
	entries := resolve(t, initial, gone, edited)
	assertContents(t, entries, "edited")
	if len(entries) != 2 {
		t.Fatalf("entries %+v", entries)
	}
	for _, e := range entries {
		if e.Path == "f" && !e.Record.Deleted {
			t.Fatal("original resurrected")
		}
	}
	imported := NewRecord("f", "c", 1, gone.Clock, manifest("imported"), false, false, true)
	entries = resolve(t, initial, gone, imported)
	assertContents(t, entries, "imported")
	for _, e := range entries {
		if e.Path == "f" && !e.Record.Deleted {
			t.Fatal("import dominates tombstone")
		}
	}
}
func TestPermutationConvergence(t *testing.T) {
	base := file("f", "base", "initial", 1, nil)
	a := file("f", "a", "a", 1, base.Clock)
	b := file("f", "b", "b", 1, base.Clock)
	c := file("f", "c", "c", 1, base.Clock)
	d := deletion("f", "d", 1, base.Clock)
	input := []model.Record{base, a, b, c, d, a}
	want := resolve(t, input...)
	assertContents(t, want, "a", "b", "c")
	rng := rand.New(rand.NewSource(77))
	for i := 0; i < 1000; i++ {
		rng.Shuffle(len(input), func(i, j int) { input[i], input[j] = input[j], input[i] })
		have := resolve(t, input...)
		if !reflect.DeepEqual(have, want) {
			t.Fatalf("order changed projection on iteration %d", i)
		}
	}
}
func TestDirectoryFileAndDeleteDescendantSafety(t *testing.T) {
	d := NewRecord("dir", "a", 1, nil, model.Manifest{}, false, true, false)
	f := file("dir", "b", "file", 1, nil)
	child := file("dir/child", "a", "child", 2, d.Clock)
	entries := resolve(t, d, f, child)
	assertContents(t, entries, "file", "child")
	for _, e := range entries {
		if e.Path == "dir" && !e.Record.Directory {
			t.Fatal("file blocks live descendants")
		}
	}
	gone := NewRecord("dir", "c", 1, nil, model.Manifest{}, true, true, false)
	entries = resolve(t, gone, child)
	assertContents(t, entries, "child")
	for _, e := range entries {
		if !e.Record.Deleted && strings.HasPrefix(e.Path, "dir/") {
			t.Fatal("child resurrects deleted parent")
		}
	}
}
func TestCollisionFailsVisibly(t *testing.T) {
	a := file("Case", "a", "a", 1, nil)
	b := file("case", "b", "b", 1, nil)
	if _, err := Resolve([]model.Record{a, b}); err == nil {
		t.Fatal("case collision accepted")
	}
}
func TestMalformedRecordsRejected(t *testing.T) {
	valid := file("f", "a", "okay", 1, nil)
	invalid := []model.Record{valid, valid, valid, valid, valid}
	invalid[0].Path = "../escape"
	invalid[1].Counter = 0
	invalid[2].Manifest.Blocks[0].Size = 1
	invalid[3].Clock = model.Vector{"a": 0}
	invalid[4].ID = "fake"
	for _, r := range invalid {
		if err := ValidateRecord(r); err == nil {
			t.Fatal("malformed record accepted")
		}
	}
}

func TestGeneratedConflictCopiesAreEditableAndDeletable(t *testing.T) {
	a := file("f", "a", "alpha", 1, nil)
	b := file("f", "b", "beta", 1, nil)
	history := []model.Record{a, b}
	entries := resolve(t, history...)
	var conflict model.Entry
	for _, e := range entries {
		if e.Path != "f" {
			conflict = e
		}
	}
	if conflict.Path == "" {
		t.Fatal("missing conflict")
	}
	clock := ClockFor(history, conflict.Path)
	if !reflect.DeepEqual(clock, conflict.Record.Clock) {
		t.Fatal("missing causal source clock")
	}
	gone := deletion(conflict.Path, "c", 1, clock)
	entries = resolve(t, a, b, gone)
	for _, e := range entries {
		if e.Path == conflict.Path && !e.Record.Deleted {
			t.Fatal("conflict deletion resurrected source")
		}
	}
	edit := file(conflict.Path, "c", "edited", 1, clock)
	entries = resolve(t, a, b, edit)
	for _, e := range entries {
		if e.Path == conflict.Path && e.Record.Manifest.Hash != manifest("edited").Hash {
			t.Fatal("conflict edit lost")
		}
	}
	if len(entries) != 2 {
		t.Fatalf("extra generated copies %+v", entries)
	}
}

func TestDeletedDirectoryKeepsOriginalChildTombstones(t *testing.T) {
	gone := NewRecord("dir", "a", 2, model.Vector{"a": 1}, model.Manifest{}, true, true, false)
	oldChild := deletion("dir/old", "a", 3, gone.Clock)
	edited := file("dir/new", "b", "concurrent", 1, nil)
	entries := resolve(t, gone, oldChild, edited)
	found := false
	for _, e := range entries {
		if e.Path == "dir/old" && e.Record.Deleted {
			found = true
		}
		if !e.Record.Deleted && strings.HasPrefix(e.Path, "dir/") {
			t.Fatal("live child revived deleted original")
		}
	}
	if !found {
		t.Fatal("old descendant tombstone moved away from original location")
	}
}

func TestMaterializedConflictSurvivesMainReplacement(t *testing.T) {
	a := file("f", "a", "alpha", 1, nil)
	b := file("f", "b", "beta", 1, nil)
	history := []model.Record{a, b}
	entries := resolve(t, history...)
	var conflict model.Entry
	for _, e := range entries {
		if e.Path != "f" {
			conflict = e
		}
	}
	source := conflict.Record
	copy := NewRecord(conflict.Path, source.Origin, source.Counter, source.Clock, source.Manifest, false, source.Directory, false)
	history = append(history, copy, copy)
	entries = resolve(t, history...)
	if len(entries) != 2 {
		t.Fatalf("materialization creates duplicate: %+v", entries)
	}
	for _, e := range entries {
		if e.Path == conflict.Path && e.Record.ID != copy.ID {
			t.Fatal("materialized copy not preferred as ordinary record")
		}
	}
	replacement := file("f", "c", "replacement", 1, ClockFor(history, "f"))
	history = append(history, replacement)
	entries = resolve(t, history...)
	assertContents(t, entries, "replacement", map[string]string{manifest("alpha").Hash: "alpha", manifest("beta").Hash: "beta"}[source.Manifest.Hash])
	var clock model.Vector
	for _, e := range entries {
		if e.Path == conflict.Path {
			clock = e.Record.Clock
		}
	}
	if clock == nil {
		t.Fatal("ordinary conflict disappeared after main superseded")
	}
	deleted := deletion(conflict.Path, "c", 2, clock)
	entries = resolve(t, append(history, deleted)...)
	assertContents(t, entries, "replacement")
}
