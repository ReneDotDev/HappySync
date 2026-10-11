package integration

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func directoryTree(root string) (map[string]bool, error) {
	result := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".HappySync" {
				return filepath.SkipDir
			}
			if path != root {
				relative, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				result[filepath.ToSlash(relative)] = true
			}
		}
		return nil
	})
	return result, err
}

func TestEmptyDirectoriesReplicateAndDeleteWhilePeerOffline(t *testing.T) {
	a, b := node(t), node(t)
	if err := os.MkdirAll(filepath.Join(a.root, "empty", "nested"), 0755); err != nil {
		t.Fatal(err)
	}
	a.start("", nil)
	b.start(a.key, []string{a.address})
	want := map[string]bool{"empty": true, "empty/nested": true}
	wait(t, 20*time.Second, "replicated empty directory hierarchy", func() bool {
		left, err := directoryTree(a.root)
		right, otherErr := directoryTree(b.root)
		return err == nil && otherErr == nil && reflect.DeepEqual(left, want) && reflect.DeepEqual(right, want)
	})
	b.stop()
	if err := os.RemoveAll(filepath.Join(a.root, "empty")); err != nil {
		t.Fatal(err)
	}
	wait(t, 10*time.Second, "persisted directory tombstones", func() bool { return persistedDeletion(a.root, "empty") && persistedDeletion(a.root, "empty/nested") })
	b.start("", []string{a.address})
	wait(t, 20*time.Second, "empty directory removal catches up without resurrection", func() bool {
		left, err := directoryTree(a.root)
		right, otherErr := directoryTree(b.root)
		return err == nil && otherErr == nil && len(left) == 0 && len(right) == 0
	})
	b.stop()
	b.start("", []string{a.address})
	if got, err := directoryTree(b.root); err != nil || len(got) != 0 {
		t.Fatal("restart resurrected removed empty directories")
	}
}

func TestCausallyNewerFileDirectoryTransitions(t *testing.T) {
	a, b := node(t), node(t)
	write(t, a.root, "shape", []byte("original file"))
	a.start("", nil)
	b.start(a.key, []string{a.address})
	converge(t, []*instance{a, b}, map[string][]byte{"shape": []byte("original file")})
	if err := os.Remove(filepath.Join(a.root, "shape")); err != nil {
		t.Fatal(err)
	}
	write(t, a.root, "shape/nested.txt", []byte("directory replacement"))
	converge(t, []*instance{a, b}, map[string][]byte{"shape/nested.txt": []byte("directory replacement")})
	// A settled shared directory is now replaced by a file on the other peer.
	if err := os.RemoveAll(filepath.Join(b.root, "shape")); err != nil {
		t.Fatal(err)
	}
	write(t, b.root, "shape", []byte("file replacement"))
	converge(t, []*instance{a, b}, map[string][]byte{"shape": []byte("file replacement")})
	b.stop()
	b.start("", []string{a.address})
	converge(t, []*instance{a, b}, map[string][]byte{"shape": []byte("file replacement")})
}

func TestComprehensiveScanDetectsSameSizeSameMtimeEdit(t *testing.T) {
	a, b := node(t), node(t)
	write(t, a.root, "same.txt", []byte("before"))
	a.start("", nil)
	b.start(a.key, []string{a.address})
	converge(t, []*instance{a, b}, map[string][]byte{"same.txt": []byte("before")})
	path := filepath.Join(b.root, "same.txt")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	write(t, b.root, "same.txt", []byte("edited"))
	if err = os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	converge(t, []*instance{a, b}, map[string][]byte{"same.txt": []byte("edited")})
}

func conflictFiles(t *testing.T, nodes []*instance, values []string) map[string][]byte {
	t.Helper()
	result := map[string][]byte{}
	wait(t, 35*time.Second, "preserved conflict content hashes", func() bool {
		files, err := tree(nodes[0].root)
		if err != nil || len(files) != len(values) {
			return false
		}
		for _, n := range nodes[1:] {
			other, err := tree(n.root)
			if err != nil || !reflect.DeepEqual(files, other) {
				return false
			}
		}
		got := map[string]bool{}
		for p := range files {
			data, err := os.ReadFile(filepath.Join(nodes[0].root, p))
			if err != nil {
				return false
			}
			got[string(data)] = true
			result[p] = data
		}
		for _, value := range values {
			if !got[value] {
				return false
			}
		}
		return true
	})
	return result
}

func TestConflictCopiesAreOrdinaryReplicatedFiles(t *testing.T) {
	a, b := node(t), node(t)
	write(t, a.root, "shared.txt", []byte("baseline"))
	a.start("", nil)
	b.start(a.key, []string{a.address})
	converge(t, []*instance{a, b}, map[string][]byte{"shared.txt": []byte("baseline")})
	a.stop()
	b.stop()
	write(t, a.root, "shared.txt", []byte("A edit"))
	write(t, b.root, "shared.txt", []byte("B edit"))
	a.start("", nil)
	b.start("", []string{a.address})
	expected := conflictFiles(t, []*instance{a, b}, []string{"A edit", "B edit"})
	var conflict string
	for p := range expected {
		if strings.Contains(p, ".happysync-conflict-") {
			conflict = p
		}
	}
	if conflict == "" {
		t.Fatal("no deterministic conflict file")
	}
	write(t, a.root, "shared.txt", []byte("causally newer original"))
	expected["shared.txt"] = []byte("causally newer original")
	converge(t, []*instance{a, b}, expected)
	write(t, b.root, conflict, []byte("user edited conflict"))
	expected[conflict] = []byte("user edited conflict")
	converge(t, []*instance{a, b}, expected)
	if err := os.Remove(filepath.Join(a.root, conflict)); err != nil {
		t.Fatal(err)
	}
	delete(expected, conflict)
	converge(t, []*instance{a, b}, expected)
	write(t, b.root, "shared.txt", []byte("another newer original"))
	expected["shared.txt"] = []byte("another newer original")
	converge(t, []*instance{a, b}, expected)
	a.stop()
	a.start("", []string{b.address})
	converge(t, []*instance{a, b}, expected)
	if !absent(a.root, conflict) || !absent(b.root, conflict) {
		t.Fatal("deleted ordinary conflict resurrected")
	}
}

func TestThreeProcessesConcurrentDeleteAndEdits(t *testing.T) {
	a, b, c := node(t), node(t), node(t)
	write(t, a.root, "shared.txt", []byte("baseline"))
	a.start("", nil)
	b.start(a.key, []string{a.address})
	c.start(a.key, []string{a.address, b.address})
	converge(t, []*instance{a, b, c}, map[string][]byte{"shared.txt": []byte("baseline")})
	a.stop()
	b.stop()
	c.stop()
	if err := os.Remove(filepath.Join(a.root, "shared.txt")); err != nil {
		t.Fatal(err)
	}
	write(t, b.root, "shared.txt", []byte("B offline edit"))
	write(t, c.root, "shared.txt", []byte("C offline edit"))
	a.start("", nil)
	b.start("", []string{a.address})
	c.start("", []string{a.address, b.address})
	expected := conflictFiles(t, []*instance{a, b, c}, []string{"B offline edit", "C offline edit"})
	for _, n := range []*instance{a, b, c} {
		if !absent(n.root, "shared.txt") {
			t.Fatal("concurrent deletion did not retain original tombstone")
		}
	}
	c.stop()
	c.start("", []string{a.address, b.address})
	converge(t, []*instance{a, b, c}, expected)
}

func TestExecutableInRootIsIgnored(t *testing.T) {
	a, b := node(t), node(t)
	data, err := os.ReadFile(a.binary)
	if err != nil {
		t.Fatal(err)
	}
	a.binary = filepath.Join(a.root, filepath.Base(a.binary))
	if err = os.WriteFile(a.binary, data, 0755); err != nil {
		t.Fatal(err)
	}
	write(t, a.root, "visible.txt", []byte("visible"))
	a.start("", nil)
	b.start(a.key, []string{a.address})
	wait(t, 20*time.Second, "visible content from root executable", func() bool { return hasFile(b.root, "visible.txt", []byte("visible")) })
	time.Sleep(2 * time.Second)
	if !absent(b.root, filepath.Base(a.binary)) {
		t.Fatal("running executable synchronized")
	}
	current, err := os.ReadFile(a.binary)
	if err != nil || !bytes.Equal(current, data) {
		t.Fatal("running executable modified")
	}
}

func TestDefaultCommandAndRelativeFolderOverride(t *testing.T) {
	n := node(t)
	launchDir := t.TempDir()
	selected := filepath.Join(launchDir, "selected")
	if err := os.Mkdir(selected, 0755); err != nil {
		t.Fatal(err)
	}
	selected, err := filepath.EvalSymlinks(selected)
	if err != nil {
		t.Fatal(err)
	}
	n.root = selected
	write(t, launchDir, "outside.txt", []byte("outside chosen root"))
	out, err := n.command(launchDir, "", "--background", "--folder", "selected", "--no-mdns", "--no-dht", "--listen", "/ip4/127.0.0.1/tcp/0")
	if err != nil {
		t.Fatalf("default subcommand with relative folder: %v", err)
	}
	n.running = true
	if !strings.Contains(out, "Folder: "+selected) || !invitationPattern.MatchString(out) {
		t.Fatal("startup did not report canonical selected root and invitation")
	}
	if !absent(launchDir, ".HappySync") {
		t.Fatal("relative folder initialized launcher root")
	}
	if _, err = n.command(launchDir, "", "status", "--folder", "selected", "--json"); err != nil {
		t.Fatalf("relative folder status: %v", err)
	}
	if s, err := n.status(); err != nil || s.PID == 0 || s.State == "stopped" {
		t.Fatal("selected root not ready")
	}
	n.stop()
}
