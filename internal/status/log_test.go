package status

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRotatingRedactedLogs(t *testing.T) {
	p := filepath.Join(t.TempDir(), "worker.log")
	w, err := NewRotatingWriter(p, 64, 2)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err = w.Write([]byte("event hsync1:ABCDEFGHIJKLMNOPQRSTUVWXYZ\n")); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(p + "*")
	if len(files) > 3 {
		t.Fatal("unbounded archives")
	}
	for _, f := range files {
		b, _ := os.ReadFile(f)
		if len(b) > 64 || strings.Contains(string(b), "hsync1:") {
			t.Fatal("unbounded or unredacted log")
		}
	}
}
