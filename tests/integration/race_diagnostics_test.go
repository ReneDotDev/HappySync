//go:build race

package integration

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// This helper deliberately races only in a subprocess. Its report verifies the
// real sanitizer parser and diagnostic destination, including Windows drive
// colons and directories containing spaces.
func TestRaceDiagnosticsHelper(t *testing.T) {
	if os.Getenv("HAPPYSYNC_RACE_PATH_HELPER") != "1" {
		return
	}
	var value int
	var workers sync.WaitGroup
	start := make(chan struct{})
	for range 2 {
		workers.Go(func() {
			<-start
			for range 1000 {
				value++
			}
		})
	}
	close(start)
	workers.Wait()
	t.Log(value)
}

func TestRaceDiagnosticsPathWithSpacesAndColon(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "race diagnostic directory")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	name := "report"
	if runtime.GOOS != "windows" {
		// Windows contributes its colon in the absolute drive prefix.
		name = "drive:report"
	}
	prefix := filepath.Join(dir, name)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestRaceDiagnosticsHelper$")
	cmd.Env = append(os.Environ(), "HAPPYSYNC_RACE_PATH_HELPER=1", "GORACE="+raceOptions(prefix)+" exitcode=66 halt_on_error=1")
	out, err := cmd.CombinedOutput()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 66 {
		t.Fatalf("race helper did not report expected race: %v: %s", err, out)
	}
	reports, err := filepath.Glob(prefix + ".*")
	if err != nil || len(reports) != 1 {
		t.Fatalf("sanitizer report missing at requested path: files=%v error=%v stderr=%s", reports, err, out)
	}
	report, err := os.ReadFile(reports[0])
	if err != nil || !bytes.Contains(report, []byte("WARNING: DATA RACE")) || !bytes.Contains(report, []byte("TestRaceDiagnosticsHelper")) {
		t.Fatalf("sanitizer diagnostic did not record helper race: error=%v report=%s", err, report)
	}
}
