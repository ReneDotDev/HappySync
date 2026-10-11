package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWindowsTargetsFailBeforeCreatingArtifacts(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	for _, selection := range []string{"windows/amd64", "windows/arm64", "linux/amd64,windows/amd64"} {
		t.Run(selection, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "artifacts")
			if err := run([]string{"--version", "v0.1.0-alpha.1", "--targets", selection, "--out", out}); err == nil {
				t.Fatal("unsupported Windows selection started a release build")
			}
			if _, err := os.Stat(out); !os.IsNotExist(err) {
				t.Fatalf("rejected target selection changed the output directory: %v", err)
			}
		})
	}
}
