// Command release builds portable, reproducible binaries without publishing them.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
)

const defaultTargets = "linux/amd64,linux/arm64,darwin/amd64,darwin/arm64"

type target struct{ OS, Arch string }

func parseTargets(value string) ([]target, error) {
	allowed := map[string]bool{}
	for _, pair := range strings.Split(defaultTargets, ",") {
		allowed[pair] = true
	}
	seen := map[string]bool{}
	var targets []target
	for _, pair := range strings.Split(value, ",") {
		if !allowed[pair] || seen[pair] {
			return nil, errors.New("targets must be unique supported OS/architecture pairs")
		}
		seen[pair] = true
		parts := strings.Split(pair, "/")
		targets = append(targets, target{parts[0], parts[1]})
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].OS+"/"+targets[i].Arch < targets[j].OS+"/"+targets[j].Arch
	})
	return targets, nil
}

type artifact struct {
	File   string `json:"file"`
	GOOS   string `json:"goos"`
	GOARCH string `json:"goarch"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

type metadata struct {
	Version     string     `json:"version"`
	GoVersion   string     `json:"go_version"`
	CGOEnabled  bool       `json:"cgo_enabled"`
	BuildFlags  []string   `json:"build_flags"`
	GoModSHA256 string     `json:"go_mod_sha256"`
	GoSumSHA256 string     `json:"go_sum_sha256"`
	Artifacts   []artifact `json:"artifacts"`
}

func digest(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	size, err := io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), size, err
}

func buildEnv(t target) []string {
	var env []string
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		switch strings.ToUpper(name) {
		case "GOOS", "GOARCH", "CGO_ENABLED", "GOFLAGS", "GOAMD64", "GOARM64":
			continue
		}
		env = append(env, entry)
	}
	return append(env, "GOOS="+t.OS, "GOARCH="+t.Arch, "CGO_ENABLED=0", "GOFLAGS=", "GOAMD64=v1", "GOARM64=v8.0")
}

func run(args []string) error {
	f := flag.NewFlagSet("release", flag.ContinueOnError)
	version := f.String("version", "", "release version, e.g. v0.1.0")
	selection := f.String("targets", defaultTargets, "comma-separated OS/architecture pairs")
	output := f.String("out", "dist", "artifact directory")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if !regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9][A-Za-z0-9.-]*)?$`).MatchString(*version) {
		return errors.New("version must be a safe vMAJOR.MINOR.PATCH[-prerelease] string")
	}
	targets, err := parseTargets(*selection)
	if err != nil {
		return err
	}
	if _, err = os.Stat("cmd/HappySync/main.go"); err != nil {
		return errors.New("run release from the HappySync repository root")
	}
	if err = os.MkdirAll(*output, 0755); err != nil {
		return err
	}
	flags := []string{"-mod=readonly", "-trimpath", "-buildvcs=false", "-ldflags=-s -w -X main.version=" + *version}
	m := metadata{Version: *version, GoVersion: runtime.Version(), BuildFlags: flags}
	if m.GoModSHA256, _, err = digest("go.mod"); err != nil {
		return err
	}
	if m.GoSumSHA256, _, err = digest("go.sum"); err != nil {
		return err
	}
	var checksums strings.Builder
	for _, t := range targets {
		name := "HappySync-" + *version + "-" + t.OS + "-" + t.Arch
		// Publish each file only after a complete successful build. A failed
		// build must not replace a previously reviewable executable.
		stage, err := os.CreateTemp(*output, ".build-*")
		if err != nil {
			return err
		}
		stagePath := stage.Name()
		if err = stage.Close(); err != nil {
			return err
		}
		buildArgs := append(append([]string{"build"}, flags...), "-o", stagePath, "./cmd/HappySync")
		cmd := exec.Command("go", buildArgs...)
		cmd.Env = buildEnv(t)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		fmt.Println("Building", t.OS+"/"+t.Arch)
		if err = cmd.Run(); err != nil {
			_ = os.Remove(stagePath)
			return fmt.Errorf("build %s/%s: %w", t.OS, t.Arch, err)
		}
		sum, size, err := digest(stagePath)
		if err != nil {
			_ = os.Remove(stagePath)
			return err
		}
		if err = os.Rename(stagePath, filepath.Join(*output, name)); err != nil {
			_ = os.Remove(stagePath)
			return err
		}
		m.Artifacts = append(m.Artifacts, artifact{name, t.OS, t.Arch, sum, size})
		fmt.Fprintf(&checksums, "%s  %s\n", sum, name)
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(*output, "build.json"), append(b, '\n'), 0644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(*output, "SHA256SUMS"), []byte(checksums.String()), 0644)
}

func main() {
	if err := run(os.Args[1:]); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "release:", err)
		os.Exit(1)
	}
}
