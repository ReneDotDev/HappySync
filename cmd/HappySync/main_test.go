package main

import (
	"crypto/rand"
	"strings"
	"testing"

	"github.com/happysync/happysync/internal/config"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestBandwidthPreferencesSurviveBackgroundAndRestart(t *testing.T) {
	root := t.TempDir()
	cfg, err := config.Init(root, "")
	if err != nil {
		t.Fatal(err)
	}
	key, identity := cfg.Key, cfg.Identity
	_, launcher, err := parse([]string{"--folder", root, "--background", "--upload-rate", "1048576", "--download-rate", "2097152"})
	if err != nil {
		t.Fatal(err)
	}
	_, worker, err := parse(workerArgs(launcher))
	if err != nil || !worker.worker || !worker.uploadRateSet || !worker.downloadRateSet {
		t.Fatalf("background worker lost explicit bandwidth preferences: %v", err)
	}
	applyBandwidthPreferences(cfg, worker)
	if err = cfg.Save(); err != nil {
		t.Fatal(err)
	}
	cfg, err = config.Load(root)
	if err != nil || cfg.UploadBytesPerSecond != 1048576 || cfg.DownloadBytesPerSecond != 2097152 {
		t.Fatalf("stored bandwidth limits did not survive reopening: %v", err)
	}
	_, restart, err := parse([]string{"--folder", root})
	if err != nil {
		t.Fatal(err)
	}
	applyBandwidthPreferences(cfg, restart)
	if err = cfg.Save(); err != nil {
		t.Fatal(err)
	}
	cfg, err = config.Load(root)
	if err != nil || cfg.UploadBytesPerSecond != 1048576 || cfg.DownloadBytesPerSecond != 2097152 {
		t.Fatalf("omitted flags reset stored bandwidth limits: %v", err)
	}
	_, launcher, err = parse([]string{"--folder", root, "--background", "--upload-rate", "0"})
	if err != nil {
		t.Fatal(err)
	}
	_, worker, err = parse(workerArgs(launcher))
	if err != nil {
		t.Fatal(err)
	}
	applyBandwidthPreferences(cfg, worker)
	if err = cfg.Save(); err != nil {
		t.Fatal(err)
	}
	cfg, err = config.Load(root)
	if err != nil || cfg.UploadBytesPerSecond != 0 || cfg.DownloadBytesPerSecond != 2097152 {
		t.Fatalf("explicit zero did not clear exactly the selected limit: %v", err)
	}
	if cfg.Key != key || !cfg.Identity.Equals(identity) {
		t.Fatal("updating bandwidth preferences changed group membership or identity")
	}
}

func TestInvalidBandwidthFlagsFailBeforeInitialization(t *testing.T) {
	for _, name := range []string{"--upload-rate", "--download-rate"} {
		root := t.TempDir()
		if err := run([]string{"--folder", root, name, "-1"}); err == nil {
			t.Fatal("accepted negative bandwidth limit")
		}
		if _, err := config.Load(root); err == nil {
			t.Fatal("invalid argument initialized a synchronization group")
		}
	}
}

func TestRelayAllowlistFlagAcceptsOnlyBoundedPeerIDs(t *testing.T) {
	var ids []string
	for range 2 {
		private, _, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		id, err := peer.IDFromPrivateKey(private)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id.String())
	}
	command, o, err := parse([]string{"serve", "--relay", "--allow-peer", ids[0], "--allow-peer", ids[1]})
	if err != nil || command != "serve" || !o.relay || len(o.allowPeers) != 2 || o.allowPeers[0] != ids[0] || o.allowPeers[1] != ids[1] {
		t.Fatalf("relay allowlist flags were not preserved: %v", err)
	}
	if _, _, err = parse([]string{"serve", "--relay", "--allow-peer", "not-a-peer-id"}); err == nil || strings.Contains(err.Error(), "not-a-peer-id") {
		t.Fatal("malformed peer ID accepted or echoed in error")
	}
	args := []string{"serve", "--relay"}
	for range 257 {
		args = append(args, "--allow-peer", ids[0])
	}
	if _, _, err = parse(args); err == nil {
		t.Fatal("accepted unbounded relay allowlist")
	}
}
