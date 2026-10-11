package network

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/happysync/happysync/internal/model"
	netcore "github.com/libp2p/go-libp2p/core/network"
)

type summaryBackend struct {
	*testBackend
	mu      sync.RWMutex
	records []model.Record
}

func (b *summaryBackend) Digest(context.Context) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	b.calls.Add(1)
	var ids []string
	for _, r := range b.records {
		ids = append(ids, r.ID)
	}
	sort.Strings(ids)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(ids, "\n")))), nil
}

func TestStateDigestEqualityDifferenceAndAuthentication(t *testing.T) {
	a, b := testNode(t, testConfig()), testNode(t, testConfig())
	backend := &summaryBackend{testBackend: &testBackend{}, records: []model.Record{{ID: "one"}, {ID: "two"}}}
	b.SetBackend(backend)
	connect(t, a, b)
	first, err := a.StateDigest(context.Background(), b.host.ID())
	if err != nil || !validDigest(first) {
		t.Fatalf("digest %q %v", first, err)
	}
	backend.mu.Lock()
	backend.records = []model.Record{{ID: "two"}, {ID: "one"}}
	backend.mu.Unlock()
	equal, err := a.StateDigest(context.Background(), b.host.ID())
	if err != nil || equal != first {
		t.Fatalf("same record IDs summary mismatch %s %v", equal, err)
	}
	backend.mu.Lock()
	backend.records = append(backend.records, model.Record{ID: "three"})
	backend.mu.Unlock()
	different, err := a.StateDigest(context.Background(), b.host.ID())
	if err != nil || different == first {
		t.Fatal("different record sets produced equal summary")
	}
	cfg := testConfig()
	cfg.Secret = bytes.Repeat([]byte{9}, 32)
	outsider := testNode(t, cfg)
	connect(t, outsider, b)
	before := backend.calls.Load()
	if digest, err := outsider.StateDigest(context.Background(), b.host.ID()); err == nil || digest != "" {
		t.Fatal("wrong-group state summary accepted")
	}
	if backend.calls.Load() != before {
		t.Fatal("unauthenticated peer invoked digest backend")
	}
}

func TestStateDigestFallbackAndMalformedResponse(t *testing.T) {
	a, b := testNode(t, testConfig()), testNode(t, testConfig())
	b.SetBackend(&testBackend{})
	connect(t, a, b)
	if _, err := a.StateDigest(context.Background(), b.host.ID()); err == nil {
		t.Fatal("unsupported backend summary accepted")
	}
	b.host.SetStreamHandler(ControlProtocol, func(s netcore.Stream) {
		defer s.Close()
		if authenticate(s, b.key, false) != nil {
			return
		}
		var req digestRequest
		if readFrame(s, &req, 1024) != nil {
			return
		}
		_ = writeFrame(s, digestReply{Digest: "invalid"}, 1024)
	})
	if _, err := a.StateDigest(context.Background(), b.host.ID()); err == nil {
		t.Fatal("malformed summary accepted")
	}
}
