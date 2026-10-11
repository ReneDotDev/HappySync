package config

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInvitationFormat(t *testing.T) {
	key, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	id, secret, err := DecodeKey(key)
	if err != nil || len(id) != 16 || len(secret) != 32 {
		t.Fatalf("decode sizes: %v", err)
	}
	for _, bad := range []string{"", key + "=", key[:len(key)-1], "hsync2:" + key[7:], strings.Repeat("x", 100000), key[:len(key)-4] + "aaaa"} {
		if _, _, err = DecodeKey(bad); err == nil {
			t.Fatal("accepted malformed key")
		} else if strings.Contains(err.Error(), key) {
			t.Fatal("secret in error")
		}
	}
	p, _ := base64.RawURLEncoding.DecodeString(key[len(prefix):])
	p[0] = 2
	if _, _, err = DecodeKey(prefix + base64.RawURLEncoding.EncodeToString(p)); err == nil {
		t.Fatal("accepted unsupported version")
	}
}

func TestPersistentMembershipAndIdentity(t *testing.T) {
	root := t.TempDir()
	c, err := Init(root, "")
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := Init(root, c.Key)
	if err != nil {
		t.Fatal(err)
	}
	if c.Key != reloaded.Key || !c.Identity.Equals(reloaded.Identity) || !bytes.Equal(c.Secret, reloaded.Secret) {
		t.Fatal("state changed on restart")
	}
	if strings.Contains(fmt.Sprint(c), c.Key) || strings.Contains(fmt.Sprintf("%+v", c), c.Key) || strings.Contains(fmt.Sprintf("%#v", c), c.Key) {
		t.Fatal("configuration diagnostic leaks key")
	}
	diagnostic, err := json.Marshal(c)
	if err != nil || bytes.Contains(diagnostic, []byte(c.Key)) {
		t.Fatal("structured config diagnostic leaks key")
	}
	other, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(StateDir(root), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Init(root, other); err == nil {
		t.Fatal("accepted different group")
	}
	after, _ := os.ReadFile(filepath.Join(StateDir(root), "config.json"))
	if !bytes.Equal(before, after) {
		t.Fatal("mismatch changed config")
	}
	for _, path := range []string{StateDir(root), filepath.Join(StateDir(root), "config.json"), filepath.Join(StateDir(root), "identity.key")} {
		st, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode().Perm()&0077 != 0 {
			t.Fatalf("unprotected %s", path)
		}
	}
}

func TestJoinUsesIndependentIdentity(t *testing.T) {
	a, err := Init(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	b, err := Init(t.TempDir(), a.Key)
	if err != nil {
		t.Fatal(err)
	}
	if a.Identity.Equals(b.Identity) {
		t.Fatal("group secret reused as device identity")
	}
	if !bytes.Equal(a.SyncID, b.SyncID) {
		t.Fatal("wrong group")
	}
}

func TestSavePreferencesPreservesMembership(t *testing.T) {
	root := t.TempDir()
	c, err := Init(root, "")
	if err != nil {
		t.Fatal(err)
	}
	originalKey := c.Key
	c.Ignore = append(c.Ignore, "build/")
	c.Peers = []string{"/ip4/127.0.0.1/tcp/1234/p2p/example"}
	c.DisableDHT = true
	if err = c.Save(); err != nil {
		t.Fatal(err)
	}
	loaded, err := Init(root, "")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Key != originalKey || !loaded.Identity.Equals(c.Identity) || !loaded.DisableDHT || len(loaded.Ignore) != 3 || len(loaded.Peers) != 1 {
		t.Fatal("preferences lost or membership modified")
	}
	c.Key, _ = GenerateKey()
	if err = Save(c); err == nil {
		t.Fatal("Save changed persisted group")
	}
	loaded, err = Load(root)
	if err != nil || loaded.Key != originalKey {
		t.Fatal("failed Save damaged group")
	}
}

func TestRejectSymlinkedStateAndIdentity(t *testing.T) {
	root := t.TempDir()
	target := t.TempDir()
	if err := os.Symlink(target, StateDir(root)); err != nil {
		t.Fatal(err)
	}
	if _, err := Init(root, ""); err == nil {
		t.Fatal("followed state symlink")
	}
	root = t.TempDir()
	c, err := Init(root, "")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(StateDir(root), "identity.key")
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(filepath.Join(StateDir(c.Root), "config.json"), path); err != nil {
		t.Fatal(err)
	}
	if _, err = Load(root); err == nil {
		t.Fatal("followed identity symlink")
	}
}

func TestBandwidthValidationPreservesStoredConfiguration(t *testing.T) {
	root := t.TempDir()
	c, err := Init(root, "")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(StateDir(root), "config.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, upload := range []bool{true, false} {
		c.UploadBytesPerSecond, c.DownloadBytesPerSecond = 0, 0
		if upload {
			c.UploadBytesPerSecond = -1
		} else {
			c.DownloadBytesPerSecond = -1
		}
		if err := c.Save(); err == nil {
			t.Fatal("accepted negative persisted bandwidth preference")
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("invalid preference damaged existing private configuration")
		}
		bad, err := json.Marshal((*diskConfig)(c))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, bad, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err = Load(root); err == nil || strings.Contains(err.Error(), c.Key) {
			t.Fatal("accepted malformed stored bandwidth limit or leaked invitation")
		}
		if err = os.WriteFile(path, before, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func FuzzDecodeKey(f *testing.F) {
	key, _ := GenerateKey()
	f.Add(key)
	f.Add("hsync1:")
	f.Fuzz(func(t *testing.T, input string) { _, _, _ = DecodeKey(input) })
}
