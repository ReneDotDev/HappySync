package daemon

import (
	"testing"
)

func TestLockAndControl(t *testing.T) {
	root := t.TempDir()
	lock, e := Lock(root)
	if e != nil {
		t.Fatal(e)
	}
	defer lock.Close()
	if another, e := Lock(root); e == nil {
		another.Close()
		t.Fatal("duplicate root owner allowed")
	}
	c, e := Listen(root, func(cmd string) any { return map[string]string{"state": "idle"} })
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	var out map[string]string
	if e = Request(root, "status", &out); e != nil || out["state"] != "idle" {
		t.Fatalf("control: %v %v", out, e)
	}
}
