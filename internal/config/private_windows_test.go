//go:build windows

package config

import (
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestPrivateStateDACL(t *testing.T) {
	root := t.TempDir()
	if _, err := Init(root, ""); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{StateDir(root), filepath.Join(StateDir(root), "config.json"), filepath.Join(StateDir(root), "identity.key")} {
		sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
		if err != nil {
			t.Fatal(err)
		}
		control, _, err := sd.Control()
		if err != nil || control&windows.SE_DACL_PROTECTED == 0 {
			t.Fatalf("state DACL inherits untrusted permissions: %s", path)
		}
		sddl := sd.String()
		if strings.Count(sddl, "(A;") != 2 || strings.Contains(sddl, ";;;WD)") || strings.Contains(sddl, ";;;AU)") || strings.Contains(sddl, ";;;BU)") {
			t.Fatalf("state DACL grants unintended access: %s", sddl)
		}
	}
}
