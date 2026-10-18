//go:build darwin || linux

// The ioctl path (tty_unix.go) is exercised on the two platforms CI runs, ubuntu and macOS; the
// other ioctl unixes (the BSDs) share it over the same requests, and the tty_other.go fallback
// is the unchanged legacy heuristic. The check is the negative direction — a pipe, a regular
// file, and the non-terminal character device whose misclassification this package exists to fix
// — all of which both ioctls must reject. The positive direction is not covered: a real terminal
// under `go test` means opening a pty, which has no portable form, and a background job on one
// needs a shell's job control besides, so both are left out here.

package tty

import (
	"os"
	"testing"
)

// TestNegatives pins the streams that must NOT read as terminals, for both questions the package
// answers: a pipe, a regular file, and — the defect this package was written to close — a non-
// terminal character device. The superseded os.ModeCharDevice heuristic reported /dev/null and its
// kin as terminals; the termios ioctl, asked directly, does not. IsForeground holds only of the
// process's own controlling terminal, so every stream IsTerminal rejects it must reject too — a
// status line drawn on a pipe or a file would be bytes in someone's data. A device that cannot be
// opened in this environment is logged and skipped rather than failing the run.
func TestNegatives(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	f, err := os.CreateTemp(t.TempDir(), "buff")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	streams := map[string]*os.File{"pipe read end": r, "pipe write end": w, "regular file": f}
	for _, name := range []string{"/dev/null", "/dev/zero"} {
		dev, err := os.Open(name)
		if err != nil {
			t.Logf("skipping %s: %v", name, err)
			continue
		}
		defer dev.Close()
		streams[name] = dev
	}

	for name, s := range streams {
		if IsTerminal(s) {
			t.Errorf("IsTerminal(%s) = true, want a non-terminal rejected", name)
		}
		if IsForeground(s) {
			t.Errorf("IsForeground(%s) = true, want a non-terminal rejected", name)
		}
	}
}
