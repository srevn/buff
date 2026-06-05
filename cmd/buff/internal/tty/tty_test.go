//go:build darwin || linux

// The ioctl path (tty_unix.go) is exercised on the two platforms CI runs, ubuntu and macOS; the
// other ioctl unixes (the BSDs) share it over the same TIOCGETA constant, and the tty_other.go
// fallback is the unchanged legacy heuristic. The check is the negative direction — a pipe, a
// regular file, and the non-terminal character device whose misclassification this package exists
// to fix — all of which the ioctl must reject. The positive direction is not covered: a real
// terminal under `go test` means opening a pty, which has no portable form and is left out here.

package tty

import (
	"os"
	"testing"
)

// TestIsTerminalNegatives pins the streams that must NOT read as terminals: a pipe, a regular file,
// and — the defect this package was written to close — a non-terminal character device. The
// superseded os.ModeCharDevice heuristic reported /dev/null and its kin as terminals; the ioctl,
// asked directly, does not. A device that cannot be opened in this environment is logged and skipped
// rather than failing the run.
func TestIsTerminalNegatives(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	if IsTerminal(r) {
		t.Error("pipe read end reported as a terminal")
	}
	if IsTerminal(w) {
		t.Error("pipe write end reported as a terminal")
	}

	f, err := os.CreateTemp(t.TempDir(), "buff")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if IsTerminal(f) {
		t.Error("regular file reported as a terminal")
	}

	for _, name := range []string{"/dev/null", "/dev/zero"} {
		dev, err := os.Open(name)
		if err != nil {
			t.Logf("skipping %s: %v", name, err)
			continue
		}
		if IsTerminal(dev) {
			t.Errorf("IsTerminal(%s) = true; a non-terminal character device must not read as a terminal", name)
		}
		dev.Close()
	}
}
