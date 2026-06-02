package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestClientExit pins the signal-to-130 boundary as a pure table, the part of the exit contract no
// other test reaches: cli.Run's typed-error mapping is exercised in the cli package, but the
// translation of "the run failed because a signal fired" into 130 lives only here, and a real signal
// is neither needed nor wanted to check it. A non-zero code with a cancelled context is the signal
// case; everything else keeps the code cli.Run computed.
func TestClientExit(t *testing.T) {
	for _, c := range []struct {
		name   string
		code   int
		ctxErr error
		want   int
	}{
		{"clean run, no signal", 0, nil, 0},
		{"clean run despite a late signal stays success", 0, context.Canceled, 0},
		{"failure without a signal keeps its code", 8, nil, 8},
		{"truncation under a signal maps to 130", 7, context.Canceled, 130},
		{"unreachable under a signal maps to 130", 8, context.Canceled, 130},
		{"usage error without a signal stays 1", 1, nil, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := clientExit(c.code, c.ctxErr); got != c.want {
				t.Errorf("clientExit(%d, %v) = %d, want %d", c.code, c.ctxErr, got, c.want)
			}
		})
	}
}

// runMain drives buffMain with an injected environment and the three standard streams backed by temp
// files, returning the exit code and the captured stdout and stderr — the same shape main computes
// from the real files, but without a subprocess, a terminal, or a delivered signal. The input file is
// empty: a regular file, so not a TTY, reading EOF if a path ever consults it.
func runMain(t *testing.T, env map[string]string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	dir := t.TempDir()
	mk := func(name string) *os.File {
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		t.Cleanup(func() { f.Close() })
		return f
	}
	in, out, errf := mk("in"), mk("out"), mk("err")
	code = buffMain(args, getenvFrom(env), in, out, errf)
	return code, readFrom(t, out), readFrom(t, errf)
}

// readFrom rewinds f and reads all of it, for asserting on what buffMain wrote to a stream file.
func readFrom(t *testing.T, f *os.File) string {
	t.Helper()
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestBuffMain exercises the apex fork itself — the one piece of binary logic above cli.Run and the
// runtime, untested by the precedence and end-to-end suites. Both branches of the reserved-token fork
// are driven, each asserting the stream separation D-0075's design rests on: the client path answers
// --version to stdout and exits 0, and the server path's missing-data-dir fault goes to stderr and
// exits 1, with stdout kept clear for a client sharing the terminal. No server is stood up and no
// signal is sent; the over-the-wire client dispatch is the end-to-end suite's job.
func TestBuffMain(t *testing.T) {
	t.Run("client fork: --version to stdout, exit 0", func(t *testing.T) {
		code, stdout, stderr := runMain(t, nil, "--version")
		if code != 0 {
			t.Fatalf("exit = %d, want 0; stderr %q", code, stderr)
		}
		if strings.TrimSpace(stdout) != buildVersion() {
			t.Errorf("stdout = %q, want the resolved version %q", stdout, buildVersion())
		}
		if stderr != "" {
			t.Errorf("stderr = %q, want empty", stderr)
		}
	})

	t.Run("server fork: missing data dir to stderr, exit 1", func(t *testing.T) {
		code, stdout, stderr := runMain(t, nil, "serve")
		if code != 1 {
			t.Fatalf("exit = %d, want 1", code)
		}
		if !strings.Contains(stderr, "data directory required") {
			t.Errorf("stderr = %q, want the data-directory-required error", stderr)
		}
		if stdout != "" {
			t.Errorf("stdout = %q, want empty (the diagnostic belongs on stderr)", stdout)
		}
	})
}
