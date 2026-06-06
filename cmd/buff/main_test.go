package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/srevn/buff/api"
)

// TestClientExit pins the signal-to-130 boundary as a pure table, the part of the exit contract
// no other test reaches: cli.Run's typed-error mapping is exercised in the cli package, but the
// translation of "the run failed because a signal fired" into 130 lives only here, and a real
// signal is neither needed nor wanted to check it. A non-zero code with a canceled context is the
// signal case; everything else keeps the code cli.Run computed.
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

// runMain drives buffMain with an injected environment and the three standard streams backed by
// temp files, returning the exit code and the captured stdout and stderr — the same shape main
// computes from the real files, but without a subprocess, a terminal, or a delivered signal. The
// input file is empty: a regular file, so not a TTY, reading EOF if a path ever consults it.
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
	// No case here delivers a signal, so the injected force-quit must never fire; assert that rather
	// than passing os.Exit, which would kill the test binary if a regression ever called it. t.Errorf
	// (not t.Fatal) because the watcher runs on its own goroutine, where FailNow would be misused.
	code = buffMain(args, getenvFrom(env), in, out, errf, func(int) {
		t.Errorf("buffMain invoked the force-quit exit with no signal delivered")
	})
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
// runtime, untested by the precedence and end-to-end suites. Both branches of the reserved-token
// fork are driven, each asserting the stream separation D-0075's design rests on: the client path
// answers --version to stdout and exits 0, and the server path's missing-data-dir fault goes to
// stderr and exits 1, with stdout kept clear for a client sharing the terminal. No server is stood
// up and no signal is sent; the over-the-wire client dispatch is the end-to-end suite's job.
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

// TestWatchSignals drives the two-phase signal escalation directly over channels — the logic the
// real binary wires to os.Signal delivery and os.Exit, neither of which a unit test can drive. Each
// case sequences through the recorder channels so the assertions are deterministic, never a sleep:
// reading a recorder blocks until the watcher has taken that step, and gone proves it then retired.
func TestWatchSignals(t *testing.T) {
	type harness struct {
		sigs   chan os.Signal
		done   chan struct{}
		causes chan error    // every cancel(cause) the watcher makes
		exits  chan int      // every exit(code) the watcher makes
		gone   chan struct{} // closed once watchSignals returns
	}
	start := func() *harness {
		h := &harness{
			sigs:   make(chan os.Signal, 2),
			done:   make(chan struct{}),
			causes: make(chan error, 1),
			exits:  make(chan int, 1),
			gone:   make(chan struct{}),
		}
		cancel := context.CancelCauseFunc(func(c error) { h.causes <- c })
		exit := func(code int) { h.exits <- code }
		go func() {
			watchSignals(h.sigs, h.done, cancel, api.ErrServerStopping, exit)
			close(h.gone)
		}()
		return h
	}
	// notCalled asserts a recorder fired nothing — safe to read only after gone, since by then the
	// watcher has retired and can make no further call.
	notCalled := func(t *testing.T, causes <-chan error, exits <-chan int) {
		t.Helper()
		select {
		case c := <-causes:
			t.Errorf("cancel called with %v, want no cancel", c)
		default:
		}
		select {
		case c := <-exits:
			t.Errorf("exit called with %d, want no force-quit", c)
		default:
		}
	}

	t.Run("first signal cancels with cause, second forces exit 130", func(t *testing.T) {
		h := start()
		h.sigs <- os.Interrupt
		if got := <-h.causes; !errors.Is(got, api.ErrServerStopping) {
			t.Errorf("cancel cause = %v, want ErrServerStopping", got)
		}
		h.sigs <- os.Interrupt
		if got := <-h.exits; got != 130 {
			t.Errorf("force-quit code = %d, want 130", got)
		}
		<-h.gone // in production os.Exit would not return; under the test exit returns and the watcher unwinds
	})

	t.Run("done before any signal retires the watcher, no action taken", func(t *testing.T) {
		h := start()
		close(h.done)
		<-h.gone
		notCalled(t, h.causes, h.exits)
	})

	t.Run("first signal then a clean finish: cancel but no force-quit", func(t *testing.T) {
		h := start()
		h.sigs <- os.Interrupt
		if got := <-h.causes; !errors.Is(got, api.ErrServerStopping) {
			t.Errorf("cancel cause = %v, want ErrServerStopping", got)
		}
		close(h.done) // the graceful stop completed before a second signal
		<-h.gone
		select {
		case c := <-h.exits:
			t.Errorf("exit called with %d, want no force-quit after a clean finish", c)
		default:
		}
	})
}
