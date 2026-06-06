package cli_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/srevn/buff/api"
	"github.com/srevn/buff/cli"
	"github.com/srevn/buff/store"
)

// The cli flow tests live in an external package so they may stand up a real api server over a real
// store and drive cli.Run across an actual HTTP connection — the production import guard checks the
// cli package itself, not its tests, the same arrangement the client and api suites use. Run takes
// an injected IO with explicit TTY booleans, so the whole copy/paste and output matrix is exercised
// from an ordinary test with no pseudo-terminal.

// world is one test's store, the server over it, and the Env addressing that server.
type world struct {
	st  store.Store
	env cli.Env
}

// newWorld stands up a memory store, an api server over it, and the Env pointing at it.
func newWorld(t *testing.T, c store.Config) *world {
	t.Helper()
	st := store.NewMemory(c)
	ts := httptest.NewServer(api.New(st, api.Options{}))
	t.Cleanup(ts.Close)
	return &world{st: st, env: cli.Env{ServerURL: ts.URL, Version: "test"}}
}

// result captures one Run: its exit code and the text it wrote to each stream.
type result struct {
	code int
	out  string
	err  string
}

// run drives cli.Run with the given stdin text and TTY flags against w's server, capturing the exit
// code and the two output streams. The TTY booleans are the testable axes the real binary computes
// from its standard files; passing them explicitly is what lets one test cover every terminal-
// versus-pipe combination.
func (w *world) run(t *testing.T, in string, inTTY, outTTY bool, args ...string) result {
	t.Helper()
	return w.runIn(t, strings.NewReader(in), inTTY, outTTY, args...)
}

// runIn is run with an arbitrary stdin reader rather than a fixed string, so a test can drive a
// source that faults partway through a read — the local-failure case a strings.Reader cannot model.
// run is the string convenience over it.
func (w *world) runIn(t *testing.T, in io.Reader, inTTY, outTTY bool, args ...string) result {
	t.Helper()
	var out, errb bytes.Buffer
	code := cli.Run(context.Background(), args, w.env, cli.IO{
		In:       in,
		Out:      &out,
		Err:      &errb,
		InIsTTY:  inTTY,
		OutIsTTY: outTTY,
	})
	return result{code: code, out: out.String(), err: errb.String()}
}

// syncBuf is a goroutine-safe sink for a Run launched concurrently — the live-truncation tests poll
// its contents from the test goroutine while Run writes to it from another, which the race detector
// would flag on a plain bytes.Buffer.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// waitFor blocks until cond holds or the timeout elapses, failing the test on timeout. It is how
// the live-follow tests learn a paste has attached and made progress without a fixed sleep: the
// timeout is a ceiling that turns a hang into a failure, not the expected wait.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// soleSibling returns the single path under dir matching pattern, failing the test unless exactly
// one exists. The consume-once salvage names its sibling with the delivery's server-assigned
// generation id, which a test cannot predict, so it globs for the one entry the diversion created
// rather than reconstructing the name.
func soleSibling(t *testing.T, dir, pattern string) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		t.Fatalf("glob %q: %v", pattern, err)
	}
	if len(matches) != 1 {
		t.Fatalf("glob %q matched %v, want exactly one salvage sibling", pattern, matches)
	}
	return matches[0]
}

// assertFile fails unless the file at path holds exactly want.
func assertFile(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(got) != want {
		t.Errorf("%s = %q, want %q", path, got, want)
	}
}

// hasTempDir reports whether dir contains an extraction temp sibling, the marker that an atomic
// archive paste has begun and not yet published or rolled back.
func hasTempDir(dir string) bool {
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), ".buff-") {
			return true
		}
	}
	return false
}

// deadURL returns a URL whose port has just been freed, so a connection to it is refused — the way
// to provoke the transport-unreachable path deterministically.
func deadURL(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return url
}

// discardIO is an IO that drops both output streams, for a concurrent Run whose streams a test does
// not inspect, with an empty stdin.
func discardIO(inTTY, outTTY bool) cli.IO {
	return cli.IO{In: strings.NewReader(""), Out: io.Discard, Err: io.Discard, InIsTTY: inTTY, OutIsTTY: outTTY}
}
