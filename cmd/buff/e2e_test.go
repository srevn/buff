package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/srevn/buff/api"
	"github.com/srevn/buff/cli"
	"github.com/srevn/buff/client"
	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
)

// The end-to-end tests stand up a real runtime — disk store, recovery, the api edge, a real TCP
// listener on an ephemeral port — and drive it through the real client and the real cli, so they
// exercise the whole binary stack as one process rather than any layer in isolation. newRuntime's
// split of fallible setup from the run loop is what makes this clean: setup binds :0, Addr is
// observable before Run, and Run goes on a goroutine the test cancels.

// testServer is one running runtime over a temp data directory, with the URL to reach it and the
// machinery to stop it and learn how Run returned.
type testServer struct {
	url    string
	dir    string
	rt     *runtime
	cancel context.CancelCauseFunc
	done   chan error
	once   sync.Once
	runErr error
}

// startServer builds and runs a server over a fresh temp directory on 127.0.0.1:0, returning once it
// is serving. mutate adjusts the config before construction, for the tests that need a specific cap
// or a reaper. Durability is off and the idle bound is set generous: a test needs no physical
// flushing, and a minute-long idle bound — standing now, never disable-able — sits far above any
// test's gated pause between chunks, so a gated upload is not torn while a test arranges a follow.
// Every e2e thus runs production-faithful, with a live idle bound rather than none. The server is
// stopped at cleanup if a test did not stop it.
func startServer(t *testing.T, mutate func(*config)) *testServer {
	t.Helper()
	dir := t.TempDir()
	c, err := configFromEnv(getenvFrom(map[string]string{"BUFF_DATA_DIR": dir}))
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	c.Addr = "127.0.0.1:0"
	c.Fsync = false
	c.ReapInterval = 0
	c.UploadIdle = time.Minute
	if mutate != nil {
		mutate(&c)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	rt, err := newRuntime(c, log)
	if err != nil {
		t.Fatalf("newRuntime: %v", err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	ts := &testServer{url: "http://" + rt.Addr().String(), dir: dir, rt: rt, cancel: cancel, done: make(chan error, 1)}
	go func() { ts.done <- rt.Run(ctx) }()
	t.Cleanup(func() { ts.stop(t) })
	return ts
}

// stop cancels the runtime and waits for Run to return, recording its error, at most once. A test
// that asserts graceful shutdown calls it to read that error; the cleanup calls it too, harmlessly,
// for a test that did not. It cancels with the same shutdown cause buffMain sets on a signal, since
// it stands in for that signal here — the harness drives Run directly to observe its return — so an
// upload the stop cuts is classified exactly as it would be in the real binary.
func (ts *testServer) stop(t *testing.T) error {
	t.Helper()
	ts.once.Do(func() {
		ts.cancel(api.ErrServerStopping)
		select {
		case ts.runErr = <-ts.done:
			// Run has fully torn down the listener and root; the explicit Close is the idempotent
			// second close, proving it composes with Run's own teardown rather than double-faulting.
			if err := ts.rt.Close(); err != nil {
				t.Errorf("runtime Close after Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("server Run did not return within 5s of cancel")
		}
	})
	return ts.runErr
}

// client builds a wire client pointed at the server.
func (ts *testServer) client(t *testing.T) *client.Client {
	t.Helper()
	c, err := client.New(ts.url, nil)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	return c
}

// env is the cli environment addressing the server.
func (ts *testServer) env() cli.Env { return cli.Env{ServerURL: ts.url, Version: "test"} }

// runCLI drives cli.Run against the server with explicit stdin text and TTY flags, returning the
// exit code and the two captured streams — the same shape the real binary computes from its files.
func runCLI(env cli.Env, in string, inTTY, outTTY bool, args ...string) (int, string, string) {
	var out, errb bytes.Buffer
	code := cli.Run(context.Background(), args, env, cli.IO{
		In: strings.NewReader(in), Out: &out, Err: &errb, InIsTTY: inTTY, OutIsTTY: outTTY,
	})
	return code, out.String(), errb.String()
}

// syncBuf is a goroutine-safe sink: a concurrent reader writes into it while the test goroutine
// polls its contents, which a plain bytes.Buffer would race on.
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

// waitFor blocks until cond holds or the timeout elapses, failing the test on timeout. It turns a
// hang into a bounded failure rather than encoding an expected delay.
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

// gatedReader is a request body the test feeds one chunk at a time, blocking the upload between
// chunks so a follower can be observed mid-write. close releases any read still blocked on the next
// chunk — registered at cleanup so a torn upload's abandoned body read never lingers.
type gatedReader struct {
	chunks    chan []byte
	cur       []byte
	closeOnce sync.Once
}

func newGatedReader(t *testing.T) *gatedReader {
	g := &gatedReader{chunks: make(chan []byte)}
	t.Cleanup(g.close)
	return g
}

func (g *gatedReader) Read(p []byte) (int, error) {
	for len(g.cur) == 0 {
		b, ok := <-g.chunks
		if !ok {
			return 0, io.EOF
		}
		g.cur = b
	}
	n := copy(p, g.cur)
	g.cur = g.cur[n:]
	return n, nil
}

func (g *gatedReader) send(b []byte) { g.chunks <- b }
func (g *gatedReader) close()        { g.closeOnce.Do(func() { close(g.chunks) }) }

// walkFiles collects every regular file under root keyed by base name, so a round-trip assertion can
// check contents without depending on the archive's exact directory nesting.
func walkFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			out[d.Name()] = string(b)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return out
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

// TestE2ERoundTripText copies stdin into a slot and pastes it back out, the simplest proof the whole
// client path is wired: cli.Run over the real client over a real disk store.
func TestE2ERoundTripText(t *testing.T) {
	ts := startServer(t, nil)
	if code, _, errs := runCLI(ts.env(), "hello world", false, false, "@t"); code != 0 {
		t.Fatalf("copy exit %d, stderr %q", code, errs)
	}
	code, out, errs := runCLI(ts.env(), "", true, false, "@t")
	if code != 0 {
		t.Fatalf("paste exit %d, stderr %q", code, errs)
	}
	if out != "hello world" {
		t.Errorf("paste = %q, want %q", out, "hello world")
	}
}

// TestE2ERoundTripFile copies a file whose name carries a non-ASCII character and pastes it into a
// directory, where it is saved under its remembered filename. That the file reappears as café.pdf
// with its bytes intact proves the percent-encoded-filename round-trip end to end, over the wire.
func TestE2ERoundTripFile(t *testing.T) {
	ts := startServer(t, nil)
	const payload = "PDF-ish bytes \x00\x01\x02"
	srcDir := t.TempDir()
	srcFile := filepath.Join(srcDir, "café.pdf")
	if err := os.WriteFile(srcFile, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, _, errs := runCLI(ts.env(), "", true, false, srcFile, "@f"); code != 0 {
		t.Fatalf("copy exit %d, stderr %q", code, errs)
	}
	outDir := t.TempDir()
	if code, _, errs := runCLI(ts.env(), "", true, false, "@f", "-o", outDir); code != 0 {
		t.Fatalf("paste exit %d, stderr %q", code, errs)
	}
	assertFile(t, filepath.Join(outDir, "café.pdf"), payload)
}

// TestE2ERoundTripArchive copies a directory tree as an archive and extracts it into a fresh target,
// exercising the tar pipe on the way out and the confined, atomic extraction on the way back.
func TestE2ERoundTripArchive(t *testing.T) {
	ts := startServer(t, nil)
	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("A"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "b.txt"), []byte("B"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, _, errs := runCLI(ts.env(), "", true, false, srcDir, "@arc"); code != 0 {
		t.Fatalf("copy exit %d, stderr %q", code, errs)
	}
	dest := filepath.Join(t.TempDir(), "out") // absent target with an existing parent → atomic new dir
	if code, _, errs := runCLI(ts.env(), "", true, false, "@arc", "-o", dest); code != 0 {
		t.Fatalf("extract exit %d, stderr %q", code, errs)
	}
	files := walkFiles(t, dest)
	if files["a.txt"] != "A" || files["b.txt"] != "B" {
		t.Errorf("extracted = %v, want a.txt=A b.txt=B", files)
	}
}

// TestE2ELiveFollow is the unified-clip thesis over the full stack: a consumer attaches to a clip
// that is still being written — before a single byte exists — and follows it to a clean end,
// receiving bytes appended after it attached and then the clean EOF the server signals only on a
// clean finalize. Attaching to an empty live clip resolves at once because the server flushes the
// live response's headers on attach; the bytes follow as they are produced. (The header-flush itself
// is pinned by the focused api test TestGetLiveHeadersBeforeBody.)
func TestE2ELiveFollow(t *testing.T) {
	ts := startServer(t, nil)
	c := ts.client(t)
	gr := newGatedReader(t)

	putDone := make(chan clip.Clip, 1)
	putErr := make(chan error, 1)
	go func() {
		cl, err := c.Put(context.Background(), "live", gr, clip.Meta{Kind: clip.KindText}, client.PutOpts{})
		putDone <- cl
		putErr <- err
	}()

	// Attach while the clip is live and still empty. Create installs the generation before its body
	// is read, so a Get is not-found until it exists and then resolves to a follower — with no byte
	// yet written, the case that proves the attach does not wait for one.
	var rc io.ReadCloser
	waitFor(t, 5*time.Second, func() bool {
		r, cl, err := c.Get(context.Background(), "live")
		if err == nil {
			if cl.Finalized {
				t.Fatal("attached clip reports finalized; want a live follow")
			}
			rc = r
			return true
		}
		if errors.Is(err, clip.ErrNotFound) {
			return false
		}
		t.Fatalf("get live: %v", err)
		return false
	})

	var got syncBuf
	copyDone := make(chan error, 1)
	go func() {
		_, err := io.Copy(&got, rc)
		rc.Close()
		copyDone <- err
	}()

	gr.send([]byte("hello ")) // produced after the follower attached — a genuine follow, not a read
	waitFor(t, 5*time.Second, func() bool { return got.String() == "hello " })
	gr.send([]byte("world"))
	waitFor(t, 5*time.Second, func() bool { return got.String() == "hello world" })
	gr.close() // clean finalize wakes the follower to a clean EOF

	if err := <-copyDone; err != nil {
		t.Errorf("follow copy: %v", err)
	}
	if got.String() != "hello world" {
		t.Errorf("follow = %q, want %q", got.String(), "hello world")
	}
	if err := <-putErr; err != nil {
		t.Errorf("put: %v", err)
	}
	if cl := <-putDone; cl.Size != int64(len("hello world")) {
		t.Errorf("finalized size = %d, want %d", cl.Size, len("hello world"))
	}
}

// TestE2EConsumeOnce proves at-most-once delivery over the wire under a real race: two readers open
// the same consume-once clip concurrently, exactly one receives the bytes, the loser is refused with
// no bytes, and once the winner's delivery completes the clip is gone.
func TestE2EConsumeOnce(t *testing.T) {
	ts := startServer(t, nil)
	c := ts.client(t)
	const secret = "the secret"
	if _, err := c.Put(context.Background(), "s", strings.NewReader(secret), clip.Meta{Kind: clip.KindText}, client.PutOpts{ConsumeOnce: true}); err != nil {
		t.Fatalf("put: %v", err)
	}

	type res struct {
		rc  io.ReadCloser
		err error
	}
	results := make([]res, 2)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rc, _, err := c.Get(context.Background(), "s")
			results[i] = res{rc, err}
		}(i)
	}
	wg.Wait()

	winners, losers := 0, 0
	for _, r := range results {
		if r.err == nil {
			winners++
			b, err := io.ReadAll(r.rc)
			r.rc.Close()
			if err != nil {
				t.Errorf("winner read: %v", err)
			}
			if string(b) != secret {
				t.Errorf("winner got %q, want %q", b, secret)
			}
			continue
		}
		losers++
		// The loser is refused before any byte: consumed if it lost the claim race, not-found if the
		// winner's delivery and cleanup already completed. Both mean "you cannot have it".
		if !errors.Is(r.err, clip.ErrConsumed) && !errors.Is(r.err, clip.ErrNotFound) {
			t.Errorf("loser err = %v, want consumed or not-found", r.err)
		}
	}
	if winners != 1 || losers != 1 {
		t.Fatalf("got %d winners and %d losers, want exactly 1 each", winners, losers)
	}

	// After the single delivery the clip is destroyed: a later read finds nothing.
	if _, _, err := c.Get(context.Background(), "s"); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("post-consume get = %v, want not-found", err)
	}
}

// TestE2EGracefulShutdown drives the shutdown contract over the full stack. A finalized clip is
// stored, a live follow is established over an in-flight upload, and the runtime is cancelled. Run
// must return nil promptly — the in-flight upload's body read is interrupted by request-context
// cancellation rather than waiting out the drain — the live follow must tear (the truncation reaches
// the client as ErrAborted), the port must stop accepting, and a fresh store over the same directory
// must find the finalized clip but not the aborted live one. A reaper is scheduled too, so the full
// three-goroutine group plus the per-upload cancel watcher are all cancelled cleanly under race.
//
// It runs with a live idle deadline, the production-real case: the reader has already armed a future
// deadline on the parked upload read, so a prompt return proves the watcher's past-deadline poke
// wins against it. A zero idle deadline is no longer a reachable configuration — the bound is
// standing — so the watcher is never the sole deadline writer; the previous deadline-disabled regime
// would test an impossible config, and the watcher's isolated behaviour is covered by the api unit
// test TestAbortOnCancel.
func TestE2EGracefulShutdown(t *testing.T) {
	// 30s is comfortably past both the 5s stop timeout and the 15s drain, so a watcher that failed to
	// beat the live idle deadline would miss it and fail the test, rather than passing by way of the
	// idle deadline or the Close backstop being what actually aborted the upload.
	gracefulShutdown(t, 30*time.Second)
}

func gracefulShutdown(t *testing.T, uploadIdle time.Duration) {
	ts := startServer(t, func(c *config) {
		c.ReapInterval = time.Hour // spawns the reaper goroutine; never fires
		c.UploadIdle = uploadIdle
	})
	c := ts.client(t)

	const kept = "persist me"
	if _, err := c.Put(context.Background(), "keep", strings.NewReader(kept), clip.Meta{Kind: clip.KindText}, client.PutOpts{}); err != nil {
		t.Fatalf("put keep: %v", err)
	}

	gr := newGatedReader(t)
	putErr := make(chan error, 1)
	go func() {
		_, err := c.Put(context.Background(), "live", gr, clip.Meta{Kind: clip.KindText}, client.PutOpts{})
		putErr <- err
	}()

	// Establish a live follow over the in-flight upload: send one chunk so the follower has content
	// to read, attach, and read it — so the upload is parked mid-body and the follow is genuinely in
	// flight when shutdown hits. The upload then blocks on its next chunk, the parked body read that
	// context cancellation must interrupt.
	gr.send([]byte("partial"))
	var rc io.ReadCloser
	waitFor(t, 5*time.Second, func() bool {
		r, _, err := c.Get(context.Background(), "live")
		if err == nil {
			rc = r
			return true
		}
		return errors.Is(err, clip.ErrNotFound)
	})
	var got syncBuf
	followErr := make(chan error, 1)
	go func() {
		_, err := io.Copy(&got, rc)
		rc.Close()
		followErr <- err
	}()
	waitFor(t, 5*time.Second, func() bool { return got.String() == "partial" })

	// Cancel: a signal-triggered graceful shutdown. Run returns nil.
	if err := ts.stop(t); err != nil {
		t.Errorf("Run returned %v on graceful shutdown, want nil", err)
	}
	// The live follow tore: the client saw a truncation, never a clean end.
	if err := <-followErr; !errors.Is(err, clip.ErrAborted) {
		t.Errorf("follow error = %v, want ErrAborted", err)
	}
	// The in-flight upload, cut mid-body by the graceful stop, is told the server is stopping — a
	// 503 unavailable — rather than blamed for a truncated request with a 400. This pins the
	// load-bearing assumption end to end: the root cancellation's cause propagates through net/http
	// to the request context, and the put handler reads it to tell shutdown from a client truncation.
	// unavailable has no client reverse-map row, so it stays a generic HTTPError carrying the status.
	select {
	case err := <-putErr:
		var he *client.HTTPError
		if !errors.As(err, &he) {
			t.Fatalf("in-flight upload error = %v, want *client.HTTPError", err)
		}
		if he.Status != 503 || he.Sentinel != "unavailable" {
			t.Errorf("in-flight upload = %d %q, want 503 \"unavailable\"", he.Status, he.Sentinel)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight upload did not return after graceful shutdown")
	}
	// The port no longer accepts.
	hc := &http.Client{Timeout: 2 * time.Second}
	if resp, err := hc.Get(ts.url + "/health"); err == nil {
		resp.Body.Close()
		t.Error("server still accepting connections after shutdown")
	}

	// A fresh store over the same directory: the finalized clip persists, the aborted live one is
	// gone. This is the crash-recovery contract reached through a clean shutdown.
	root, err := os.OpenRoot(ts.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	fresh, err := store.NewDisk(root, store.Config{}, store.DiskOpts{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	kc, err := fresh.Stat(context.Background(), "keep")
	if err != nil {
		t.Errorf("fresh store lost the finalized clip: %v", err)
	} else if kc.Size != int64(len(kept)) {
		t.Errorf("recovered size = %d, want %d", kc.Size, len(kept))
	}
	if _, err := fresh.Stat(context.Background(), "live"); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("fresh store found the aborted live clip: %v", err)
	}
}

// TestE2EConfigOverWire proves a configured cap actually reaches the store: a tiny per-clip cap set
// in the config rejects an over-cap upload as too-large, which the cli scores as exit 5.
func TestE2EConfigOverWire(t *testing.T) {
	ts := startServer(t, func(c *config) { c.MaxClip = 8 })
	code, _, errs := runCLI(ts.env(), "way more than eight bytes", false, false, "@big")
	if code != 5 {
		t.Fatalf("over-cap copy exit %d, want 5; stderr %q", code, errs)
	}
}
