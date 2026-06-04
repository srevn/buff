package cli_test

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/srevn/buff/cli"
	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
)

// TestTarPipeSourceErrorWins is the causal-priority join end to end: copying two paths where
// one is missing makes the archiver fail, and the archiver's error — not the transport symptom
// it caused — is what surfaces, so the run exits with the generic local-error code rather than
// the unreachable code. Two paths are archived without statting them first, so the missing root
// is discovered by the archiver mid-stream, exactly the case the join exists to resolve.
func TestTarPipeSourceErrorWins(t *testing.T) {
	w := newWorld(t, store.Config{})
	base := t.TempDir()
	exists := filepath.Join(base, "exists")
	mustWrite(t, exists, "x")
	missing := filepath.Join(base, "missing") // never created
	r := w.run(t, "", true, false, exists, missing, "@p")
	if r.code != 1 {
		t.Errorf("source error should surface as exit 1, got %d (err=%q)", r.code, r.err)
	}
	// The producer's cause is what the user sees, and cli originates that line, so it must lead
	// with the buff: marker rather than reaching the terminal as a bare lstat/archive error.
	if !strings.HasPrefix(r.err, "buff:") {
		t.Errorf("producer-cause diagnostic = %q, want it to lead with buff:", r.err)
	}
}

// TestCapExit5 is the transport authority end to end: a copy larger than the per-clip cap is
// rejected mid-upload with the real too-large status, which the run reports as exit 5 rather
// than as a transport failure, even though the request body never finished streaming.
func TestCapExit5(t *testing.T) {
	w := newWorld(t, store.Config{MaxClip: 4})
	r := w.run(t, "far more than four bytes", false, true, "@big")
	if r.code != 5 {
		t.Errorf("over-cap copy: code=%d want 5 (err=%q)", r.code, r.err)
	}
}

// TestTruncationRawStdout drives a live clip through the store, attaches a paste that follows
// it to stdout, then aborts the writer mid-stream. The torn follow must exit 7 — the bytes
// already delivered may stand, but the run reports the truncation rather than a clean end.
func TestTruncationRawStdout(t *testing.T) {
	w := newWorld(t, store.Config{})
	ctx := context.Background()
	wr, err := w.st.Create(ctx, "live", clip.Meta{Kind: clip.KindText}, store.PutOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wr.Write([]byte("PART")); err != nil {
		t.Fatal(err)
	}
	out := &syncBuf{}
	done := make(chan int, 1)
	go func() {
		done <- cli.Run(ctx, []string{"@live"}, w.env, cli.IO{
			In: strings.NewReader(""), Out: out, Err: io.Discard, InIsTTY: true,
		})
	}()
	// Once the delivered bytes appear, the follower has attached; aborting now tears the stream.
	waitFor(t, 3*time.Second, func() bool { return strings.Contains(out.String(), "PART") })
	if err := wr.Abort(); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-done:
		if code != 7 {
			t.Errorf("torn raw paste: code=%d want 7", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("paste did not finish after the writer was aborted")
	}
}

// TestTruncationArchive is the same truncation through the archive sink: a torn tar must still
// exit 7 even though it is read through a tar parser that may relabel the read error, and the
// atomic extraction must leave no destination directory and no temp tree behind.
func TestTruncationArchive(t *testing.T) {
	w := newWorld(t, store.Config{})
	ctx := context.Background()
	work := t.TempDir()
	t.Chdir(work)
	wr, err := w.st.Create(ctx, "arch", clip.Meta{Kind: clip.KindArchive}, store.PutOpts{})
	if err != nil {
		t.Fatal(err)
	}
	// A complete tar header declaring a body, with no body bytes written: the extractor reads
	// the header, creates its temp tree, then blocks copying the body that never arrives.
	var hdr bytes.Buffer
	tw := tar.NewWriter(&hdr)
	if err := tw.WriteHeader(&tar.Header{Name: "f.txt", Mode: 0o644, Size: 512, Typeflag: tar.TypeReg, Format: tar.FormatPAX}); err != nil {
		t.Fatal(err)
	}
	if _, err := wr.Write(hdr.Bytes()); err != nil {
		t.Fatal(err)
	}
	done := make(chan int, 1)
	go func() {
		done <- cli.Run(ctx, []string{"@arch"}, w.env, discardIO(true, true))
	}()
	// The temp sibling appears as soon as the atomic extraction begins, which is after the GET
	// has attached the follower; aborting then tears the in-progress extraction.
	waitFor(t, 3*time.Second, func() bool { return hasTempDir(work) })
	if err := wr.Abort(); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-done:
		if code != 7 {
			t.Errorf("torn archive paste: code=%d want 7", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("archive paste did not finish after the writer was aborted")
	}
	if _, err := os.Stat(filepath.Join(work, "arch")); !os.IsNotExist(err) {
		t.Errorf("destination should be absent after a torn extract, stat err=%v", err)
	}
	if hasTempDir(work) {
		t.Error("temp extraction tree left behind after a torn extract")
	}
}

// TestSkipWarning copies a tree containing a symlink: the symlink is skipped with a stderr
// warning naming it, and the pasted tree holds the regular file but not the symlink.
func TestSkipWarning(t *testing.T) {
	w := newWorld(t, store.Config{})
	base := t.TempDir()
	tree := filepath.Join(base, "tree")
	if err := os.MkdirAll(tree, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(tree, "real.txt"), "R")
	if err := os.Symlink("real.txt", filepath.Join(tree, "link")); err != nil {
		t.Skipf("symlinks unsupported here: %v", err)
	}
	r := w.run(t, "", true, false, tree, "@t")
	if r.code != 0 {
		t.Fatalf("copy: code=%d err=%q", r.code, r.err)
	}
	if !strings.Contains(r.err, "skipping") || !strings.Contains(r.err, "link") {
		t.Errorf("expected a skip warning naming the link, stderr=%q", r.err)
	}
	work := t.TempDir()
	t.Chdir(work)
	if r := w.run(t, "", true, true, "@t"); r.code != 0 {
		t.Fatalf("paste: code=%d err=%q", r.code, r.err)
	}
	assertFile(t, filepath.Join(work, "t", "tree", "real.txt"), "R")
	if _, err := os.Lstat(filepath.Join(work, "t", "tree", "link")); !os.IsNotExist(err) {
		t.Errorf("the symlink should not have transferred, lstat err=%v", err)
	}
}

// TestSoleDirSymlinkRefusedEmpty documents the frozen no-follow policy at the source meeting the
// zero-entry refusal: a sole argument that is a symlink to a directory is taken as an archive
// source (os.Stat follows it to a directory), but the archiver does not follow the root symlink,
// so it is skipped and the archive comes to nothing. Rather than send a silent empty clip, Stream
// refuses with ErrEmptyArchive, so the copy fails loudly — still emitting the skip warning — and
// no clip is published.
func TestSoleDirSymlinkRefusedEmpty(t *testing.T) {
	w := newWorld(t, store.Config{})
	base := t.TempDir()
	realDir := filepath.Join(base, "real")
	if err := os.MkdirAll(realDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(realDir, "f"), "F")
	if err := os.Symlink(realDir, filepath.Join(base, "dlink")); err != nil {
		t.Skipf("symlinks unsupported here: %v", err)
	}
	r := w.run(t, "", true, false, filepath.Join(base, "dlink"), "@e")
	if r.code != 1 {
		t.Fatalf("copy of a sole unfollowed dir-symlink: code=%d want 1 (empty archive refused), err=%q", r.code, r.err)
	}
	if !strings.Contains(r.err, "skipping") {
		t.Errorf("expected a skip warning for the unfollowed root symlink, stderr=%q", r.err)
	}
	if !strings.Contains(r.err, "no entries to archive") {
		t.Errorf("expected the empty-archive refusal in stderr, got %q", r.err)
	}
}

// TestExitUnreachable points the run at a refused port: a transport failure with no response
// is the unreachable code, exit 8.
func TestExitUnreachable(t *testing.T) {
	w := &world{env: cli.Env{ServerURL: deadURL(t)}}
	r := w.run(t, "", true, false, "@x")
	if r.code != 8 {
		t.Errorf("paste to a refused port: code=%d want 8 (err=%q)", r.code, r.err)
	}
}

// TestExitUnmappedStatus points the run at a server returning a status with no Buff-Error
// sentinel — as a foreign proxy might — which the client cannot map to a domain error, so it
// becomes a generic HTTP error and the generic exit 1, distinct from the sentinel-backed codes.
func TestExitUnmappedStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusTeapot) // 418, no Buff-Error header
	}))
	defer ts.Close()
	w := &world{env: cli.Env{ServerURL: ts.URL}}
	r := w.run(t, "", true, false, "@x")
	if r.code != 1 {
		t.Errorf("unmapped status: code=%d want 1 (err=%q)", r.code, r.err)
	}
}

// faultingReader yields its bytes once, then fails every later read — a copy source (a file,
// standard input) whose underlying device dies mid-upload.
type faultingReader struct {
	data []byte
	err  error
}

func (f *faultingReader) Read(p []byte) (int, error) {
	if len(f.data) > 0 {
		n := copy(p, f.data)
		f.data = f.data[n:]
		return n, nil
	}
	return 0, f.err
}

// TestSourceFaultNotNetwork is the local-source-fault lifecycle end to end: a stdin copy whose
// source faults mid-stream must not be reported as a network failure (exit 8). The transport sees
// only a torn request body, but the client distinguishes a body-read fault from an unreachable
// server, so the run lands on the generic local-error class (exit 1) — the same class a vanished
// file uses — never 8, which a script would read as "the server is down, retry".
func TestSourceFaultNotNetwork(t *testing.T) {
	w := newWorld(t, store.Config{})
	body := &faultingReader{data: []byte("a partial upload that then faults"), err: errors.New("input/output error")}
	r := w.runIn(t, body, false, true, "@x") // piped stdin (not a TTY) ⇒ copy
	if r.code == 8 {
		t.Fatalf("a local source fault was reported as a network failure (exit 8); err=%q", r.err)
	}
	if r.code != 1 {
		t.Errorf("source fault: code=%d want 1 (the local-error class), err=%q", r.code, r.err)
	}
}

// errWriter fails every Write, modeling a consumer that closed the pipe early (buff -l | head).
type errWriter struct{}

func (errWriter) Write(p []byte) (int, error) { return 0, errors.New("broken pipe") }

// TestListDiagnosticPrefix pins that a management command whose stdout write fails — a closed pipe
// downstream — still produces a buff:-prefixed diagnostic. The tabwriter flush error used to reach
// the user bare ("write …: broken pipe"); marking it keeps every line cli prints reading as buff's.
func TestListDiagnosticPrefix(t *testing.T) {
	w := newWorld(t, store.Config{})
	var errb bytes.Buffer
	code := cli.Run(context.Background(), []string{"-l"}, w.env, cli.IO{
		In: strings.NewReader(""), Out: errWriter{}, Err: &errb, InIsTTY: true, OutIsTTY: false,
	})
	if code == 0 {
		t.Fatal("list to a broken pipe unexpectedly succeeded")
	}
	if !strings.HasPrefix(errb.String(), "buff:") {
		t.Errorf("broken-pipe list diagnostic = %q, want it to lead with buff:", errb.String())
	}
}

// TestArchiveExtractDiagnosticPrefix drives a sink-originated archive error to the user: a terminal
// archive paste into a working directory that already holds the slot-named directory is refused by
// the atomic extractor. That refusal reaches the user through the sink — previously bare, now
// marked buff: — and lands on the conflict exit code 6.
func TestArchiveExtractDiagnosticPrefix(t *testing.T) {
	w := newWorld(t, store.Config{})
	work := t.TempDir()
	t.Chdir(work)
	src := filepath.Join(work, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(src, "f"), "x")
	if r := w.run(t, "", true, false, src, "@a"); r.code != 0 {
		t.Fatalf("copy archive: code=%d err=%q", r.code, r.err)
	}
	// Pre-create the destination named for the slot, so the atomic ExtractNew refuses it.
	if err := os.MkdirAll(filepath.Join(work, "a"), 0o755); err != nil {
		t.Fatal(err)
	}
	r := w.run(t, "", true, true, "@a") // an archive at a TTY ⇒ newDirSink extract into ./a
	if r.code != 6 {
		t.Errorf("extract into an existing dir: code=%d want 6 (conflict), err=%q", r.code, r.err)
	}
	if !strings.HasPrefix(r.err, "buff:") {
		t.Errorf("archive extract diagnostic = %q, want it to lead with buff:", r.err)
	}
}
