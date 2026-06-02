package archive

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// mustWrite creates dir/rel (and any parent directories) with the given content and perm.
func mustWrite(t *testing.T, dir, rel, body string, perm fs.FileMode) {
	t.Helper()
	p := filepath.Join(dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), perm); err != nil {
		t.Fatal(err)
	}
}

// readTar parses a tar into its headers, in order.
func readTar(t *testing.T, b []byte) []*tar.Header {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(b))
	var hs []*tar.Header
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar read: %v", err)
		}
		hs = append(hs, h)
	}
	return hs
}

// relNames returns the set of entry names with the root basename prefix and any trailing
// slash removed, so a test can assert membership without depending on the random temp-dir
// name. The root itself becomes "".
func relNames(t *testing.T, b []byte, base string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, h := range readTar(t, b) {
		n := strings.TrimSuffix(h.Name, "/")
		n = strings.TrimPrefix(n, base)
		out[strings.TrimPrefix(n, "/")] = true
	}
	return out
}

// TestStreamSkipsNonRegular proves the source-side skip-with-warning: a symlink to a file
// and a symlink to a directory are both absent from the tar, reported through OnSkip, and
// the symlinked directory is not descended — while the real files survive.
func TestStreamSkipsNonRegular(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, src, "real.txt", "data", 0o644)
	mustWrite(t, src, "d/inner.txt", "in", 0o644)
	if err := os.Symlink("real.txt", filepath.Join(src, "flink")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := os.Symlink("d", filepath.Join(src, "dlink")); err != nil {
		t.Fatal(err)
	}

	var skipped []string
	var modes []fs.FileMode
	opts := StreamOpts{OnSkip: func(rel string, m fs.FileMode) {
		skipped = append(skipped, rel)
		modes = append(modes, m)
	}}
	var buf bytes.Buffer
	if err := Stream(context.Background(), []string{src}, &buf, opts); err != nil {
		t.Fatalf("Stream: %v", err)
	}

	base := filepath.Base(src)
	names := relNames(t, buf.Bytes(), base)
	for n := range names {
		if strings.Contains(n, "link") {
			t.Errorf("a symlink leaked into the tar: %q", n)
		}
	}
	for _, want := range []string{"", "real.txt", "d", "d/inner.txt"} {
		if !names[want] {
			t.Errorf("expected entry %q missing from tar; have %v", want, names)
		}
	}
	if len(skipped) != 2 {
		t.Fatalf("OnSkip called %d times (%v), want 2", len(skipped), skipped)
	}
	for i, m := range modes {
		if m&fs.ModeSymlink == 0 {
			t.Errorf("skipped %q has mode %v, expected a symlink", skipped[i], m)
		}
	}
}

// TestStreamDeterministic proves the byte stream is reproducible: the same tree twice, and
// the same roots in reversed argument order, all yield identical bytes.
func TestStreamDeterministic(t *testing.T) {
	parent := t.TempDir()
	one := filepath.Join(parent, "one")
	two := filepath.Join(parent, "two")
	mustWrite(t, one, "a.txt", "1", 0o644)
	mustWrite(t, one, "sub/c.txt", "3", 0o644)
	mustWrite(t, two, "b.txt", "2", 0o644)

	stream := func(roots []string) []byte {
		var buf bytes.Buffer
		if err := Stream(context.Background(), roots, &buf, StreamOpts{}); err != nil {
			t.Fatalf("Stream(%v): %v", roots, err)
		}
		return buf.Bytes()
	}
	a := stream([]string{one, two})
	if b := stream([]string{one, two}); !bytes.Equal(a, b) {
		t.Error("the same tree produced different bytes on a second pass")
	}
	if c := stream([]string{two, one}); !bytes.Equal(a, c) {
		t.Error("reversing the argument order changed the bytes")
	}
}

// TestStreamHeaderSanitized proves the per-entry sanitizer: the modification time is kept,
// owner ids and names are dropped, access and change times are zeroed (so a read cannot
// perturb the bytes), and setuid/setgid/sticky are masked off.
func TestStreamHeaderSanitized(t *testing.T) {
	src := t.TempDir()
	p := filepath.Join(src, "f.txt")
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, 0o4755); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(p); fi.Mode()&fs.ModeSetuid == 0 {
		t.Skip("filesystem did not retain the setuid bit; the mask check would be vacuous")
	}
	mt := time.Now().Add(-100 * time.Hour).Truncate(time.Second)
	if err := os.Chtimes(p, mt, mt); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := Stream(context.Background(), []string{src}, &buf, StreamOpts{}); err != nil {
		t.Fatal(err)
	}
	var h *tar.Header
	for _, x := range readTar(t, buf.Bytes()) {
		if strings.HasSuffix(x.Name, "f.txt") {
			h = x
		}
	}
	if h == nil {
		t.Fatal("f.txt missing from the tar")
	}
	if !h.ModTime.Equal(mt) {
		t.Errorf("mtime = %v, want %v (must be preserved)", h.ModTime, mt)
	}
	if h.Uid != 0 || h.Gid != 0 || h.Uname != "" || h.Gname != "" {
		t.Errorf("ownership leaked: uid=%d gid=%d uname=%q gname=%q", h.Uid, h.Gid, h.Uname, h.Gname)
	}
	if !h.AccessTime.IsZero() || !h.ChangeTime.IsZero() {
		t.Errorf("access/change times not zeroed: atime=%v ctime=%v", h.AccessTime, h.ChangeTime)
	}
	if h.Mode&^0o777 != 0 {
		t.Errorf("mode %#o keeps bits above 0o777 (setuid/setgid/sticky not masked)", h.Mode)
	}
}

// closeRecorder is a writer that records whether Close was called.
type closeRecorder struct {
	io.Writer
	closed bool
}

func (c *closeRecorder) Close() error { c.closed = true; return nil }

// TestStreamNeverClosesWriter proves Stream leaves w open: the CLI's pipe owns the writer
// and is the one that closes it (with any error), so Stream closing it would break the abort
// signal.
func TestStreamNeverClosesWriter(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, src, "f", "x", 0o644)
	rec := &closeRecorder{Writer: &bytes.Buffer{}}
	if err := Stream(context.Background(), []string{src}, rec, StreamOpts{}); err != nil {
		t.Fatal(err)
	}
	if rec.closed {
		t.Fatal("Stream closed w, but the caller owns it")
	}
}

// TestStreamContextCancel checks a pre-cancelled context stops Stream at the first entry.
func TestStreamContextCancel(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, src, "f", "x", 0o644)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var buf bytes.Buffer
	if err := Stream(ctx, []string{src}, &buf, StreamOpts{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// TestStreamMissingRoot proves a named root that cannot be read is a hard error, not a
// silently-skipped entry: skipping is only ever for non-regular entry types, never for a
// path the caller asked to archive but that is absent or unreadable.
func TestStreamMissingRoot(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	var buf bytes.Buffer
	if err := Stream(context.Background(), []string{missing}, &buf, StreamOpts{}); err == nil {
		t.Fatal("Stream of a missing root returned nil, want a hard error")
	}
}

// TestStreamMultipleFileRoots covers the multi-path copy source (several bare paths at once): a
// set of plain files is archived with each named under its own basename and no enclosing
// directory, and a round trip restores them side by side. The single-directory case is exercised
// elsewhere; this pins the file-list case, which has its own normRoots path (basenames, not a
// walked tree).
func TestStreamMultipleFileRoots(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, src, "a.txt", "alpha", 0o644)
	mustWrite(t, src, "b.txt", "beta", 0o644)
	roots := []string{filepath.Join(src, "a.txt"), filepath.Join(src, "b.txt")}

	var buf bytes.Buffer
	if err := Stream(context.Background(), roots, &buf, StreamOpts{}); err != nil {
		t.Fatalf("Stream: %v", err)
	}

	dst := t.TempDir()
	root, err := os.OpenRoot(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := Extract(context.Background(), root, &buf, ExtractOpts{}); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	for name, want := range map[string]string{"a.txt": "alpha", "b.txt": "beta"} {
		got, err := os.ReadFile(filepath.Join(dst, name))
		if err != nil || string(got) != want {
			t.Errorf("%s = %q err=%v, want %q (each file at top level under its basename)", name, got, err, want)
		}
	}
}

// TestStreamRoundTrip ties the two directions together: a tree streamed and then extracted
// reproduces its structure and content, keeps an empty directory, guarantees the extracting
// user can use every entry, and never restores a setuid/setgid/sticky bit.
func TestStreamRoundTrip(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, src, "a.txt", "alpha", 0o644)
	mustWrite(t, src, "sub/b.txt", "beta", 0o644)
	mustWrite(t, src, "secret", "s", 0o600)
	if err := os.Mkdir(filepath.Join(src, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := Stream(context.Background(), []string{src}, &buf, StreamOpts{}); err != nil {
		t.Fatalf("Stream: %v", err)
	}

	dst := t.TempDir()
	root, err := os.OpenRoot(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := Extract(context.Background(), root, &buf, ExtractOpts{}); err != nil {
		t.Fatalf("Extract: %v", err)
	}

	base := filepath.Base(src)
	at := func(rel string) string { return filepath.Join(dst, base, filepath.FromSlash(rel)) }

	for rel, want := range map[string]string{"a.txt": "alpha", "sub/b.txt": "beta", "secret": "s"} {
		got, err := os.ReadFile(at(rel))
		if err != nil || string(got) != want {
			t.Errorf("%s = %q err=%v, want %q", rel, got, err, want)
		}
	}
	if fi, err := os.Stat(at("empty")); err != nil || !fi.IsDir() {
		t.Errorf("empty directory not reproduced: err=%v", err)
	}

	// Permissions are asserted umask-independently: the owner can always use each entry,
	// and no special bit is ever restored. (Exact perm values would depend on the test
	// process's umask; the owner bits and the absence of special bits do not.)
	special := fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky
	for _, rel := range []string{"a.txt", "sub/b.txt", "secret", "sub", "empty"} {
		fi, err := os.Stat(at(rel))
		if err != nil {
			t.Errorf("stat %s: %v", rel, err)
			continue
		}
		var ownerNeed fs.FileMode = 0o600
		if fi.IsDir() {
			ownerNeed = 0o700
		}
		if fi.Mode().Perm()&ownerNeed != ownerNeed {
			t.Errorf("%s perm %#o lacks owner bits %#o", rel, fi.Mode().Perm(), ownerNeed)
		}
		if fi.Mode()&special != 0 {
			t.Errorf("%s carries a special bit: %v", rel, fi.Mode())
		}
	}
}
