package archive

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSafeName pins the path validator that stands in front of the *os.Root boundary: the
// forms standard tar tools emit are accepted and normalized, every escape is rejected. The
// embedded-NUL row matters because the stdlib tar writer refuses to encode such a name, so
// this — and the fuzz target — is where that rejection is actually exercised.
func TestSafeName(t *testing.T) {
	accept := map[string]string{
		"a/b":    "a/b",
		"a/":     "a",   // trailing slash on a directory entry
		"./a":    "a",   // leading "./"
		"a//b":   "a/b", // doubled separator
		"a/./b":  "a/b",
		"a/../b": "b", // a safe interior ".."
	}
	for in, want := range accept {
		got, err := safeName(in)
		if err != nil {
			t.Errorf("safeName(%q) = error %v, want %q", in, err, want)
			continue
		}
		// want is slash-form; compare against safeName's OS-separated result.
		if filepath.FromSlash(want) != got {
			t.Errorf("safeName(%q) = %q, want %q", in, got, want)
		}
	}
	reject := []string{
		"/etc/passwd", "../x", "a/../../b", "..", ".", "", "//", "a\x00b",
	}
	for _, in := range reject {
		if _, err := safeName(in); !errors.Is(err, ErrUnsafePath) {
			t.Errorf("safeName(%q) = %v, want ErrUnsafePath", in, err)
		}
	}
}

// TestClampPerms proves the mode clamp: setuid/setgid/sticky are masked off and the owner
// bits are forced on, so a hostile archive can neither plant a setuid binary nor produce a
// file or directory the extracting user cannot use.
func TestClampPerms(t *testing.T) {
	cases := []struct {
		in       int64
		wantFile fs.FileMode
		wantDir  fs.FileMode
	}{
		{0o644, 0o644, 0o744},
		{0o600, 0o600, 0o700},
		{0o000, 0o600, 0o700},
		{0o4755, 0o755, 0o755}, // setuid masked
		{0o2755, 0o755, 0o755}, // setgid masked
		{0o1777, 0o777, 0o777}, // sticky masked
		{0o777, 0o777, 0o777},
	}
	for _, c := range cases {
		if got := clampFile(c.in); got != c.wantFile {
			t.Errorf("clampFile(%#o) = %#o, want %#o", c.in, got, c.wantFile)
		}
		if got := clampDir(c.in); got != c.wantDir {
			t.Errorf("clampDir(%#o) = %#o, want %#o", c.in, got, c.wantDir)
		}
		if clampFile(c.in)&^0o777 != 0 || clampDir(c.in)&^0o777 != 0 {
			t.Errorf("clamp(%#o) left bits above 0o777", c.in)
		}
	}
}

// TestNonRegular exercises the source-side type classifier with synthetic modes, since
// creating a real device, FIFO or socket needs privileges. Only regular files and
// directories are kept; everything else is skipped.
func TestNonRegular(t *testing.T) {
	cases := []struct {
		desc string
		mode fs.FileMode
		want bool
	}{
		{"regular", 0, false},
		{"directory", fs.ModeDir, false},
		{"symlink", fs.ModeSymlink, true},
		{"block device", fs.ModeDevice, true},
		{"char device", fs.ModeDevice | fs.ModeCharDevice, true},
		{"named pipe", fs.ModeNamedPipe, true},
		{"socket", fs.ModeSocket, true},
		{"irregular", fs.ModeIrregular, true},
	}
	for _, c := range cases {
		// OR in permission bits to confirm only the type bits decide.
		if got := nonRegular(c.mode | 0o644); got != c.want {
			t.Errorf("nonRegular(%s) = %v, want %v", c.desc, got, c.want)
		}
	}
}

// TestCopyEntry pins the short-read translation that guards the upload-abort contract. A source
// shorter than the declared size — a file truncated between the stat that sized the header and
// this read — must surface as io.ErrUnexpectedEOF, never the benign io.EOF that io.CopyN returns,
// because Stream hands its returned error to pipeWriter.CloseWithError and CloseWithError(io.EOF)
// is a clean close: the server would commit the truncated tar as complete instead of aborting. A
// source at or beyond the size is not an error (a consistent prefix snapshot).
func TestCopyEntry(t *testing.T) {
	t.Run("short source is ErrUnexpectedEOF", func(t *testing.T) {
		var buf bytes.Buffer
		err := copyEntry(&buf, strings.NewReader("ab"), 5)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
		}
		if errors.Is(err, io.EOF) {
			t.Fatal("err is io.EOF, which CloseWithError treats as a clean close")
		}
	})
	t.Run("exact source is nil", func(t *testing.T) {
		var buf bytes.Buffer
		if err := copyEntry(&buf, strings.NewReader("abcde"), 5); err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if buf.String() != "abcde" {
			t.Fatalf("copied %q, want %q", buf.String(), "abcde")
		}
	})
	t.Run("longer source copies the prefix", func(t *testing.T) {
		var buf bytes.Buffer
		if err := copyEntry(&buf, strings.NewReader("abcdefgh"), 5); err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if buf.String() != "abcde" {
			t.Fatalf("a file that grew copied %q, want the prefix %q", buf.String(), "abcde")
		}
	})
}

// TestEntryErr proves the per-entry diagnostic never carries the entry's name out. A path-bearing
// filesystem error embeds the offending path, and Localize lets control characters through, so an
// *os.PathError could inject terminal escapes — entryErr strips it to its bare cause while keeping
// the ordinal. A bare sentinel passes through and stays matchable with errors.Is.
func TestEntryErr(t *testing.T) {
	hostile := "evil\x1b[31m\x07.txt"
	pe := &os.PathError{Op: "openat", Path: hostile, Err: errors.New("file name too long")}
	got := entryErr(6, pe).Error()
	if strings.Contains(got, hostile) || strings.Contains(got, "evil") || strings.ContainsRune(got, '\x1b') {
		t.Fatalf("entryErr leaked the hostile entry path: %q", got)
	}
	if !strings.Contains(got, "entry 7") {
		t.Errorf("entryErr dropped the 1-based ordinal: %q", got)
	}
	if !strings.Contains(got, "file name too long") {
		t.Errorf("entryErr dropped the underlying cause: %q", got)
	}
	if !errors.Is(entryErr(0, ErrUnsupportedEntry), ErrUnsupportedEntry) {
		t.Error("entryErr broke errors.Is on a bare sentinel")
	}
}

// TestNormRoots covers the determinism-preserving root normalization: an empty list, roots
// with no usable basename, a duplicate basename across distinct roots, exact-duplicate
// collapse, and the basename sort that makes argument order irrelevant.
func TestNormRoots(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		if _, err := normRoots(nil); !errors.Is(err, ErrNoRoots) {
			t.Fatalf("err = %v, want ErrNoRoots", err)
		}
	})
	t.Run("unusable basename", func(t *testing.T) {
		for _, r := range []string{".", "..", "/", "", "a/.."} {
			if _, err := normRoots([]string{r}); !errors.Is(err, ErrUnusableRoot) {
				t.Errorf("normRoots(%q) = %v, want ErrUnusableRoot", r, err)
			}
		}
	})
	t.Run("duplicate basename", func(t *testing.T) {
		if _, err := normRoots([]string{"a/x", "b/x"}); !errors.Is(err, ErrDuplicateRoot) {
			t.Fatalf("err = %v, want ErrDuplicateRoot", err)
		}
	})
	t.Run("exact duplicate collapses", func(t *testing.T) {
		got, err := normRoots([]string{"a/x", "a/x"})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 {
			t.Fatalf("len = %d, want 1 (exact duplicate deduped)", len(got))
		}
	})
	t.Run("sorted by basename", func(t *testing.T) {
		got, err := normRoots([]string{"p/z", "q/a", "r/m"})
		if err != nil {
			t.Fatal(err)
		}
		var bases []string
		for _, r := range got {
			bases = append(bases, r.base)
		}
		if strings.Join(bases, ",") != "a,m,z" {
			t.Fatalf("bases = %v, want [a m z]", bases)
		}
	})
}
