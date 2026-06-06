package archive_test

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/srevn/buff/archive"
)

// The malicious-tar table is the heart of the phase. Each hostile archive is hand-built from raw
// headers — writing bytes, not real device nodes or symlinks, so no privileges are needed — and
// extracted into a destination that sits beside a sentinel file. Every row asserts both the right
// rejection and that nothing escaped: the sentinel is untouched and no file appears outside the
// destination directory.

const sentinelBody = "do-not-touch"

// sandbox builds an isolated tree for one extraction test:
//
//	base/
//	  sentinel   a file beside the destination, to prove nothing escapes to it
//	  dst/       the extraction destination
//
// It returns base; inspecting base afterwards proves the destination boundary held.
func sandbox(t *testing.T) string {
	t.Helper()
	base := t.TempDir()
	if err := os.WriteFile(filepath.Join(base, "sentinel"), []byte(sentinelBody), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(base, "dst"), 0o700); err != nil {
		t.Fatal(err)
	}
	return base
}

// tentry is one entry to put in a test archive. A zero Mode is filled in by buildTar.
type tentry struct {
	name string
	flag byte
	body string
	link string
}

// buildTar assembles the raw bytes of a tar from entries. The stdlib writer refuses to encode a
// name containing a NUL, which is itself telling: a NUL-name attack cannot be produced by an honest
// tar writer, so safeName's NUL rejection is proven directly at the validator and by the fuzz
// target, not through a constructed archive here.
func buildTar(t *testing.T, es ...tentry) []byte {
	t.Helper()
	var b bytes.Buffer
	tw := tar.NewWriter(&b)
	for _, e := range es {
		h := &tar.Header{Name: e.name, Typeflag: e.flag, Mode: 0o644, Linkname: e.link}
		if e.flag == tar.TypeDir {
			h.Mode = 0o755
		}
		if e.flag == tar.TypeReg {
			h.Size = int64(len(e.body))
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatalf("WriteHeader(%q): %v", e.name, err)
		}
		if len(e.body) > 0 {
			if _, err := io.WriteString(tw, e.body); err != nil {
				t.Fatalf("Write(%q): %v", e.name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar.Close: %v", err)
	}
	return b.Bytes()
}

// extractInto runs Extract on data against base/dst and returns the error.
func extractInto(t *testing.T, base string, data []byte, opts archive.ExtractOpts) error {
	t.Helper()
	root, err := os.OpenRoot(filepath.Join(base, "dst"))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	return archive.Extract(context.Background(), root, bytes.NewReader(data), opts)
}

// assertBaseIntact proves the destination boundary held: the sentinel is unchanged and the only
// children of base are dst and the sentinel — nothing was written to the parent.
func assertBaseIntact(t *testing.T, base string) {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(base, "sentinel"))
	if err != nil || string(got) != sentinelBody {
		t.Fatalf("sentinel changed or unreadable: content=%q err=%v", got, err)
	}
	ents, err := os.ReadDir(base) // sorted by name: dst < sentinel
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 2 || ents[0].Name() != "dst" || ents[1].Name() != "sentinel" {
		var names []string
		for _, e := range ents {
			names = append(names, e.Name())
		}
		t.Fatalf("base escaped: entries = %v, want [dst sentinel]", names)
	}
}

// assertNoTemp checks ExtractNew left no temporary directory behind in dir.
func assertNoTemp(t *testing.T, dir string) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), ".buff-") {
			t.Errorf("temp leftover not cleaned: %s", e.Name())
		}
	}
}

func TestExtractRejects(t *testing.T) {
	tests := []struct {
		desc    string
		entries []tentry
		want    error
	}{
		{"absolute path", []tentry{{name: "/etc/passwd", flag: tar.TypeReg, body: "x"}}, archive.ErrUnsafePath},
		{"parent escape", []tentry{{name: "../escape", flag: tar.TypeReg, body: "x"}}, archive.ErrUnsafePath},
		{"interior escape", []tentry{{name: "a/../../b", flag: tar.TypeReg, body: "x"}}, archive.ErrUnsafePath},
		{"sentinel overwrite", []tentry{{name: "../sentinel", flag: tar.TypeReg, body: "pwned"}}, archive.ErrUnsafePath},
		{"dot name", []tentry{{name: ".", flag: tar.TypeReg, body: "x"}}, archive.ErrUnsafePath},
		{"symlink", []tentry{{name: "link", flag: tar.TypeSymlink, link: "../../etc"}}, archive.ErrUnsupportedEntry},
		// The symlink is rejected as entry 1, so its write-through target x/evil is never reached; even
		// if it were, the *os.Root would block writing through the link.
		{"symlink then write-through", []tentry{
			{name: "x", flag: tar.TypeSymlink, link: "/tmp"},
			{name: "x/evil", flag: tar.TypeReg, body: "x"},
		}, archive.ErrUnsupportedEntry},
		{"hardlink", []tentry{{name: "h", flag: tar.TypeLink, link: "sentinel"}}, archive.ErrUnsupportedEntry},
		{"char device", []tentry{{name: "dev", flag: tar.TypeChar}}, archive.ErrUnsupportedEntry},
		{"block device", []tentry{{name: "blk", flag: tar.TypeBlock}}, archive.ErrUnsupportedEntry},
		{"fifo", []tentry{{name: "fifo", flag: tar.TypeFifo}}, archive.ErrUnsupportedEntry},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			base := sandbox(t)
			err := extractInto(t, base, buildTar(t, tt.entries...), archive.ExtractOpts{})
			if !errors.Is(err, tt.want) {
				t.Fatalf("Extract error = %v, want %v", err, tt.want)
			}
			assertBaseIntact(t, base)
		})
	}
}

// TestExtractMalformed feeds garbage to the parser: a malformed archive must surface as an error,
// never a panic, and must write nothing.
func TestExtractMalformed(t *testing.T) {
	base := sandbox(t)
	garbage := bytes.Repeat([]byte{0xff}, 600) // not a valid header block
	if err := extractInto(t, base, garbage, archive.ExtractOpts{}); err == nil {
		t.Fatal("Extract of malformed input returned nil, want an error")
	}
	assertBaseIntact(t, base)
}

// TestExtractNoClobber is the load-bearing O_EXCL row: a second entry of the same name is rejected,
// and — the trap this guards — the first file is NOT overwritten.
func TestExtractNoClobber(t *testing.T) {
	base := sandbox(t)
	data := buildTar(t,
		tentry{name: "f", flag: tar.TypeReg, body: "first"},
		tentry{name: "f", flag: tar.TypeReg, body: "second"},
	)
	if err := extractInto(t, base, data, archive.ExtractOpts{}); !errors.Is(err, archive.ErrExists) {
		t.Fatalf("Extract error = %v, want ErrExists", err)
	}
	got, err := os.ReadFile(filepath.Join(base, "dst", "f"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "first" {
		t.Fatalf("file content = %q, want %q — the no-clobber guarantee failed", got, "first")
	}
}

// TestExtractEntryCap proves the off-by-one boundary of the tar-bomb backstop: exactly the cap is
// allowed, one more is rejected.
func TestExtractEntryCap(t *testing.T) {
	dirs := func(n int) []tentry {
		es := make([]tentry, n)
		for i := range es {
			es[i] = tentry{name: fmt.Sprintf("d%d", i), flag: tar.TypeDir}
		}
		return es
	}
	t.Run("over cap", func(t *testing.T) {
		base := sandbox(t)
		err := extractInto(t, base, buildTar(t, dirs(4)...), archive.ExtractOpts{MaxEntries: 3})
		if !errors.Is(err, archive.ErrTooManyEntries) {
			t.Fatalf("error = %v, want ErrTooManyEntries", err)
		}
	})
	t.Run("at cap", func(t *testing.T) {
		base := sandbox(t)
		if err := extractInto(t, base, buildTar(t, dirs(3)...), archive.ExtractOpts{MaxEntries: 3}); err != nil {
			t.Fatalf("error = %v, want nil at exactly the cap", err)
		}
	})
}

// TestExtractGood is the well-formed counterpart: a deep, long, Unicode-named tree extracts
// faithfully and stays inside the destination.
func TestExtractGood(t *testing.T) {
	base := sandbox(t)
	long := strings.Repeat("n", 200)
	files := map[string]string{
		"dir/file.txt":      "hello",
		"a/b/c/d/e/" + long: "deep",
		"café/résumé.txt":   "unicode",
	}
	data := buildTar(t,
		tentry{name: "dir/", flag: tar.TypeDir},
		tentry{name: "dir/file.txt", flag: tar.TypeReg, body: "hello"},
		tentry{name: "a/b/c/d/e/" + long, flag: tar.TypeReg, body: "deep"},
		tentry{name: "café/résumé.txt", flag: tar.TypeReg, body: "unicode"},
	)
	if err := extractInto(t, base, data, archive.ExtractOpts{}); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	dst := filepath.Join(base, "dst")
	for name, want := range files {
		got, err := os.ReadFile(filepath.Join(dst, filepath.FromSlash(name)))
		if err != nil {
			t.Errorf("reading %s: %v", name, err)
			continue
		}
		if string(got) != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	assertBaseIntact(t, base)
}

// TestExtractContextCancel checks the between-entries cancellation: a pre-cancelled ctx stops
// extraction before any byte is written.
func TestExtractContextCancel(t *testing.T) {
	base := sandbox(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	root, err := os.OpenRoot(filepath.Join(base, "dst"))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	data := buildTar(t, tentry{name: "f", flag: tar.TypeReg, body: "x"})
	if err := archive.Extract(ctx, root, bytes.NewReader(data), archive.ExtractOpts{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	assertBaseIntact(t, base)
}

// TestExtractNewSuccess is the atomic-publish happy path: a clean archive lands whole under a fresh
// name, and the temporary directory is gone.
func TestExtractNewSuccess(t *testing.T) {
	base := sandbox(t)
	parent, err := os.OpenRoot(base)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	data := buildTar(t,
		tentry{name: "sub/", flag: tar.TypeDir},
		tentry{name: "sub/a.txt", flag: tar.TypeReg, body: "A"},
		tentry{name: "b.txt", flag: tar.TypeReg, body: "B"},
	)
	if err := archive.ExtractNew(context.Background(), parent, "out", bytes.NewReader(data), archive.ExtractOpts{}); err != nil {
		t.Fatalf("ExtractNew: %v", err)
	}
	for name, want := range map[string]string{"sub/a.txt": "A", "b.txt": "B"} {
		got, err := os.ReadFile(filepath.Join(base, "out", filepath.FromSlash(name)))
		if err != nil || string(got) != want {
			t.Errorf("out/%s = %q err=%v, want %q", name, got, err, want)
		}
	}
	assertNoTemp(t, base)
}

// TestExtractNewDrainedReaderPlantsEmptyTree pins the silent-success hazard a caller that loops
// ExtractNew across names must never trigger: an already-drained reader extracts to an empty
// tree and publishes it with a nil error, indistinguishable from real success. A reader past its
// last byte reads as a clean empty stream, so the tar parser sees immediate EOF, materializes
// nothing, and the publish renames an empty directory into place. This is exactly why the consume-
// once salvage commits ExtractNew once, on the unique sibling it will keep: the body is spent by
// the first extraction, so a second ExtractNew on a different name would plant this bogus empty
// directory while the delivery is already lost. The property is the salvage's standing proof, kept
// here even though no production caller loops today.
func TestExtractNewDrainedReaderPlantsEmptyTree(t *testing.T) {
	base := sandbox(t)
	parent, err := os.OpenRoot(base)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	r := bytes.NewReader(buildTar(t, tentry{name: "f.txt", flag: tar.TypeReg, body: "real"}))
	if _, err := io.Copy(io.Discard, r); err != nil { // drain the body, as a first salvage extraction would
		t.Fatal(err)
	}
	if err := archive.ExtractNew(context.Background(), parent, "ghost", r, archive.ExtractOpts{}); err != nil {
		t.Fatalf("ExtractNew of a drained reader = %v, want nil (the hazard is that empty success)", err)
	}
	ents, err := os.ReadDir(filepath.Join(base, "ghost"))
	if err != nil {
		t.Fatalf("read ghost dir: %v", err)
	}
	if len(ents) != 0 {
		t.Fatalf("ghost holds %d entries, want 0 — a drained reader extracts an empty tree", len(ents))
	}
}

// TestExtractNewAtomicFailure is the atomicity guarantee: a hostile entry after good ones fails
// the whole publish — the destination name never appears and the temp is removed, so nothing
// materializes.
func TestExtractNewAtomicFailure(t *testing.T) {
	base := sandbox(t)
	parent, err := os.OpenRoot(base)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	data := buildTar(t,
		tentry{name: "good.txt", flag: tar.TypeReg, body: "ok"},
		tentry{name: "sub/", flag: tar.TypeDir},
		tentry{name: "evil", flag: tar.TypeSymlink, link: "/etc"},
	)
	err = archive.ExtractNew(context.Background(), parent, "out", bytes.NewReader(data), archive.ExtractOpts{})
	if !errors.Is(err, archive.ErrUnsupportedEntry) {
		t.Fatalf("error = %v, want ErrUnsupportedEntry", err)
	}
	if _, err := os.Lstat(filepath.Join(base, "out")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("out must not exist after a failed publish; Lstat err = %v", err)
	}
	assertNoTemp(t, base)
	assertBaseIntact(t, base)
}

// TestExtractNewDestExists refuses to publish over an existing name (the sandbox's dst).
func TestExtractNewDestExists(t *testing.T) {
	base := sandbox(t)
	parent, err := os.OpenRoot(base)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	data := buildTar(t, tentry{name: "f", flag: tar.TypeReg, body: "x"})
	if err := archive.ExtractNew(context.Background(), parent, "dst", bytes.NewReader(data), archive.ExtractOpts{}); !errors.Is(err, archive.ErrDestExists) {
		t.Fatalf("error = %v, want ErrDestExists", err)
	}
	assertNoTemp(t, base)
}

// TestExtractNewBadName rejects a destination that is not a single fresh component, the defense-in-
// depth in front of the *os.Root boundary.
func TestExtractNewBadName(t *testing.T) {
	base := sandbox(t)
	parent, err := os.OpenRoot(base)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	for _, name := range []string{"a/b", "..", ".", ""} {
		if err := archive.ExtractNew(context.Background(), parent, name, bytes.NewReader(nil), archive.ExtractOpts{}); !errors.Is(err, archive.ErrUnsafePath) {
			t.Errorf("ExtractNew(name=%q) = %v, want ErrUnsafePath", name, err)
		}
	}
}

// TestExtractNewConcurrentPublish drives the race the up-front Lstat cannot win: several
// publishes into the same fresh name at once. The publish is os.Root.Rename — renameat(2) without
// RENAME_NOREPLACE — so exactly one wins and each loser's rename fails onto the winner's populated
// tree (ENOTEMPTY), a raw *os.LinkError the contract owes back as the typed ErrDestExists. This
// pins the re-derive: every loser sees ErrDestExists, never a raw rename error, and no extraction
// temp is left behind on any losing (post-extract, rename-failed) publish. It fails on the un-
// patched extractor — where the losers carry the raw LinkError — and is meant to run under -race.
func TestExtractNewConcurrentPublish(t *testing.T) {
	data := buildTar(t,
		tentry{name: "sub/", flag: tar.TypeDir},
		tentry{name: "sub/a.txt", flag: tar.TypeReg, body: "A"},
		tentry{name: "b.txt", flag: tar.TypeReg, body: "B"},
	)
	const goroutines = 4
	// Each iteration reliably has one winner and goroutines-1 losers; the iteration count makes the
	// losers take the post-Lstat rename path (not the occasional up-front fail-fast, when a goroutine
	// starts after another has already published) and stresses the deferred temp cleanup on every
	// losing publish.
	for iter := range 200 {
		base := t.TempDir()
		parent, err := os.OpenRoot(base)
		if err != nil {
			t.Fatal(err)
		}

		start := make(chan struct{}) // released all at once, so the publishes genuinely race
		errs := make([]error, goroutines)
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for i := range goroutines {
			go func(i int) {
				defer wg.Done()
				<-start
				errs[i] = archive.ExtractNew(context.Background(), parent, "work", bytes.NewReader(data), archive.ExtractOpts{})
			}(i)
		}
		close(start)
		wg.Wait()
		parent.Close()

		winners := 0
		for i, e := range errs {
			switch {
			case e == nil:
				winners++
			case errors.Is(e, archive.ErrDestExists):
				// the typed refusal the contract owes — good
			default:
				t.Fatalf("iter %d goroutine %d: loser error = %v, want ErrDestExists; a raw rename error means the typed identity was lost", iter, i, e)
			}
		}
		if winners != 1 {
			t.Fatalf("iter %d: %d winners, want exactly 1", iter, winners)
		}
		if _, err := os.Stat(filepath.Join(base, "work", "b.txt")); err != nil {
			t.Fatalf("iter %d: winner's tree not published under work/: %v", iter, err)
		}
		assertNoTemp(t, base) // every loser's temp must be cleaned by its deferred RemoveAll
	}
}
