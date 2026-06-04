package cli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/srevn/buff/cli"
	"github.com/srevn/buff/store"
)

// TestTextRoundTrip copies piped stdin to a slot and pastes it back to stdout unchanged. A
// piped stdin (InIsTTY false) with no path argument is a text copy; a paste reads the slot.
func TestTextRoundTrip(t *testing.T) {
	w := newWorld(t, store.Config{})
	if r := w.run(t, "hello, buff", false, true, "@greet"); r.code != 0 {
		t.Fatalf("copy: code=%d err=%q", r.code, r.err)
	}
	r := w.run(t, "", true, false, "@greet")
	if r.code != 0 {
		t.Fatalf("paste: code=%d err=%q", r.code, r.err)
	}
	if r.out != "hello, buff" {
		t.Errorf("paste out=%q, want %q", r.out, "hello, buff")
	}
}

// TestDefaultSlot exercises the implicit slot: a copy and a paste with no @ both address
// "default", so a round trip with no slot named works.
func TestDefaultSlot(t *testing.T) {
	w := newWorld(t, store.Config{})
	if r := w.run(t, "no slot named", false, true); r.code != 0 {
		t.Fatalf("copy default: code=%d err=%q", r.code, r.err)
	}
	if r := w.run(t, "", true, false); r.code != 0 || r.out != "no slot named" {
		t.Fatalf("paste default: code=%d out=%q err=%q", r.code, r.out, r.err)
	}
}

// TestFileRoundTrip copies a single regular file — keeping its basename, including a non-ASCII
// one through the percent codec — and pastes it with -o into a directory, where it is restored
// under that remembered name.
func TestFileRoundTrip(t *testing.T) {
	w := newWorld(t, store.Config{})
	src := filepath.Join(t.TempDir(), "café.pdf")
	if err := os.WriteFile(src, []byte("PDF-DATA"), 0o644); err != nil {
		t.Fatal(err)
	}
	if r := w.run(t, "", true, false, src, "@doc"); r.code != 0 {
		t.Fatalf("copy: code=%d err=%q", r.code, r.err)
	}

	outDir := t.TempDir()
	if r := w.run(t, "", true, false, "@doc", "-o", outDir); r.code != 0 {
		t.Fatalf("paste -o dir: code=%d err=%q", r.code, r.err)
	}
	assertFile(t, filepath.Join(outDir, "café.pdf"), "PDF-DATA")

	// -o naming a file path writes there directly, clobbering like a redirect.
	target := filepath.Join(t.TempDir(), "renamed.bin")
	if r := w.run(t, "", true, false, "@doc", "-o", target); r.code != 0 {
		t.Fatalf("paste -o file: code=%d err=%q", r.code, r.err)
	}
	assertFile(t, target, "PDF-DATA")
}

// TestFileToStdout pastes a file clip with no -o to a pipe, getting its raw bytes — the
// cat-like behaviour that makes a file clip's content its output either way.
func TestFileToStdout(t *testing.T) {
	w := newWorld(t, store.Config{})
	src := filepath.Join(t.TempDir(), "data.txt")
	if err := os.WriteFile(src, []byte("raw bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	if r := w.run(t, "", true, false, src, "@f"); r.code != 0 {
		t.Fatalf("copy: %d %q", r.code, r.err)
	}
	if r := w.run(t, "", true, false, "@f"); r.code != 0 || r.out != "raw bytes" {
		t.Errorf("paste to pipe: code=%d out=%q err=%q", r.code, r.out, r.err)
	}
}

// TestTextNoFilenameToDir is the no-filename guard: a text clip has no remembered name, so
// pasting it with -o naming a directory cannot choose a filename and fails clearly rather
// than inventing one.
func TestTextNoFilenameToDir(t *testing.T) {
	w := newWorld(t, store.Config{})
	if r := w.run(t, "just text", false, true, "@t"); r.code != 0 {
		t.Fatalf("copy: %d %q", r.code, r.err)
	}
	dir := t.TempDir()
	r := w.run(t, "", true, false, "@t", "-o", dir)
	if r.code != 1 {
		t.Errorf("paste text -o dir: code=%d want 1", r.code)
	}
	if !strings.Contains(r.err, "no filename") {
		t.Errorf("stderr=%q, want a no-filename diagnostic", r.err)
	}
}

// TestDirArchiveRoundTrip copies a single directory as an archive and pastes it at a terminal
// into a new slot-named directory, verifying both the tree and the documented single-directory
// double-nesting: the basename-prefixed tar reconstructs as proj/src/... under the new dir.
func TestDirArchiveRoundTrip(t *testing.T) {
	w := newWorld(t, store.Config{})
	base := t.TempDir()
	srcDir := filepath.Join(base, "src")
	if err := os.MkdirAll(filepath.Join(srcDir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(srcDir, "a.txt"), "A")
	mustWrite(t, filepath.Join(srcDir, "sub", "b.txt"), "B")

	if r := w.run(t, "", true, false, srcDir, "@proj"); r.code != 0 {
		t.Fatalf("copy dir: code=%d err=%q", r.code, r.err)
	}

	work := t.TempDir()
	t.Chdir(work)
	if r := w.run(t, "", true, true, "@proj"); r.code != 0 { // OutIsTTY true → extract
		t.Fatalf("paste archive: code=%d err=%q", r.code, r.err)
	}
	assertFile(t, filepath.Join(work, "proj", "src", "a.txt"), "A")
	assertFile(t, filepath.Join(work, "proj", "src", "sub", "b.txt"), "B")
}

// TestMultiFileArchive copies two files as an archive; multiple roots group cleanly under the
// slot with no extra nesting, the counterpart to the single-directory case.
func TestMultiFileArchive(t *testing.T) {
	w := newWorld(t, store.Config{})
	base := t.TempDir()
	a, b := filepath.Join(base, "a"), filepath.Join(base, "b")
	mustWrite(t, a, "AA")
	mustWrite(t, b, "BB")

	if r := w.run(t, "", true, false, a, b, "@p"); r.code != 0 {
		t.Fatalf("copy multi: code=%d err=%q", r.code, r.err)
	}
	work := t.TempDir()
	t.Chdir(work)
	if r := w.run(t, "", true, true, "@p"); r.code != 0 {
		t.Fatalf("paste: code=%d err=%q", r.code, r.err)
	}
	assertFile(t, filepath.Join(work, "p", "a"), "AA")
	assertFile(t, filepath.Join(work, "p", "b"), "BB")
}

// TestArchiveToPipeIsRawTar pastes an archive to a pipe and gets the raw tar bytes — so
// piping to tar or redirecting to a file behaves the Unix way — rather than an extraction.
func TestArchiveToPipeIsRawTar(t *testing.T) {
	w := newWorld(t, store.Config{})
	base := t.TempDir()
	one, two := filepath.Join(base, "one"), filepath.Join(base, "two")
	mustWrite(t, one, "data")
	mustWrite(t, two, "more")
	if r := w.run(t, "", true, false, one, two, "@a"); r.code != 0 {
		t.Fatalf("copy: code=%d err=%q", r.code, r.err)
	}
	r := w.run(t, "", true, false, "@a") // OutIsTTY false → raw tar, not extraction
	if r.code != 0 {
		t.Fatalf("paste to pipe: code=%d err=%q", r.code, r.err)
	}
	// tar stores entry names as plain text in the header blocks, so a raw tar carries them.
	if !strings.Contains(r.out, "one") || !strings.Contains(r.out, "two") {
		t.Errorf("raw tar output missing entry names; len=%d", len(r.out))
	}
}

// TestArchiveOutputTargets covers the -o resolution for an archive: an absent target is
// published atomically as a new directory, and an existing directory is merged into.
func TestArchiveOutputTargets(t *testing.T) {
	w := newWorld(t, store.Config{})
	base := t.TempDir()
	srcDir := filepath.Join(base, "tree")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(srcDir, "f.txt"), "F")
	if r := w.run(t, "", true, false, srcDir, "@a"); r.code != 0 {
		t.Fatalf("copy: %d %q", r.code, r.err)
	}

	work := t.TempDir()
	t.Chdir(work)

	t.Run("absent target is atomic new dir", func(t *testing.T) {
		if r := w.run(t, "", true, false, "@a", "-o", "fresh"); r.code != 0 {
			t.Fatalf("paste -o absent: %d %q", r.code, r.err)
		}
		assertFile(t, filepath.Join(work, "fresh", "tree", "f.txt"), "F")
	})

	t.Run("existing dir is merged into", func(t *testing.T) {
		if err := os.MkdirAll(filepath.Join(work, "existing"), 0o755); err != nil {
			t.Fatal(err)
		}
		if r := w.run(t, "", true, false, "@a", "-o", "existing"); r.code != 0 {
			t.Fatalf("paste -o existing: %d %q", r.code, r.err)
		}
		assertFile(t, filepath.Join(work, "existing", "tree", "f.txt"), "F")
	})
}

// TestArchiveTerminalCollision is the conservative terminal default: pasting an archive at a
// terminal into a slot whose directory already exists refuses rather than merging, surfacing the
// typed ErrDestExists as a conflict (exit 6) rather than the generic usage 1.
func TestArchiveTerminalCollision(t *testing.T) {
	w := newWorld(t, store.Config{})
	base := t.TempDir()
	srcDir := filepath.Join(base, "tree")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(srcDir, "f.txt"), "F")
	if r := w.run(t, "", true, false, srcDir, "@a"); r.code != 0 {
		t.Fatalf("copy: %d %q", r.code, r.err)
	}
	work := t.TempDir()
	t.Chdir(work)
	if err := os.MkdirAll(filepath.Join(work, "a"), 0o755); err != nil { // pre-existing ./a
		t.Fatal(err)
	}
	if r := w.run(t, "", true, true, "@a"); r.code != 6 {
		t.Errorf("paste onto existing dir: code=%d want 6 (a conflict — the directory name is taken)", r.code)
	}
}

// TestConsumeOnce copies a consume-once clip, pastes it once with the spent-delivery warning,
// and confirms a second paste finds it gone.
func TestConsumeOnce(t *testing.T) {
	w := newWorld(t, store.Config{})
	if r := w.run(t, "the secret", false, true, "--consume", "@s"); r.code != 0 {
		t.Fatalf("copy: %d %q", r.code, r.err)
	}
	r := w.run(t, "", true, false, "@s")
	if r.code != 0 || r.out != "the secret" {
		t.Fatalf("first paste: code=%d out=%q err=%q", r.code, r.out, r.err)
	}
	if !strings.Contains(r.err, "consume-once") {
		t.Errorf("first paste stderr=%q, want a consume-once warning", r.err)
	}
	// At-most-once: the second read never delivers the secret. Whether the server reports the
	// clip consumed (410, in the brief claim-to-cleanup window) or already gone (404, after the
	// first read's cleanup removed it) is a timing detail of the server-side teardown; both mean
	// "you cannot have it" and both are non-zero. What must hold is that no bytes came back.
	r2 := w.run(t, "", true, false, "@s")
	if r2.code != 3 && r2.code != 4 {
		t.Errorf("second paste: code=%d want 3 (gone) or 4 (consumed)", r2.code)
	}
	if r2.out != "" {
		t.Errorf("second paste delivered %q, want nothing (at-most-once)", r2.out)
	}
}

// TestManagement covers the listing, stat, delete, and version actions, including that a
// delete then a stat reports not-found as exit 3, and that version answers with no server.
func TestManagement(t *testing.T) {
	w := newWorld(t, store.Config{})

	t.Run("empty list is header only", func(t *testing.T) {
		r := w.run(t, "", true, false, "-l")
		if r.code != 0 {
			t.Fatalf("list: %d %q", r.code, r.err)
		}
		if !strings.Contains(r.out, "NAME") {
			t.Errorf("list header missing: %q", r.out)
		}
	})

	t.Run("populated list and stat", func(t *testing.T) {
		if r := w.run(t, "x", false, true, "@one"); r.code != 0 {
			t.Fatalf("seed: %d %q", r.code, r.err)
		}
		r := w.run(t, "", true, false, "-l")
		if !strings.Contains(r.out, "one") {
			t.Errorf("list missing clip: %q", r.out)
		}
		s := w.run(t, "", true, false, "-s", "@one")
		if s.code != 0 || !strings.Contains(s.out, "generation:") || !strings.Contains(s.out, "kind:") {
			t.Errorf("stat: code=%d out=%q", s.code, s.out)
		}
	})

	t.Run("delete then stat is not found", func(t *testing.T) {
		if r := w.run(t, "bye", false, true, "@gone"); r.code != 0 {
			t.Fatalf("seed: %d %q", r.code, r.err)
		}
		if r := w.run(t, "", true, false, "-d", "@gone"); r.code != 0 {
			t.Fatalf("delete: %d %q", r.code, r.err)
		}
		if r := w.run(t, "", true, false, "-s", "@gone"); r.code != 3 {
			t.Errorf("stat after delete: code=%d want 3", r.code)
		}
	})
}

// TestVersionNeedsNoServer points the Env at a refused address and still gets a version,
// proving the client is built lazily and version answers from configuration alone.
func TestVersionNeedsNoServer(t *testing.T) {
	w := &world{env: cli.Env{ServerURL: deadURL(t), Version: "test"}}
	r := w.run(t, "", true, false, "--version")
	if r.code != 0 {
		t.Fatalf("version: code=%d err=%q", r.code, r.err)
	}
	if strings.TrimSpace(r.out) != "test" {
		t.Errorf("version out=%q, want test", r.out)
	}
}

// TestHelpNeedsNoServer points the Env at a refused address and still prints usage, proving help —
// like version — answers offline before any client is built. It also pins that -h wins over a
// management flag (buff -l -h is help, not a "conflicting actions" error) and that usage goes to
// stdout with a clean exit, never to stderr as a diagnostic.
func TestHelpNeedsNoServer(t *testing.T) {
	w := &world{env: cli.Env{ServerURL: deadURL(t), Version: "test"}}
	for _, args := range [][]string{{"-h"}, {"--help"}, {"-l", "-h"}} {
		r := w.run(t, "", true, false, args...)
		if r.code != 0 {
			t.Fatalf("help %v: code=%d err=%q", args, r.code, r.err)
		}
		if !strings.Contains(r.out, "content relay") || !strings.Contains(r.out, "@slot") {
			t.Errorf("help %v: out=%q, want the usage text", args, r.out)
		}
		if r.err != "" {
			t.Errorf("help %v: err=%q, want empty (usage is not a diagnostic)", args, r.err)
		}
	}
}

// mustWrite writes content to path or fails the test.
func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
