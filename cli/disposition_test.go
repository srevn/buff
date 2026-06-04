package cli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/srevn/buff/store"
)

// TestTerminalDisposition covers the one cell this feature changes: a finalized file or blob pasted
// at a terminal with no -o. Text shows on the terminal as before; binary, which used to dump as
// garbage, now saves to a file with a stderr note — the symmetry the archive case already kept. The
// salvage and collision cases pin the consume-once guarantee and the conflict exit. Every other
// destination (a pipe, -o, a live follow, an archive) is unchanged and guarded elsewhere; these
// pastes set outTTY true, the axis that selects the new sink.
func TestTerminalDisposition(t *testing.T) {
	const binary = "\x00\x01\x02 not text \xff"
	const text = "hello, terminal"

	t.Run("binary file is saved under its remembered name", func(t *testing.T) {
		w := newWorld(t, store.Config{})
		src := filepath.Join(t.TempDir(), "photo.bin")
		mustWrite(t, src, binary)
		if r := w.run(t, "", true, false, src, "@b"); r.code != 0 {
			t.Fatalf("copy file: code=%d err=%q", r.code, r.err)
		}
		work := t.TempDir()
		t.Chdir(work)
		r := w.run(t, "", true, true, "@b") // outTTY true → terminalSink
		if r.code != 0 {
			t.Fatalf("paste: code=%d err=%q", r.code, r.err)
		}
		if r.out != "" {
			t.Errorf("binary went to stdout (%q), want it saved to a file instead", r.out)
		}
		if !strings.Contains(r.err, "binary") || !strings.Contains(r.err, "photo.bin") {
			t.Errorf("stderr=%q, want a note naming the saved file", r.err)
		}
		assertFile(t, filepath.Join(work, "photo.bin"), binary)
	})

	t.Run("binary blob is saved under the slot name", func(t *testing.T) {
		w := newWorld(t, store.Config{})
		if r := w.run(t, binary, false, true, "@blob"); r.code != 0 { // piped stdin → KindText blob
			t.Fatalf("copy blob: code=%d err=%q", r.code, r.err)
		}
		work := t.TempDir()
		t.Chdir(work)
		if r := w.run(t, "", true, true, "@blob"); r.code != 0 {
			t.Fatalf("paste: code=%d err=%q", r.code, r.err)
		}
		assertFile(t, filepath.Join(work, "blob"), binary)
	})

	t.Run("anonymous binary blob is saved under the default slot", func(t *testing.T) {
		w := newWorld(t, store.Config{})
		if r := w.run(t, binary, false, true); r.code != 0 { // no slot → default
			t.Fatalf("copy default: code=%d err=%q", r.code, r.err)
		}
		work := t.TempDir()
		t.Chdir(work)
		if r := w.run(t, "", true, true); r.code != 0 { // paste default at a terminal
			t.Fatalf("paste: code=%d err=%q", r.code, r.err)
		}
		assertFile(t, filepath.Join(work, "default"), binary)
	})

	t.Run("text is shown, no file written", func(t *testing.T) {
		w := newWorld(t, store.Config{})
		if r := w.run(t, text, false, true, "@t"); r.code != 0 {
			t.Fatalf("copy text: code=%d err=%q", r.code, r.err)
		}
		work := t.TempDir()
		t.Chdir(work)
		r := w.run(t, "", true, true, "@t")
		if r.code != 0 || r.out != text {
			t.Fatalf("paste: code=%d out=%q err=%q", r.code, r.out, r.err)
		}
		if _, err := os.Stat(filepath.Join(work, "t")); !os.IsNotExist(err) {
			t.Errorf("shown text should write no file, stat ./t err=%v", err)
		}
	})
}

// TestPasteOutputDash pins the -o - footgun fix: -o - means raw bytes to stdout for any kind, and
// no longer writes a file literally named "-" the way os.Create("-") once did. It is interpreted at
// routing, so it overrides the terminal heuristic too — here a binary clip at a terminal that would
// otherwise be saved goes raw to stdout because the user asked for it explicitly.
func TestPasteOutputDash(t *testing.T) {
	w := newWorld(t, store.Config{})
	const payload = "\x00raw bytes please\xff"
	if r := w.run(t, payload, false, true, "@x"); r.code != 0 {
		t.Fatalf("copy: code=%d err=%q", r.code, r.err)
	}
	work := t.TempDir()
	t.Chdir(work)
	r := w.run(t, "", true, true, "@x", "-o", "-") // outTTY true, yet -o - forces raw stdout
	if r.code != 0 || r.out != payload {
		t.Fatalf("paste -o -: code=%d out=%q err=%q", r.code, r.out, r.err)
	}
	if _, err := os.Stat(filepath.Join(work, "-")); !os.IsNotExist(err) {
		t.Errorf("-o - wrote a file named %q; it must mean stdout, stat err=%v", "-", err)
	}
}

// TestConsumeOnceTerminalSalvage pins the salvage invariant: a consume-once delivery is spent at the
// server the moment it is opened, so a consumer-side save that cannot begin must not lose it. A
// binary consume-once clip whose no-clobber save collides with an existing file is written raw to
// stdout instead, exit 0 — the delivery reaches the user, the colliding file is left untouched, and
// a note explains the diversion.
func TestConsumeOnceTerminalSalvage(t *testing.T) {
	w := newWorld(t, store.Config{})
	const secret = "\x00the one copy of the secret\xff"
	if r := w.run(t, secret, false, true, "--consume", "@s"); r.code != 0 {
		t.Fatalf("copy consume-once: code=%d err=%q", r.code, r.err)
	}
	work := t.TempDir()
	t.Chdir(work)
	mustWrite(t, filepath.Join(work, "s"), "pre-existing") // collides with the save name
	r := w.run(t, "", true, true, "@s")
	if r.code != 0 {
		t.Fatalf("salvaged paste: code=%d want 0 (delivery not wasted), err=%q", r.code, r.err)
	}
	if r.out != secret {
		t.Errorf("stdout=%q, want the secret salvaged raw", r.out)
	}
	if !strings.Contains(r.err, "writing raw to stdout") {
		t.Errorf("stderr=%q, want the salvage note", r.err)
	}
	assertFile(t, filepath.Join(work, "s"), "pre-existing") // no-clobber: the collision is untouched
}

// TestReplaceableTerminalCollision is the salvage's opposite: a replaceable (not consume-once)
// binary clip whose no-clobber save collides has nothing to lose, so the collision surfaces as a
// conflict (exit 6, like the archive terminal collision) rather than being diverted. Nothing is
// written to stdout and the existing file is left as it was.
func TestReplaceableTerminalCollision(t *testing.T) {
	w := newWorld(t, store.Config{})
	const payload = "\x00replaceable binary\xff"
	if r := w.run(t, payload, false, true, "@r"); r.code != 0 {
		t.Fatalf("copy: code=%d err=%q", r.code, r.err)
	}
	work := t.TempDir()
	t.Chdir(work)
	mustWrite(t, filepath.Join(work, "r"), "pre-existing")
	r := w.run(t, "", true, true, "@r")
	if r.code != 6 {
		t.Errorf("save collision: code=%d want 6 (conflict), err=%q", r.code, r.err)
	}
	if r.out != "" {
		t.Errorf("stdout=%q, want nothing written on a refused save", r.out)
	}
	assertFile(t, filepath.Join(work, "r"), "pre-existing")
}
