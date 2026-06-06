package cli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/srevn/buff/store"
)

// TestGestureDisposition covers the terminal cells of the routing table: a clip pasted at a terminal
// with no -o is disposed by its kind — the gesture that made it — and never by its bytes. A file clip
// saves to a file; a text clip shows on the terminal; whichever way the content reads. The two
// crossed cases are the load-bearing ones, since they are exactly where a content sniff would have
// disagreed: a file clip whose bytes are readable text still saves, and a text clip whose bytes are
// binary still shows. Every other destination (a pipe, -o, an archive, a live follow) is unchanged
// and guarded elsewhere; these pastes set outTTY true, the axis that selects a terminal disposition.
func TestGestureDisposition(t *testing.T) {
	const binary = "\x00\x01\x02 not text \xff"
	const text = "hello, terminal"

	t.Run("file clip with binary content saves", func(t *testing.T) {
		w := newWorld(t, store.Config{})
		src := filepath.Join(t.TempDir(), "photo.bin")
		mustWrite(t, src, binary)
		if r := w.run(t, "", true, false, src, "@b"); r.code != 0 {
			t.Fatalf("copy file: code=%d err=%q", r.code, r.err)
		}
		work := t.TempDir()
		t.Chdir(work)
		r := w.run(t, "", true, true, "@b") // outTTY true → saveSink
		if r.code != 0 {
			t.Fatalf("paste: code=%d err=%q", r.code, r.err)
		}
		if r.out != "" {
			t.Errorf("a file clip went to stdout (%q), want it saved to a file instead", r.out)
		}
		if !strings.Contains(r.err, "saving") || !strings.Contains(r.err, "photo.bin") {
			t.Errorf("stderr=%q, want a note naming the file being saved", r.err)
		}
		assertFile(t, filepath.Join(work, "photo.bin"), binary)
	})

	t.Run("file clip with text content still saves", func(t *testing.T) {
		// The inversion a content sniff would get wrong: readable bytes in a file clip are still saved,
		// because the gesture — a single file copied — is what decides, not how the bytes read.
		w := newWorld(t, store.Config{})
		src := filepath.Join(t.TempDir(), "notes.txt")
		mustWrite(t, src, text)
		if r := w.run(t, "", true, false, src, "@n"); r.code != 0 {
			t.Fatalf("copy file: code=%d err=%q", r.code, r.err)
		}
		work := t.TempDir()
		t.Chdir(work)
		r := w.run(t, "", true, true, "@n")
		if r.code != 0 {
			t.Fatalf("paste: code=%d err=%q", r.code, r.err)
		}
		if r.out != "" {
			t.Errorf("a file clip must save, not show, even when its bytes are text; stdout=%q", r.out)
		}
		assertFile(t, filepath.Join(work, "notes.txt"), text)
	})

	t.Run("text clip with binary content still shows", func(t *testing.T) {
		// The opposite inversion: piping in binary makes a text clip, and a text clip shows raw at a
		// terminal — the binary auto-save the old content sniff did is deliberately gone, recovered
		// with -o - or a pipe. The slot name is never used to write a file here.
		w := newWorld(t, store.Config{})
		if r := w.run(t, binary, false, true, "@blob"); r.code != 0 { // piped stdin → KindText
			t.Fatalf("copy blob: code=%d err=%q", r.code, r.err)
		}
		work := t.TempDir()
		t.Chdir(work)
		r := w.run(t, "", true, true, "@blob")
		if r.code != 0 {
			t.Fatalf("paste: code=%d err=%q", r.code, r.err)
		}
		if r.out != binary {
			t.Errorf("a text clip must stream raw to the terminal; out=%q want %q", r.out, binary)
		}
		if _, err := os.Stat(filepath.Join(work, "blob")); !os.IsNotExist(err) {
			t.Errorf("a text clip must write no file, stat ./blob err=%v", err)
		}
	})

	t.Run("text clip with text content shows", func(t *testing.T) {
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
// routing, so it overrides the terminal disposition too — here a file clip at a terminal that would
// otherwise be saved goes raw to stdout because the user asked for it explicitly.
func TestPasteOutputDash(t *testing.T) {
	w := newWorld(t, store.Config{})
	const payload = "\x00raw bytes please\xff"
	src := filepath.Join(t.TempDir(), "blob.bin")
	mustWrite(t, src, payload)
	if r := w.run(t, "", true, false, src, "@x"); r.code != 0 {
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
	if _, err := os.Stat(filepath.Join(work, "blob.bin")); !os.IsNotExist(err) {
		t.Errorf("-o - saved a file under the remembered name; it must mean stdout, stat err=%v", err)
	}
}

// TestConsumeOnceTerminalSalvage pins the file salvage: a consume-once delivery is spent at the
// server the moment it is opened, so a consumer-side save that collides must not lose it. A
// consume-once file clip whose no-clobber save collides lands on a free sibling beside the colliding
// name — ./secret.<gen>.bin, the generation id spliced before the extension so the file stays
// type-usable — keeping the bytes off the terminal and the existing file untouched, exit 0, with a
// note that names the diversion. The salvage lives in saveSink, which a file clip at a terminal
// reaches; a text clip never reaches it (it streams to stdout with no save to fail).
func TestConsumeOnceTerminalSalvage(t *testing.T) {
	w := newWorld(t, store.Config{})
	const secret = "\x00the one copy of the secret\xff"
	src := filepath.Join(t.TempDir(), "secret.bin")
	mustWrite(t, src, secret)
	if r := w.run(t, "", true, false, "--consume", src, "@s"); r.code != 0 {
		t.Fatalf("copy consume-once: code=%d err=%q", r.code, r.err)
	}
	work := t.TempDir()
	t.Chdir(work)
	mustWrite(t, filepath.Join(work, "secret.bin"), "pre-existing") // collides with the save name
	r := w.run(t, "", true, true, "@s")
	if r.code != 0 {
		t.Fatalf("salvaged paste: code=%d want 0 (delivery not wasted), err=%q", r.code, r.err)
	}
	if r.out != "" {
		t.Errorf("stdout=%q, want nothing — the salvage lands on disk, not the terminal", r.out)
	}
	if !strings.Contains(r.err, "saving the consume-once delivery") || !strings.Contains(r.err, "secret.bin") {
		t.Errorf("stderr=%q, want the diversion note naming the collision and its sibling", r.err)
	}
	assertFile(t, filepath.Join(work, "secret.bin"), "pre-existing") // no-clobber: the collision is untouched
	assertFile(t, soleSibling(t, work, "secret.*.bin"), secret)      // the spent delivery, whole, on a free sibling
}

// TestConsumeOnceArchiveTerminalSalvage is the asymmetry fix this change exists for: a consume-once
// archive whose terminal extract collides with an existing directory name no longer loses its spent
// delivery to exit 6, but lands the whole tree in a free sibling directory beside the colliding name
// — ./a-<gen>/ — exit 0, the collision untouched (not merged into), the diversion narrated. It is the
// archive counterpart of TestConsumeOnceTerminalSalvage; the two are now symmetric.
func TestConsumeOnceArchiveTerminalSalvage(t *testing.T) {
	w := newWorld(t, store.Config{})
	base := t.TempDir()
	srcDir := filepath.Join(base, "tree")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(srcDir, "f.txt"), "F")
	if r := w.run(t, "", true, false, "--consume", srcDir, "@a"); r.code != 0 {
		t.Fatalf("copy consume-once archive: code=%d err=%q", r.code, r.err)
	}
	work := t.TempDir()
	t.Chdir(work)
	if err := os.MkdirAll(filepath.Join(work, "a"), 0o755); err != nil { // pre-existing ./a
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(work, "a", "keep"), "untouched") // a sentinel inside the collision
	r := w.run(t, "", true, true, "@a")
	if r.code != 0 {
		t.Fatalf("salvaged archive paste: code=%d want 0 (delivery not wasted), err=%q", r.code, r.err)
	}
	if !strings.Contains(r.err, "extracted the consume-once delivery") || !strings.Contains(r.err, "a-") {
		t.Errorf("stderr=%q, want the diversion note naming the sibling directory", r.err)
	}
	// The collision is untouched: its sentinel survives and the tree did not merge into it.
	assertFile(t, filepath.Join(work, "a", "keep"), "untouched")
	if _, err := os.Stat(filepath.Join(work, "a", "tree")); !os.IsNotExist(err) {
		t.Errorf("the delivery merged into the colliding ./a, stat ./a/tree err=%v", err)
	}
	// The spent delivery landed whole in the sibling; entries carry the source basename (tree/...).
	assertFile(t, filepath.Join(soleSibling(t, work, "a-*"), "tree", "f.txt"), "F")
}

// TestConsumeOnceSalvageNameTooLong is the salvage's honest floor: a filename so long that splicing
// the generation id overflows ValidFilename's 255-byte bound leaves no valid sibling to form, so the
// delivery is lost — but the loss is reported with a nonzero exit and no half-made sibling, never a
// silent exit 0 with an empty result. The >255 check is openInDir's ValidFilename inside the salvage
// open, not the O_EXCL, so it fires before any byte is written.
func TestConsumeOnceSalvageNameTooLong(t *testing.T) {
	w := newWorld(t, store.Config{})
	const secret = "irreplaceable"
	long := strings.Repeat("n", 240) // 240 + ".<32-hex gen>" = 273 > 255, the ValidFilename bound
	src := filepath.Join(t.TempDir(), long)
	mustWrite(t, src, secret)
	if r := w.run(t, "", true, false, "--consume", src, "@s"); r.code != 0 {
		t.Fatalf("copy consume-once: code=%d err=%q", r.code, r.err)
	}
	work := t.TempDir()
	t.Chdir(work)
	mustWrite(t, filepath.Join(work, long), "pre-existing") // collides with the save name
	r := w.run(t, "", true, true, "@s")
	if r.code == 0 {
		t.Fatalf("over-long salvage: code=0, want a nonzero reported loss (err=%q)", r.err)
	}
	if !strings.Contains(r.err, "could not be salvaged") {
		t.Errorf("stderr=%q, want the salvage to explain the loss, not hide it", r.err)
	}
	assertFile(t, filepath.Join(work, long), "pre-existing") // collision untouched
	// No partial sibling: the only entry whose name begins with the stem is the collision itself.
	if matches, _ := filepath.Glob(filepath.Join(work, long+"*")); len(matches) != 1 {
		t.Errorf("salvage left a partial sibling: glob matched %v, want only the collision", matches)
	}
}

// TestConsumeOnceSalvageSiblingsDistinct pins that the generation id makes siblings delivery-unique:
// two spent deliveries colliding on the same name land distinct siblings rather than one clobbering
// the other. Two consume-once file clips of the same filename, each pasted into a working directory
// whose name is already taken, leave two siblings holding their two different secrets.
func TestConsumeOnceSalvageSiblingsDistinct(t *testing.T) {
	w := newWorld(t, store.Config{})
	work := t.TempDir()
	t.Chdir(work)
	mustWrite(t, filepath.Join(work, "secret.bin"), "pre-existing") // both salvages collide here
	secrets := map[string]string{"@s1": "first secret", "@s2": "second secret"}
	for slot, secret := range secrets {
		src := filepath.Join(t.TempDir(), "secret.bin")
		mustWrite(t, src, secret)
		if r := w.run(t, "", true, false, "--consume", src, slot); r.code != 0 {
			t.Fatalf("copy %s: code=%d err=%q", slot, r.code, r.err)
		}
		if r := w.run(t, "", true, true, slot); r.code != 0 {
			t.Fatalf("salvage %s: code=%d err=%q", slot, r.code, r.err)
		}
	}
	matches, _ := filepath.Glob(filepath.Join(work, "secret.*.bin"))
	if len(matches) != 2 {
		t.Fatalf("two salvages produced %d siblings, want 2 distinct", len(matches))
	}
	got := map[string]bool{}
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			t.Fatal(err)
		}
		got[string(b)] = true
	}
	for _, secret := range secrets {
		if !got[secret] {
			t.Errorf("salvaged secret %q is missing; siblings hold %v", secret, got)
		}
	}
	assertFile(t, filepath.Join(work, "secret.bin"), "pre-existing") // collision still untouched
}

// TestReplaceableTerminalCollision is the salvage's opposite: a replaceable (not consume-once) file
// clip whose no-clobber save collides has nothing to lose, so the collision surfaces as a conflict
// (exit 6, like the archive terminal collision) rather than being diverted. Nothing is written to
// stdout and the existing file is left as it was.
func TestReplaceableTerminalCollision(t *testing.T) {
	w := newWorld(t, store.Config{})
	const payload = "\x00replaceable binary\xff"
	src := filepath.Join(t.TempDir(), "data.bin")
	mustWrite(t, src, payload)
	if r := w.run(t, "", true, false, src, "@r"); r.code != 0 {
		t.Fatalf("copy: code=%d err=%q", r.code, r.err)
	}
	work := t.TempDir()
	t.Chdir(work)
	mustWrite(t, filepath.Join(work, "data.bin"), "pre-existing")
	r := w.run(t, "", true, true, "@r")
	if r.code != 6 {
		t.Errorf("save collision: code=%d want 6 (conflict), err=%q", r.code, r.err)
	}
	if r.out != "" {
		t.Errorf("stdout=%q, want nothing written on a refused save", r.out)
	}
	assertFile(t, filepath.Join(work, "data.bin"), "pre-existing")
}
