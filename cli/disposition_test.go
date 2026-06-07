package cli_test

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
)

// TestGestureDisposition covers the terminal cells of the routing table: a clip pasted at a
// terminal with no -o is disposed by its kind — the gesture that made it — and never by its bytes.
// A file clip saves to a file; a bytes clip shows on the terminal; whichever way the content
// reads. The two crossed cases are the load-bearing ones — where the bytes and the gesture pull
// opposite ways: a file clip whose bytes are readable text still saves, and a bytes clip whose
// bytes are binary still shows. Every other destination (a pipe, -o, an archive, a live follow) is
// unchanged and guarded elsewhere; these pastes set outTTY true, the axis that selects a terminal
// disposition.
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
		// Gesture, not content, decides: readable bytes in a file clip are still saved, because a single
		// file copied is what made it, not how the bytes read.
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

	t.Run("bytes clip with binary content still shows", func(t *testing.T) {
		// The opposite inversion: piping in binary makes a bytes clip, and a bytes clip shows raw at a
		// terminal even when the bytes are binary, never saved to a file. The slot name is not used to
		// write a file here.
		w := newWorld(t, store.Config{})
		if r := w.run(t, binary, false, true, "@blob"); r.code != 0 { // piped stdin → KindBytes
			t.Fatalf("copy blob: code=%d err=%q", r.code, r.err)
		}
		work := t.TempDir()
		t.Chdir(work)
		r := w.run(t, "", true, true, "@blob")
		if r.code != 0 {
			t.Fatalf("paste: code=%d err=%q", r.code, r.err)
		}
		if r.out != binary {
			t.Errorf("a bytes clip must stream raw to the terminal; out=%q want %q", r.out, binary)
		}
		if _, err := os.Stat(filepath.Join(work, "blob")); !os.IsNotExist(err) {
			t.Errorf("a bytes clip must write no file, stat ./blob err=%v", err)
		}
	})

	t.Run("bytes clip with text content shows", func(t *testing.T) {
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

// TestPasteOutputDash pins -o -: it means raw bytes to stdout for any kind, never a file literally
// named "-". It is interpreted at routing, so it overrides the terminal disposition too — here a
// file clip at a terminal that would otherwise be saved goes raw to stdout because the user asked
// for it explicitly.
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

// TestConsumeOnceTerminalSalvage pins the file salvage: a consume-once delivery is spent at
// the server the moment it is opened, so a consumer-side save that collides must not lose it.
// A consume-once file clip whose no-clobber save collides lands on a free sibling beside the
// colliding name — ./secret.<gen>.bin, the generation id spliced before the extension so the file
// stays type-usable — keeping the bytes off the terminal and the existing file untouched, exit
// 0, with a note that names the diversion. The salvage lives in saveSink, which a file clip at a
// terminal reaches; a bytes clip never reaches it (it streams to stdout with no save to fail).
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

// TestConsumeOnceArchiveTerminalSalvage covers the archive salvage: a consume-once archive whose
// terminal extract collides with an existing directory name lands the whole tree in a free sibling
// directory beside the colliding name — ./a-<gen>/ — exit 0, the collision untouched (not merged
// into), the diversion narrated. It is the archive counterpart of TestConsumeOnceTerminalSalvage,
// symmetric with it.
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
// the generation id overflows ValidFilename's 255-byte bound leaves no usable sibling to form,
// so the delivery is lost — but the loss is reported as the same conflict (exit 6) as any other
// unsalvageable collision, with no half-made sibling, never a silent exit 0 with an empty result.
// The over-long name is caught by the flow's ValidFilename gate (divertConsumeOnce), uniformly with
// a hostile generation, before landBeside reads a byte — not deep in the sink's own open.
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
	if r.code != 6 {
		t.Fatalf("over-long salvage: code=%d, want 6 (collision stands, delivery reported lost), err=%q", r.code, r.err)
	}
	if !strings.Contains(r.err, "usable sibling") || !strings.Contains(r.err, "lost") {
		t.Errorf("stderr=%q, want the flow to name the unusable sibling and the lost delivery", r.err)
	}
	assertFile(t, filepath.Join(work, long), "pre-existing") // collision untouched
	// No partial sibling: the only entry whose name begins with the stem is the collision itself.
	if matches, _ := filepath.Glob(filepath.Join(work, long+"*")); len(matches) != 1 {
		t.Errorf("salvage left a partial sibling: glob matched %v, want only the collision", matches)
	}
}

// TestConsumeOnceSalvageSiblingsDistinct pins that the generation id makes siblings delivery-
// unique: two spent deliveries colliding on the same name land distinct siblings rather than
// one clobbering the other. Two consume-once file clips of the same filename, each pasted into a
// working directory whose name is already taken, leave two siblings holding their two different
// secrets.
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

// TestConsumeOnceDuplicateEntryArchiveNotDiverted pins that archive.ErrExists reaching a salvager
// is kept non-divertable by the PRISTINE gate, not by isCollision's sentinel list. A consume-once
// archive whose tar carries two identically-named entries is pasted at a terminal into a FREE slot
// name: newDirSink (a salvager) extracts the first entry, then ExtractNew's O_EXCL rejects the
// duplicate with archive.ErrExists. By then bytes have been read, so the body is no longer pristine
// and the divert never fires — the delivery is lost, but reported as the conflict it is (exit 6),
// with no diversion narration, no sibling directory, and no temp tree left behind. The duplicate-
// entry tar is a shape buff's own Stream never emits but a foreign peer can. It guards the combined
// regression of adding ErrExists to isCollision while dropping the pristine gate, which would re-
// extract the already-drained body and plant a bogus empty sibling.
func TestConsumeOnceDuplicateEntryArchiveNotDiverted(t *testing.T) {
	w := newWorld(t, store.Config{})
	ctx := context.Background()
	work := t.TempDir()
	t.Chdir(work)

	var tarbuf bytes.Buffer
	tw := tar.NewWriter(&tarbuf)
	for range 2 { // two entries of one name; the second trips extractReg's O_EXCL → archive.ErrExists
		if err := tw.WriteHeader(&tar.Header{Name: "f.txt", Mode: 0o644, Size: 1, Typeflag: tar.TypeReg, Format: tar.FormatPAX}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte("X")); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	// Write the crafted tar straight into a consume-once archive clip, the way the live-flow tests
	// seed the store, since no copy gesture produces a duplicate-entry tar.
	wr, err := w.st.Create(ctx, "dup", clip.Meta{Kind: clip.KindArchive}, store.PutOpts{ConsumeOnce: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wr.Write(tarbuf.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := wr.Close(); err != nil {
		t.Fatal(err)
	}

	r := w.run(t, "", true, true, "@dup") // an archive at a TTY into the free slot "dup" → newDirSink
	if r.code != 6 {
		t.Fatalf("duplicate-entry consume-once archive: code=%d want 6 (conflict, not diverted), err=%q", r.code, r.err)
	}
	if strings.Contains(r.err, "extracted the consume-once delivery") {
		t.Errorf("a non-pristine duplicate-entry delivery was diverted: stderr=%q", r.err)
	}
	if r.out != "" {
		t.Errorf("stdout=%q, want nothing", r.out)
	}
	// No diversion sibling (dup-<gen>/) and no published slot dir of any kind.
	if matches, _ := filepath.Glob(filepath.Join(work, "dup*")); len(matches) != 0 {
		t.Errorf("a sibling or slot directory was created: %v, want none", matches)
	}
	if hasTempDir(work) {
		t.Error("an extraction temp tree was left behind after the rolled-back extract")
	}
}

// TestConsumeOnceOutputLoss pins the -o loss paths. A consume-once archive routed to an explicit
// -o target is never salvaged — the user named that destination, so extractSink is not a salvager —
// yet the delivery is still spent at the server the instant it is opened. Both arms must therefore
// report the loss the way every other unsalvaged path does: the upfront "spent it" notice plus
// the flow's one "consume-once delivery lost" tail on the final line, and neither may divert (no
// sibling, no diversion narration). The arms differ only in their exit code — the conflict 6 of a
// merge collision versus the generic 1 of a not-a-directory target.
//
// The merge arm also pins the spent state's sequential re-fetch: once the claiming reader's Close
// has cleaned up server-side (waited on through Stat, since the handler's defer Close races a bare
// re-fetch), the clip is gone, so a second paste is not-found (exit 3), not consumed (exit 4) — the
// common timing, distinct from the mid-drain exit 4 a concurrent reader would see.
func TestConsumeOnceOutputLoss(t *testing.T) {
	w := newWorld(t, store.Config{})
	ctx := context.Background()

	// seed writes a single-entry consume-once archive clip under slot. A crafted tar, not a copy
	// gesture, so the lone entry's name is known exactly (the merge arm pre-creates it to collide) and
	// both arms read the same body shape.
	seed := func(t *testing.T, slot string) {
		t.Helper()
		var tarbuf bytes.Buffer
		tw := tar.NewWriter(&tarbuf)
		if err := tw.WriteHeader(&tar.Header{Name: "f.txt", Mode: 0o644, Size: 1, Typeflag: tar.TypeReg, Format: tar.FormatPAX}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte("X")); err != nil {
			t.Fatal(err)
		}
		if err := tw.Close(); err != nil {
			t.Fatal(err)
		}
		wr, err := w.st.Create(ctx, slot, clip.Meta{Kind: clip.KindArchive}, store.PutOpts{ConsumeOnce: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := wr.Write(tarbuf.Bytes()); err != nil {
			t.Fatal(err)
		}
		if err := wr.Close(); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("merge collision into an existing -o directory", func(t *testing.T) {
		seed(t, "merge")
		work := t.TempDir()
		t.Chdir(work)
		dest := filepath.Join(work, "dest")
		if err := os.MkdirAll(dest, 0o755); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, filepath.Join(dest, "f.txt"), "pre-existing") // collides with the archive's lone entry

		r := w.run(t, "", true, true, "@merge", "-o", "dest") // extractSink merges; per-entry O_EXCL refuses
		if r.code != 6 {
			t.Fatalf("merge collision: code=%d want 6 (conflict), err=%q", r.code, r.err)
		}
		if !strings.Contains(r.err, "spent it") {
			t.Errorf("stderr=%q, want the upfront spent-it notice", r.err)
		}
		if !strings.Contains(r.err, "consume-once delivery lost") {
			t.Errorf("stderr=%q, want the flow's uniform loss tail on an -o path", r.err)
		}
		if strings.Contains(r.err, "extracted the consume-once delivery") {
			t.Errorf("an -o delivery was diverted: stderr=%q", r.err)
		}
		assertFile(t, filepath.Join(dest, "f.txt"), "pre-existing") // collision untouched, not merged over
		if matches, _ := filepath.Glob(filepath.Join(work, "dest-*")); len(matches) != 0 {
			t.Errorf("an -o sink formed a salvage sibling %v, want none", matches)
		}

		// The spent state is two codes by timing. Wait for the server-side cleanup (the api handler's
		// defer Close runs cleanupConsumed after the response), then stat — the existence probe,
		// which resolves without attaching a reader — reports the slot not-found (exit 3), the common
		// sequential case, distinct from the mid-drain ErrConsumed a concurrent reader sees.
		waitFor(t, 3*time.Second, func() bool {
			_, err := w.st.Stat(ctx, "merge")
			return errors.Is(err, clip.ErrNotFound)
		})
		if r := w.run(t, "", true, false, "-s", "@merge"); r.code != 3 {
			t.Fatalf("re-fetch after cleanup: code=%d want 3 (not found, the sequential spent state), err=%q", r.code, r.err)
		}
	})

	t.Run("target is an existing file", func(t *testing.T) {
		seed(t, "isfile")
		work := t.TempDir()
		t.Chdir(work)
		mustWrite(t, filepath.Join(work, "afile"), "pre-existing") // -o names a file; an archive needs a dir

		r := w.run(t, "", true, true, "@isfile", "-o", "afile") // extractSink refuses before reading a byte
		if r.code != 1 {
			t.Fatalf("is-a-file target: code=%d want 1 (generic), err=%q", r.code, r.err)
		}
		if !strings.Contains(r.err, "spent it") {
			t.Errorf("stderr=%q, want the upfront spent-it notice", r.err)
		}
		if !strings.Contains(r.err, "consume-once delivery lost") {
			t.Errorf("stderr=%q, want the flow's uniform loss tail on an -o path", r.err)
		}
		if strings.Contains(r.err, "extracted the consume-once delivery") {
			t.Errorf("an -o delivery was diverted: stderr=%q", r.err)
		}
		assertFile(t, filepath.Join(work, "afile"), "pre-existing") // untouched
	})
}
