package buffer

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

// These are the sealed backing's white-box tests, exercised against a real file so the read
// descriptor lifecycle and POSIX unlink-while-open behaviour are proven for real. The sealed
// backing shares its entire read side with the disk backing via the embedded readShare, so the
// refcount and inode-pin proofs mirror the disk backing's — re-run here to confirm the shared
// path holds with no write side, and that a finished-at-birth Buffer opens no descriptor until
// it is first read.

// sealedTemp writes content to a fresh temp file and returns an opener that reopens it O_RDONLY
// while counting how many times the source is actually opened, plus the file's path. The counter
// makes "no descriptor until first read" and "one descriptor shared across readers" observable.
func sealedTemp(t *testing.T, content string) (open func() (*os.File, error), opens *int, path string) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "data")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	var n int
	open = func() (*os.File, error) {
		n++
		return os.Open(path)
	}
	return open, &n, path
}

// TestSealedLazyOpensNothing proves a sealed Buffer opens no descriptor at construction —
// recovering many clips costs no read descriptors until each is first read — and that its size
// is the one handed in, reported without touching disk. The first Section opens the one shared
// descriptor and reads the fixed range.
func TestSealedLazyOpensNothing(t *testing.T) {
	open, opens, _ := sealedTemp(t, "payload")
	b := NewSealed(open, 7)
	if *opens != 0 {
		t.Fatalf("opens = %d at construction, want 0 (lazy: no descriptor until first read)", *opens)
	}
	if b.Size() != 7 {
		t.Fatalf("Size = %d, want 7 (reported from RAM, not a stat)", b.Size())
	}

	rc, err := b.Section()
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if *opens != 1 {
		t.Fatalf("opens = %d after first Section, want 1", *opens)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "payload" {
		t.Errorf("read %q, want payload", got)
	}
}

// TestSealedEmptySection proves a 0-byte sealed clip reads as empty — the recovered counterpart
// of the empty-clip off-by-one: a finished log of size 0 yields no bytes, not an error.
func TestSealedEmptySection(t *testing.T) {
	open, _, _ := sealedTemp(t, "")
	b := NewSealed(open, 0)
	rc, err := b.Section()
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("read %q, want empty", got)
	}
}

// TestSealedBackingSharedFD proves the refcount the sealed backing inherits from readShare: many
// readers share one descriptor, closing some keeps it open while any reader remains, a double
// Close never decrements twice, and the last close releases the descriptor so the next reader
// reopens it. It is the disk backing's shared-fd proof, re-run on the read-only path.
func TestSealedBackingSharedFD(t *testing.T) {
	open, opens, _ := sealedTemp(t, "payload")
	sb := newSealedBacking(open)

	const N = 4
	hs := make([]readHandle, N)
	for i := range N {
		h, err := sb.openRead()
		if err != nil {
			t.Fatal(err)
		}
		hs[i] = h
	}
	if *opens != 1 {
		t.Fatalf("opens = %d, want 1 (one descriptor shared across %d readers)", *opens, N)
	}

	for i := 0; i < N-1; i++ {
		if err := hs[i].Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := hs[0].Close(); err != nil { // double close must not decrement twice
		t.Fatal(err)
	}
	if *opens != 1 {
		t.Fatalf("opens = %d after partial and double close, want 1", *opens)
	}
	p := make([]byte, 7)
	if n, err := hs[N-1].ReadAt(p, 0); n != 7 || err != nil || string(p) != "payload" {
		t.Fatalf("surviving handle ReadAt = (%d,%v,%q), want (7,nil,\"payload\")", n, err, p)
	}

	if err := hs[N-1].Close(); err != nil { // last close releases the descriptor
		t.Fatal(err)
	}
	h, err := sb.openRead()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if *opens != 2 {
		t.Fatalf("opens = %d after reopen-from-zero, want 2", *opens)
	}
}

// TestSealedBackingInodePinSurvivesUnlink proves a recovered generation pins its inode exactly as
// a live one does: a read descriptor opened before the data file is unlinked keeps reading the
// now-nameless inode to completion. It is what lets a superseding write RemoveAll a recovered
// generation's directory while a slow reader drains it.
func TestSealedBackingInodePinSurvivesUnlink(t *testing.T) {
	open, _, path := sealedTemp(t, "pinned")
	sb := newSealedBacking(open)
	h, err := sb.openRead() // opened before the unlink
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 6)
	if n, err := h.ReadAt(p, 0); n != 6 || err != nil || string(p) != "pinned" {
		t.Fatalf("ReadAt after unlink = (%d,%v,%q), want (6,nil,\"pinned\"); the open fd must pin the inode", n, err, p)
	}
}

// TestSealedBackingNoWriteSide proves there is no append side to leak or fault: the write-side
// methods the contract names are pure no-ops that never touch a descriptor, so a finished log
// carries no append fd and the read share is untouched by them.
func TestSealedBackingNoWriteSide(t *testing.T) {
	open, opens, _ := sealedTemp(t, "payload")
	sb := newSealedBacking(open)
	if n, err := sb.append([]byte("more")); n != 0 || err != nil {
		t.Errorf("append = (%d,%v), want (0,nil) — a sealed log stores nothing", n, err)
	}
	if err := sb.sync(); err != nil {
		t.Errorf("sync = %v, want nil", err)
	}
	if err := sb.closeWrite(); err != nil {
		t.Errorf("closeWrite = %v, want nil", err)
	}
	if *opens != 0 {
		t.Errorf("a write-side call opened a descriptor (opens=%d), want 0", *opens)
	}
}
