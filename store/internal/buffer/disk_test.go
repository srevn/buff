package buffer

import (
	"os"
	"path/filepath"
	"testing"
)

// These are the disk backing's white-box tests, exercised against a real file on the test
// filesystem so the descriptor lifecycle and POSIX unlink-while-open behaviour are proven for
// real, not emulated. The shared contract every backing owes is checked by the disk row of
// TestBackingContract; here we pin the two properties unique to the disk backing — one shared
// descriptor per generation, and an open read descriptor pinning an unlinked inode.

// diskTemp creates an empty data file in a fresh temp dir and returns its append descriptor, an
// opener that reopens it O_RDONLY while counting how many times the source is actually opened,
// and the file's path. The counter is what makes "one descriptor shared across many readers"
// observable: it ticks once per real open, so a shared descriptor leaves it at one.
func diskTemp(t *testing.T) (appendFD *os.File, open func() (*os.File, error), opens *int, path string) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "data")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	var n int
	open = func() (*os.File, error) {
		n++
		return os.Open(path)
	}
	return f, open, &n, path
}

// TestDiskBackingSharedFD proves the refcount the memory backing's no-op open/close cannot:
// many readers share one descriptor, closing some keeps it open while any reader remains, a
// double Close never decrements twice, and the last close releases the descriptor so the next
// reader reopens it.
func TestDiskBackingSharedFD(t *testing.T) {
	appendFD, open, opens, _ := diskTemp(t)
	d := newDiskBacking(appendFD, open, false)
	if _, err := d.append([]byte("payload")); err != nil {
		t.Fatal(err)
	}

	const N = 4
	hs := make([]readHandle, N)
	for i := range N {
		h, err := d.openRead()
		if err != nil {
			t.Fatal(err)
		}
		hs[i] = h
	}
	if *opens != 1 {
		t.Fatalf("opens = %d, want 1 (one descriptor shared across %d readers)", *opens, N)
	}

	// Close all but one, then close the first again. A double close must not decrement twice —
	// if it did, the shared descriptor would close while the surviving reader still holds it.
	for i := 0; i < N-1; i++ {
		if err := hs[i].Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := hs[0].Close(); err != nil {
		t.Fatal(err)
	}
	if *opens != 1 {
		t.Fatalf("opens = %d after partial and double close, want 1", *opens)
	}
	// The surviving handle still reads through the shared descriptor — proof it stayed open.
	p := make([]byte, 7)
	if n, err := hs[N-1].ReadAt(p, 0); n != 7 || err != nil || string(p) != "payload" {
		t.Fatalf("surviving handle ReadAt = (%d,%v,%q), want (7,nil,\"payload\")", n, err, p)
	}

	// The last close releases the descriptor; the next reader reopens it from zero.
	if err := hs[N-1].Close(); err != nil {
		t.Fatal(err)
	}
	h, err := d.openRead()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if *opens != 2 {
		t.Fatalf("opens = %d after reopen-from-zero, want 2", *opens)
	}
}

// TestDiskBackingInodePinSurvivesUnlink proves the eager-GC guarantee at the descriptor level:
// a read descriptor opened before the data file is unlinked keeps reading the now-nameless
// inode to completion. It is the POSIX behaviour the store relies on to RemoveAll a superseded
// generation's whole directory the moment it is replaced, while a slow reader drains it.
func TestDiskBackingInodePinSurvivesUnlink(t *testing.T) {
	appendFD, open, _, path := diskTemp(t)
	d := newDiskBacking(appendFD, open, false)
	if _, err := d.append([]byte("pinned")); err != nil {
		t.Fatal(err)
	}
	h, err := d.openRead() // the read descriptor is opened before the unlink
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if err := os.Remove(path); err != nil { // the directory entry vanishes
		t.Fatal(err)
	}
	p := make([]byte, 6)
	if n, err := h.ReadAt(p, 0); n != 6 || err != nil || string(p) != "pinned" {
		t.Fatalf("ReadAt after unlink = (%d,%v,%q), want (6,nil,\"pinned\"); the open fd must pin the inode", n, err, p)
	}
}
