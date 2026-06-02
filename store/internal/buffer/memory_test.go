package buffer

import (
	"errors"
	"io"
	"testing"
)

// TestMemHandleReadAt pins the memory handle's io.ReaderAt contract directly. The
// follower never exercises these branches — it clamps every read to the published size,
// so it never asks the handle for an end-of-stream signal — and io.SectionReader stops
// at its own limit before the handle would report EOF. They are tested here so the
// general io.ReaderAt behaviour the handle promises is actually proven, not assumed.
func TestMemHandleReadAt(t *testing.T) {
	m := newMemBacking()
	if n, err := m.append([]byte("hello world")); n != 11 || err != nil {
		t.Fatalf("append: got (%d,%v), want (11,nil)", n, err)
	}
	h, err := m.openRead()
	if err != nil {
		t.Fatalf("openRead: %v", err)
	}
	defer h.Close()

	tests := []struct {
		name    string
		bufLen  int
		off     int64
		wantN   int
		wantEOF bool
		wantStr string
	}{
		{"full within bounds", 5, 0, 5, false, "hello"},
		{"from an offset", 5, 6, 5, false, "world"},
		{"short read at the tail returns bytes and EOF", 10, 6, 5, true, "world"},
		{"exactly at the end returns zero and EOF", 4, 11, 0, true, ""},
		{"past the end returns zero and EOF", 4, 100, 0, true, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := make([]byte, tc.bufLen)
			n, err := h.ReadAt(p, tc.off)
			if n != tc.wantN {
				t.Errorf("n = %d, want %d", n, tc.wantN)
			}
			if tc.wantEOF && !errors.Is(err, io.EOF) {
				t.Errorf("err = %v, want io.EOF", err)
			}
			if !tc.wantEOF && err != nil {
				t.Errorf("err = %v, want nil", err)
			}
			if string(p[:n]) != tc.wantStr {
				t.Errorf("read %q, want %q", p[:n], tc.wantStr)
			}
		})
	}
}

// TestMemHandleNegativeOffset pins the io.ReaderAt edge the handle promises to honour: a
// negative offset is an error, not a panic from a negative slice index.
func TestMemHandleNegativeOffset(t *testing.T) {
	m := newMemBacking()
	if _, err := m.append([]byte("data")); err != nil {
		t.Fatal(err)
	}
	h, err := m.openRead()
	if err != nil {
		t.Fatalf("openRead: %v", err)
	}
	defer h.Close()
	if n, err := h.ReadAt(make([]byte, 4), -1); n != 0 || !errors.Is(err, errOffset) {
		t.Errorf("ReadAt(-1) = (%d,%v), want (0, errOffset)", n, err)
	}
}

// TestMemHandleSharesGrowingBacking proves a handle opened before any data still sees
// bytes appended later: it re-reads the backing's current slice header on each ReadAt
// rather than capturing a snapshot at open time. This is the mechanism a follower relies
// on — one handle, opened once, that tracks the log as the writer extends it.
func TestMemHandleSharesGrowingBacking(t *testing.T) {
	m := newMemBacking()
	h, err := m.openRead() // opened while the backing is still empty
	if err != nil {
		t.Fatalf("openRead: %v", err)
	}
	defer h.Close()

	if _, err := m.append([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 3)
	if n, err := h.ReadAt(p, 0); n != 3 || err != nil || string(p) != "abc" {
		t.Errorf("first ReadAt: got (%d,%v,%q), want (3,nil,\"abc\")", n, err, p)
	}

	if _, err := m.append([]byte("def")); err != nil {
		t.Fatal(err)
	}
	if n, err := h.ReadAt(p, 3); n != 3 || err != nil || string(p) != "def" {
		t.Errorf("second ReadAt: got (%d,%v,%q), want (3,nil,\"def\")", n, err, p)
	}
}
