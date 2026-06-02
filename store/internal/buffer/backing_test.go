package buffer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/srevn/buff/clip"
)

// errBoom is an injected backing failure used to drive the error paths the infallible
// memory backing cannot reach.
var errBoom = errors.New("boom")

// fakeBacking does what the memory backing cannot, so the contract paths memory leaves
// untested get an executable proof in this phase. It actually refcounts a shared source —
// the way the disk backing will refcount one O_RDONLY fd, opening it on the first reader
// and closing it on the last — and it can be told to short-count or fail its writer-side
// calls. With it, three otherwise-unreachable promises become provable now: Append
// publishing a partial store before returning the error, Sync/closeWrite error
// propagation, and the open-on-first/close-on-last refcount with reopen-after-zero. It is
// the conformance harness (testBackingContract) the disk backing reuses unchanged later.
type fakeBacking struct {
	mu         sync.Mutex
	data       []byte
	refs       int  // outstanding read handles
	opens      int  // times the shared source was actually opened (reopen-after-zero counts again)
	closes     int  // times the shared source was actually closed
	sourceOpen bool // is the shared source currently open?
	writes     int  // times closeWrite was called

	// writer-side fault injection, set at construction.
	appendStore   int   // bytes a failing append actually stores before returning appendErr
	appendErr     error // if non-nil, append stores appendStore bytes then returns this
	syncErr       error // returned by sync
	closeWriteErr error // returned by closeWrite
}

// append stores the bytes, honouring the contract that a short count with an error means
// only that many bytes were stored.
func (f *fakeBacking) append(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.appendErr != nil {
		k := min(f.appendStore, len(p))
		f.data = append(f.data, p[:k]...)
		return k, f.appendErr
	}
	f.data = append(f.data, p...)
	return len(p), nil
}

func (f *fakeBacking) sync() error { return f.syncErr }

// closeWrite tolerates a redundant call — the close-once discipline the contract asks of
// every backing — recording each call so a test can confirm the Buffer makes exactly one.
func (f *fakeBacking) closeWrite() error {
	f.mu.Lock()
	f.writes++
	f.mu.Unlock()
	return f.closeWriteErr
}

// openRead opens the shared source on the first outstanding handle and shares it after.
func (f *fakeBacking) openRead() (readHandle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refs == 0 {
		f.opens++
		f.sourceOpen = true
	}
	f.refs++
	return &fakeHandle{f: f}, nil
}

// release drops one reference, closing the shared source when the last handle goes.
func (f *fakeBacking) release() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refs--
	if f.refs == 0 {
		f.closes++
		f.sourceOpen = false
	}
}

// fakeHandle is one reader's view of a fakeBacking, close-once so a double Close (which the
// abort unwind can produce) never double-decrements the refcount.
type fakeHandle struct {
	f    *fakeBacking
	once sync.Once
}

func (h *fakeHandle) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errOffset
	}
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	if !h.f.sourceOpen {
		return 0, errors.New("fakeBacking: read after the shared source was released")
	}
	if off >= int64(len(h.f.data)) {
		return 0, io.EOF
	}
	n := copy(p, h.f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (h *fakeHandle) Close() error {
	h.once.Do(h.f.release)
	return nil
}

// testBackingContract asserts the behaviour every backing owes the followable machinery
// above it, exercised through the backing/readHandle interface alone so the identical
// checks run against the memory backing now and the disk backing later. newBacking returns
// a fresh, empty backing on each call.
func testBackingContract(t *testing.T, newBacking func() backing) {
	t.Helper()

	t.Run("reads back appended bytes", func(t *testing.T) {
		b := newBacking()
		if _, err := b.append([]byte("hello world")); err != nil {
			t.Fatal(err)
		}
		h, err := b.openRead()
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		p := make([]byte, 11)
		if n, err := h.ReadAt(p, 0); n != 11 || err != nil || string(p) != "hello world" {
			t.Errorf("ReadAt = (%d,%v,%q), want (11, nil, \"hello world\")", n, err, p)
		}
	})

	t.Run("a handle sees bytes appended after it opened", func(t *testing.T) {
		b := newBacking()
		h, err := b.openRead() // opened while the backing is still empty
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		if _, err := b.append([]byte("abc")); err != nil {
			t.Fatal(err)
		}
		p := make([]byte, 3)
		if n, err := h.ReadAt(p, 0); n != 3 || err != nil || string(p) != "abc" {
			t.Errorf("ReadAt after append = (%d,%v,%q), want (3, nil, \"abc\")", n, err, p)
		}
	})

	t.Run("a negative offset returns an error", func(t *testing.T) {
		b := newBacking()
		if _, err := b.append([]byte("data")); err != nil {
			t.Fatal(err)
		}
		h, err := b.openRead()
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		if n, err := h.ReadAt(make([]byte, 4), -1); n != 0 || err == nil {
			t.Errorf("ReadAt(-1) = (%d,%v), want (0, non-nil error)", n, err)
		}
	})

	t.Run("Close is idempotent", func(t *testing.T) {
		b := newBacking()
		h, err := b.openRead()
		if err != nil {
			t.Fatal(err)
		}
		if err := h.Close(); err != nil {
			t.Errorf("first Close: %v", err)
		}
		if err := h.Close(); err != nil {
			t.Errorf("second Close: %v", err)
		}
	})

	t.Run("a source reopens after every handle has closed", func(t *testing.T) {
		b := newBacking()
		if _, err := b.append([]byte("data")); err != nil {
			t.Fatal(err)
		}
		h1, err := b.openRead()
		if err != nil {
			t.Fatal(err)
		}
		if err := h1.Close(); err != nil { // refcount falls back to zero
			t.Fatal(err)
		}
		h2, err := b.openRead() // must reopen cleanly
		if err != nil {
			t.Fatalf("reopen after zero: %v", err)
		}
		defer h2.Close()
		p := make([]byte, 4)
		if n, err := h2.ReadAt(p, 0); n != 4 || err != nil || string(p) != "data" {
			t.Errorf("ReadAt after reopen = (%d,%v,%q), want (4, nil, \"data\")", n, err, p)
		}
	})

	t.Run("sibling handles read concurrently", func(t *testing.T) {
		b := newBacking()
		want := bytes.Repeat([]byte("xy"), 512)
		if _, err := b.append(want); err != nil {
			t.Fatal(err)
		}
		const N = 8
		var wg sync.WaitGroup
		ok := make([]bool, N)
		for i := range N {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				h, err := b.openRead()
				if err != nil {
					return
				}
				defer h.Close()
				got := make([]byte, len(want))
				n, err := h.ReadAt(got, 0)
				ok[i] = n == len(want) && err == nil && bytes.Equal(got, want)
			}(i)
		}
		wg.Wait()
		for i := range N {
			if !ok[i] {
				t.Errorf("sibling handle %d did not read the full bytes", i)
			}
		}
	})
}

// TestBackingContract runs the shared contract against every backing. The memory and fake
// backings prove it in process; the disk row proves the real file-backed implementation
// honours the identical contract — its newBacking opens a fresh data file in a temp dir per
// call and reads it back O_RDONLY, so the contract runs against actual descriptors.
func TestBackingContract(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		testBackingContract(t, func() backing { return newMemBacking() })
	})
	t.Run("fake", func(t *testing.T) {
		testBackingContract(t, func() backing { return &fakeBacking{} })
	})
	t.Run("disk", func(t *testing.T) {
		testBackingContract(t, func() backing {
			path := filepath.Join(t.TempDir(), "data")
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = f.Close() })
			return newDiskBacking(f, func() (*os.File, error) { return os.Open(path) }, false)
		})
	})
}

// TestFakeBackingRefcount proves the refcount contract on a backing that actually
// refcounts — the path the memory backing's no-op open/close cannot exercise, and the
// shape the disk backing's shared fd will take: open on first, share between, close on
// last, reopen after zero, and never double-decrement on a double Close.
func TestFakeBackingRefcount(t *testing.T) {
	f := &fakeBacking{}

	h1, err := f.openRead()
	if err != nil {
		t.Fatal(err)
	}
	if f.opens != 1 || f.refs != 1 || !f.sourceOpen {
		t.Fatalf("first open: opens=%d refs=%d sourceOpen=%v, want 1, 1, true", f.opens, f.refs, f.sourceOpen)
	}

	h2, err := f.openRead() // shares the already-open source
	if err != nil {
		t.Fatal(err)
	}
	if f.opens != 1 || f.refs != 2 {
		t.Fatalf("second open: opens=%d refs=%d, want opens still 1, refs 2", f.opens, f.refs)
	}

	if err := h1.Close(); err != nil {
		t.Fatal(err)
	}
	if f.closes != 0 || f.refs != 1 || !f.sourceOpen {
		t.Fatalf("one close: closes=%d refs=%d sourceOpen=%v, want 0, 1, true", f.closes, f.refs, f.sourceOpen)
	}

	if err := h1.Close(); err != nil { // double close must not double-decrement
		t.Fatal(err)
	}
	if f.refs != 1 {
		t.Fatalf("double Close decremented the refcount: refs=%d, want 1", f.refs)
	}

	if err := h2.Close(); err != nil { // last handle closes the source
		t.Fatal(err)
	}
	if f.closes != 1 || f.refs != 0 || f.sourceOpen {
		t.Fatalf("last close: closes=%d refs=%d sourceOpen=%v, want 1, 0, false", f.closes, f.refs, f.sourceOpen)
	}

	h3, err := f.openRead() // reopen after zero
	if err != nil {
		t.Fatal(err)
	}
	if f.opens != 2 || f.refs != 1 || !f.sourceOpen {
		t.Fatalf("reopen: opens=%d refs=%d sourceOpen=%v, want 2, 1, true", f.opens, f.refs, f.sourceOpen)
	}
	if err := h3.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestAppendPublishesPartialStoreThenError drives the path the memory backing cannot: a
// backing that stores some bytes and then fails. Append must publish exactly the stored
// bytes — so a follower can still read them — and return the backing's error; after the
// writer aborts, a follower reads those bytes and then clip.ErrAborted.
func TestAppendPublishesPartialStoreThenError(t *testing.T) {
	f := &fakeBacking{appendStore: 3, appendErr: errBoom}
	b := newBuffer(f)

	n, err := b.Append([]byte("hello"))
	if n != 3 || !errors.Is(err, errBoom) {
		t.Fatalf("Append = (%d,%v), want (3, errBoom)", n, err)
	}
	if got := b.Size(); got != 3 {
		t.Fatalf("Size = %d, want 3 (only the stored bytes are published)", got)
	}

	if err := b.Fail(); err != nil { // the writer aborts on the error
		t.Fatal(err)
	}
	rc, err := b.Reader(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if string(got) != "hel" {
		t.Errorf("read %q before the tear, want %q", got, "hel")
	}
	if !errors.Is(err, clip.ErrAborted) {
		t.Errorf("error = %v, want clip.ErrAborted", err)
	}
}

// TestSyncPropagatesBackingError pins that Sync surfaces the backing's flush failure
// unchanged — where the disk backing returns a real fsync error.
func TestSyncPropagatesBackingError(t *testing.T) {
	b := newBuffer(&fakeBacking{syncErr: errBoom})
	if err := b.Sync(); !errors.Is(err, errBoom) {
		t.Errorf("Sync = %v, want errBoom", err)
	}
}

// TestTerminalPropagatesCloseWriteError pins that a terminal surfaces the backing's
// release failure — the disk backing returns the append fd's Close error here.
func TestTerminalPropagatesCloseWriteError(t *testing.T) {
	if err := newBuffer(&fakeBacking{closeWriteErr: errBoom}).Finish(); !errors.Is(err, errBoom) {
		t.Errorf("Finish = %v, want errBoom", err)
	}
	if err := newBuffer(&fakeBacking{closeWriteErr: errBoom}).Fail(); !errors.Is(err, errBoom) {
		t.Errorf("Fail = %v, want errBoom", err)
	}
}

// TestTerminalReleasesAppendSideOnce confirms the Buffer's normal-operation guarantee that
// underpins the close-once contract: each terminal calls closeWrite exactly once.
func TestTerminalReleasesAppendSideOnce(t *testing.T) {
	finish := &fakeBacking{}
	if err := newBuffer(finish).Finish(); err != nil {
		t.Fatal(err)
	}
	if finish.writes != 1 {
		t.Errorf("Finish called closeWrite %d times, want 1", finish.writes)
	}

	fail := &fakeBacking{}
	if err := newBuffer(fail).Fail(); err != nil {
		t.Fatal(err)
	}
	if fail.writes != 1 {
		t.Errorf("Fail called closeWrite %d times, want 1", fail.writes)
	}
}

// TestReaderRejectsNegativeOffset proves the fail-fast boundary guard: a negative offset is
// rejected before a read handle is opened, so a doomed reader never wastes (on disk, leaks)
// a descriptor.
func TestReaderRejectsNegativeOffset(t *testing.T) {
	f := &fakeBacking{}
	b := newBuffer(f)
	if _, err := b.Reader(context.Background(), -1); !errors.Is(err, errOffset) {
		t.Errorf("Reader(-1) error = %v, want errOffset", err)
	}
	if f.opens != 0 {
		t.Errorf("Reader(-1) opened a handle (opens=%d); the offset must be checked first", f.opens)
	}
}
