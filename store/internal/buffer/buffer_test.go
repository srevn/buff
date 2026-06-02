package buffer_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store/internal/buffer"
)

// TestFanOutExactOrder is the headline property: every follower of one live log observes
// exactly the bytes the writer appended, in order, and a clean io.EOF at the end. The
// writer interleaves appends with synctest.Wait, which returns only once every follower
// is durably blocked, so the fan-out is observed deterministically rather than by luck.
func TestFanOutExactOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := buffer.NewMemory()
		chunks := [][]byte{
			[]byte("the quick "),
			[]byte("brown fox "),
			[]byte("jumps over the lazy dog"),
		}
		var want []byte
		for _, c := range chunks {
			want = append(want, c...)
		}

		const N = 8
		got := make([][]byte, N)
		errs := make([]error, N)
		var wg sync.WaitGroup
		for i := range N {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				rc, err := b.Reader(context.Background(), 0)
				if err != nil {
					errs[i] = err
					return
				}
				defer rc.Close()
				got[i], errs[i] = io.ReadAll(rc)
			}(i)
		}

		synctest.Wait() // all followers caught up at size 0, blocked in the wait
		for _, c := range chunks {
			if _, err := b.Append(c); err != nil {
				t.Fatalf("Append: %v", err)
			}
			synctest.Wait() // every follower drains this chunk, then blocks again
		}
		if err := b.Finish(); err != nil {
			t.Fatalf("Finish: %v", err)
		}
		wg.Wait()

		for i := range N {
			if errs[i] != nil {
				t.Errorf("follower %d: ReadAll returned %v", i, errs[i])
			}
			if !bytes.Equal(got[i], want) {
				t.Errorf("follower %d: read %q, want %q", i, got[i], want)
			}
		}
	})
}

// TestFinishDeliversBytesThenEOF checks the clean-close path explicitly: after the bytes,
// the next read is io.EOF — io.ReadAll would hide that EOF, so it is asserted directly.
// Everything is present before the read, so it never blocks and needs no bubble.
func TestFinishDeliversBytesThenEOF(t *testing.T) {
	b := buffer.NewMemory()
	if _, err := b.Append([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := b.Finish(); err != nil {
		t.Fatal(err)
	}

	rc, err := b.Reader(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "payload" {
		t.Errorf("read %q, want %q", got, "payload")
	}
	if n, err := rc.Read(make([]byte, 4)); n != 0 || !errors.Is(err, io.EOF) {
		t.Errorf("read past end: got (%d,%v), want (0, io.EOF)", n, err)
	}
}

// TestFailDeliversBufferedThenAborted is the other half of the EOF rule: a torn log hands
// a follower every buffered byte and then clip.ErrAborted, never io.EOF. The buffered
// bytes must arrive before the terminal — a follower drains what exists before it reports
// the tear.
func TestFailDeliversBufferedThenAborted(t *testing.T) {
	b := buffer.NewMemory()
	if _, err := b.Append([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if err := b.Fail(); err != nil {
		t.Fatal(err)
	}

	rc, err := b.Reader(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if !errors.Is(err, clip.ErrAborted) {
		t.Errorf("error = %v, want clip.ErrAborted", err)
	}
	if string(got) != "partial" {
		t.Errorf("read %q before the tear, want %q", got, "partial")
	}
}

// TestCancelMidWaitNoLeak is risk #1 retired: a follower blocked waiting for bytes whose
// context is cancelled returns ctx.Err() and its goroutine exits. wg.Wait after cancel is
// the leak detector — if the Read leaked it would block here forever and synctest would
// report the whole bubble deadlocked.
func TestCancelMidWaitNoLeak(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := buffer.NewMemory()
		ctx, cancel := context.WithCancel(context.Background())
		errc := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Go(func() {
			rc, err := b.Reader(ctx, 0)
			if err != nil {
				errc <- err
				return
			}
			defer rc.Close()
			_, err = rc.Read(make([]byte, 8))
			errc <- err
		})

		synctest.Wait() // the follower is durably blocked in the wait
		select {
		case err := <-errc:
			t.Fatalf("follower returned before cancel: %v", err)
		default:
		}

		cancel()
		wg.Wait() // returns only once the follower's Read unblocked and the goroutine exited
		if err := <-errc; !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want context.Canceled", err)
		}
	})
}

// TestNoLostWakeup exercises the capture-under-lock hinge: a follower blocked with the
// current notify captured wakes when the writer appends, reads the new bytes, re-blocks,
// and finally reaches io.EOF on Finish. The second synctest.Wait confirms it processed
// the chunk and re-blocked rather than spinning or stalling.
func TestNoLostWakeup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := buffer.NewMemory()
		got := make(chan []byte, 1)
		var wg sync.WaitGroup
		wg.Go(func() {
			rc, err := b.Reader(context.Background(), 0)
			if err != nil {
				got <- nil
				return
			}
			defer rc.Close()
			data, _ := io.ReadAll(rc)
			got <- data
		})

		synctest.Wait() // blocked, having captured the current notify with no data yet
		if _, err := b.Append([]byte("chunk")); err != nil {
			t.Fatalf("Append: %v", err)
		}
		synctest.Wait() // must have woken, read "chunk", and re-blocked
		if err := b.Finish(); err != nil {
			t.Fatalf("Finish: %v", err)
		}
		wg.Wait()
		if g := <-got; string(g) != "chunk" {
			t.Errorf("read %q, want %q", g, "chunk")
		}
	})
}

// TestAbortWakesWaiter is the abort counterpart of the wakeup test: a follower blocked
// with nothing buffered wakes to clip.ErrAborted when the writer fails the log.
func TestAbortWakesWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := buffer.NewMemory()
		errc := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Go(func() {
			rc, err := b.Reader(context.Background(), 0)
			if err != nil {
				errc <- err
				return
			}
			defer rc.Close()
			_, err = rc.Read(make([]byte, 8))
			errc <- err
		})

		synctest.Wait() // blocked, caught up at size 0
		if err := b.Fail(); err != nil {
			t.Fatalf("Fail: %v", err)
		}
		wg.Wait()
		if err := <-errc; !errors.Is(err, clip.ErrAborted) {
			t.Errorf("error = %v, want clip.ErrAborted", err)
		}
	})
}

// TestPromptCancelWithDataBuffered proves the top-of-loop cancellation check: with a full
// buffer of unread bytes but a context already cancelled, the first Read returns
// ctx.Err() rather than the data — a reader that has gone away is not copied to.
func TestPromptCancelWithDataBuffered(t *testing.T) {
	b := buffer.NewMemory()
	if _, err := b.Append(bytes.Repeat([]byte("x"), 4096)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before any read, with the data fully buffered

	rc, err := b.Reader(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if n, err := rc.Read(make([]byte, 8)); n != 0 || !errors.Is(err, context.Canceled) {
		t.Errorf("got (%d,%v), want (0, context.Canceled)", n, err)
	}
}

// TestEmptyClip covers the zero-byte boundary: a log finished without a single append
// gives a follower (0, io.EOF) immediately and a Section that reads no bytes cleanly. The
// follower is created after Finish, so it also covers a reader attaching to an already
// finished empty log.
func TestEmptyClip(t *testing.T) {
	b := buffer.NewMemory()
	if err := b.Finish(); err != nil {
		t.Fatal(err)
	}

	rc, err := b.Reader(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := rc.Read(make([]byte, 8)); n != 0 || !errors.Is(err, io.EOF) {
		t.Errorf("follower: got (%d,%v), want (0, io.EOF)", n, err)
	}
	if err := rc.Close(); err != nil {
		t.Errorf("follower close: %v", err)
	}

	sec, err := b.Section()
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(sec)
	if err != nil || len(data) != 0 {
		t.Errorf("section: got (%q,%v), want empty with nil error", data, err)
	}
	if err := sec.Close(); err != nil {
		t.Errorf("section close: %v", err)
	}
}

// TestSectionFinalizedReads checks the finished-log fast path: Section returns the full
// bytes, and many concurrent Section readers each read them in full. Section builds an
// io.SectionReader and never touches the notifier, so this is also the path the disk
// refcount will travel; running it under -race covers concurrent ReadAt on one backing.
func TestSectionFinalizedReads(t *testing.T) {
	b := buffer.NewMemory()
	want := []byte("the complete, finished contents of a clip")
	if _, err := b.Append(want); err != nil {
		t.Fatal(err)
	}
	if err := b.Finish(); err != nil {
		t.Fatal(err)
	}

	sec, err := b.Section()
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(sec)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("read %q, want %q", got, want)
	}
	if err := sec.Close(); err != nil {
		t.Errorf("close: %v", err)
	}

	const N = 8
	var wg sync.WaitGroup
	errs := make([]error, N)
	oks := make([]bool, N)
	for i := range N {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := b.Section()
			if err != nil {
				errs[i] = err
				return
			}
			defer s.Close()
			d, err := io.ReadAll(s)
			errs[i] = err
			oks[i] = bytes.Equal(d, want)
		}(i)
	}
	wg.Wait()
	for i := range N {
		if errs[i] != nil {
			t.Errorf("reader %d: %v", i, errs[i])
		}
		if !oks[i] {
			t.Errorf("reader %d: bytes did not match", i)
		}
	}
}

// TestReaderOffset exercises the offset parameter that the public surface freezes but v1
// store callers always pass as 0: a follower started at offset 2 reads the bytes from
// there to the clean EOF.
func TestReaderOffset(t *testing.T) {
	b := buffer.NewMemory()
	if _, err := b.Append([]byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	if err := b.Finish(); err != nil {
		t.Fatal(err)
	}

	rc, err := b.Reader(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "cdef" {
		t.Errorf("read %q, want %q", got, "cdef")
	}
}

// TestCloseOnce pins the close-once contract on both reader types: a second Close is safe
// and releases nothing further. It guards the disk backing's shared-descriptor refcount,
// which a double release would corrupt.
func TestCloseOnce(t *testing.T) {
	b := buffer.NewMemory()
	if _, err := b.Append([]byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := b.Finish(); err != nil {
		t.Fatal(err)
	}

	rc, err := b.Reader(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Errorf("follower close 1: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Errorf("follower close 2: %v", err)
	}

	sec, err := b.Section()
	if err != nil {
		t.Fatal(err)
	}
	if err := sec.Close(); err != nil {
		t.Errorf("section close 1: %v", err)
	}
	if err := sec.Close(); err != nil {
		t.Errorf("section close 2: %v", err)
	}
}

// TestFollowerEmptyReadDoesNotBlock pins io.Reader's len(p)==0 rule for the follower: an
// empty read returns (0,nil) at once even when caught up to a still-live writer — the one
// case that would otherwise block forever waiting for bytes it has no room to deliver. The
// reader starts at off==size with the log unfinished, exactly that caught-up-and-live
// state; without the guard this call never returns and the test times out.
func TestFollowerEmptyReadDoesNotBlock(t *testing.T) {
	b := buffer.NewMemory()
	if _, err := b.Append([]byte("data")); err != nil {
		t.Fatal(err)
	}
	rc, err := b.Reader(context.Background(), 4) // off == size, log still live: caught up
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if n, err := rc.Read([]byte{}); n != 0 || err != nil {
		t.Errorf("empty Read = (%d,%v), want (0, nil)", n, err)
	}
}

// TestAppendReturnsCountAndSizeTracks is the plain mechanical check: Append reports the
// bytes stored, Size accumulates them, and an empty append publishes nothing.
func TestAppendReturnsCountAndSizeTracks(t *testing.T) {
	b := buffer.NewMemory()
	if got := b.Size(); got != 0 {
		t.Errorf("initial size = %d, want 0", got)
	}

	if n, err := b.Append([]byte("hello")); n != 5 || err != nil {
		t.Errorf("Append: got (%d,%v), want (5, nil)", n, err)
	}
	if got := b.Size(); got != 5 {
		t.Errorf("size = %d, want 5", got)
	}

	if n, err := b.Append([]byte(" world")); n != 6 || err != nil {
		t.Errorf("Append: got (%d,%v), want (6, nil)", n, err)
	}
	if got := b.Size(); got != 11 {
		t.Errorf("size = %d, want 11", got)
	}

	if n, err := b.Append(nil); n != 0 || err != nil {
		t.Errorf("empty Append: got (%d,%v), want (0, nil)", n, err)
	}
	if got := b.Size(); got != 11 {
		t.Errorf("size after empty append = %d, want 11", got)
	}
}
