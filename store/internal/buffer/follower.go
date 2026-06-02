package buffer

import (
	"context"
	"io"
	"sync"

	"github.com/srevn/buff/clip"
)

// follower streams a live Buffer to a single reader, blocking when it catches up to the
// writer and resuming when more bytes arrive. It is the reader half of the followable log
// and the one place the project's "no lost wakeup, no leaked goroutine" guarantees are
// earned, so it is kept small and deliberate.
//
// The context is held in the struct because Read cannot take one — io.Reader fixes its
// signature — yet a blocked follower must unblock when its request is cancelled. A
// follower is created per read and lives no longer than that read, so the stored context
// is scoped exactly to the value it governs.
type follower struct {
	b    *Buffer
	ctx  context.Context
	off  int64
	h    readHandle
	once sync.Once
}

// Read delivers the next bytes of the log. It returns published bytes as soon as any are
// available; when caught up it reports io.EOF if the log finished cleanly, clip.ErrAborted
// if it was torn, or otherwise blocks until more bytes arrive, the log ends, or the
// context is cancelled. Cancellation is honoured at the top of every turn, so a follower
// with bytes still buffered stops promptly rather than copying to a reader that has gone
// away — the buffered bytes drain only while the read is still wanted.
func (r *follower) Read(p []byte) (int, error) {
	if len(p) == 0 {
		// io.Reader's rule for an empty buffer: report nothing rather than block for
		// bytes there is no room to deliver. A real caller (io.Copy) never asks this;
		// honouring it keeps the follower a well-behaved io.Reader.
		return 0, nil
	}
	for {
		select {
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		default:
		}

		r.b.mu.Lock()
		size, closed, aborted, notify := r.b.size, r.b.closed, r.b.aborted, r.b.notify
		r.b.mu.Unlock()

		switch {
		case r.off < size:
			// [0,size) is immutable and fully stored, so this copy needs no Buffer
			// lock. Clamp to size so the read never depends on the handle for EOF: the
			// closed flag, not the backing, is the sole authority on end-of-stream.
			want := min(int64(len(p)), size-r.off)
			n, err := r.h.ReadAt(p[:want], r.off)
			r.off += int64(n)
			return n, err
		case aborted:
			// Checked before closed so that if both were ever set the torn signal wins;
			// a torn stream must never resolve to a clean io.EOF.
			return 0, clip.ErrAborted
		case closed:
			return 0, io.EOF
		default:
			// Caught up while still live. notify was captured under the same lock as
			// size just above, so if the writer changes state before this wait begins it
			// has already closed this very channel — the wait then returns at once and no
			// wakeup is lost.
			select {
			case <-notify:
			case <-r.ctx.Done():
				return 0, r.ctx.Err()
			}
		}
	}
}

// Close releases the follower's read handle. It is safe to call more than once — the
// abort unwind of a request may close the same reader twice — and releases the handle
// exactly once.
func (r *follower) Close() error {
	var err error
	r.once.Do(func() { err = r.h.Close() })
	return err
}
