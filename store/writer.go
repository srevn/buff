package store

import (
	"fmt"

	"github.com/srevn/buff/clip"
)

// writer is the handle a producer holds for one generation's upload. It owns the generation from
// Create until exactly one terminal — Close to finalize, or Abort to discard — and holds the name's
// lease across the whole upload so the handle outlives every step.
//
// A writer is driven by a single request goroutine: the caller copies the body into Write then
// calls Close or Abort, never concurrently. That single-owner contract is why done is a plain bool
// needing no atomic, and why one terminal releasing the lease cannot race another. The terminal
// guard makes the release happen exactly once: each entry point returns early if the writer is
// already done, so the lease-releasing finish runs for the first terminal only.
type writer struct {
	s       *store
	h       *clipHandle
	g       *generation
	written int64 // bytes accepted so far; the running total the per-clip cap measures
	done    bool
}

// Write admits a chunk against the caps, then appends it to the live generation. The per-clip
// ceiling is checked first, locally and without touching shared state, so an oversized chunk is
// rejected whole as ErrTooLarge — no partial write to make it fit. Only then is the chunk's room
// reserved from the total budget, so a reservation that loses to the total cap (ErrNoSpace) leaves
// nothing reserved. After a terminal it reports ErrClosed.
//
// The reserve is for the whole chunk; if the backing accepts fewer bytes than asked — which a disk
// backing can, a memory one never does — the unclaimed tail is returned at once, so the budget
// always reflects bytes that truly landed. Write never aborts on a cap rejection: it returns
// the typed error and leaves the single, idempotent abort to its caller, exactly as it does for
// a backing write error. The append itself is lock-free, so a fast producer is never serialized
// against readers fanning out from the same log.
func (w *writer) Write(p []byte) (int, error) {
	if w.done {
		return 0, clip.ErrClosed
	}
	n := int64(len(p))
	if !w.s.quota.perClipOK(w.written, n) {
		return 0, clip.ErrTooLarge
	}
	if !w.s.quota.reserve(n) {
		return 0, clip.ErrNoSpace
	}
	written, err := w.g.buf.Append(p)
	if int64(written) < n {
		w.s.quota.release(n - int64(written))
	}
	w.written += int64(written)
	return written, err
}

// Close finalizes the generation: it makes the bytes durable, publishes the metadata, then flips
// this generation in as the name's readable current and wakes any followers to a clean EOF. The
// durable steps run with no lock held — only the in-memory pointer flip takes the handle lock — so
// a slow sync never stalls another operation on the same name.
//
// The finalized time is stamped before the durable publish, because the published record carries
// it; the absolute expiry is computed from it here too, since the retention span only becomes a
// deadline once the finalize instant is known. Both are read back only once the generation is no
// longer live, so stamping them here without the handle lock cannot race a concurrent reader's
// snapshot. Any failure before the flip routes through the abort path, so a half-finalized
// generation is never presented as a clean stream and the previous current value is left standing.
func (w *writer) Close() error {
	if w.done {
		return clip.ErrClosed
	}
	w.g.finalized = w.s.now()
	if w.g.ttl > 0 {
		w.g.expires = w.g.finalized.Add(w.g.ttl)
	}
	if err := w.g.buf.Sync(); err != nil {
		return w.fail(err)
	}
	if err := w.s.med.finalize(w.g); err != nil {
		return w.fail(err)
	}

	w.h.mu.Lock()
	prev := w.h.current
	// Whether the previous generation was a consume-once mid-delivery is read here, under the same
	// lock as the flip, so the off-lock reclamation below cannot race a concurrent claim.
	prevConsumed := prev != nil && prev.state == genConsumed
	w.g.state = genFinalized
	w.h.current = w.g
	w.h.live = nil
	// The finalized value is published and current points at it: wake any reader waiting on this name.
	// This is the delivering wake for a consume-once rendezvous — a waiter that parked through the
	// whole invisible upload now resolves the finished value and claims it. For a plain write it is
	// spurious: that waiter already left on the live follower at the install and rides the byte-log
	// notifier to EOF. The two are the load-bearing pair, one wake per write mode.
	w.h.wakeLocked()
	w.h.mu.Unlock()

	// Wake followers to EOF and reclaim the superseded generation after the flip, off the handle
	// lock. The append side closes here too; its close error is intentionally ignored — the bytes are
	// already durable from the Sync above and the append fd is process-scoped, reclaimed at exit, so
	// a failed close reports nothing actionable. Reclaiming the previous generation frees its quota
	// footprint — the one release that matches the reserve its Create made — but a consumed previous
	// generation is owned by its reader's Close, so the supersede leaves it alone; reclaiming it here
	// would double-release.
	_ = w.g.buf.Finish()
	if prev != nil && !prevConsumed {
		w.s.reclaim(prev)
	}
	w.finish()
	return nil
}

// Abort discards the generation: it detaches the live generation, tears its byte log so any
// follower reads an aborted error rather than EOF, and reclaims its home. It is idempotent so
// a handler may abort then do nothing on a path that has already terminated, and it leaves the
// previous current value untouched.
func (w *writer) Abort() error {
	if w.done {
		return nil
	}
	w.discard()
	return nil
}

// fail turns a finalize failure into an abort and returns the wrapped cause. The generation is
// discarded exactly as Abort would, so its followers see a torn stream and the previous current
// value stands; the caller maps the returned error to an internal failure.
func (w *writer) fail(cause error) error {
	w.discard()
	return fmt.Errorf("finalize %s: %w", w.g.name, cause)
}

// discard detaches and tears down the live generation, the shared tail of Abort and a failed Close.
// It clears live and wakes the name's waiters under the handle lock — a live→nil transition, woken
// for uniformity and spurious-safe, since any parked waiter re-resolves the same not-readable state
// — then tears the byte log and reclaims the home off the lock. current is never touched, so the
// last complete value remains readable, and reclaiming frees the generation's footprint — the bytes
// it reserved as it wrote and the count it reserved at Create — so an aborted write leaks nothing.
func (w *writer) discard() {
	w.h.mu.Lock()
	w.h.live = nil
	w.h.wakeLocked()
	w.h.mu.Unlock()
	_ = w.g.buf.Fail()
	w.s.reclaim(w.g)
	w.finish()
}

// finish marks the writer terminated and releases the name's lease. The terminal guards on Close
// and Abort ensure it runs once, so the lease is released exactly once even if the handler calls a
// second terminal.
func (w *writer) finish() {
	w.done = true
	w.s.reg.release(w.h)
}

// Clip returns the generation's current view for response headers: bytes-so-far and an unfinalized
// marker while live, the final count and finalized marker once Close has run.
func (w *writer) Clip() clip.Clip {
	w.h.mu.Lock()
	defer w.h.mu.Unlock()
	return w.g.clip()
}
