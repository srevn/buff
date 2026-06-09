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
// calls Close or Abort, never concurrently. That single-owner contract is why done and framing are
// plain fields needing no atomic, and why one terminal releasing the lease cannot race another.
// The terminal guard makes the release happen exactly once: each entry point returns early if the
// writer is already done, so the lease-releasing finish runs for the first terminal only. framing
// is the generation's view captured under the gate lock at that terminal, so Clip() can answer
// after the lease — and with it the writer's standing to peek the handle — is already gone.
type writer struct {
	s       *store
	h       *clipHandle
	g       *generation
	written int64     // bytes accepted so far; the running total the per-clip cap measures
	done    bool      // set by the first terminal; gates Write/Close and selects Clip()'s cached framing
	framing clip.Clip // the generation's view, fixed under the gate lock at the terminal; read by Clip() once done
}

// Write admits a chunk against the caps, then appends it to the live generation. The per-clip
// ceiling is checked first, locally and without touching shared state, so an oversized chunk is
// rejected whole as ErrTooLarge — no partial write to make it fit. Only then is the chunk's room
// reserved from the total budget; a reservation that loses to the total cap triggers one on-demand
// expiry sweep and re-probes, so an expired clip still holding the needed bytes self-heals into
// room rather than failing the write. Only a budget the sweep cannot satisfy is ErrNoSpace, and it
// leaves nothing reserved. After a terminal it reports ErrClosed.
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
		// Total cap hit: an expired clip may be holding the bytes this chunk needs. Sweep on demand, then
		// re-probe unconditionally. The re-probe is a single atomic, so — unlike Create, whose retry is a
		// whole admission cycle and so gates on its own sweep freeing space — this need not check whether
		// OUR sweep did the freeing: a near-simultaneous sweep by another writer (which throttles ours to
		// a no-op) may have freed exactly these bytes, and the cheap re-check picks them up rather than
		// failing a write the budget can now satisfy. reclaimExpired runs off any handle lock — Write
		// holds only a lease and the append below is lock-free — so its registry.mu acquisition stays
		// hierarchy-safe.
		w.s.reclaimExpired(w.s.now())
		if !w.s.quota.reserve(n) {
			return 0, clip.ErrNoSpace
		}
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
		return w.fail(false, err) // pre-publish: the bytes never synced, so nothing on disk to retire
	}
	if committed, err := w.s.med.finalize(w.g); err != nil {
		return w.fail(committed, err)
	}

	// The in-memory flip runs as one transitionResult closure, handing back the superseded generation
	// to reclaim off the lock — nil when there is none, or when the previous value was a consume-once
	// mid-delivery its reader owns. Close always wakes: live → finalized current is a readable move.
	prev, _ := transitionResult(&w.h.gate, func() (*generation, bool, error) {
		cur := w.h.current
		// Whether the previous generation was a consume-once mid-delivery is read here, under the
		// same lock as the flip, so the off-lock reclamation below cannot race a concurrent claim. A
		// consumed previous generation is owned by its reader's Close, so the supersede leaves it alone —
		// reclaiming it here would double-release — and the closure returns nil for it.
		prevConsumed := cur != nil && cur.state == genConsumed
		w.g.state = genFinalized
		w.h.current = w.g
		w.h.live = nil
		// Fix the finalized framing under this same hold, the snapshot Clip() hands back once the lease
		// is gone. Captured inside the flip rather than after it, it is the exact finalize view — immune
		// to a consume-once claim that may flip current to consumed the instant this lock is released.
		w.framing = w.g.clip()
		// The finalized value is published and current points at it. This is the delivering wake for a
		// consume-once rendezvous — a waiter that parked through the whole invisible upload now resolves
		// the finished value and claims it. For a plain write it is spurious: that waiter already left on
		// the live follower at the install and rides the byte-log notifier to EOF. The two are the load-
		// bearing pair, one wake per write mode.
		if cur != nil && !prevConsumed {
			return cur, true, nil
		}
		return nil, true, nil
	})

	// Wake followers to EOF and reclaim the superseded generation after the flip, off the handle
	// lock. The append side closes here too; its close error is intentionally ignored — the bytes are
	// already durable from the Sync above and the append fd is process-scoped, reclaimed at exit, so
	// a failed close reports nothing actionable. Reclaiming the previous generation frees its quota
	// footprint — the one release that matches the reserve its Create made.
	_ = w.g.buf.Finish()
	if prev != nil {
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

// fail turns a finalize failure into an abort and returns the wrapped cause. committed reports
// whether the publish rename took effect — finalize forwarding commit's flag. When it did, the
// generation carries a present meta.json a crash could let recovery reinstate (it is the newest id
// for its name, so it would win the boot contest), so it is durably retired aside BEFORE discard's
// best-effort RemoveAll — the finalize twin of the retire Delete owes, closing the one reclaim
// in the store that could resurrect a write the client was told failed. When it did not (a pre-
// publish failure, or a rename that never took), the home is already markerless and discard's
// plain teardown suffices. Either way the generation is discarded exactly as Abort would, so its
// followers see a torn stream and the previous current value stands; the caller maps the returned
// error to an internal failure.
func (w *writer) fail(committed bool, cause error) error {
	if committed {
		w.s.med.abortPublish(w.g) // the publish took: durably retire the present meta.json before discard removes it
	}
	w.discard()
	return fmt.Errorf("finalize %s: %w", w.g.name, cause)
}

// discard detaches and tears down the live generation, the shared tail of Abort and a failed Close.
// It clears live through the gate — a live→nil transition, woken for uniformity and spurious-
// safe, since any parked waiter re-resolves the same not-readable state — then tears the byte log
// and reclaims the home off the lock. current is never touched, so the last complete value remains
// readable, and reclaiming frees the generation's footprint — the bytes it reserved as it wrote and
// the count it reserved at Create — so an aborted write leaks nothing.
func (w *writer) discard() {
	w.h.transition(func() bool {
		// Fix the live framing under the lock before clearing live, so a post-Abort Clip() returns this
		// snapshot — unfinalized, bytes-so-far — rather than a zero value. No caller reads Clip() after
		// an Abort today, but capturing in both terminals is what makes the contract total and the field
		// safe.
		w.framing = w.g.clip()
		w.h.live = nil
		return true
	})
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

// Clip returns the generation's view for response headers: bytes-so-far and an unfinalized marker
// while live, the final count and finalized marker once a terminal has run. While live it peeks
// the handle under the gate lock, since the size is still moving; after a terminal it returns the
// framing fixed under that terminal's lock, asking nothing of a handle the writer no longer leases.
// That split is the point — the post-terminal read touches no lock the writer has no lease to take,
// and no g.state a concurrent consume claim may have moved since the finalize.
func (w *writer) Clip() clip.Clip {
	if w.done {
		return w.framing // a terminal fixed this under its lock; no post-release handle reach, no g.state race
	}
	var c clip.Clip
	w.h.peek(func() { c = w.g.clip() }) // still live: the lease is held, so peeking the handle is safe
	return c
}
