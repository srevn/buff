package store

import (
	"context"
	"time"
)

// Reaper is the retention-sweep capability: run one sweep now, at the store's own clock. The one
// store implementation satisfies it, yet it is kept off the Store interface on purpose — a sweep is
// an operational concern, not part of the read/write seam every caller and every Store fake depends
// on, so folding it into Store would force fakes to grow a method they never exercise. A server
// that wants background reaping asserts for this narrow capability and schedules RunReaper; nothing
// else need know reaping exists. It is the io.WriterTo move: assert the extra ability at the edge
// that cares, never widen the core seam to advertise it.
type Reaper interface {
	ReapOnce()
}

// The one implementation provides the capability, checked at compile time.
var _ Reaper = (*store)(nil)

// ReapOnce runs one retention sweep at the store's current time. It is the exported face of the
// internal sweep, binding the injected clock so the reaper goroutine needs no clock of its own and
// a test that drives the store's clock drives reaping in lockstep.
func (s *store) ReapOnce() { s.reapOnce(s.now()) }

// RunReaper ticks ReapOnce on each interval until ctx is done, then returns. It is the long-lived
// loop a server schedules as one member of its goroutine group, so the retention policy stays here
// beside the sweep it drives and the server only schedules — it owns no reaping logic.
//
// A non-positive interval means "do not reap" and returns at once. The store reads zero as
// disabled in every other knob — no cap, no expiry, keep forever — so an embedder that derives this
// interval from the same configuration and lands on zero gets a quiet no-op here, not the panic
// time.NewTicker raises on a non-positive period. r is non-nil by construction at any real call
// site: a store that cannot reap is not a Reaper and cannot be passed. The binary still gates on a
// positive interval before scheduling this loop, so the guard here is the exported function's own
// contract for a direct embedder, not the path the binary takes.
func RunReaper(ctx context.Context, r Reaper, interval time.Duration) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.ReapOnce()
		}
	}
}

// reapCand is a candidate for reaping: a name, and the exact generation id observed expired under
// it. It deliberately carries no handle pointer. A handle captured during the registry walk may
// be evicted or replaced before the candidate is acted on, so the reaper re-acquires by name and
// rechecks the id — never operating on a stale handle, and never dropping a generation it did not
// itself observe expired.
type reapCand struct {
	name string
	id   genID
}

// reapOnce performs one retention sweep at the given instant: snapshot which finalized clips have
// expired, then remove each that is still expired when re-checked. The snapshot and the removals
// are two steps with no lock held between them — which is exactly what lets a concurrent write slip
// a fresh generation in under one of the names, and why the per-candidate recheck exists to spare
// that fresh generation from being reaped in its place.
//
// It is the reaper's whole unit of work, taking the time as an argument so a test can drive it with
// an injected clock. RunReaper above ticks it on an interval and cancels it on shutdown.
func (s *store) reapOnce(now time.Time) {
	for _, c := range s.reapCandidates(now) {
		s.reapRemove(now, c)
	}
}

// reapCandidates collects the name and id of every clip whose current generation is finalized and
// past its expiry. It copies the handle set under the registry lock, then locks each handle off
// that lock — so a handle held across a create or claim fsync stalls only this sweep's reach of it,
// never another name's operation — and captures names and ids only, never a handle that could go
// stale before reapRemove acts on it. A live generation has no expiry and a consumed one is owned
// by its reader, so the finalized-state test excludes both; a kept clip has a zero expiry and is
// skipped the same way.
func (s *store) reapCandidates(now time.Time) []reapCand {
	var out []reapCand
	for _, h := range s.reg.snapshot() {
		h.peek(func() {
			if g := h.current; g != nil && g.state == genFinalized &&
				!g.expires.IsZero() && now.After(g.expires) {
				out = append(out, reapCand{name: g.name, id: g.id})
			}
		})
	}
	return out
}

// reapRemove removes one candidate, but only after re-acquiring its handle and confirming the
// candidate is still there: the same finalized generation, by id, still past its expiry. Between
// the snapshot and now a replacement may have superseded it — the id no longer matches — a delete
// may have cleared it — current is nil — or it may have been claimed — no longer finalized. In each
// case the candidate is spared and only what was actually observed expired is dropped. The recheck
// and the durable retire run as one transition closure, then the home is reclaimed off the lock —
// the same crash-atomic unpublish Delete does, save that reap ignores a retire failure: the clip
// simply stays and the next sweep retries it, where Delete surfaces the fault to its caller.
// Re-acquiring may mint a fresh, empty handle if the old one was already evicted; releasing it
// then evicts it straight back.
func (s *store) reapRemove(now time.Time, c reapCand) {
	h := s.reg.acquire(c.name)
	var g *generation
	var moved bool
	h.transition(func() bool {
		g = h.current
		if g == nil || g.state != genFinalized || g.id != c.id ||
			g.expires.IsZero() || !now.After(g.expires) {
			return false // superseded, deleted, or claimed since the snapshot: spare it
		}
		moved, _ = s.retire(h, g) // a failed retire keeps the clip for the next sweep
		return moved
	})
	if moved {
		s.reclaim(g) // off the lock, after the transition's unlock
	}
	s.reg.release(h)
}
