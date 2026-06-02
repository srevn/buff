package store

import (
	"context"
	"time"
)

// Reaper is the retention-sweep capability: run one sweep now, at the store's own clock. The one
// store implementation satisfies it, yet it is kept off the Store interface on purpose — a sweep
// is an operational concern, not part of the read/write seam every caller and every Store fake
// depends on, so folding it into Store would force fakes to grow a method they never exercise. A
// server that wants background reaping asserts for this narrow capability and schedules RunReaper;
// nothing else need know reaping exists. It is the io.WriterTo move: assert the extra ability at
// the edge that cares, never widen the core seam to advertise it.
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
// interval must be positive and r non-nil; both hold by construction at the sole call site, where
// the concrete store and its configuration are in hand and the question "reap at all, and how
// often" is decided. There is deliberately no degenerate branch: a store that cannot reap is not a
// Reaper and cannot be passed, and a non-positive interval is a caller bug — NewTicker would
// panic — not a case to quietly absorb as a forever-block. Keeping the loop total over its
// arguments is what lets it read as a plain ticker rather than a guard around cases that cannot
// arise.
func RunReaper(ctx context.Context, r Reaper, interval time.Duration) {
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

// reapCand is a candidate for reaping: a name, and the exact generation id observed expired
// under it. It deliberately carries no handle pointer. A handle captured during the registry
// walk may be evicted or replaced before the candidate is acted on, so the reaper re-acquires
// by name and rechecks the id — never operating on a stale handle, and never dropping a
// generation it did not itself observe expired.
type reapCand struct {
	name string
	id   genID
}

// reapOnce performs one retention sweep at the given instant: snapshot which finalized clips
// have expired, then remove each that is still expired when re-checked. The snapshot and the
// removals are two steps with no lock held between them — which is exactly what lets a
// concurrent write slip a fresh generation in under one of the names, and why the
// per-candidate recheck exists to spare that fresh generation from being reaped in its place.
//
// It is the reaper's whole unit of work, taking the time as an argument so a test can drive
// it with an injected clock. RunReaper above ticks it on an interval and cancels it on
// shutdown.
func (s *store) reapOnce(now time.Time) {
	for _, c := range s.reapCandidates(now) {
		s.reapRemove(now, c)
	}
}

// reapCandidates collects the name and id of every clip whose current generation is finalized and
// past its expiry. It copies the handle set under the registry lock, then locks each handle off
// that lock — so a handle held across a create or claim fsync stalls only this sweep's reach of
// it, never another name's operation — and captures names and ids only, never a handle that could
// go stale before reapRemove acts on it. A live generation has no expiry and a consumed one is
// owned by its reader, so the finalized-state test excludes both; a kept clip has a zero expiry
// and is skipped the same way.
func (s *store) reapCandidates(now time.Time) []reapCand {
	var out []reapCand
	for _, h := range s.reg.snapshot() {
		h.mu.Lock()
		if g := h.current; g != nil && g.state == genFinalized &&
			!g.expires.IsZero() && now.After(g.expires) {
			out = append(out, reapCand{name: g.name, id: g.id})
		}
		h.mu.Unlock()
	}
	return out
}

// reapRemove removes one candidate, but only after re-acquiring its handle and confirming the
// candidate is still there: the same finalized generation, by id, still past its expiry.
// Between the snapshot and now a replacement may have superseded it — the id no longer
// matches — a delete may have cleared it — current is nil — or it may have been claimed — no
// longer finalized. In each case the candidate is spared and only what was actually observed
// expired is dropped. The recheck reads current under the handle lock; the reclamation and the
// quota release run off it, as every removal does. Re-acquiring may mint a fresh, empty handle
// if the old one was already evicted; releasing it then evicts it straight back.
func (s *store) reapRemove(now time.Time, c reapCand) {
	h := s.reg.acquire(c.name)
	h.mu.Lock()
	g := h.current
	if g != nil && g.state == genFinalized && g.id == c.id &&
		!g.expires.IsZero() && now.After(g.expires) {
		h.current = nil
		h.mu.Unlock()
		_ = s.med.remove(g)
		s.quota.releaseGen(g)
	} else {
		h.mu.Unlock()
	}
	s.reg.release(h)
}
