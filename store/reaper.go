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
// internal sweep, binding the injected clock so the reaper goroutine needs no clock of its own
// and a test that drives the store's clock drives reaping in lockstep. The background loop only
// proactively reclaims disk, so it discards the count reapExpired returns; reclaimExpired keeps it.
func (s *store) ReapOnce() { s.reapExpired(s.now()) }

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

// minReclaimInterval bounds how often write-pressure may trigger an on-demand expiry sweep, so a
// store genuinely full of LIVE clips cannot turn every rejected write into an O(N) scan. It doubles
// as the spurious-rejection window on the pressure path — far tighter than the background interval,
// which now only does disk hygiene — so a write that loses the throttle waits at most this long for
// the client's retry to find the space a near-simultaneous sweep frees. An internal constant, not a
// config knob: the operator-facing surface is deliberately just the background interval.
const minReclaimInterval = time.Second

// reclaimExpired is the write-pressure self-heal: when a cap rejects a write, sweep the expired
// clips still holding space and report whether any was freed, so the caller can retry its
// reservation. It reuses the background sweep wholesale; its only new state is the lastReclaim
// throttle.
//
// The throttle makes the sweep safe to call from a hot reject path. A store full of live clips
// would otherwise rescan every name on every rejected write — O(N) amplification exactly when the
// store is busiest. Bounding it to one sweep per minReclaimInterval caps that to a rescan a second,
// store-wide. It is benign-racy on purpose: the load is a plain read, the claim a single CAS, no
// lock. A backward clock makes n-last negative, which is < the interval, so it reads as throttled
// and the CAS never runs — lastReclaim stays monotonic, and the background ticker is the backstop.
// A lost CAS means another writer is sweeping concurrently; this one declines rather than pile a
// second scan on.
//
// It MUST run off handle.mu — reapExpired takes registry.mu, and the hierarchy is registry-before-
// handle. Both call sites honor that: Create retries between createOnce calls (no gate held), and
// Write holds only a lease (the append is lock-free).
func (s *store) reclaimExpired(now time.Time) bool {
	n := now.UnixNano()
	last := s.lastReclaim.Load()
	if last != 0 && n-last < int64(minReclaimInterval) {
		return false // swept too recently; a fruitless rescan would only amplify the pressure
	}
	if !s.lastReclaim.CompareAndSwap(last, n) {
		return false // another writer claimed the sweep window; let it do the one scan
	}
	return s.reapExpired(now) > 0 // freed space ⇒ the caller should retry its reservation once
}

// reapExpired performs one retention sweep at the given instant: snapshot which finalized clips
// have expired, then remove each that is still expired when re-checked, and report how many it
// reclaimed. The snapshot and the removals are two steps with no lock held between them — which is
// exactly what lets a concurrent write slip a fresh generation in under one of the names, and why
// the per-candidate recheck exists to spare that fresh generation from being reaped in its place.
//
// It is the reaper's whole unit of work, shared by the background loop (which discards the count)
// and reclaimExpired (which retries when it is positive). It takes the time as an argument so a
// test can drive it with an injected clock; RunReaper ticks it on an interval and cancels it on
// shutdown.
func (s *store) reapExpired(now time.Time) int {
	reclaimed := 0
	for _, c := range s.reapCandidates(now) {
		if s.reapRemove(now, c) {
			reclaimed++
		}
	}
	return reclaimed
}

// reapCandidates collects the name and id of every clip expiredCurrent reports for this instant
// — a finalized current past its deadline. It copies the handle set under the registry lock, then
// locks each handle off that lock — so a handle held across a create or claim fsync stalls only
// this sweep's reach of it, never another name's operation — and captures names and ids only,
// never a handle that could go stale before reapRemove acts on it. expiredCurrent excludes a live
// generation (no deadline yet), a consumed one (its reader's to outrun), and a kept one (zero
// expiry), so the candidate set is exactly the finalized currents whose retention has lapsed.
func (s *store) reapCandidates(now time.Time) []reapCand {
	var out []reapCand
	for _, h := range s.reg.snapshot() {
		h.peek(func() {
			if g := expiredCurrent(h, now); g != nil {
				out = append(out, reapCand{name: g.name, id: g.id})
			}
		})
	}
	return out
}

// reapRemove removes one candidate, but only after re-acquiring its handle and confirming the
// candidate is still there: the same finalized generation, by id, still past its expiry. Between
// the snapshot and now a replacement may have superseded it — the id no longer matches — a delete
// may have cleared it — current is nil — or it may have been claimed — no longer finalized. In
// each case the candidate is spared and only what was actually observed expired is dropped. The
// recheck and the durable retire run as one transition closure, then the home is reclaimed off the
// lock — the same crash-atomic unpublish Delete does, save that reap ignores the retire fault where
// Delete surfaces it to its caller: a rename that never took leaves the clip standing for the next
// sweep, and a committed-but-undurable retire still drops it (the residual self-heals — a crash
// that reinstates it is re-reaped). Re-acquiring may mint a fresh, empty handle if the old one
// was already evicted; releasing it then evicts it straight back. It reports whether it actually
// reclaimed, so reapExpired can count what it freed for the pressure path's retry decision.
func (s *store) reapRemove(now time.Time, c reapCand) bool {
	h := s.reg.acquire(c.name)
	// The recheck and the durable retire run as one transitionResult closure, handing back the
	// generation to reclaim off the lock — nil when the candidate was superseded, deleted, or claimed
	// since the snapshot, or when the retire's rename never took and the clip stays for the next
	// sweep.
	prev, _ := transitionResult(&h.gate, func() (*generation, bool, error) {
		g := expiredCurrent(h, now)
		if g == nil || g.id != c.id {
			return nil, false, nil // superseded, deleted, or claimed since the snapshot: spare it
		}
		// retire returns nil only when the rename never took (the clip stays for the next sweep); a
		// committed-but-undurable retire still clears and reclaims, its fault ignored here — reap is
		// best-effort, and an undurable retire a crash reinstates is simply re-reaped.
		reclaim, _ := s.retire(h, g)
		return reclaim, reclaim != nil, nil
	})
	if prev != nil {
		s.reclaim(prev) // off the lock, after the transition's unlock
	}
	s.reg.release(h)
	return prev != nil
}
