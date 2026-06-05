package store

import "sync/atomic"

// quota is the store's hard ceiling on what it will hold: a per-clip byte limit, a total
// byte limit across every clip, and a limit on how many generations exist at once. It is
// the one piece of shared mutable state every writer touches, so it is built from atomic
// counters rather than a lock — a writer reserves room with a compare-and-swap before it
// appends a chunk, and the store never physically overshoots, even when many writers race
// against different names.
//
// Each limit is enforced only when it is non-zero; a zero limit means unlimited. The
// counters are tracked either way, so even an unlimited store reports its live footprint —
// which is what a later recovery pass recomputes after a crash, and what lets the tests
// detect a single leaked byte or count.
//
// The balance is structural, not bookkeeping bolted on after the fact. A generation
// reserves one count slot the instant Create admits it and one byte for every byte it
// accepts; it gives back exactly that footprint — buf.Size() bytes and one count — at the
// single moment its home is reclaimed, through releaseGen. One reserve at the entrance, one
// release at the one exit: the counters return to zero precisely when the store empties.
type quota struct {
	maxClip  int64 // per-clip byte limit; 0 = unlimited
	maxTotal int64 // total byte limit across all clips; 0 = unlimited
	maxClips int64 // limit on the number of extant generations; 0 = unlimited

	bytes atomic.Int64 // reserved-plus-committed bytes; lives only in RAM
	clips atomic.Int64 // reserved-plus-committed generation count
}

// newQuota builds a quota from the store's configured ceilings. The byte and count
// counters start at zero: a fresh store holds nothing.
func newQuota(c Config) *quota {
	return &quota{maxClip: c.MaxClip, maxTotal: c.MaxTotal, maxClips: int64(c.MaxClips)}
}

// restore sets the counters to an absolute footprint, recomputed at startup from the generations
// that survived recovery — it does not reserve against the caps, and must not. A server restarted
// with a smaller cap than when its clips were written should reflect the real footprint it
// inherited; the next reserve then correctly rejects new writes until deletion or reaping frees
// room. Enforcing here instead would wrongly drop already-durable survivors. It runs once,
// single-threaded, before any writer can race it.
func (q *quota) restore(bytes, clips int64) {
	q.bytes.Store(bytes)
	q.clips.Store(clips)
}

// reserve claims n bytes of the total budget, returning false if that would cross a set
// limit. It is the write path's admission check: the compare-and-swap retries until it
// either wins its claim against the current total or loses to the limit, so concurrent
// writers can never sum past the ceiling. A zero limit admits every claim but still tracks
// the bytes.
func (q *quota) reserve(n int64) bool {
	for {
		cur := q.bytes.Load()
		if q.maxTotal != 0 && cur+n > q.maxTotal {
			return false
		}
		if q.bytes.CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

// release returns n bytes to the total budget. It always pairs with an earlier reserve:
// the unused tail of a short write, or part of a generation's whole footprint coming back.
func (q *quota) release(n int64) {
	if n != 0 {
		q.bytes.Add(-n)
	}
}

// reserveClip claims one generation slot of the count budget, returning false if that would
// cross a set limit. It is reserve's twin on the count counter, admitting the one slot a new
// generation needs before Create mints its id and builds its home.
func (q *quota) reserveClip() bool {
	for {
		cur := q.clips.Load()
		if q.maxClips != 0 && cur+1 > q.maxClips {
			return false
		}
		if q.clips.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// releaseClip returns one generation slot to the count budget.
func (q *quota) releaseClip() { q.clips.Add(-1) }

// releaseGen gives back a whole generation's footprint at once: its bytes and its one count
// slot — the exact reservations Create and the writes made, undone together. It is reclaim's
// quota half and is called only from there, so the count reserved at Create and the bytes
// reserved across its writes are undone exactly once. It reads buf.Size() — the lone size
// authority, answered from memory and so fixed for a generation no longer growing — which is
// what lets reclaim release a generation's slot after its home is already gone; the ordering
// rationale lives with reclaim.
func (q *quota) releaseGen(g *generation) {
	q.release(g.buf.Size())
	q.releaseClip()
}

// perClipOK reports whether appending n more bytes keeps a generation that has already
// accepted running bytes within the per-clip limit. It mutates nothing and reads no shared
// counter — the per-clip limit is local to one writer — so the write path checks it first
// and rejects an oversized chunk whole, before it ever touches the shared byte budget.
func (q *quota) perClipOK(running, n int64) bool {
	return q.maxClip == 0 || running+n <= q.maxClip
}
