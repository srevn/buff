// Package store is buff's content store: the seam every higher layer talks to, and the machinery
// that makes a flat namespace of named clips safe under concurrency. It owns the leased registry
// and per-name locking, the generation lifecycle (live, finalized, and later consumed), write
// admission of one live generation per name, and the read resolution that lets a reader follow a
// clip that is still being written.
//
// One unexported implementation provides all of that. Where a generation physically lives is the
// one thing that varies, behind a small medium seam: an in-memory medium today, a durable disk
// medium later. The subtle, race-prone lifecycle code — leases, the lock hierarchy, the supersede
// flip, read resolution — is therefore written and proven once, and a second medium adds only its
// IO, never a second copy of the concurrency spine.
//
// The lock hierarchy is the load-bearing invariant: the registry mutex is always acquired before
// any handle mutex, never the reverse, and the streaming body copy holds no lock at all. A read
// handle is opened under the handle lock, before that lock is released, so the generation's bytes
// are pinned before any concurrent supersede can reclaim its home — which is what makes eager
// reclamation safe.
package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/srevn/buff/clip"
)

// Store is the content store seam: create or replace a clip, open one for reading, stat or delete
// it, and list what exists. Callers depend on this interface, never a concrete backing, so a future
// storage medium is a new implementation behind it rather than a change to anything above. Every
// method takes a context that governs the operation and any stream it opens.
type Store interface {
	// Create opens a new generation for name, returning a Writer to stream its bytes into. With
	// PutOpts.IfMatch set it is a conditional write: if the current finalized generation does
	// not match, it reports ErrPreconditionFailed — evaluated before the busy gate, so a stale
	// precondition is refused as itself rather than masked as a retryable ErrBusy. It reports
	// ErrBusy if a generation is already being written to the name, and ErrNameInvalid for a name the
	// namespace does not allow.
	Create(ctx context.Context, name string, m clip.Meta, o PutOpts) (Writer, error)

	// Open attaches a reader to the readable generation of name: the latest finalized value, or a
	// first-ever write followed to its clean end. It returns the reader, a snapshot of the clip, and
	// ErrNotFound when there is nothing to read — unless GetOpts.Wait is set, in which case it blocks
	// until the name becomes readable rather than reporting ErrNotFound, bounded only by ctx. With
	// GetOpts.FollowNext it instead skips the value current at entry and resolves the next generation
	// written to the name (implying Wait), so it always parks for a write past the one already there.
	// The reader must be closed.
	Open(ctx context.Context, name string, o GetOpts) (io.ReadCloser, clip.Clip, error)

	// Stat returns a snapshot of name's readable generation without opening its bytes or changing its
	// state, or ErrNotFound when there is nothing to read.
	Stat(ctx context.Context, name string) (clip.Clip, error)

	// Delete removes name's finalized generation, reporting ErrNotFound if none exists. It never
	// disturbs a generation still being written — that one is the writer's to finish or abort.
	Delete(ctx context.Context, name string) error

	// List returns a snapshot of every finalized clip, in no particular order. Generations still being
	// written, or mid-delivery, are not listed.
	List(ctx context.Context) ([]clip.Clip, error)
}

// Writer streams one generation's bytes and ends it with exactly one terminal. It is owned by a
// single goroutine: write the body, then call Close to finalize or Abort to discard. Close after a
// terminal reports ErrClosed; Abort is idempotent.
type Writer interface {
	io.Writer        // append bytes to the live generation
	Close() error    // finalize: make durable, publish, become the readable current
	Abort() error    // discard this generation, leaving any previous value standing
	Clip() clip.Clip // the generation's current view, for response headers
}

// PutOpts carries the write-time choices for a new generation: how long the finalized clip is
// kept, whether it is kept forever, whether it is delivered to a single reader and then destroyed,
// and an optional conditional-write guard. TTL of zero with Keep false means the store's default
// retention; Keep overrides any TTL. Every field's zero value disables its policy, so a zero
// PutOpts is a plain unconditional write — IfMatch empty means no CAS, the same convention as TTL
// zero and the false flags.
type PutOpts struct {
	TTL         time.Duration // retention measured from finalize; zero means the store default
	Keep        bool          // never expire, overriding TTL
	ConsumeOnce bool          // deliver to at most one reader, then destroy
	IfMatch     string        // conditional write: replace only if the current finalized generation's id matches this, or "*" for any present finalized clip; empty is unconditional. A mismatch is ErrPreconditionFailed
}

// GetOpts carries read-time choices. Wait blocks Open until the name has a readable generation,
// bounded only by ctx; it defaults false so the store stays a non-surprising library — an
// embedder's Open of an absent clip still returns ErrNotFound at once. Waiting is opt-in at the
// api/ edge, not the store's policy: the GET handler sets Wait from the client's Buff-Wait
// directive, so the library keeps its immediate contract and the relay edge gets rendezvous on
// request — a consumer that asked to wait for a producer arriving before it.
type GetOpts struct {
	Wait bool // block until the name becomes readable, bounded only by ctx; the api GET sets it from Buff-Wait
	// FollowNext skips the value current at entry and resolves to the next generation written to the
	// name instead, then follows it to completion. It implies Wait — there is by definition no next
	// generation yet at entry, so the read always parks for one — and Open normalizes it so. The skip
	// is asymmetric and deliberately so: a finalized current is skipped, but a live current (a write
	// already in progress) is followed, since there is no settled value to step past — skip a settled
	// value, join a stream already underway. Its parks are the heaviest-tailed the store has: unlike
	// a Wait read of a populated slot, which returns at once, this one waits for a next write
	// that may never come, so it leans hardest on ctx-cancel as the only guaranteed unblock.
	FollowNext bool
}

// Config carries the store-level policy a constructor needs: the three hard caps the quota enforces
// and the default retention applied to a write that names no TTL of its own. Every field's zero
// value disables that policy — unlimited caps, and no default expiry — so the zero Config is an
// unbounded store that keeps clips forever, the right default for tests and embedding. A server
// maps its environment defaults into this struct; the store itself imposes none. The caps live
// here, not on a medium, because the quota sits above both the memory and the disk medium and is
// enforced identically for each.
type Config struct {
	MaxClip    int64         // per-clip byte cap; 0 = unlimited
	MaxTotal   int64         // total byte cap across all clips; 0 = unlimited
	MaxClips   int           // cap on the number of extant generations; 0 = unlimited
	DefaultTTL time.Duration // retention for a write with no TTL and not kept; 0 = no default expiry
}

// Interface conformance, checked at compile time so a drifting method signature is a build error
// rather than a runtime surprise.
var (
	_ Store         = (*store)(nil)
	_ Writer        = (*writer)(nil)
	_ io.ReadCloser = (*leasedReader)(nil)
)

// store is the one Store implementation. It binds a registry, a medium, a clock, the quota that
// caps what it holds, and the default retention: the medium decides where generations live, the
// clock is injected so id minting, finalize times, and reaping are deterministic under test, the
// quota is the store-wide admission gate, and the registry holds everything else.
type store struct {
	reg        *registry
	med        medium
	now        func() time.Time
	quota      *quota
	defaultTTL time.Duration
}

// newStore assembles a store over a medium, a clock, and a configuration. It is the internal
// constructor every public constructor funnels through, so tests can inject a fault-injecting
// medium, a clock that runs backwards, or specific caps and retention.
func newStore(med medium, now func() time.Time, c Config) *store {
	return &store{reg: newRegistry(), med: med, now: now, quota: newQuota(c), defaultTTL: c.DefaultTTL}
}

// NewMemory returns a store that keeps clips in memory, using the real clock, bounded and aged by
// c. Bytes live only as long as the process and any reader holding them; it is the store for tests,
// ephemeral use, and embedding. A zero Config is unbounded and keeps clips forever.
func NewMemory(c Config) Store {
	return newStore(memMedium{}, time.Now, c)
}

// resolveTTL turns a write's retention choice into the single span its generation carries. Keep
// means never expire; an explicit positive TTL is taken as given; anything else — no TTL given,
// or a non-positive one — falls back to the store's default, which is itself zero when the store
// has no default. So an unspecified or even malformed TTL resolves conservatively to "no expiry",
// never to the footgun of "expire immediately". The absolute deadline is computed from this span at
// Close, once the finalize instant it measures from is known.
func (s *store) resolveTTL(o PutOpts) time.Duration {
	if o.Keep {
		return 0
	}
	if o.TTL > 0 {
		return o.TTL
	}
	return s.defaultTTL
}

// Create opens a new generation for name. It leases the handle for the whole upload, refuses a
// second concurrent write with ErrBusy, allocates the id and builds the byte log under the handle
// lock, then installs the generation as live and returns its Writer. Every early exit releases the
// lease, so a Create that never installs a generation leaves no handle behind.
func (s *store) Create(ctx context.Context, name string, m clip.Meta, o PutOpts) (Writer, error) {
	if err := clip.ValidName(name); err != nil {
		return nil, err
	}
	// Normalize the metadata at the same admission step the name passes through. The store is the
	// integrity authority for what it persists, so a file-scoped field on a kind that cannot carry
	// it (an executable bit on a bytes clip) is cleared here, before g.meta below is set — the same
	// reason ValidName guards the name at this seam and not in the HTTP layer. Both the durable
	// meta.json and the live projection g.clip() read g.meta, so cleaning it once leaves neither able
	// to hold an illegal combination, whatever the caller passed: a raw PUT, an embedder, or a test.
	m = m.Normalized()
	h := s.reg.acquire(name)
	// The whole admission runs as one transitionResult closure: the gate holds the lock unbroken from
	// the IfMatch check through the install, wakes iff a live generation is actually installed, and
	// hands back the generation it built or the rejection error directly — no result smuggled through
	// an outer local. Each rejection arm returns (nil, false, err); the install arm returns (g, true,
	// nil). The one off-lock obligation — releasing the lease when nothing was installed — runs once
	// after, on a non-nil error, replacing the five copies of unlock-release-return the inline form
	// repeated.
	g, err := transitionResult(&h.gate, func() (*generation, bool, error) {
		// Conditional write: the caller asserts the readable value is a specific generation, or any
		// ("*"). Compare against the finalized current under the same lock that admits the write — held
		// unbroken from here through the install below — so the value cannot move between the check and
		// the replace. An absent or non-finalized current — nothing, a live first write, a consume-once
		// mid-delivery — is not the readable value and matches nothing, so it is the same 412 as a stale
		// id: every state but a matching finalized current refuses. Evaluated before the live-incumbent
		// gate so a stale precondition is refused as itself, not masked as busy — busy invites a retry
		// a definitively superseded id can never satisfy, where 412 says re-read. Nothing is minted yet,
		// so a refusal wastes no id, no quota slot, no home — the early return mirrors the busy path
		// just below.
		if o.IfMatch != "" {
			ok := h.current != nil && h.current.state == genFinalized &&
				(o.IfMatch == "*" || h.current.id.String() == o.IfMatch)
			if !ok {
				return nil, false, clip.ErrPreconditionFailed
			}
		}
		if h.live != nil {
			return nil, false, clip.ErrBusy
		}
		// Reserve the count slot before minting an id or building a home, so the cheap check fails fast
		// and no work is wasted on a clip the store has no room for. A busy name was already rejected
		// above, so it never reserves. Both error paths below give the slot back; nothing else has been
		// allocated yet, so the count is all there is to release.
		if !s.quota.reserveClip() {
			return nil, false, clip.ErrNoSpace
		}
		// One clock read for the whole creation: the id derives its monotonic prefix from this instant
		// and the generation records it as its creation time, so the time embedded in the id and the time
		// reported as CreatedAt cannot drift apart across the work create does.
		now := s.now()
		id, err := h.allocate(now)
		if err != nil {
			s.quota.releaseClip()
			return nil, false, fmt.Errorf("create %s: %w", name, err)
		}
		buf, err := s.med.create(id)
		if err != nil {
			s.quota.releaseClip()
			return nil, false, fmt.Errorf("create %s: %w", name, err)
		}
		g := &generation{
			id:      id,
			name:    name,
			meta:    m,
			created: now,
			ttl:     s.resolveTTL(o),
			consume: o.ConsumeOnce,
			state:   genLive,
			buf:     buf,
		}
		h.live = g
		// Installing a live generation can newly make a name readable. The wake delivers a parked waiter
		// only for a plain write onto a name with no readable value — the waiter resolves this generation
		// and attaches a follower. A consume-once generation stays invisible while live, so its waiter
		// re-parks here and is served later by the Close finalize: the two are the load-bearing pair
		// the notifier exists for, one wake per write mode. Every other waking site only clears or flips
		// state no waiter is parked on.
		return g, true, nil
	})
	if err != nil {
		s.reg.release(h)
		return nil, err
	}
	return &writer{s: s, h: h, g: g}, nil
}

// Open attaches a reader to name's readable generation. It resolves the target, claims it if it
// is a consume-once clip, and opens its read handle — all under one unbroken gate hold — so the
// bytes are pinned before any concurrent supersede can reclaim the home, and a consume-once clip is
// claimed for exactly one reader before a single byte ships. A finished or just-claimed generation
// reads as a fixed section, a live one as a follower. The lease is held until the reader is closed,
// keeping the handle alive across the lock-free stream; for a consumed generation the close also
// destroys it.
//
// The resolve-or-wait and the claim are the gate's await: resolve picks the target (or reports
// it not-here-yet), and on success commitRead claims and opens it under the same hold the resolve
// ran under — the unbroken hold is what keeps the claim at-most-once. When GetOpts.Wait is set,
// a resolve that finds nothing readable parks on the notifier and re-resolves on each wake rather
// than returning ErrNotFound, until the name becomes readable or ctx is canceled; the lease
// is acquired once and held across every wait turn, pinning the possibly-empty handle against
// eviction. A waiter's only guaranteed unblock is ctx-cancel, since no write to the name is ever
// promised — the liveness asymmetry the gate is built around.
func (s *store) Open(ctx context.Context, name string, o GetOpts) (io.ReadCloser, clip.Clip, error) {
	if err := clip.ValidName(name); err != nil {
		return nil, clip.Clip{}, err
	}
	// follow-next is wait-for-next: there is by definition no next generation at entry, so the read
	// must park for one. Normalize the implication once, here, so the wait gate, the commit, and a
	// direct embedder all see a single coherent intent rather than two flags to keep in step.
	if o.FollowNext {
		o.Wait = true
	}
	// A consume-once Open claims its one delivery before shipping a byte, and the claim cannot be taken
	// back. Were the request already canceled, claiming would spend that delivery on a reader that has
	// gone away — so decline before acquiring anything, sparing an already-dead request the whole walk.
	// This is the first of two layered ctx checks: this one fast-fails a request canceled before Open,
	// and a second at the claim itself (in commitRead) catches a cancel that lands during a wait, so
	// between them only a cancel inside the claim's own rename+fsync still spends the delivery.
	if err := ctx.Err(); err != nil {
		return nil, clip.Clip{}, err
	}
	h := s.reg.acquire(name)

	// baseline is follow-next's cursor: the id current at entry, the value it must step past. It
	// is captured on the first resolve, under the same lock as that resolve — so no write can land
	// between snapshot and use — and never recaptured, so a generation that finalizes mid-wait does
	// not silently become the new thing to skip. An empty slot leaves it the zero genID, which every
	// real id sorts after (a real prefix is a creation UnixNano), so the first write is "newer than
	// nothing" and follow-next degenerates to a plain wait. captured, not a zero baseline, marks the
	// snapshot taken, since zero is itself a legitimate captured value. baseline/captured persist
	// across the await loop's turns because the closure closes over them, exactly as the inline for-
	// loop's locals did.
	var baseline genID
	captured := false
	res, err := await(&h.gate, ctx,
		func() (*generation, error) { // resolve — the read-resolution policy, run under the gate lock
			if o.FollowNext && !captured {
				if h.current != nil {
					baseline = h.current.id
				}
				captured = true
			}
			if o.FollowNext {
				return followResolve(h, baseline)
			}
			return resolveRead(h)
		},
		// waitable — wait only while the clip is merely not-here-yet. ErrConsumed — a consume-once
		// another reader claimed, seen mid-delivery — is terminal for this request: waiting cannot
		// bring it back, and the claimant's later cleanup would wake us to ErrNotFound only to park
		// forever. So it, and any other error resolve might add, surfaces at once: the same "gone" a non-
		// waiting reader gets. resolveRead returns only nil, ErrConsumed, or ErrNotFound, so gating on
		// ErrNotFound is the exact "still waitable" predicate; followResolve only narrows that set, never
		// returning ErrConsumed.
		func(err error) bool { return o.Wait && errors.Is(err, clip.ErrNotFound) },
		func(g *generation) (openResult, bool, error) { return s.commitRead(ctx, h, g, name) }, // commit
	)
	if err != nil {
		// A forfeit claim or a reader-open failure returns a cleanup obligation to destroy the claimed
		// generation off the lock; a plain not-here, consumed, or canceled resolve returns none.
		if res.cleanup != nil {
			res.cleanup()
		}
		s.reg.release(h)
		return nil, clip.Clip{}, err
	}
	return &leasedReader{rc: res.rc, release: func() { s.reg.release(h) }, cleanup: res.cleanup}, res.clip, nil
}

// openResult is what Open's commit produces under the gate lock: the reader to stream, the clip
// snapshot whose Finalized flag fixes the response framing, and a consume-once cleanup obligation
// that must run OFF the lock — deferred to the reader's Close on success, or run at once on a
// claim/open failure. A nil cleanup is a plain read with nothing to reclaim.
type openResult struct {
	rc      io.ReadCloser
	clip    clip.Clip
	cleanup func()
}

// commitRead is Open's commit: with the gate lock held and a readable generation g resolved, it
// claims a finalized consume-once generation, then opens the reader and snapshots the clip. It
// runs inside await under the unbroken hold the resolve ran under, so no second Open can interleave
// between the resolve that picked g and the claim that flips it — which is what makes the claim at-
// most-once.
//
// The flip to the consumed state is the serialization point: a racing reader either loses this
// lock and then resolves the consumed state to ErrConsumed with no bytes, or it already lost the
// resolve. The flip is irreversible, so one last ctx check guards it: the entry guard declined an
// already-canceled Open and await's post-park check caught a cancel at the wakeup, but a cancel
// landing in the window between that check and this commit would otherwise spend the one delivery on
// a reader already gone — so a canceled ctx returns before the flip, narrowing that window to the
// claim's own rename+fsync. A claim that does flip can then fail two ways that call for opposite
// responses, which is why the medium reports whether it committed. Never took (committed false):
// revert to finalized so the clip stays claimable — a no-op no waiter can observe, so wake stays
// false. Took but undurable (committed true, with an error): the marker is gone, the secret forfeit,
// so destroy it off the lock rather than reverting to a claimable state it cannot honour — wake stays
// false here too, the transient consumed state cleared by cleanupConsumed, whose own wake is as
// beneficiary-less as this edge (below). Either way no byte ships, so at-most-once holds with zero
// delivery.
//
// The finalized→consumed edge has no parked beneficiary, which is what makes both wake choices above
// safe: the finalize already woke any rendezvous waiter — who is this very claiming reader — and a
// later arrival resolves the consumed outcome itself rather than parking on the absence. So a stuck
// claim's wake serves no one, yet it fires (wake=true) for rule-totality — every change to what a
// read resolves wakes, with no genState carve-out — while a forfeit's identical edge suppresses it,
// equally safe. The wake bool is returned independent of the error: a stuck claim that then fails to
// open its reader still wakes, returning (cleanup, wake=true, err≠nil), and await fires the wake
// before surfacing the error.
func (s *store) commitRead(ctx context.Context, h *clipHandle, g *generation, name string) (openResult, bool, error) {
	// consumed doubles as the wake the commit returns: the claim's finalized→consumed flip is the only
	// readable-state move commitRead makes, so a stuck claim both wakes and is the lone thing needing
	// off-lock reclaim, and the two can never disagree. The three early returns each report a literal
	// false: the canceled-ctx guard and the revert move nothing, and a forfeit's flip is beneficiary-
	// less, its clear left to cleanupConsumed.
	consumed := false
	if g.state == genFinalized && g.consume {
		// The flip below is irreversible. A request canceled during the await park — after await's own
		// post-park check, before this commit — would otherwise spend the one delivery on a reader gone;
		// decline here, the claim-time half of the entry guard, narrowing the window to the flip itself.
		if err := ctx.Err(); err != nil {
			return openResult{}, false, err
		}
		g.state = genConsumed
		if committed, err := s.med.claim(g); err != nil {
			if committed {
				return openResult{cleanup: func() { s.cleanupConsumed(h, g) }}, false, fmt.Errorf("open %s: %w", name, err)
			}
			g.state = genFinalized
			return openResult{}, false, fmt.Errorf("open %s: %w", name, err)
		}
		consumed = true
	}
	var rc io.ReadCloser
	var err error
	if g.state == genLive {
		rc, err = g.buf.Reader(ctx, 0)
	} else {
		rc, err = g.buf.Section()
	}
	if err != nil {
		// A claim that succeeded but cannot open its reader has delivered to no one; at-most-once still
		// holds, so destroy the consumed generation in place rather than risking a second delivery. The
		// wake still fires — the claim really did flip the state — and the cleanup runs off the lock.
		var cleanup func()
		if consumed {
			cleanup = func() { s.cleanupConsumed(h, g) }
		}
		return openResult{cleanup: cleanup}, consumed, fmt.Errorf("open %s: %w", name, err)
	}
	res := openResult{rc: rc, clip: g.clip()} // g.clip() under the lock — the framing snapshot
	if consumed {
		res.cleanup = func() { s.cleanupConsumed(h, g) }
	}
	return res, consumed, nil
}

// Stat snapshots name's readable generation, resolving it by the same rule Open applies but
// opening no bytes and claiming nothing, so a stat never consumes a consume-once clip. It runs no
// wait loop: an absent name resolves to ErrNotFound at once, never a block — which is what makes
// HEAD the immediate existence probe, the prompt 404 a GET also gives unless it opts into waiting
// with Buff-Wait. The lease is released before returning; no stream outlives it.
func (s *store) Stat(ctx context.Context, name string) (clip.Clip, error) {
	if err := clip.ValidName(name); err != nil {
		return clip.Clip{}, err
	}
	h := s.reg.acquire(name)
	var c clip.Clip
	var err error
	h.peek(func() {
		var g *generation
		if g, err = resolveRead(h); err == nil {
			c = g.clip()
		}
	})
	s.reg.release(h)
	return c, err
}

// Delete removes name's finalized generation, or reports ErrNotFound when only a live generation
// (or nothing) exists — a live generation belongs to its writer. The recheck and the durable retire
// run as one transition closure under the gate lock — the same crash-atomic unpublish a removal
// owes that a finalize already keeps — then the home is reclaimed off the lock; releasing the lease
// evicts the handle if nothing else remains. A retire that cannot be made durable fails the Delete,
// wrapped, rather than reporting a success a crash could silently undo: that is the one error this
// returns beyond ErrNotFound. Racing a finalize, the gate serializes them into a deterministic
// last-writer- wins.
func (s *store) Delete(ctx context.Context, name string) error {
	if err := clip.ValidName(name); err != nil {
		return err
	}
	h := s.reg.acquire(name)
	// The recheck and the durable retire run as one transitionResult closure, handing back the
	// generation to reclaim off the lock — nil when there is nothing finalized to retire or the rename
	// never took — and the error to surface. Absence is its own sentinel; a retire fault is wrapped.
	prev, err := transitionResult(&h.gate, func() (*generation, bool, error) {
		cur := h.current
		if cur == nil || cur.state != genFinalized {
			return nil, false, clip.ErrNotFound
		}
		// retire returns the generation it cleared (or nil if the rename never took); that pointer is
		// both the off-lock reclaim target and the wake, the two coinciding because clearing current is
		// what moves readable state. A committed-but-unflushed forfeit returns the generation with a non-
		// nil fault — cleared and reclaimed, but reported failed.
		reclaim, fault := s.retire(h, cur)
		return reclaim, reclaim != nil, fault
	})
	if prev != nil {
		s.reclaim(prev) // off the lock, after the transition's unlock — never under it
	}
	s.reg.release(h)
	if err != nil {
		if errors.Is(err, clip.ErrNotFound) {
			return err // the absence sentinel, surfaced as itself, not wrapped
		}
		return fmt.Errorf("delete %s: %w", name, err)
	}
	return nil
}

// List snapshots every finalized clip. It copies the handle set under the registry lock, then peeks
// each handle individually with the registry lock released — so a handle mid-create or mid-claim,
// holding its gate across a disk fsync, stalls only this walk's reach of that one handle and never
// an operation on another name. It skips handles whose current generation is absent or not yet
// finalized. The order is unspecified; a presentation layer sorts.
func (s *store) List(ctx context.Context) ([]clip.Clip, error) {
	var out []clip.Clip
	for _, h := range s.reg.snapshot() {
		h.peek(func() {
			if h.current != nil && h.current.state == genFinalized {
				out = append(out, h.current.clip())
			}
		})
	}
	return out, nil
}

// reclaim frees a generation's whole footprint — its on-disk home and its quota slot — and is
// the one place every reclamation routes through: the writer's supersede and discard, Delete, the
// reaper's sweep, and a consumed clip's cleanup all call it. Centralising the pair keeps the home
// and the quota always freed together and in one order, so no path can release the slot yet strand
// the home, and a sixth reclamation cannot quietly forget one half.
//
// remove runs first, then releaseGen — an order that is free only because releaseGen reads
// buf.Size() from memory: remove deletes the on-disk directory but never touches the in-RAM buffer,
// so its size is still there to give back afterwards. A Size() that re-stat'd the now-deleted data
// would invert this, so the order and the in-memory size are one pair.
//
// remove is best-effort: it never fails the operation that called reclaim — a home it cannot delete
// is recorded by the medium and left for a later reclamation — so it returns nothing to act on,
// while releaseGen runs regardless, balancing the counters even when the disk delete could not.
// Every caller is already off the handle lock, and reclaim itself takes none.
func (s *store) reclaim(g *generation) {
	s.med.remove(g)
	s.quota.releaseGen(g)
}

// cleanupConsumed destroys a consumed generation. It reclaims the home and frees the quota off the
// handle lock, then takes the lock just long enough to clear current — but only if it still points
// at this generation, so a replacement that has since superseded it is left untouched. It is the
// sole owner of a consumed generation's reclamation: the writer's supersede defers to it (so the
// two never both reclaim), and it runs on every reader Close — clean end, mid-stream error, or
// cancellation — so a finished or failed consume never leaves plaintext behind.
func (s *store) cleanupConsumed(h *clipHandle, g *generation) {
	s.reclaim(g)
	h.transition(func() bool {
		if h.current == g {
			h.current = nil
			return true // consumed→absent: a uniformity wake, no waiter parks on this edge (a follow-next
			// reader re-resolves the same not-readable state, a plain one already left on ErrConsumed);
			// kept true so the rule needs no carve-out
		}
		return false // a replacement has since superseded g: leave it, nothing observable moved
	})
}

// retire durably retires h's finalized current generation g — the shared core of Delete and the
// reaper, giving a removal the crash-atomicity a finalize already has. It is run inside a
// transitionResult closure with the gate lock held and h.current confirmed to be g; unpublish runs
// under that lock for the same reason the claim does — while it renames, current still points at g,
// so the lock is what stops a concurrent supersede from reading prev == g and reclaiming g's home
// underneath the in-flight rename.
//
// unpublish renames g's meta.json aside, so the instant it commits g no longer resolves on disk and a
// crash GCs the markerless leftover rather than resurrecting it. retire returns the generation the
// caller must reclaim off the lock — g when the rename committed, nil when it never took — and that
// pointer doubles as the gate's wake signal, non-nil exactly when readable state moved, so a caller
// passes `reclaim != nil` as the transition's wake. The err follows unpublish's committed/err split,
// the mirror of the claim's three ways:
//   - !committed: the rename never took, nothing changed on disk — leave current standing (the clip
//     stays readable), return nil to reclaim alongside the fault.
//   - committed, err != nil: the rename took but its flush did not; meta.json is already gone, so a
//     crash cannot bring g back — clear current to match disk and return g to reclaim, but still
//     report the fault, the forfeit mirror of the claim's destroy-in-place.
//   - committed, err == nil: the retire is fully durable — clear current, return g to reclaim, and
//     succeed.
//
// Both committed arms share the clear and return g; only the err differs, nil exactly when the
// retire is durable. retire touches neither the lock nor the reclaim — the transition owns the
// wake, the caller runs the off-lock reclaim on the returned generation and is the one site that
// releases the lease — so it composes inside a transitionResult exactly as the claim composes
// inside commitRead.
func (s *store) retire(h *clipHandle, g *generation) (reclaim *generation, err error) {
	committed, err := s.med.unpublish(g)
	if !committed {
		return nil, err
	}
	h.current = nil
	return g, err
}

// leasedReader couples a read stream to the lease that keeps its handle alive, and for a consume-
// once read to the cleanup that destroys the claimed generation once delivery ends. It hands reads
// straight through and, on Close, releases the read handle first, then runs any cleanup, then
// releases the lease — dropping the pin on the bytes, reclaiming a consumed generation, and only
// then letting the handle be evicted. Close is safe to call more than once, as the unwind of an
// aborted request may; the read-handle close, the cleanup, and the lease release each run exactly
// once, the whole sequence owned by one sync.Once rather than split across this guard and the inner
// reader's own idempotence.
type leasedReader struct {
	rc      io.ReadCloser
	cleanup func() // consume-once only: destroy the claimed generation; nil for an ordinary read
	release func()
	once    sync.Once
}

// Read streams the clip's bytes.
func (l *leasedReader) Read(p []byte) (int, error) { return l.rc.Read(p) }

// Close releases the read handle, then runs any consume-once cleanup, then releases the lease — all
// three exactly once, under one guard. The read handle is dropped first so a cleanup that reclaims
// the bytes finds no reader of its own still pinning them; folding it into the Once is what lets this
// reader own its whole double-close defense rather than lean on the inner reader's idempotence — a
// second Close becomes a true no-op returning nil, the same outcome as before, now from one place.
func (l *leasedReader) Close() error {
	var err error
	l.once.Do(func() {
		err = l.rc.Close() // read handle first (drop the byte pin) …
		if l.cleanup != nil {
			l.cleanup() // … then cleanup (reclaim a consumed generation) …
		}
		l.release() // … then the lease (allow eviction). All three, exactly once.
	})
	return err
}
