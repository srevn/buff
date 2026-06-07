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
	// until the name becomes readable rather than reporting ErrNotFound, bounded only by ctx. The
	// reader must be closed.
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
// embedder's Open of an absent clip still returns ErrNotFound at once. Default-wait is an api/
// policy, not the store's: the GET handler sets Wait, so the library keeps its immediate contract
// and the relay edge gets rendezvous — a consumer arriving before its producer, made to wait for
// it.
type GetOpts struct {
	Wait bool // block until the name becomes readable, bounded only by ctx; the api GET sets it
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
	h.mu.Lock()
	// Conditional write: the caller asserts the readable value is a specific generation, or any
	// ("*"). Compare against the finalized current under the same lock that admits the write — held
	// unbroken from here through the install below — so the value cannot move between the check and
	// the replace. An absent or non-finalized current — nothing, a live first write, a consume-once
	// mid-delivery — is not the readable value and matches nothing, so it is the same 412 as a stale
	// id: every state but a matching finalized current refuses. Evaluated before the live-incumbent
	// gate so a stale precondition is refused as itself, not masked as busy — busy invites a retry
	// a definitively superseded id can never satisfy, where 412 says re-read. Nothing is minted yet,
	// so a refusal wastes no id, no quota slot, no home — the early return mirrors the busy path just
	// below.
	if o.IfMatch != "" {
		ok := h.current != nil && h.current.state == genFinalized &&
			(o.IfMatch == "*" || h.current.id.String() == o.IfMatch)
		if !ok {
			h.mu.Unlock()
			s.reg.release(h)
			return nil, clip.ErrPreconditionFailed
		}
	}
	if h.live != nil {
		h.mu.Unlock()
		s.reg.release(h)
		return nil, clip.ErrBusy
	}
	// Reserve the count slot before minting an id or building a home, so the cheap check fails fast
	// and no work is wasted on a clip the store has no room for. A busy name was already rejected
	// above, so it never reserves. Both error paths below give the slot back; nothing else has been
	// allocated yet, so the count is all there is to release.
	if !s.quota.reserveClip() {
		h.mu.Unlock()
		s.reg.release(h)
		return nil, clip.ErrNoSpace
	}
	// One clock read for the whole creation: the id derives its monotonic prefix from this instant
	// and the generation records it as its creation time, so the time embedded in the id and the time
	// reported as CreatedAt cannot drift apart across the work create does.
	now := s.now()
	id, err := h.allocate(now)
	if err != nil {
		s.quota.releaseClip()
		h.mu.Unlock()
		s.reg.release(h)
		return nil, fmt.Errorf("create %s: %w", name, err)
	}
	buf, err := s.med.create(id)
	if err != nil {
		s.quota.releaseClip()
		h.mu.Unlock()
		s.reg.release(h)
		return nil, fmt.Errorf("create %s: %w", name, err)
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
	// Installing the first live generation makes a name readable that was not, so wake any reader
	// waiting on this handle. This wake and the Close finalize are the load-bearing pair the notifier
	// exists for — the only two transitions that can unblock a parked waiter onto a value; every other
	// site only clears or flips state no waiter is parked on.
	h.wakeLocked()
	h.mu.Unlock()
	return &writer{s: s, h: h, g: g}, nil
}

// Open attaches a reader to name's readable generation. It resolves the target, claims it if it
// is a consume-once clip, and opens its read handle — all under the handle lock — so the bytes are
// pinned before any concurrent supersede can reclaim the home, and a consume-once clip is claimed
// for exactly one reader before a single byte ships. A finished or just-claimed generation reads as
// a fixed section, a live one as a follower. The lease is held until the reader is closed, keeping
// the handle alive across the lock-free stream; for a consumed generation the close also destroys
// it.
//
// When GetOpts.Wait is set, a resolve that finds nothing readable does not return ErrNotFound: it
// parks on the handle notifier and re-resolves each time a generation transition wakes it, until
// the name becomes readable or ctx is canceled. The lease is acquired once and held across every
// wait turn, pinning the possibly-empty handle against eviction; a waiter's only guaranteed unblock
// is ctx-cancel, since no write to the name is ever promised — the liveness asymmetry the handle
// notifier is built around. Two hazards the wait path must respect are documented at the gate and
// the post-select check below.
func (s *store) Open(ctx context.Context, name string, o GetOpts) (io.ReadCloser, clip.Clip, error) {
	if err := clip.ValidName(name); err != nil {
		return nil, clip.Clip{}, err
	}
	// A consume-once Open claims its one delivery before shipping a byte, and the claim cannot be
	// taken back. Were the request already canceled, claiming would spend that delivery on a reader
	// that has gone away. Decline before acquiring anything. This narrows the window, not closes it —
	// a cancel after the claim still spends the delivery — but sparing an already-dead request costs
	// nothing.
	if err := ctx.Err(); err != nil {
		return nil, clip.Clip{}, err
	}
	h := s.reg.acquire(name)
	// Resolve-or-wait. The success arm below is reached only with a readable generation in hand and
	// always returns, so the loop turns solely on the wait path: a wake re-locks the same handle and
	// re-resolves. The lease taken just above is held across every turn and released exactly once, by
	// whichever terminal exit fires.
	for {
		h.mu.Lock()
		g, err := resolveRead(h)
		if err != nil {
			// Wait only while the clip is merely not-here-yet. ErrConsumed — a consume-once another reader
			// claimed, seen mid-delivery — is terminal for this request: waiting cannot bring it back,
			// and the claimant's later cleanup would wake us to ErrNotFound only to park forever. So it,
			// and any other error resolveRead might add, returns at once: the same "gone" a non-waiting
			// reader already gets. resolveRead returns only nil, ErrConsumed, or ErrNotFound, so gating on
			// ErrNotFound is the exact "still waitable" predicate.
			if o.Wait && errors.Is(err, clip.ErrNotFound) {
				notify := h.notify // captured under the SAME lock as the resolve — the no-lost-wakeup hinge
				h.mu.Unlock()
				select {
				case <-notify: // a transition landed — re-resolve
				case <-ctx.Done():
				}
				// One ctx check after the select covers a pure cancel and a wake that raced a cancel alike. A
				// select with both notify and ctx.Done ready picks at random; without this a request canceled
				// during a consume-once finalize would, half the time, re-resolve and claim the secret for a
				// client that has gone away. The check re-applies the pre-acquire guard's decline at the top of
				// each turn — the follower's honour-cancellation idiom one scale out — so the wait path is no
				// laxer than the immediate one.
				if err := ctx.Err(); err != nil {
					s.reg.release(h)
					return nil, clip.Clip{}, err
				}
				continue
			}
			h.mu.Unlock()
			s.reg.release(h)
			return nil, clip.Clip{}, err
		}

		// Claim a finalized consume-once generation before any byte is read. The flip to the consumed
		// state, made here under the handle lock, is the serialization point: a racing reader either
		// loses this lock and then resolves the consumed state to ErrConsumed with no bytes, or it
		// already lost the resolve. A claim can fail two ways, and they call for opposite responses,
		// which is why the medium reports whether the claim committed. If it never took (committed false
		// — the durable marker was not written), revert to finalized so the clip stays claimable for
		// the next reader. If it took but could not be made durable (committed true with an error — the
		// marker was written, the flush was not), the secret is forfeit: the clip can no longer resolve,
		// so destroy it in place rather than reverting to a claimable state it cannot honour. Either way
		// no byte ships, so at-most-once holds with zero delivery; the destroy releases under no lock, so
		// the unlock comes first.
		consumed := false
		if g.state == genFinalized && g.consume {
			g.state = genConsumed
			if committed, err := s.med.claim(g); err != nil {
				if committed {
					h.mu.Unlock()
					s.cleanupConsumed(h, g)
					s.reg.release(h)
					return nil, clip.Clip{}, fmt.Errorf("open %s: %w", name, err)
				}
				g.state = genFinalized
				h.mu.Unlock()
				s.reg.release(h)
				return nil, clip.Clip{}, fmt.Errorf("open %s: %w", name, err)
			}
			consumed = true
			// The durable claim stuck, flipping the generation finalized→consumed — the one read-state
			// change that is a state transition, not a pointer move. Wake here, after it sticks rather
			// than before, so a claim that reverted just above announces nothing it took back. No waiter
			// is parked on this change — the finalize already woke them, and the next reader resolves the
			// consumed outcome on its own — so the wake is spurious-safe, the price of the rule staying
			// total: every change to what a read resolves wakes, with no genState carve-out to remember.
			h.wakeLocked()
		}

		var rc io.ReadCloser
		if g.state == genLive {
			rc, err = g.buf.Reader(ctx, 0)
		} else {
			rc, err = g.buf.Section()
		}
		if err != nil {
			h.mu.Unlock()
			// A claim that succeeded but cannot open its reader has delivered the secret to no one; at-most-
			// once still holds, so destroy the consumed generation in place rather than un-claiming it and
			// risking a second delivery.
			if consumed {
				s.cleanupConsumed(h, g)
			}
			s.reg.release(h)
			return nil, clip.Clip{}, fmt.Errorf("open %s: %w", name, err)
		}
		c := g.clip()
		h.mu.Unlock()

		lr := &leasedReader{rc: rc, release: func() { s.reg.release(h) }}
		if consumed {
			lr.cleanup = func() { s.cleanupConsumed(h, g) }
		}
		return lr, c, nil
	}
}

// Stat snapshots name's readable generation, resolving it by the same rule Open applies but
// opening no bytes and claiming nothing, so a stat never consumes a consume-once clip. It runs no
// wait loop: an absent name resolves to ErrNotFound at once, never a block — which is what makes
// HEAD the immediate existence probe a default-wait GET is the standing opt-out of. The lease is
// released before returning; no stream outlives it.
func (s *store) Stat(ctx context.Context, name string) (clip.Clip, error) {
	if err := clip.ValidName(name); err != nil {
		return clip.Clip{}, err
	}
	h := s.reg.acquire(name)
	h.mu.Lock()
	g, err := resolveRead(h)
	var c clip.Clip
	if err == nil {
		c = g.clip()
	}
	h.mu.Unlock()
	s.reg.release(h)
	return c, err
}

// Delete removes name's finalized generation, or reports ErrNotFound when only a live generation
// (or nothing) exists — a live generation belongs to its writer. It detaches the generation under
// the handle lock, then reclaims its home off the lock; releasing the lease evicts the handle if
// nothing else remains. Racing a finalize, the handle lock serializes them into a deterministic
// last-writer-wins.
func (s *store) Delete(ctx context.Context, name string) error {
	if err := clip.ValidName(name); err != nil {
		return err
	}
	h := s.reg.acquire(name)
	h.mu.Lock()
	prev := h.current
	if prev == nil || prev.state != genFinalized {
		h.mu.Unlock()
		s.reg.release(h)
		return clip.ErrNotFound
	}
	h.current = nil
	h.wakeLocked()
	h.mu.Unlock()
	s.reclaim(prev)
	s.reg.release(h)
	return nil
}

// List snapshots every finalized clip. It copies the handle set under the registry lock, then locks
// each handle individually with the registry lock released — so a handle mid-create or mid-claim,
// holding its own lock across a disk fsync, stalls only this walk's reach of that one handle and
// never an operation on another name. It skips handles whose current generation is absent or not
// yet finalized. The order is unspecified; a presentation layer sorts.
func (s *store) List(ctx context.Context) ([]clip.Clip, error) {
	var out []clip.Clip
	for _, h := range s.reg.snapshot() {
		h.mu.Lock()
		if h.current != nil && h.current.state == genFinalized {
			out = append(out, h.current.clip())
		}
		h.mu.Unlock()
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
	h.mu.Lock()
	if h.current == g {
		h.current = nil
		h.wakeLocked()
	}
	h.mu.Unlock()
}

// leasedReader couples a read stream to the lease that keeps its handle alive, and for a consume-
// once read to the cleanup that destroys the claimed generation once delivery ends. It hands reads
// straight through and, on Close, releases the read handle first, then runs any cleanup, then
// releases the lease — dropping the pin on the bytes, reclaiming a consumed generation, and only
// then letting the handle be evicted. Close is safe to call more than once, as the unwind of an
// aborted request may; the cleanup and the lease release each run exactly once.
type leasedReader struct {
	rc      io.ReadCloser
	cleanup func() // consume-once only: destroy the claimed generation; nil for an ordinary read
	release func()
	once    sync.Once
}

// Read streams the clip's bytes.
func (l *leasedReader) Read(p []byte) (int, error) { return l.rc.Read(p) }

// Close releases the read handle, then runs any consume-once cleanup, then releases the lease — the
// latter two exactly once. The read handle is dropped first so a cleanup that reclaims the bytes
// finds no reader of its own still pinning them.
func (l *leasedReader) Close() error {
	err := l.rc.Close()
	l.once.Do(func() {
		if l.cleanup != nil {
			l.cleanup()
		}
		l.release()
	})
	return err
}
