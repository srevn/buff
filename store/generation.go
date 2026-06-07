package store

import (
	"time"

	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store/internal/buffer"
)

// genState is a generation's place in its lifecycle, and the single source of truth for it: the
// store never keeps a parallel boolean for "is this finalized" or "is this consumed", so the three
// states cannot disagree with each other.
type genState uint8

const (
	genLive      genState = iota // being written; reachable as a handle's live generation
	genFinalized                 // durably committed; reachable as a handle's current generation
	genConsumed                  // a claimed consume-once generation, mid-delivery before removal
)

// String names the state for logs and diagnostics. All three states are reachable — store.Open
// flips a claimed consume-once generation to genConsumed — so the set and its rendering are kept
// complete and named in one place.
func (s genState) String() string {
	switch s {
	case genLive:
		return "live"
	case genFinalized:
		return "finalized"
	case genConsumed:
		return "consumed"
	default:
		return "unknown"
	}
}

// generation is one complete write of bytes and metadata to a name: the unit that identity,
// replacement, retention, and deletion all key on. A new write makes a new generation that replaces
// the prior one; the two never share a byte log.
//
// There is deliberately no size field. The byte log is the one authority on size — growing while
// the generation is live, fixed once it is finished — so a separate count could only drift from it.
// The descriptive fields (id, name, meta, created, ttl, consume) are set once at creation and never
// change; finalized and the absolute expires are filled at Close, before the publish flip; state
// moves under the handle lock. That split is why clip can be read concurrently without a race.
type generation struct {
	id        genID
	name      string
	meta      clip.Meta
	created   time.Time
	ttl       time.Duration // pre-finalize only: resolved at Create, consumed at Close to set expires, then never read (a finalized or recovered generation's authority is expires); zero means never expire
	finalized time.Time     // filled at Close; meaningful only once the generation is not live
	expires   time.Time     // absolute deadline finalized+ttl, filled at Close; zero means never
	consume   bool          // created consume-once: deliver to one reader, claimed at Open
	state     genState
	buf       *buffer.Buffer
}

// clip snapshots the generation as the runtime view callers see. The caller holds handle.mu, so
// state, the pointers, and the descriptive fields are read consistently; the byte log takes its own
// brief lock to report size.
//
// FinalizedAt and ExpiresAt are read only once the generation is no longer live, for two reasons
// that point the same way: a live clip genuinely has neither yet, and the writer fills finalized
// just before the publish flip without holding the handle lock, so reading it while the generation
// is still live would race that fill. Gating the read on state keeps the snapshot both truthful and
// race-free.
func (g *generation) clip() clip.Clip {
	c := clip.Clip{
		Name:        g.name,
		Generation:  g.id.String(),
		Meta:        g.meta,
		Size:        g.buf.Size(),
		CreatedAt:   g.created,
		ConsumeOnce: g.consume,
		Finalized:   g.state != genLive,
	}
	if g.state != genLive {
		c.FinalizedAt = g.finalized
		c.ExpiresAt = g.expires
	}
	return c
}

// resolveRead picks which generation a read sees, by one rule with no caller-facing flags, with the
// caller holding handle.mu. In order: the finalized current if there is one; else a first-ever live
// generation to follow, unless it is consume-once; else, if the current generation has been claimed
// but not yet cleaned up, the already-consumed outcome; else nothing. A replacement being written
// is invisible while a finalized value still stands, so readers always see the latest complete
// value and never a torn one.
//
// consume-once shapes two of those arms. A live consume-once generation is not followable — two
// readers could each attach and both receive the secret — so it is skipped by the live arm and
// falls through to not-found, staying invisible until it finalizes (which also avoids confirming
// a secret exists mid-upload). Once claimed, the current generation is consumed rather than
// finalized; until its reader finishes and clears it, a second reader is told it is already gone.
// resolveRead only reports these outcomes; the claim itself happens in Open.
func resolveRead(h *clipHandle) (*generation, error) {
	if h.current != nil && h.current.state == genFinalized {
		return h.current, nil
	}
	if h.live != nil && !h.live.consume {
		return h.live, nil
	}
	if h.current != nil && h.current.state == genConsumed {
		return nil, clip.ErrConsumed
	}
	return nil, clip.ErrNotFound
}

// followResolve picks the generation a follow-next read attaches to: the next write to the name
// past the baseline the reader captured at entry. It is the future-facing twin of resolveRead,
// differing only where skipping the current value demands. Its finalized arm is gated on the
// current sorting strictly after the baseline — genID.after, the ordering question "is the current
// newer?", not an id inequality that asks "is it a different id?" and coincides with newer only
// because a name's counter never regresses. Asking the ordering directly keeps baseline a true
// cursor: a later resumable follow-after holding an older id would reuse this arm unchanged. And
// there is no ErrConsumed arm — follow-next never reports "you missed one": a consumed current
// another reader claimed mid-delivery is not a newer write, so it falls through to wait for the
// next rather than reporting the gone the rendezvous reader gets.
//
// The live arm is resolveRead's verbatim and needs no baseline check: h.live is only ever a freshly
// minted write, always newer than any finalized current, so it can never be the captured baseline.
//
// Arm order is finalized-first, and it is a deliberate choice — not the mechanism that exposes
// the replacement, which is the baseline filter alone. The order decides only the rare race where
// a newer-finalized AND a live generation both exist at one wake (the baseline already replaced
// twice): finalized-first then returns the immediately-next, complete generation, which sections
// without tearing, over the freshest live one that could still abort and strand both. In the
// ordinary flow — a settled value, then one next write arriving live — the baseline filters the
// current out of arm 1 and arm 2 returns that write to follow, so a live next-write is still
// followed.
//
// The arms read asymmetrically by design: a finalized current is skipped, a live current is
// followed — skip a settled value, join a stream already in progress. A finalized consume-once
// past the baseline is returned like any clip and claimed once by Open's shared claim block; a live
// consume-once is unfollowable, since two followers would each get the secret, so it is skipped and
// waited past exactly as resolveRead skips it.
func followResolve(h *clipHandle, baseline genID) (*generation, error) {
	if h.current != nil && h.current.state == genFinalized && h.current.id.after(baseline) {
		return h.current, nil
	}
	if h.live != nil && !h.live.consume {
		return h.live, nil
	}
	return nil, clip.ErrNotFound
}
