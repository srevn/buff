package store

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/srevn/buff/clip"

	"github.com/srevn/buff/store/internal/buffer"
)

// These are the reaper's white-box tests. The reaper is pure store machinery — its only medium
// touch, remove, is already contract-proven — so it is exercised here with an injected clock
// and the unexported reapExpired, where the snapshot-then-recheck can be driven step by step.
// The two-method split (reapCandidates then reapRemove) is what lets the TOCTOU window be opened
// deterministically between two calls, with no sleeps. The lazy-expiry, reclaim-under-pressure, and
// throttle tests below cover the rest of the retention machinery the reaper now shares.

// fixedClock returns a clock stuck at t, so a finalize stamps a known instant and the reap time can
// be chosen relative to it.
func fixedClock(t time.Time) func() time.Time { return func() time.Time { return t } }

// advancingClock returns a clock that steps forward by step on every read, so successive Creates
// mint strictly increasing generation ids regardless of wall-clock resolution — what makes which of
// two generations wins recovery's greatest-id contest deterministic rather than a clock race.
func advancingClock(base time.Time, step time.Duration) func() time.Time {
	var n int64
	return func() time.Time {
		t := base.Add(time.Duration(n) * step)
		n++
		return t
	}
}

// mutClock is a clock the test moves explicitly: a clip is finalized at one instant, then the clock
// is set forward so a later pressuring Create or Write sees that clip expired — the jump fixedClock
// cannot make and advancingClock cannot place precisely. The store reads it through now() while the
// test calls set(), so the instant is mutex-guarded; the pressure tests drive it from one goroutine,
// but the guard keeps it -race-clean for any future use and costs nothing.
type mutClock struct {
	mu sync.Mutex
	t  time.Time
}

func newMutClock(t time.Time) *mutClock { return &mutClock{t: t} }

func (c *mutClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *mutClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// finalize creates, writes, and finalizes a clip through the real store paths, returning the
// finalized view (its generation id is how the disk tests locate the clip on disk). It is the
// white-box analogue of the contract suite's mustPut, which lives in the external test package and
// so cannot be called from here. Callers that only need the side effect ignore the return.
func finalize(t *testing.T, s *store, name string, o PutOpts, data []byte) clip.Clip {
	t.Helper()
	w, err := s.Create(context.Background(), name, clip.Meta{Kind: clip.KindBytes}, o)
	if err != nil {
		t.Fatalf("Create %s: %v", name, err)
	}
	if len(data) > 0 {
		if _, err := w.Write(data); err != nil {
			t.Fatalf("Write %s: %v", name, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close %s: %v", name, err)
	}
	return w.Clip()
}

// TestReapRemovesExpired proves the sweep removes a finalized clip once it is past its expiry, not
// before, and that removal frees the quota and evicts the now-empty handle.
func TestReapRemovesExpired(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	s := newStore(memMedium{}, fixedClock(base), Config{})
	ctx := context.Background()

	finalize(t, s, "x", PutOpts{TTL: time.Hour}, []byte("payload"))

	s.reapExpired(base.Add(30 * time.Minute)) // before expiry: untouched
	if _, err := s.Stat(ctx, "x"); err != nil {
		t.Fatalf("clip reaped before its expiry: %v", err)
	}

	s.reapExpired(base.Add(2 * time.Hour)) // past expiry: reaped
	if _, _, err := s.Open(ctx, "x", GetOpts{}); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Open after reap = %v, want ErrNotFound", err)
	}
	if b, c := s.quota.bytes.Load(), s.quota.clips.Load(); b != 0 || c != 0 {
		t.Errorf("after reap, quota bytes/clips = %d/%d, want 0/0", b, c)
	}
	if n := handleCount(s.reg); n != 0 {
		t.Errorf("after reap, registry holds %d handles, want 0", n)
	}
}

// TestReapCandidatesFilter proves the snapshot picks exactly the expired finalized clips and
// nothing else: a not-yet-expired clip, a kept clip with no expiry, a consumed generation owned by
// its reader, and a live generation are all skipped. The generations are placed directly so every
// lifecycle state can be presented at once, deterministically.
func TestReapCandidatesFilter(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	past, future := base.Add(-time.Hour), base.Add(time.Hour)
	s := newStore(memMedium{}, fixedClock(base), Config{})

	put := func(name string, g *generation) {
		h := s.reg.acquire(name)
		h.mu.Lock()
		if g.state == genLive {
			h.live = g
		} else {
			h.current = g
		}
		h.mu.Unlock()
		s.reg.release(h) // a present generation pins the handle, so it survives the release
	}
	mk := func(name string, st genState, expires time.Time) *generation {
		return &generation{name: name, state: st, expires: expires, buf: buffer.NewMemory()}
	}
	put("expired", mk("expired", genFinalized, past))
	put("future", mk("future", genFinalized, future))
	put("kept", mk("kept", genFinalized, time.Time{}))
	put("consumed", mk("consumed", genConsumed, past))
	put("live", mk("live", genLive, time.Time{}))

	cands := s.reapCandidates(base)
	if len(cands) != 1 || cands[0].name != "expired" {
		t.Fatalf("reapCandidates = %+v, want exactly one candidate named \"expired\"", cands)
	}
}

// TestReapTOCTOUSupersede proves the recheck spares an in-flight replacement. A candidate is
// captured, then superseded by a fresh generation before the removal step runs; the id mismatch
// makes the reaper leave the replacement alone, and the quota reflects only the survivor — the old
// generation was released once, by the supersede, never again by the reap.
func TestReapTOCTOUSupersede(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	s := newStore(memMedium{}, fixedClock(base), Config{})
	ctx := context.Background()

	finalize(t, s, "x", PutOpts{TTL: time.Hour}, []byte("AAAA")) // A: 4 bytes, expires base+1h
	reapTime := base.Add(2 * time.Hour)

	cands := s.reapCandidates(reapTime) // captures A
	if len(cands) != 1 {
		t.Fatalf("want one candidate, got %d", len(cands))
	}
	finalize(t, s, "x", PutOpts{}, []byte("BBBBBB")) // B supersedes A, freeing A's quota

	for _, c := range cands {
		s.reapRemove(reapTime, c) // sees B, not A: id mismatch, spared
	}

	rc, _, err := s.Open(ctx, "x", GetOpts{})
	if err != nil {
		t.Fatalf("Open after TOCTOU reap = %v, want B readable", err)
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(data) != "BBBBBB" {
		t.Errorf("read %q, want BBBBBB (the replacement survived)", data)
	}
	if b, c := s.quota.bytes.Load(), s.quota.clips.Load(); b != 6 || c != 1 {
		t.Errorf("quota bytes/clips = %d/%d, want 6/1 (A released once, not double-reaped)", b, c)
	}
}

// TestReapTOCTOUDelete proves a candidate deleted between snapshot and removal is simply skipped:
// the recheck finds no current generation, so nothing is removed or released a second time, and the
// handle the reaper re-acquired is evicted straight back.
func TestReapTOCTOUDelete(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	s := newStore(memMedium{}, fixedClock(base), Config{})
	ctx := context.Background()

	finalize(t, s, "x", PutOpts{TTL: time.Hour}, []byte("AAAA"))
	reapTime := base.Add(2 * time.Hour)

	cands := s.reapCandidates(reapTime) // captures A
	if err := s.Delete(ctx, "x"); err != nil {
		t.Fatalf("Delete: %v", err) // A gone, quota freed once
	}
	for _, c := range cands {
		s.reapRemove(reapTime, c) // current is nil: skipped
	}

	if b, c := s.quota.bytes.Load(), s.quota.clips.Load(); b != 0 || c != 0 {
		t.Errorf("quota bytes/clips = %d/%d, want 0/0 (delete freed once, reap skipped)", b, c)
	}
	if n := handleCount(s.reg); n != 0 {
		t.Errorf("registry holds %d handles, want 0", n)
	}
}

// TestRunReaper proves the exported loop, distinct from the sweep logic the tests above pin: one
// tick past a clip's expiry sweeps it, and a canceled ctx returns the loop. It runs in a synctest
// bubble so the ticker is deterministic without real sleeps — the bubble's fake clock is the store
// clock, so it both stamps the clip's expiry and drives the tick. The reaper goroutine must observe
// the cancel and return before the bubble drains; were the loop to ignore ctx, synctest would fail
// the test on the stuck goroutine, so the clean drain is itself the no-leak proof.
func TestRunReaper(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStore(memMedium{}, time.Now, Config{}) // the bubble's fake clock is the store clock
		ctx, cancel := context.WithCancel(context.Background())

		finalize(t, s, "x", PutOpts{TTL: time.Hour}, []byte("payload")) // expires one hour from now

		go RunReaper(ctx, s, 2*time.Hour)
		synctest.Wait() // the loop is durably blocked in its select, no tick due yet

		time.Sleep(3 * time.Hour) // advance the bubble clock; the single two-hour tick fires within
		synctest.Wait()           // the loop has run that tick's sweep and re-blocked

		if _, err := s.Stat(context.Background(), "x"); !errors.Is(err, clip.ErrNotFound) {
			t.Fatalf("after a tick past expiry, Stat = %v, want ErrNotFound", err)
		}
		if b, c := s.quota.bytes.Load(), s.quota.clips.Load(); b != 0 || c != 0 {
			t.Errorf("after reap, quota bytes/clips = %d/%d, want 0/0", b, c)
		}

		cancel()
		synctest.Wait() // the loop observes ctx.Done and returns; the bubble drains with none left
	})
}

// TestRunReaperDisabled proves the non-positive-interval contract: RunReaper returns at once
// rather than panicking in NewTicker or blocking forever, the 0 = disabled an embedder gets from
// a configuration that names no reap interval. The call is watched on a goroutine so a regression
// that blocks fails in two seconds rather than hanging out to the suite timeout.
//
// What proves "the disabled loop ran no sweep" is the quota residue, not read-visibility. Lazy
// expiry hides an expired clip from Stat the instant it expires, with or without a reaper, so
// Stat-ability is no longer a proxy for physical presence. The clip is in fact untouched — still in
// memory, still holding its bytes and its count slot — and that held quota is exactly what shows no
// physical reclaim happened.
func TestRunReaperDisabled(t *testing.T) {
	s := newStore(memMedium{}, time.Now, Config{})
	finalize(t, s, "x", PutOpts{TTL: time.Nanosecond}, []byte("payload")) // expires at once: lazy-hidden, but physically present

	done := make(chan struct{})
	go func() {
		RunReaper(context.Background(), s, 0) // non-positive: must return immediately, not tick or panic
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunReaper with a non-positive interval did not return — it must no-op, not block")
	}

	if b, c := s.quota.bytes.Load(), s.quota.clips.Load(); b == 0 || c == 0 {
		t.Errorf("disabled reaper physically reclaimed a clip: quota bytes/clips = %d/%d, want both nonzero", b, c)
	}
}

// TestExpiredClipResolvesAbsent proves lazy read-time expiry: the instant a finalized clip passes
// its deadline it reads as absent at every resolution and admission seam — Stat, Open, Delete,
// List, and an IfMatch incumbent check — with no sweep run. A not-yet-expired clip and a kept
// (never-expiring) clip in the same store are untouched, so the gate is expiry-specific, not a
// blanket hide.
func TestExpiredClipResolvesAbsent(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	mc := newMutClock(base)
	s := newStore(memMedium{}, mc.now, Config{})
	ctx := context.Background()

	finalize(t, s, "expired", PutOpts{TTL: time.Hour}, []byte("gone"))    // expires base+1h
	finalize(t, s, "fresh", PutOpts{TTL: 24 * time.Hour}, []byte("here")) // expires base+24h
	finalize(t, s, "kept", PutOpts{Keep: true}, []byte("forever"))        // never expires

	mc.set(base.Add(2 * time.Hour)) // only "expired" is past its deadline now

	if _, err := s.Stat(ctx, "expired"); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Stat expired = %v, want ErrNotFound", err)
	}
	if _, _, err := s.Open(ctx, "expired", GetOpts{}); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Open expired = %v, want ErrNotFound", err)
	}
	if err := s.Delete(ctx, "expired"); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Delete expired = %v, want ErrNotFound", err)
	}
	// IfMatch "*" finds no incumbent over an expired clip, so the CAS precondition fails.
	if _, err := s.Create(ctx, "expired", clip.Meta{Kind: clip.KindBytes}, PutOpts{IfMatch: "*"}); !errors.Is(err, clip.ErrPreconditionFailed) {
		t.Errorf("Create IfMatch=* over expired = %v, want ErrPreconditionFailed", err)
	}
	// List omits the expired clip and keeps the other two.
	listed := map[string]bool{}
	all, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, c := range all {
		listed[c.Name] = true
	}
	if listed["expired"] || !listed["fresh"] || !listed["kept"] {
		t.Errorf("List names = %v, want fresh and kept without expired", listed)
	}
	// The not-yet-expired and kept clips resolve normally — the gate is expiry-specific.
	if _, err := s.Stat(ctx, "fresh"); err != nil {
		t.Errorf("Stat fresh = %v, want it readable", err)
	}
	if _, err := s.Stat(ctx, "kept"); err != nil {
		t.Errorf("Stat kept = %v, want it readable", err)
	}
}

// TestCreateReclaimsExpiredUnderCountCap proves the count-cap self-heal: a store at MaxClips with
// an expired clip still holding the slot rejects a new name — until the rejected Create sweeps
// it on demand and succeeds with no manual reap. This is the latent quota bug the reframe fixes,
// asserted as the fix: an expired clip must not wedge the count cap.
func TestCreateReclaimsExpiredUnderCountCap(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	mc := newMutClock(base)
	s := newStore(memMedium{}, mc.now, Config{MaxClips: 1})
	ctx := context.Background()

	finalize(t, s, "a", PutOpts{TTL: time.Hour}, []byte("aaaa")) // holds the one slot, expires base+1h

	// Before expiry the cap genuinely blocks a second name: nothing to reclaim.
	mc.set(base.Add(30 * time.Minute))
	if _, err := s.Create(ctx, "b", clip.Meta{Kind: clip.KindBytes}, PutOpts{}); !errors.Is(err, clip.ErrNoSpace) {
		t.Fatalf("Create at cap before expiry = %v, want ErrNoSpace", err)
	}

	// Past a's expiry the rejected Create reclaims it and succeeds — no manual sweep.
	mc.set(base.Add(2 * time.Hour))
	w, err := s.Create(ctx, "b", clip.Meta{Kind: clip.KindBytes}, PutOpts{})
	if err != nil {
		t.Fatalf("Create after expiry = %v, want success via reclaim-under-pressure", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close b: %v", err)
	}

	if _, err := s.Stat(ctx, "a"); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Stat a after reclaim = %v, want ErrNotFound", err)
	}
	if _, err := s.Stat(ctx, "b"); err != nil {
		t.Errorf("Stat b = %v, want it readable", err)
	}
	if c := s.quota.clips.Load(); c != 1 {
		t.Errorf("clips = %d, want 1 (a reclaimed, b present)", c)
	}
}

// TestWriteReclaimsExpiredUnderByteCap proves the byte-cap self-heal on the Write path: a store at
// MaxTotal with an expired clip holding the bytes rejects a fresh write's reservation, sweeps the
// expired clip on demand, and lands the write. It exercises Write's always-re-probe — the reserve
// is retried after the sweep, not gated on whether this caller's own sweep did the freeing.
func TestWriteReclaimsExpiredUnderByteCap(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	mc := newMutClock(base)
	s := newStore(memMedium{}, mc.now, Config{MaxTotal: 8})
	ctx := context.Background()

	finalize(t, s, "a", PutOpts{TTL: time.Hour}, []byte("AAAAAAAA")) // 8 bytes fill MaxTotal, expires base+1h

	mc.set(base.Add(2 * time.Hour)) // a now expired, still holding its 8 bytes

	w, err := s.Create(ctx, "b", clip.Meta{Kind: clip.KindBytes}, PutOpts{}) // count cap off, so admission is fine
	if err != nil {
		t.Fatalf("Create b: %v", err)
	}
	// The reserve for these bytes loses to MaxTotal (a still holds 8); the sweep frees a's bytes and
	// the re-probe lands the write.
	n, err := w.Write([]byte("BBBBBBBB"))
	if err != nil || n != 8 {
		t.Fatalf("Write after reclaim = (%d, %v), want (8, nil)", n, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close b: %v", err)
	}

	if _, err := s.Stat(ctx, "a"); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Stat a after reclaim = %v, want ErrNotFound", err)
	}
	if _, data := mustReadGen(t, s, "b"); string(data) != "BBBBBBBB" {
		t.Errorf("b content = %q, want BBBBBBBB", data)
	}
	if b, c := s.quota.bytes.Load(), s.quota.clips.Load(); b != 8 || c != 1 {
		t.Errorf("quota bytes/clips = %d/%d, want 8/1 (a reclaimed, b's footprint held)", b, c)
	}
}

// TestReclaimExpiredThrottle proves the pressure sweep is rate-limited: two reclaims within
// minReclaimInterval run at most one sweep, so a store full of live clips cannot turn every
// rejected write into an O(N) scan; advancing past the interval re-enables a sweep. Driven by
// calling reclaimExpired with explicit instants, so the boundary is exact with no goroutine timing.
func TestReclaimExpiredThrottle(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	s := newStore(memMedium{}, fixedClock(base), Config{})

	finalize(t, s, "a", PutOpts{TTL: time.Hour}, []byte("aaaa")) // finalized at base, expires base+1h
	t0 := base.Add(2 * time.Hour)
	if !s.reclaimExpired(t0) {
		t.Fatal("first reclaim past expiry should sweep a and report freed")
	}

	// Stage a second expired clip, then reclaim INSIDE the throttle window: it must decline without
	// sweeping b, even though b is reclaimable.
	finalize(t, s, "b", PutOpts{TTL: time.Hour}, []byte("bbbb")) // also expires base+1h, already past at t0
	if s.reclaimExpired(t0.Add(minReclaimInterval - time.Nanosecond)) {
		t.Fatal("a reclaim inside the throttle window must not sweep")
	}
	if c := s.quota.clips.Load(); c != 1 {
		t.Fatalf("b reclaimed inside the throttle window: clips = %d, want 1 (b still held)", c)
	}

	// At the interval boundary the sweep is re-enabled and b is reclaimed.
	if !s.reclaimExpired(t0.Add(minReclaimInterval)) {
		t.Fatal("past the throttle window a reclaim must sweep again")
	}
	if c := s.quota.clips.Load(); c != 0 {
		t.Fatalf("b not reclaimed past the throttle window: clips = %d, want 0", c)
	}
}
