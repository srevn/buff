package store

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/srevn/buff/clip"

	"github.com/srevn/buff/store/internal/buffer"
)

// These are the reaper's white-box tests. The reaper is pure store machinery — its only
// medium touch, remove, is already contract-proven — so it is exercised here with an injected
// clock and the unexported reapOnce, where the snapshot-then-recheck can be driven step by
// step. The two-method split (reapCandidates then reapRemove) is what lets the TOCTOU window
// be opened deterministically between two calls, with no sleeps.

// fixedClock returns a clock stuck at t, so a finalize stamps a known instant and the reap
// time can be chosen relative to it.
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

// finalize creates, writes, and finalizes a clip through the real store paths, returning the
// finalized view (its generation id is how the disk tests locate the clip on disk). It is the
// white-box analogue of the contract suite's mustPut, which lives in the external test package
// and so cannot be called from here. Callers that only need the side effect ignore the return.
func finalize(t *testing.T, s *store, name string, o PutOpts, data []byte) clip.Clip {
	t.Helper()
	w, err := s.Create(context.Background(), name, clip.Meta{Kind: clip.KindText}, o)
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

// TestReapRemovesExpired proves the sweep removes a finalized clip once it is past its expiry,
// not before, and that removal frees the quota and evicts the now-empty handle.
func TestReapRemovesExpired(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	s := newStore(memMedium{}, fixedClock(base), Config{})
	ctx := context.Background()

	finalize(t, s, "x", PutOpts{TTL: time.Hour}, []byte("payload"))

	s.reapOnce(base.Add(30 * time.Minute)) // before expiry: untouched
	if _, err := s.Stat(ctx, "x"); err != nil {
		t.Fatalf("clip reaped before its expiry: %v", err)
	}

	s.reapOnce(base.Add(2 * time.Hour)) // past expiry: reaped
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
// nothing else: a not-yet-expired clip, a kept clip with no expiry, a consumed generation
// owned by its reader, and a live generation are all skipped. The generations are placed
// directly so every lifecycle state can be presented at once, deterministically.
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
// captured, then superseded by a fresh generation before the removal step runs; the id
// mismatch makes the reaper leave the replacement alone, and the quota reflects only the
// survivor — the old generation was released once, by the supersede, never again by the reap.
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

// TestReapTOCTOUDelete proves a candidate deleted between snapshot and removal is simply
// skipped: the recheck finds no current generation, so nothing is removed or released a second
// time, and the handle the reaper re-acquired is evicted straight back.
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
// tick past a clip's expiry sweeps it, and a cancelled ctx returns the loop. It runs in a synctest
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

// TestRunReaperDisabled proves the non-positive-interval contract: RunReaper returns at once rather
// than panicking in NewTicker or blocking forever, the 0 = disabled an embedder gets from a
// configuration that names no reap interval. An already-expired clip stands in for "a sweep would
// have work": its survival shows the disabled loop ran none. The call is watched on a goroutine so a
// regression that blocks fails in two seconds rather than hanging out to the suite timeout.
func TestRunReaperDisabled(t *testing.T) {
	s := newStore(memMedium{}, time.Now, Config{})
	finalize(t, s, "x", PutOpts{TTL: time.Nanosecond}, []byte("payload")) // expired by the time we check

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

	if _, err := s.Stat(context.Background(), "x"); err != nil {
		t.Errorf("disabled reaper swept a clip: Stat = %v, want it untouched", err)
	}
}
