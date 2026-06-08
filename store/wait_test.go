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
)

// These drive the real Open(GetOpts{Wait:true}) over a memory store inside a synctest bubble, where
// "is the waiter still parked or has it returned?" is a decidable question — synctest.Wait blocks
// until every goroutine in the bubble is durably blocked, so a non-blocking receive afterwards
// tells "parked" from "returned" with no timing guess. They prove the delta the real loop adds
// over the isolated gate proofs in gate_test.go: that a waiter wakes onto a live write and follows
// it, that it waits through an invisible consume-once upload and claims it once on finalize, that
// a consumed loser is refused rather than hung, and that a canceled waiter exits and lets its
// empty handle evict. They deliberately do not re-prove the no-lost-wakeup hinge or the fan-out in
// isolation — the gate proofs do that on the real await, without the lease/claim machinery layered
// on.
//
// The memory medium is used because synctest cannot durably block a real disk syscall; the cross-
// medium wait proof lives in the contract suite (testRendezvousWait), which runs both backings with
// real goroutines and a time ceiling. Each store is built inside the bubble so every operation runs
// on the bubble's fake clock and is detected by synctest.Wait.

var waitMeta = clip.Meta{Kind: clip.KindBytes}

// readResult carries a waiting reader's outcome back to the test goroutine: the bytes it drained,
// the error if any, and whether the clip was still live at the moment it attached — the last
// distinguishes a follow of a live write from a section of an already-finalized one.
type readResult struct {
	data []byte
	live bool
	err  error
}

// drainOpen runs an Open with opts o in a bubble goroutine and reports the outcome on a buffered
// channel, so a test can both assert the result and decide parked-vs-returned with a non-
// blocking receive. o is the whole read policy — GetOpts{Wait:true} for a plain rendezvous,
// GetOpts{FollowNext: true} for a follow-next park — so one helper serves every waiting-read proof.
func drainOpen(s *store, name string, o GetOpts) (<-chan readResult, *sync.WaitGroup) {
	out := make(chan readResult, 1)
	wg := &sync.WaitGroup{}
	wg.Go(func() {
		rc, c, err := s.Open(context.Background(), name, o)
		if err != nil {
			out <- readResult{err: err}
			return
		}
		data, err := io.ReadAll(rc)
		if cerr := rc.Close(); cerr != nil && err == nil {
			err = cerr
		}
		out <- readResult{data: data, live: !c.Finalized, err: err}
	})
	return out, wg
}

// TestOpenWaitWakesIntoLiveFollow is the headline: rendezvous-wait is the zero-byte prefix of a
// follow. A waiter parked on an empty name wakes when the first write installs its live generation,
// attaches to that generation at offset zero, and follows it across two writes to a clean EOF
// on Close. The synctest.Wait between Create and the first Write is load-bearing — it forces
// the waiter to attach while the generation is still live (asserted by result.live), proving it
// followed the write rather than racing ahead to section a finished clip, the distinction the real-
// goroutine contract test cannot pin deterministically.
func TestOpenWaitWakesIntoLiveFollow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStore(memMedium{}, time.Now, Config{})
		out, wg := drainOpen(s, "stream", GetOpts{Wait: true})

		synctest.Wait() // the waiter is durably parked at ErrNotFound, notify captured under the resolve lock

		w, err := s.Create(context.Background(), "stream", waitMeta, PutOpts{})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		synctest.Wait() // woken by the install, the waiter attached the live follower and now blocks on the byte log

		if _, err := w.Write([]byte("chunk-1;")); err != nil {
			t.Fatalf("Write 1: %v", err)
		}
		if _, err := w.Write([]byte("chunk-2")); err != nil {
			t.Fatalf("Write 2: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		wg.Wait()
		r := <-out
		if r.err != nil {
			t.Fatalf("waiting Open: %v", r.err)
		}
		if !r.live {
			t.Error("waiter attached to a finalized clip, want a live follow")
		}
		if string(r.data) != "chunk-1;chunk-2" {
			t.Errorf("waiter drained %q, want chunk-1;chunk-2", r.data)
		}
	})
}

// TestOpenWaitConsumeOnceRendezvous proves the wait survives an invisible upload and claims exactly
// once on finalize. A consume-once generation is unreadable while live, so the waiter woken by
// its install re-resolves ErrNotFound and re-blocks on a freshly armed notify — the no-lost-wakeup
// hinge at the real-Open scale, confirmed by the second synctest.Wait plus the still-empty channel.
// Only Close makes the clip claimable; the waiter then wins the claim under the handle lock and
// reads the secret, the one delivery a waiting consumer was built to receive.
func TestOpenWaitConsumeOnceRendezvous(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStore(memMedium{}, time.Now, Config{})
		out, wg := drainOpen(s, "secret", GetOpts{Wait: true})

		synctest.Wait() // parked at ErrNotFound, notify #1 captured

		w, err := s.Create(context.Background(), "secret", waitMeta, PutOpts{ConsumeOnce: true})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if _, err := w.Write([]byte("top-secret")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		synctest.Wait() // the live consume-once is invisible: the waiter re-resolved ErrNotFound and re-blocked on notify #2
		select {
		case r := <-out:
			t.Fatalf("waiter returned before finalize: %+v", r)
		default:
		}

		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		wg.Wait()
		r := <-out
		if r.err != nil {
			t.Fatalf("waiting Open: %v", r.err)
		}
		if string(r.data) != "top-secret" {
			t.Errorf("waiter read %q, want top-secret", r.data)
		}
	})
}

// TestOpenWaitConsumedLoserDoesNotHang is the crux of the wait gate: a wait must not swallow the
// consume-once "gone" signal. With one finalized consume-once clip claimed by a held, undrained
// reader — the genConsumed mid-delivery window — a second waiting Open resolves ErrConsumed, which
// the gate does not wait on, so it returns at once rather than parking. Were the gate to wait on
// any error, the loser would park and the claimant's later cleanup would wake it to ErrNotFound
// to wait forever; synctest makes "did it park?" decidable, so that regression fails here instead
// of hanging. The trailing cancel drains a regressed loser's parked goroutine so the bubble still
// exits cleanly.
func TestOpenWaitConsumedLoserDoesNotHang(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStore(memMedium{}, time.Now, Config{})

		w, err := s.Create(context.Background(), "secret", waitMeta, PutOpts{ConsumeOnce: true})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if _, err := w.Write([]byte("payload")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		winner, _, err := s.Open(context.Background(), "secret", GetOpts{Wait: true})
		if err != nil {
			t.Fatalf("winning Open (the claim): %v", err)
		}
		defer winner.Close() // hold the claim across the test; its cleanup runs at teardown

		ctx, cancel := context.WithCancel(context.Background())
		got := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Go(func() {
			_, _, err := s.Open(ctx, "secret", GetOpts{Wait: true})
			got <- err
		})

		synctest.Wait() // the loser has run to a terminal or parked — decide which
		select {
		case err := <-got:
			if !errors.Is(err, clip.ErrConsumed) {
				t.Errorf("consumed loser returned %v, want ErrConsumed", err)
			}
		default:
			t.Error("consumed loser parked instead of returning ErrConsumed — the gate waited on a terminal error")
		}
		cancel()
		wg.Wait()
	})
}

// TestOpenWaitCancelEvictsHandle is the liveness-and-eviction proof the resource story rests on.
// A waiter on a name that never appears has no guaranteed wake, so ctx-cancel is its only unblock:
// a canceled waiter must return context.Canceled and exit (wg.Wait is the leak detector). While
// parked it pins its handle against eviction (hasHandle), and once it leaves, the now-empty handle
// must evict so the registry stays bounded by live names plus waiting reads, never by history — the
// white-box assertion the bound depends on.
func TestOpenWaitCancelEvictsHandle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStore(memMedium{}, time.Now, Config{})
		ctx, cancel := context.WithCancel(context.Background())
		got := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Go(func() {
			_, _, err := s.Open(ctx, "never", GetOpts{Wait: true})
			got <- err
		})

		synctest.Wait() // durably blocked on a name that will never appear
		if !hasHandle(s.reg, "never") {
			t.Fatal("a parked waiter did not pin its handle")
		}
		select {
		case err := <-got:
			t.Fatalf("waiter returned before cancel: %v", err)
		default:
		}

		cancel()
		wg.Wait()
		if err := <-got; !errors.Is(err, context.Canceled) {
			t.Errorf("canceled waiter returned %v, want context.Canceled", err)
		}
		if n := handleCount(s.reg); n != 0 {
			t.Errorf("canceled waiter left %d handles, want 0 (the empty handle must evict)", n)
		}
	})
}

// TestOpenWaitMaxExpiresToNotFound proves the park's one buff-owned bound. A waiting Open with a
// positive WaitMax on a name nothing writes parks, and when its deadline elapses on the bubble's
// fake clock it returns clip.ErrNotFound — the same not-found a non-waiting GET of an absent name
// gives, WaitMax adding no new outcome, only a bound on how long before that outcome is reported —
// then lets its now-empty handle evict exactly as the canceled waiter above does. The parked-then-
// elapsed shape is what tells the bound apart from a bug that fast-returns: synctest.Wait settles
// the bubble with the waiter still parked and the channel empty WITHOUT moving the clock (proven
// separately: Wait is a barrier, not a clock-advance), then wg.Wait blocks the test goroutine so
// the bubble auto-advances to the deadline and fires the park's timer.
func TestOpenWaitMaxExpiresToNotFound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStore(memMedium{}, time.Now, Config{})
		out, wg := drainOpen(s, "never", GetOpts{Wait: true, WaitMax: time.Minute})

		synctest.Wait() // durably parked at ErrNotFound; the deadline has not fired (Wait does not move the clock)
		select {
		case r := <-out:
			t.Fatalf("waiter returned before its WaitMax elapsed: %+v", r)
		default:
		}

		wg.Wait() // blocking the test goroutine lets the bubble advance to the deadline; the park's timer fires
		r := <-out
		if !errors.Is(r.err, clip.ErrNotFound) {
			t.Errorf("elapsed wait returned %v, want clip.ErrNotFound", r.err)
		}
		if n := handleCount(s.reg); n != 0 {
			t.Errorf("elapsed waiter left %d handles, want 0 (the empty handle must evict)", n)
		}
	})
}

// TestOpenWaitMaxDoesNotTearFollow is the reader-capture proof: WaitMax bounds the park, never the
// stream it resolves into. It is TestOpenWaitWakesIntoLiveFollow with a WaitMax set far above the
// work the test does, so the waiter wakes into the live write by notify long before the deadline
// could fire — and drains the follower to a clean EOF. That clean drain IS the proof the reader
// kept the ORIGINAL request context: were it born from waitCtx instead, Open's defer cancel() —
// which fires the instant Open returns, right after the follower attaches and well before its bytes
// are read — would tear the follower, surfacing a cancellation error rather than the full bytes
// and EOF.
func TestOpenWaitMaxDoesNotTearFollow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStore(memMedium{}, time.Now, Config{})
		out, wg := drainOpen(s, "stream", GetOpts{Wait: true, WaitMax: time.Minute})

		synctest.Wait() // the waiter is durably parked at ErrNotFound

		w, err := s.Create(context.Background(), "stream", waitMeta, PutOpts{})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		synctest.Wait() // woken by the install, the waiter attached the live follower; Open's defer cancel() has fired

		if _, err := w.Write([]byte("chunk-1;")); err != nil {
			t.Fatalf("Write 1: %v", err)
		}
		if _, err := w.Write([]byte("chunk-2")); err != nil {
			t.Fatalf("Write 2: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		wg.Wait()
		r := <-out
		if r.err != nil {
			t.Fatalf("bounded waiter follow: %v — a follower torn at the WaitMax mark would surface here", r.err)
		}
		if !r.live {
			t.Error("waiter attached to a finalized clip, want a live follow")
		}
		if string(r.data) != "chunk-1;chunk-2" {
			t.Errorf("waiter drained %q, want chunk-1;chunk-2", r.data)
		}
	})
}
