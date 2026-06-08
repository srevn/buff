package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

// These prove the gate's notifier hinge at the scale the buffer proves its own — but on the REAL
// await the production Open runs, not a hand-written parallel model of the resolve-or-wait loop.
// Once await IS the production primitive, the isolated mechanic is await itself, so a parallel
// model would only be a second thing to keep in step; proving await directly is what makes the gate
// first-class.
//
// await is driven here by a fake predicate — a foreign phase counter the gate knows nothing of,
// the structural twin of the buffer follower's fake backing — so these pin the notifier mechanic
// alone, with none of the lease or consume-once machinery the real Open layers on. That machinery
// is proven in wait_test.go / follow_test.go (the policy composed with the gate) and the contract
// suite (both backings); these and those are complementary, since a regression in either is caught
// here too once the integration paths route through await internally.
//
// They mirror the follower proofs in buffer_test.go — wake on change, no lost wakeup across a
// re- block, cancel with no leak, one wake fanning out to many waiters — because the gate IS that
// follower notifier one scale out. Two more pin what the extraction newly introduces: a negative
// control that the capture-under-one-lock hinge is load-bearing, and the at-most-once seam of
// await's unbroken hold.

var errNotReady = errors.New("fake: not ready yet")

// fakeState is the gate's isolation harness: a foreign phase counter guarded by the gate's own
// lock, advanced through transition and resolved through await — production-shaped use of the real
// coordinator with no generation, claim, or lease in sight. ready is the phase at which resolve
// starts to succeed; it is set once at construction and then read-only, so reading it inside
// resolve needs no extra guard.
type fakeState struct {
	gate
	phase int // guarded by gate.mu; advanced by transition, read by resolve
	ready int // resolve succeeds once phase reaches this; write-once at construction
}

func newFakeState(ready int) *fakeState {
	return &fakeState{gate: newGate(), ready: ready}
}

// wait parks until phase reaches ready, then returns the phase it resolved — await with a fake
// predicate. commit makes no change and reports no wake: a waiter resolving is not itself a
// transition, exactly as a plain finalized read opens, snapshots, and moves nothing.
func (fs *fakeState) wait(ctx context.Context) (int, error) {
	return await(&fs.gate, ctx,
		func() (int, error) {
			if fs.phase >= fs.ready {
				return fs.phase, nil
			}
			return 0, errNotReady
		},
		func(err error) bool { return errors.Is(err, errNotReady) },
		func(p int) (int, bool, error) { return p, false, nil },
	)
}

// advance bumps the phase by one through a transition, waking every parked waiter to re-resolve.
func (fs *fakeState) advance() {
	fs.transition(func() bool {
		fs.phase++
		return true
	})
}

// TestGateWakesOnAdvance is the base property: a waiter parked because the state is not yet ready
// wakes when a transition advances it and resolves to the new value — the install wake observed
// from the waiting side, with the install standing in for any readable change.
func TestGateWakesOnAdvance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newFakeState(1)

		got := make(chan int, 1)
		var wg sync.WaitGroup
		wg.Go(func() {
			p, err := fs.wait(context.Background())
			if err != nil {
				t.Errorf("wait: %v", err)
			}
			got <- p
		})

		synctest.Wait() // durably parked at errNotReady, notify captured under the resolve lock
		fs.advance()    // phase 0→1: resolve now succeeds

		wg.Wait()
		if p := <-got; p != 1 {
			t.Errorf("waiter resolved at phase %d, want 1", p)
		}
	})
}

// TestGateNoLostWakeup is the capture-under-lock hinge across a re-block — the property that would
// rot silently if notify were captured anywhere but under the resolve lock. The first advance
// leaves the state still below ready, so the waiter re-resolves errNotReady and re-blocks on a
// FRESH notify; the second advance's wake lands on that re-captured channel. A hinge that re-
// armed outside the resolve lock would drop it. It is the gate twin of the buffer's append→re-
// block→finish.
func TestGateNoLostWakeup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newFakeState(2) // two advances: the first re-parks the waiter, the second resolves it

		got := make(chan int, 1)
		var wg sync.WaitGroup
		wg.Go(func() {
			p, err := fs.wait(context.Background())
			if err != nil {
				t.Errorf("wait: %v", err)
			}
			got <- p
		})

		synctest.Wait() // parked at phase 0, notify #1 captured
		fs.advance()    // phase 0→1: still below ready, so the waiter re-resolves and re-blocks

		synctest.Wait() // woken, re-resolved errNotReady, re-blocked on the freshly armed notify #2
		fs.advance()    // phase 1→2: this wake must land on notify #2, the one captured after the first

		wg.Wait()
		if p := <-got; p != 2 {
			t.Errorf("waiter resolved at phase %d, want 2", p)
		}
	})
}

// TestGateCancelMidWaitNoLeak is the liveness proof the foundation must carry. A gate waiter on
// a state that never advances has no guaranteed wake — unlike a buffer follower, whose writer
// is promised to terminate — so ctx-cancel is its only unblock. A parked waiter whose context
// is canceled must return ctx.Err() and exit; wg.Wait is the leak detector. The resource story a
// waiting reader rests on — it holds its caller only until the context is canceled — rests entirely
// on this.
func TestGateCancelMidWaitNoLeak(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newFakeState(1) // never advanced: the only unblock is ctx-cancel
		ctx, cancel := context.WithCancel(context.Background())

		errc := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Go(func() {
			_, err := fs.wait(ctx)
			errc <- err
		})

		synctest.Wait() // durably parked on a phase that will never advance
		select {
		case err := <-errc:
			t.Fatalf("waiter returned before cancel: %v", err)
		default:
		}

		cancel()
		wg.Wait() // returns only once the waiter observed the cancel and its goroutine exited
		if err := <-errc; !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want context.Canceled", err)
		}
	})
}

// TestGateManyWaitersOneAdvance pins the broadcast: N waiters on one un-ready state all wake from
// a single transition, because wakeLocked closes the shared channel rather than sending — a send
// would wake exactly one and strand the rest. It is the gate-scale fan-out for the wake itself.
func TestGateManyWaitersOneAdvance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newFakeState(1)

		const N = 8
		got := make([]int, N)
		errs := make([]error, N)
		var wg sync.WaitGroup
		for i := range N {
			wg.Go(func() {
				got[i], errs[i] = fs.wait(context.Background())
			})
		}

		synctest.Wait() // all N durably parked on the one shared notify
		fs.advance()    // a single close-and-replace must wake them all — a send would hang the rest

		wg.Wait()
		for i := range N {
			if errs[i] != nil {
				t.Errorf("waiter %d: %v", i, errs[i])
			}
			if got[i] != 1 {
				t.Errorf("waiter %d resolved at phase %d, want 1", i, got[i])
			}
		}
	})
}

// brokenWait is the split-lock anti-shape: it captures notify in a SEPARATE lock acquisition from
// the resolve, so a wake landing in the gap between the two is lost. It is the deliberate inverse
// of await's capture-under-one-lock hinge, kept ONLY to give that hinge teeth — its hang is the
// assertion. DO NOT "clean it up" by folding the two critical sections into one or routing it
// through await: the split, and the resulting lost wakeup, are the whole point. inGap runs in the
// waiter's own goroutine between the two sections, which is what makes the lost wake deterministic
// under synctest rather than a race.
func brokenWait(ctx context.Context, g *gate, ready func() bool, inGap func()) error {
	for {
		g.mu.Lock()
		r := ready()
		g.mu.Unlock() // BUG: the resolve's lock is dropped here, before notify is captured
		if r {
			return nil
		}
		inGap() // a wake forced in this gap closes a channel this turn will never select on
		g.mu.Lock()
		notify := g.notify // captured under a SECOND acquisition — the wake above has already passed
		g.mu.Unlock()
		select {
		case <-notify:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// TestGateSplitLockLosesWake is the negative control that gives the capture-under-one-lock hinge
// teeth. brokenWait resolves and captures notify in two separate critical sections; the wake fired
// in the gap (fs.advance, which also makes the state ready) closes the channel the waiter has
// not yet captured, so the waiter parks on the freshly armed successor and never re-resolves — it
// hangs, freed only by ctx-cancel. await catches the very wake brokenWait drops, so this hang is
// exactly what the migrated proofs above assert the absence of. Green-on-await plus hang-on-split
// is what proves the proofs can fail.
func TestGateSplitLockLosesWake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newFakeState(1)
		ctx, cancel := context.WithCancel(context.Background())

		errc := make(chan error, 1)
		go func() {
			errc <- brokenWait(ctx, &fs.gate, func() bool { return fs.phase >= fs.ready }, fs.advance)
		}()

		synctest.Wait() // the split-lock waiter parked on the post-wake notify, which never recloses
		select {
		case err := <-errc:
			t.Fatalf("split-lock waiter returned %v; the lost wake must leave it parked", err)
		default:
		}

		cancel() // the only thing that can free a lost-wakeup park
		if err := <-errc; !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want context.Canceled", err)
		}
	})
}

// claimState is the at-most-once harness: a single claimable token behind the gate. tryClaim
// resolves trivially — the token is always present — and commits the claim under the gate lock,
// the flip false→true being the serialization point, the mechanism-level twin of the consume-once
// finalized→consumed flip. Among N racing tryClaims, exactly one may win.
type claimState struct {
	gate
	claimed bool // guarded by gate.mu
}

func (cs *claimState) tryClaim(ctx context.Context) (bool, error) {
	return await(&cs.gate, ctx,
		func() (struct{}, error) { return struct{}{}, nil }, // resolve: the token is always present
		func(error) bool { return false },                   // waitable: resolve never errors, so never park
		func(struct{}) (bool, bool, error) {
			if cs.claimed {
				return false, false, nil // lost: nothing moved, no wake
			}
			cs.claimed = true
			return true, true, nil // won: a real claim flip, wake (spurious-safe, as the production claim is)
		},
	)
}

// TestGateClaimAtMostOnce pins the exact seam the refactor introduces: a commit run under await's
// unbroken hold is the serialization point, so among N readers racing one claimable token exactly
// one wins. It is the mechanism-level twin of the contract suite's policy-level consume-once proof;
// run under -race (and -count) it catches a hold that broke between resolve and commit. Pure await
// — no medium, no lease — so it isolates the seam from the policy that wraps it.
func TestGateClaimAtMostOnce(t *testing.T) {
	const N = 64
	cs := &claimState{gate: newGate()}

	var winners atomic.Int64
	var wg sync.WaitGroup
	for range N {
		wg.Go(func() {
			won, err := cs.tryClaim(context.Background())
			if err != nil {
				t.Errorf("tryClaim: %v", err)
			}
			if won {
				winners.Add(1)
			}
		})
	}
	wg.Wait()

	if got := winners.Load(); got != 1 {
		t.Errorf("claims won = %d, want exactly 1", got)
	}
}
