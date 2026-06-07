package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
)

// These prove the handle lifecycle notifier at the scale the buffer proves its own: a deterministic
// synctest model of the wait the read features will build, run against the real clipHandle.notify
// / wakeLocked. Nothing in the store waits on the notifier yet, so this is the only place its
// mechanic is exercised — the existing contract suite proves the wakes break nothing (they wake
// nobody), and these prove the wake itself is sound, so the first reader to wait wires a proven
// primitive.
//
// They mirror the follower proofs in buffer_test.go — wake on change, no lost wakeup across a re-
// block, cancel with no leak, one wake fanning out to many waiters — because the handle notifier
// is that follower notifier one scale out. Each test mints its handle through acquire, the real
// production path, so a regression that forgot to arm notify would panic close(nil) here rather
// than in production.

// modelWait is the resolve-or-wait loop a waiting reader will run, in miniature: resolve under
// the lock, and finding nothing readable, capture notify under that same lock and wait off it. It
// returns when a generation becomes readable or ctx is canceled. It carries none of the lease or
// consume-claim machinery a real Open adds — this isolates the notifier hinge, the part that must
// be sound before any of that is layered on.
func modelWait(ctx context.Context, h *clipHandle) (*generation, error) {
	for {
		h.mu.Lock()
		g, err := resolveRead(h)
		if err == nil {
			h.mu.Unlock()
			return g, nil
		}
		notify := h.notify // captured under the SAME lock as resolveRead — the no-lost-wakeup hinge
		h.mu.Unlock()
		select {
		case <-notify:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// TestHandleWaiterWakesOnInstall is the base property: a waiter parked on an empty handle wakes
// when a live generation is installed and resolves to it — the Create-install wake observed from
// the waiting side.
func TestHandleWaiterWakesOnInstall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newRegistry().acquire("x")
		gen := &generation{name: "x", state: genLive}

		got := make(chan *generation, 1)
		var wg sync.WaitGroup
		wg.Go(func() {
			g, err := modelWait(context.Background(), h)
			if err != nil {
				t.Errorf("modelWait: %v", err)
			}
			got <- g
		})

		synctest.Wait() // durably blocked, having captured notify at ErrNotFound
		h.mu.Lock()
		h.live = gen
		h.wakeLocked()
		h.mu.Unlock()

		wg.Wait()
		if g := <-got; g != gen {
			t.Errorf("waiter resolved to %p, want the installed generation %p", g, gen)
		}
	})
}

// TestHandleNoLostWakeup is the capture-under-lock hinge across a re-block, the property that would
// rot silently if notify were captured anywhere but under the resolve lock. A consume-once name
// keeps the waiter parked through the live install — a live consume-once is unreadable, so the
// waiter re-resolves ErrNotFound and re-blocks on a FRESH notify — then wakes it on finalize. The
// second wake lands on the channel re-captured after the first, so a hinge that re-armed outside
// the lock would drop it. It is the handle twin of the buffer's append→re-block→finish.
func TestHandleNoLostWakeup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newRegistry().acquire("secret")
		gen := &generation{name: "secret", consume: true, state: genLive}

		got := make(chan *generation, 1)
		var wg sync.WaitGroup
		wg.Go(func() {
			g, err := modelWait(context.Background(), h)
			if err != nil {
				t.Errorf("modelWait: %v", err)
			}
			got <- g
		})

		synctest.Wait() // blocked at ErrNotFound, notify #1 captured
		h.mu.Lock()
		h.live = gen // a live consume-once is unreadable: the waiter must re-resolve and re-block
		h.wakeLocked()
		h.mu.Unlock()

		synctest.Wait() // woken, re-resolved ErrNotFound, re-blocked on notify #2
		h.mu.Lock()
		gen.state = genFinalized // finalize: now resolveRead returns it
		h.current = gen
		h.live = nil
		h.wakeLocked()
		h.mu.Unlock()

		wg.Wait()
		if g := <-got; g != gen {
			t.Errorf("waiter resolved to %p, want the finalized generation %p", g, gen)
		}
	})
}

// TestHandleCancelMidWaitNoLeak is the liveness proof the foundation must carry. A handle waiter
// on a clip that never appears has no guaranteed wake — unlike a buffer follower, whose writer
// is promised to terminate — so ctx-cancel is its only unblock. A parked waiter whose context
// is canceled must return ctx.Err() and exit; wg.Wait is the leak detector. The resource story
// a waiting reader will rest on — it holds its caller only until the context is canceled — rests
// entirely on this.
func TestHandleCancelMidWaitNoLeak(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newRegistry().acquire("never")
		ctx, cancel := context.WithCancel(context.Background())

		errc := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Go(func() {
			_, err := modelWait(ctx, h)
			errc <- err
		})

		synctest.Wait() // durably blocked on a clip that will never appear
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

// TestHandleManyWaitersOneInstall pins the broadcast: N waiters on one empty handle all wake from
// a single wakeLocked, because it closes the shared channel rather than sending — a send would wake
// exactly one and strand the rest. It is the handle-scale fan-out for the wake itself.
func TestHandleManyWaitersOneInstall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newRegistry().acquire("x")
		gen := &generation{name: "x", state: genLive}

		const N = 8
		got := make([]*generation, N)
		errs := make([]error, N)
		var wg sync.WaitGroup
		for i := range N {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				got[i], errs[i] = modelWait(context.Background(), h)
			}(i)
		}

		synctest.Wait() // all N durably blocked on the one shared notify
		h.mu.Lock()
		h.live = gen
		h.wakeLocked()
		h.mu.Unlock()

		wg.Wait() // every waiter must have woken from the single close — a send would hang the rest
		for i := range N {
			if errs[i] != nil {
				t.Errorf("waiter %d: %v", i, errs[i])
			}
			if got[i] != gen {
				t.Errorf("waiter %d resolved to %p, want %p", i, got[i], gen)
			}
		}
	})
}
