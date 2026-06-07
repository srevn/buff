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

// These drive the real Open(GetOpts{FollowNext:true}) over a memory store inside a synctest bubble,
// where "is the waiter still parked or has it returned?" is decidable — synctest.Wait blocks until
// every goroutine is durably blocked, so a non-blocking receive afterwards tells "parked" from
// "returned" with no timing guess. They prove the one thing the tolerant contract test cannot pin:
// that follow-next durably skips the value current at entry and resolves only a strictly newer
// generation — onto a live next-write, onto a consume-once claimed on its finalize, and never onto
// the baseline. The wake mechanics underneath are follow-next's verbatim reuse of rendezvous-wait,
// already proven in wait_test.go; these add only the baseline-skip predicate layered over them.
//
// The memory medium is used because synctest cannot durably block a real disk syscall, and follow-
// next adds no disk wake site of its own — the install and finalize wakes it rides are pinned
// over disk by rendezvous_test.go, and the baseline-skip is medium-independent RAM logic. So the
// cross-medium follow-next coverage is the tolerant contract testFollowNext; the strict isolation
// is here.

// seedFinalized writes and finalizes name=data on s — the white-box analogue of the contract
// suite's mustPut, for laying down the baseline a follow-next read must step past.
func seedFinalized(t *testing.T, s *store, name string, data []byte) {
	t.Helper()
	w, err := s.Create(context.Background(), name, waitMeta, PutOpts{})
	if err != nil {
		t.Fatalf("seed Create %s: %v", name, err)
	}
	if len(data) > 0 {
		if _, err := w.Write(data); err != nil {
			t.Fatalf("seed Write %s: %v", name, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("seed Close %s: %v", name, err)
	}
}

// handleLeases reads a name's lease count under the registry lock, or -1 if the handle is gone. It
// is how the cancel proof confirms a parked follow-next held its lease and then released it.
func handleLeases(r *registry, name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h := r.handles[name]; h != nil {
		return h.leases
	}
	return -1
}

// TestFollowNextSkipsCurrentAttachesLive is the headline: follow-next steps past the value current
// at entry and attaches LIVE to the next write. With finalized v1 seeded, the waiter is durably
// parked (it did not return v1 — the baseline-skip), then the next write installs a live v2 and the
// waiter wakes onto it and follows it, never sectioning v1. The synctest.Wait after the v2 Create
// is load-bearing: it forces the attach while v2 is still live (asserted by result.live), proving
// follow-next followed the next write rather than racing ahead to a finished section — and the
// first synctest.Wait proves the skip, since a predicate that returned the baseline would have v1
// on the channel by then.
func TestFollowNextSkipsCurrentAttachesLive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStore(memMedium{}, time.Now, Config{})
		seedFinalized(t, s, "slot", []byte("v1-current"))

		out, wg := drainOpen(s, "slot", GetOpts{FollowNext: true})

		synctest.Wait() // durably parked: v1 is the baseline, skipped — not delivered
		select {
		case r := <-out:
			t.Fatalf("follow-next returned %q before any next write; it did not skip the current value", r.data)
		default:
		}

		w, err := s.Create(context.Background(), "slot", waitMeta, PutOpts{})
		if err != nil {
			t.Fatalf("Create v2: %v", err)
		}
		synctest.Wait() // woken by the install, attached the live follower, now blocked on the byte log
		if _, err := w.Write([]byte("v2-next")); err != nil {
			t.Fatalf("Write v2: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close v2: %v", err)
		}

		wg.Wait()
		r := <-out
		if r.err != nil {
			t.Fatalf("follow-next Open: %v", r.err)
		}
		if !r.live {
			t.Error("follow-next attached to a finalized section, want a live follow of the next write")
		}
		if string(r.data) != "v2-next" {
			t.Errorf("follow-next drained %q, want v2-next (the next write, never the current v1)", r.data)
		}
	})
}

// TestFollowNextConsumeOnceClaimsOnFinalize proves follow-next's consume-once arm: it skips a
// live consume-once next write — unfollowable, since two followers would each get the secret —
// and claims it on finalize, the one delivery. With finalized v1 seeded, the waiter parks past v1,
// then a live consume-once v2 is installed and written: the waiter woken by the install re-resolves
// ErrNotFound (the live secret is invisible) and re-blocks, confirmed by the second synctest.Wait
// plus the still-empty channel — the no-lost-wakeup hinge under follow-next. Only Close makes v2
// claimable; the waiter then wins the claim and reads the secret, and a later Open finds it gone,
// so the delivery was exactly once. A regression that skipped both live and finalized consume-once
// would leave the waiter parked past Close — a hard failure at wg.Wait.
func TestFollowNextConsumeOnceClaimsOnFinalize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStore(memMedium{}, time.Now, Config{})
		seedFinalized(t, s, "slot", []byte("v1-current"))

		out, wg := drainOpen(s, "slot", GetOpts{FollowNext: true})
		synctest.Wait() // parked past v1

		w, err := s.Create(context.Background(), "slot", waitMeta, PutOpts{ConsumeOnce: true})
		if err != nil {
			t.Fatalf("Create v2: %v", err)
		}
		if _, err := w.Write([]byte("top-secret")); err != nil {
			t.Fatalf("Write v2: %v", err)
		}
		synctest.Wait() // still parked: a live consume-once is unfollowable, so it re-resolved and re-blocked
		select {
		case r := <-out:
			t.Fatalf("follow-next returned before finalize: %+v", r)
		default:
		}

		if err := w.Close(); err != nil {
			t.Fatalf("Close v2: %v", err)
		}

		wg.Wait()
		r := <-out
		if r.err != nil {
			t.Fatalf("follow-next Open: %v", r.err)
		}
		if string(r.data) != "top-secret" {
			t.Errorf("follow-next read %q, want top-secret", r.data)
		}
		// Delivered exactly once: the reader's Close ran cleanup, so the secret is gone.
		if _, _, err := s.Open(context.Background(), "slot", GetOpts{}); !errors.Is(err, clip.ErrNotFound) {
			t.Errorf("after the follow-next consume, Open = %v, want ErrNotFound", err)
		}
	})
}

// TestFollowNextCancelReleasesLease is the liveness-and-leak proof for follow-next's heaviest-
// tailed park: a waiter on a populated slot whose next write never comes. It parks past the
// finalized v1 — holding one lease, the handle pinned — and ctx-cancel is its only unblock, since
// no next write is ever promised. On cancel it returns context.Canceled and releases the lease
// (the handle persists, because v1 still stands, but pins no waiter), and v1 is untouched, so the
// abandoned follow-next leaks nothing and disturbs nothing. The empty-slot eviction this mirrors is
// already proven for the plain wait it degenerates to in TestOpenWaitCancelEvictsHandle.
func TestFollowNextCancelReleasesLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStore(memMedium{}, time.Now, Config{})
		seedFinalized(t, s, "slot", []byte("v1-current"))

		ctx, cancel := context.WithCancel(context.Background())
		got := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Go(func() {
			_, _, err := s.Open(ctx, "slot", GetOpts{FollowNext: true})
			got <- err
		})

		synctest.Wait() // durably parked, skipping v1 — never returned
		select {
		case err := <-got:
			t.Fatalf("follow-next returned %v before any next write; it did not park past v1", err)
		default:
		}
		if n := handleLeases(s.reg, "slot"); n != 1 {
			t.Fatalf("parked follow-next holds %d leases, want 1", n)
		}

		cancel()
		wg.Wait()
		if err := <-got; !errors.Is(err, context.Canceled) {
			t.Errorf("canceled follow-next returned %v, want context.Canceled", err)
		}
		if n := handleLeases(s.reg, "slot"); n != 0 {
			t.Errorf("after cancel, handle holds %d leases, want 0 (the waiter's lease must release)", n)
		}
		// v1 is untouched: the abandoned follow-next stepped past it without disturbing it.
		if _, data := mustReadGen(t, s, "slot"); string(data) != "v1-current" {
			t.Errorf("after a canceled follow-next, plain read = %q, want v1-current", data)
		}
	})
}

// mustReadGen opens a name with default options, drains it, and closes it — the white-box read used
// by the follow-next proofs to confirm a baseline survived an abandoned follow.
func mustReadGen(t *testing.T, s *store, name string) (clip.Clip, []byte) {
	t.Helper()
	rc, c, err := s.Open(context.Background(), name, GetOpts{})
	if err != nil {
		t.Fatalf("Open %s: %v", name, err)
	}
	data, err := io.ReadAll(rc)
	if cerr := rc.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return c, data
}
