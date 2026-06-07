package store

import (
	"context"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/srevn/buff/clip"
)

// These prove the two readability-creating wakes — the Create install and the Close finalize —
// over the real disk medium, the cross-medium coverage the behavioural contract suite cannot give.
// A wake only ever lands on a parked waiter, so isolating one demands knowing a reader has parked
// before the transition fires; synctest decides that for the memory proofs in wait_test.go, but its
// bubble cannot durably block the disk medium's syscalls, so here a reader's park is read instead
// from the lease it holds while waiting. That barrier is load-bearing: without it the writer
// routinely wins the start-up race, the reader resolves the generation outright, and a missing wake
// is never exercised — which is exactly why the black-box contract rendezvous, blind to the lease,
// can pass on the finalize fallback while the install wake is broken.
//
// They are the disk counterpart to wait_test.go, not a duplicate: that file pins the wake mechanic
// on a fake clock and the memory medium; this one drives the same two transitions through real
// on-disk creates, renames, and reads, so the wake composed with disk delivery — a parked reader
// following a live file, a parked reader claiming a consume-once generation across a real rename —
// is covered end to end.

// waitParked spins until name's handle holds want leases, the real-goroutine stand-in for
// synctest's durable-block detection. A waiting Open holds exactly its one lease on the handle for
// the whole wait, so the count reaching want is the signal that every expected party has acquired
// and the reader is in — or one uninterruptible step from — its select, the point past which only a
// wake or a cancel moves it. leases is read under registry.mu, the lock that guards it; the ceiling
// turns a reader that never parks into a failure rather than a spin that never ends.
func waitParked(t *testing.T, r *registry, name string, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		r.mu.Lock()
		n := 0
		if h := r.handles[name]; h != nil {
			n = h.leases
		}
		r.mu.Unlock()
		if n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("handle %q holds %d leases, want %d within 3s", name, n, want)
		}
		runtime.Gosched()
	}
}

// TestDiskRendezvousLiveFollowWakes pins the create-install wake on disk: a reader parked on an
// absent name must be delivered onto a LIVE follow when the first write installs its generation.
// waitParked confirms the reader has parked before the create, so the install wake is provably its
// only path in — withholding the close until the reader has drained a first live chunk then makes
// the live attach itself deterministic without a clock. Lose the install wake and the parked reader
// is never woken, never reads that chunk, and the handshake's ceiling fails the test; it cannot be
// rescued by the later finalize wake, because the writer never reaches the close. The reader then
// follows across the finalize to a clean EOF.
func TestDiskRendezvousLiveFollowWakes(t *testing.T) {
	s, _ := newDiskStore(t, Config{}, DiskOpts{})
	ctx := context.Background()

	type result struct {
		data []byte
		live bool // attached while the generation was live, not sectioned after finalize
		err  error
	}
	res := make(chan result, 1)
	attached := make(chan struct{}) // closed once the reader has drained the live first chunk
	go func() {
		rc, c, err := s.Open(ctx, "late", GetOpts{Wait: true})
		if err != nil {
			res <- result{err: err}
			return
		}
		// Draining the first chunk before signalling proves the reader attached while the generation was
		// live: the writer holds the close until this lands, so no finalized section could yet exist.
		first := make([]byte, len("chunk-1;"))
		if _, err := io.ReadFull(rc, first); err != nil {
			_ = rc.Close()
			res <- result{err: err}
			return
		}
		close(attached)
		rest, err := io.ReadAll(rc)
		if cerr := rc.Close(); cerr != nil && err == nil {
			err = cerr
		}
		res <- result{data: append(first, rest...), live: !c.Finalized, err: err}
	}()

	waitParked(t, s.reg, "late", 1) // the reader has parked: the install wake is now its only way in

	w, err := s.Create(ctx, "late", clip.Meta{Kind: clip.KindBytes}, PutOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("chunk-1;")); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	select {
	case <-attached: // woken by the install, the reader resolved the live generation and is following it
	case <-time.After(3 * time.Second):
		t.Fatal("parked reader was not woken onto the live generation within 3s")
	}
	if _, err := w.Write([]byte("chunk-2")); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case r := <-res:
		if r.err != nil {
			t.Fatalf("waiting Open: %v", r.err)
		}
		if !r.live {
			t.Error("waiter attached to a finalized clip, want a live follow")
		}
		if string(r.data) != "chunk-1;chunk-2" {
			t.Errorf("waiter drained %q, want chunk-1;chunk-2", r.data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waiting Open did not complete within 3s")
	}
}

// TestDiskRendezvousConsumeOnceWakes pins the close-finalize wake on disk, the consume-once half of
// the rendezvous and the one shape no fallback masks: a consume-once generation is invisible while
// live, so a parked waiter can be served only by the finalize wake. The writer creates and writes
// the invisible generation first; waitParked then confirms the reader has parked on it — the lease
// count rising to two, the writer's plus the reader's — before the finalize fires, so the wake is
// provably exercised rather than skipped by a reader that resolved a finalized clip outright. Lose
// the finalize wake and the reader parks past the close, claims nothing, and the ceiling fails the
// test. Over the disk medium this drives the parked-then-claimed path across a real on-disk claim
// rename and section read.
func TestDiskRendezvousConsumeOnceWakes(t *testing.T) {
	s, _ := newDiskStore(t, Config{}, DiskOpts{})
	ctx := context.Background()

	w, err := s.Create(ctx, "secret", clip.Meta{Kind: clip.KindBytes}, PutOpts{ConsumeOnce: true})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("top-secret")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	type result struct {
		data []byte
		err  error
	}
	res := make(chan result, 1)
	go func() {
		rc, _, err := s.Open(ctx, "secret", GetOpts{Wait: true})
		if err != nil {
			res <- result{err: err}
			return
		}
		data, err := io.ReadAll(rc)
		if cerr := rc.Close(); cerr != nil && err == nil {
			err = cerr
		}
		res <- result{data: data, err: err}
	}()

	// The writer still holds its lease; the reader parks on the invisible live generation, lifting the
	// count to two. Only then is the generation finalized — the single transition that can serve the
	// waiter, since the live consume-once was never resolvable.
	waitParked(t, s.reg, "secret", 2)

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case r := <-res:
		if r.err != nil {
			t.Fatalf("waiting Open: %v", r.err)
		}
		if string(r.data) != "top-secret" {
			t.Errorf("waiter read %q, want top-secret", r.data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("consume-once waiter was not woken on finalize within 3s")
	}
}
