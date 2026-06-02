package store

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/srevn/buff/store/internal/buffer"
)

// These are the quota's white-box tests. The quota is the store's hard cap, so the
// properties that matter — a set limit is never crossed, an unlimited one always admits,
// every reserve has a matching release, and concurrent writers never sum past the ceiling —
// are proven directly against the atomic counters, under the race detector.

// TestQuotaEnforcesByteCap proves the total-byte limit is exact: reservations are admitted
// up to the cap and refused past it, the refused reservation mutates nothing, and a release
// frees room for a later reserve.
func TestQuotaEnforcesByteCap(t *testing.T) {
	q := newQuota(Config{MaxTotal: 10})

	if !q.reserve(6) {
		t.Fatal("reserve(6) against cap 10 was refused")
	}
	if !q.reserve(4) {
		t.Fatal("reserve(4) filling the cap exactly was refused")
	}
	if q.reserve(1) {
		t.Fatal("reserve(1) over a full cap was admitted")
	}
	if got := q.bytes.Load(); got != 10 {
		t.Fatalf("after a refused reserve, bytes = %d, want 10 (no partial reservation)", got)
	}
	q.release(4)
	if !q.reserve(3) {
		t.Fatal("reserve(3) after freeing room was refused")
	}
	if got := q.bytes.Load(); got != 9 {
		t.Fatalf("bytes = %d, want 9", got)
	}
}

// TestQuotaEnforcesClipCap proves the generation-count limit is exact in the same way the
// byte limit is, on its own counter.
func TestQuotaEnforcesClipCap(t *testing.T) {
	q := newQuota(Config{MaxClips: 2})

	if !q.reserveClip() {
		t.Fatal("first clip reservation against cap 2 was refused")
	}
	if !q.reserveClip() {
		t.Fatal("second clip reservation against cap 2 was refused")
	}
	if q.reserveClip() {
		t.Fatal("a third clip reservation over cap 2 was admitted")
	}
	if got := q.clips.Load(); got != 2 {
		t.Fatalf("after a refused reservation, clips = %d, want 2", got)
	}
	q.releaseClip()
	if !q.reserveClip() {
		t.Fatal("a clip reservation after freeing a slot was refused")
	}
}

// TestQuotaUnlimited proves a zero cap admits everything while still tracking the footprint,
// so an unlimited store can report what it holds.
func TestQuotaUnlimited(t *testing.T) {
	q := newQuota(Config{})
	for i := range 1000 {
		if !q.reserve(1 << 20) {
			t.Fatalf("unlimited reserve was refused at iteration %d", i)
		}
		if !q.reserveClip() {
			t.Fatalf("unlimited reserveClip was refused at iteration %d", i)
		}
	}
	if got := q.bytes.Load(); got != 1000<<20 {
		t.Errorf("bytes = %d, want %d", got, 1000<<20)
	}
	if got := q.clips.Load(); got != 1000 {
		t.Errorf("clips = %d, want 1000", got)
	}
}

// TestQuotaPerClipOK proves the local per-clip check: it admits a chunk that keeps the
// running total within the cap, refuses one that would cross it, and a zero cap admits any
// chunk. It mutates no counter — only the write path's later reserve does.
func TestQuotaPerClipOK(t *testing.T) {
	q := newQuota(Config{MaxClip: 10})
	if !q.perClipOK(0, 10) {
		t.Error("perClipOK(0, 10) against cap 10 was false")
	}
	if !q.perClipOK(8, 2) {
		t.Error("perClipOK(8, 2) reaching the cap exactly was false")
	}
	if q.perClipOK(8, 3) {
		t.Error("perClipOK(8, 3) over the cap was true")
	}
	if !newQuota(Config{}).perClipOK(1<<40, 1<<40) {
		t.Error("an unlimited per-clip cap refused a chunk")
	}
}

// TestQuotaReleaseGen proves releaseGen returns a generation's whole footprint — its
// buffered bytes and its one count slot — reading the size straight off the byte log.
func TestQuotaReleaseGen(t *testing.T) {
	q := newQuota(Config{})
	q.reserveClip()
	q.reserve(5)

	buf := buffer.NewMemory()
	if _, err := buf.Append([]byte("hello")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	q.releaseGen(&generation{buf: buf})

	if b, c := q.bytes.Load(), q.clips.Load(); b != 0 || c != 0 {
		t.Fatalf("after releaseGen, bytes/clips = %d/%d, want 0/0", b, c)
	}
}

// TestQuotaConcurrentNoOvershoot is the no-overshoot proof: many goroutines storm a small
// budget at once, and the total reserved must never exceed the cap and must equal exactly
// the bytes the winners claimed. Under the race detector this also exercises the CAS loop
// for a torn counter.
func TestQuotaConcurrentNoOvershoot(t *testing.T) {
	const cap, chunk, goroutines = 100, 10, 64
	q := newQuota(Config{MaxTotal: cap})

	var wins atomic.Int64
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			if q.reserve(chunk) {
				wins.Add(1)
			}
		})
	}
	wg.Wait()

	if got := q.bytes.Load(); got > cap {
		t.Errorf("reserved %d bytes, over the cap of %d", got, cap)
	}
	if got, want := q.bytes.Load(), wins.Load()*chunk; got != want {
		t.Errorf("reserved bytes = %d, want winners*chunk = %d", got, want)
	}
	if got := wins.Load(); got != cap/chunk {
		t.Errorf("winners = %d, want exactly %d (the cap admits no more, no fewer)", got, cap/chunk)
	}
}
