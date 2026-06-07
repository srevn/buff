package store_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
)

// This is the Store contract suite: the behaviour every store must exhibit, written once and run
// against a store built by a factory. The memory store runs it now; the disk store joins by adding
// one factory row, and the same assertions then prove the two media are interchangeable — most
// pointedly that the memory store faithfully emulates the way an open reader keeps superseded bytes
// alive on disk.
//
// It runs with ordinary goroutines under the race detector, not a fake clock: the disk store it
// will also cover does real syscalls that no synctest bubble can durably block, so the one timing-
// sensitive case — following a live write — is made deterministic by attaching the reader before
// the writer closes, never by controlling time.

func newMemory(_ *testing.T, c store.Config) store.Store { return store.NewMemory(c) }

// newDisk builds a disk store over a fresh temp root for each call, with the zero DiskOpts:
// durability off, no checksum, the default logger. The logic the contract proves is medium-
// independent, and it is fsync-agnostic: byte visibility is page-cache coherence, not flushing,
// and durability is a crash-only property no in-process test observes. Leaving fsync off keeps the
// suite fast and deterministic; the real Sync path is exercised by the focused disk tests. Each
// root is fresh and empty, so the recovery pass NewDisk now runs finds nothing and installs nothing
// — recovery is a no-op here, which is itself the proof that an empty store recovers cleanly. The
// root is closed when the test ends.
func newDisk(t *testing.T, c store.Config) store.Store {
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	s, err := store.NewDisk(root, c, store.DiskOpts{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestStoreContract(t *testing.T) {
	t.Run("memory", func(t *testing.T) { testStoreContract(t, newMemory) })
	t.Run("disk", func(t *testing.T) { testStoreContract(t, newDisk) })
}

// testStoreContract runs every scenario against a store the factory builds. Scenarios that only
// need default behaviour build an unbounded store with the zero Config; the cap, consume-once, and
// TTL scenarios take the factory itself so they can build a store with the policy they exercise —
// which is why the factory, not a pre-built store, is the parameter.
func testStoreContract(t *testing.T, factory func(t *testing.T, c store.Config) store.Store) {
	t.Run("round trip", func(t *testing.T) { testRoundTrip(t, factory(t, store.Config{})) })
	t.Run("executable file survives", func(t *testing.T) { testExecutableSurvives(t, factory(t, store.Config{})) })
	t.Run("one live generation", func(t *testing.T) { testOneLive(t, factory(t, store.Config{})) })
	t.Run("if-match conditional write", func(t *testing.T) { testIfMatchCAS(t, factory(t, store.Config{})) })
	t.Run("read after supersede", func(t *testing.T) { testReadAfterSupersede(t, factory(t, store.Config{})) })
	t.Run("replacement invisible while live", func(t *testing.T) { testReplacementInvisibleWhileLive(t, factory(t, store.Config{})) })
	t.Run("list excludes live", func(t *testing.T) { testListExcludesLive(t, factory(t, store.Config{})) })
	t.Run("delete of live", func(t *testing.T) { testDeleteOfLive(t, factory(t, store.Config{})) })
	t.Run("live follow", func(t *testing.T) { testLiveFollow(t, factory(t, store.Config{})) })
	t.Run("rendezvous wait", func(t *testing.T) { testRendezvousWait(t, factory(t, store.Config{})) })
	t.Run("follow next generation", func(t *testing.T) { testFollowNext(t, factory(t, store.Config{})) })
	t.Run("empty clip", func(t *testing.T) { testEmptyClip(t, factory(t, store.Config{})) })
	t.Run("abort discards", func(t *testing.T) { testAbortDiscards(t, factory(t, store.Config{})) })
	t.Run("write after close", func(t *testing.T) { testWriteAfterClose(t, factory(t, store.Config{})) })
	t.Run("unknown and invalid names", func(t *testing.T) { testBadNames(t, factory(t, store.Config{})) })

	// The cap, TTL, and consume-once scenarios each build a store with the policy they exercise, so
	// they take the factory rather than a default store.
	t.Run("per-clip cap rejects whole chunk", func(t *testing.T) { testPerClipCap(t, factory) })
	t.Run("total cap and release on delete", func(t *testing.T) { testTotalCap(t, factory) })
	t.Run("total cap concurrent no overshoot", func(t *testing.T) { testTotalCapConcurrent(t, factory) })
	t.Run("clip count cap", func(t *testing.T) { testClipCountCap(t, factory) })
	t.Run("ttl resolves to expires", func(t *testing.T) { testTTLExpires(t, factory) })
	t.Run("consume once at most once", func(t *testing.T) { testConsumeOnceAtMostOnce(t, factory) })
	t.Run("consume once invisible while live", func(t *testing.T) { testConsumeOnceInvisibleWhileLive(t, factory) })
	t.Run("consume once claim then cleanup", func(t *testing.T) { testConsumeOnceClaimCleanup(t, factory) })
	t.Run("consume once stat never claims", func(t *testing.T) { testConsumeOnceStatNeverClaims(t, factory) })
	t.Run("consume once delete destroys", func(t *testing.T) { testConsumeOnceDeleteDestroys(t, factory) })
	t.Run("consume once canceled open never claims", func(t *testing.T) { testConsumeOnceCanceledOpenNeverClaims(t, factory) })
}

var bytesMeta = clip.Meta{Kind: clip.KindBytes}

// mustPut creates, writes, and finalizes a clip with default options, returning the writer's
// finalized view.
func mustPut(t *testing.T, s store.Store, name string, data []byte) clip.Clip {
	t.Helper()
	return mustPutOpts(t, s, name, data, store.PutOpts{})
}

// mustPutOpts is mustPut with explicit write options, for the retention and consume-once scenarios
// that turn those knobs.
func mustPutOpts(t *testing.T, s store.Store, name string, data []byte, o store.PutOpts) clip.Clip {
	t.Helper()
	w, err := s.Create(context.Background(), name, bytesMeta, o)
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

// mustGet opens a clip, reads it to completion, and closes it.
func mustGet(t *testing.T, s store.Store, name string) (clip.Clip, []byte) {
	t.Helper()
	rc, c, err := s.Open(context.Background(), name, store.GetOpts{})
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

func testRoundTrip(t *testing.T, s store.Store) {
	ctx := context.Background()
	written := mustPut(t, s, "greeting", []byte("hello, world"))
	if written.Generation == "" {
		t.Error("finalized clip has empty generation")
	}
	if !written.Finalized {
		t.Error("Clip after Close is not marked finalized")
	}
	if written.Size != 12 {
		t.Errorf("written size = %d, want 12", written.Size)
	}

	c, data := mustGet(t, s, "greeting")
	if string(data) != "hello, world" {
		t.Errorf("read %q, want %q", data, "hello, world")
	}
	if c.Generation != written.Generation {
		t.Errorf("opened generation %q != written %q", c.Generation, written.Generation)
	}

	st, err := s.Stat(ctx, "greeting")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !st.Finalized || st.Size != 12 || st.Generation != written.Generation {
		t.Errorf("stat = %+v, want finalized, size 12, gen %s", st, written.Generation)
	}

	list, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0].Name != "greeting" {
		t.Errorf("List = %+v, want one clip named greeting", list)
	}

	if err := s.Delete(ctx, "greeting"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, _, err := s.Open(ctx, "greeting", store.GetOpts{}); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Open after Delete = %v, want ErrNotFound", err)
	}
}

// testExecutableSurvives proves a file clip's executable bit rides the metadata through a Put→Get
// and a Put→Stat on either medium — the in-memory half of the executable feature, the counterpart
// to recovery_test's disk-round-trip proof. It also pins the absent⇒false default (a clip created
// without the bit reports it false, never a stray true) and the admission normalize: a bytes clip
// seated with file-scoped fields — a shape a raw PUT or an embedder can build but the type forbids
// — is cleaned to a plain bytes clip at Create, so the durability authority never persists it.
func testExecutableSurvives(t *testing.T, s store.Store) {
	ctx := context.Background()
	w, err := s.Create(ctx, "prog", clip.Meta{Kind: clip.KindFile, Filename: "prog", Executable: true}, store.PutOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("#!/bin/sh\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if c, _ := mustGet(t, s, "prog"); !c.Meta.Executable {
		t.Error("Get lost the executable bit")
	}
	st, err := s.Stat(ctx, "prog")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !st.Meta.Executable {
		t.Error("Stat lost the executable bit")
	}

	mustPut(t, s, "plain", []byte("x")) // bytesMeta, Executable left false
	if c, _ := mustGet(t, s, "plain"); c.Meta.Executable {
		t.Error("a clip created without the bit reported executable")
	}

	wr, err := s.Create(ctx, "raw", clip.Meta{Kind: clip.KindBytes, Filename: "evil", Executable: true}, store.PutOpts{})
	if err != nil {
		t.Fatalf("Create raw: %v", err)
	}
	if _, err := wr.Write([]byte("x")); err != nil {
		t.Fatalf("Write raw: %v", err)
	}
	if err := wr.Close(); err != nil {
		t.Fatalf("Close raw: %v", err)
	}
	if c, _ := mustGet(t, s, "raw"); c.Meta != (clip.Meta{Kind: clip.KindBytes}) {
		t.Errorf("Create persisted an illegal cross-field shape: %+v, want a plain bytes clip", c.Meta)
	}
}

func testOneLive(t *testing.T, s store.Store) {
	ctx := context.Background()
	w1, err := s.Create(ctx, "slot", bytesMeta, store.PutOpts{})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	if _, err := s.Create(ctx, "slot", bytesMeta, store.PutOpts{}); !errors.Is(err, clip.ErrBusy) {
		t.Errorf("second Create while live = %v, want ErrBusy", err)
	}
	// The incumbent still finalizes normally.
	if _, err := w1.Write([]byte("value")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, data := mustGet(t, s, "slot"); string(data) != "value" {
		t.Errorf("read %q, want value", data)
	}
}

// testIfMatchCAS proves the conditional write: a replace lands only if its If-Match names the
// current finalized generation (or "*" for any present finalized clip), and is refused 412
// otherwise — the optimistic-concurrency primitive built on the generation id that every read
// already carries, minting nothing on a refusal. The load-bearing arm is the chain: after a
// successful CAS the matched id no longer matches and the new one does, which is exactly what
// makes a racing second writer detectable. The precedence arm pins that the CAS is evaluated before
// the busy gate, so a stale precondition is 412 ("the value moved, re-read") where a held one is
// ErrBusy ("it still holds, but not now").
func testIfMatchCAS(t *testing.T, s store.Store) {
	ctx := context.Background()

	// Against an empty name nothing matches: a specific id and "*" alike are 412, and the refusal
	// mints no generation, so the name is still untouched for the unconditional seed below.
	if _, err := s.Create(ctx, "cas", bytesMeta, store.PutOpts{IfMatch: "00000000000000000000000000000000"}); !errors.Is(err, clip.ErrPreconditionFailed) {
		t.Fatalf("If-Match on an absent name = %v, want ErrPreconditionFailed", err)
	}
	if _, err := s.Create(ctx, "cas", bytesMeta, store.PutOpts{IfMatch: "*"}); !errors.Is(err, clip.ErrPreconditionFailed) {
		t.Fatalf(`If-Match "*" on an absent name = %v, want ErrPreconditionFailed`, err)
	}

	// Seed X unconditionally, then CAS-replace it naming its generation; the value and the id both
	// move.
	x := mustPut(t, s, "cas", []byte("X"))
	y := mustPutOpts(t, s, "cas", []byte("Y"), store.PutOpts{IfMatch: x.Generation})
	if y.Generation == x.Generation {
		t.Fatal("CAS replace kept the same generation id")
	}
	if _, data := mustGet(t, s, "cas"); string(data) != "Y" {
		t.Errorf("after CAS replace read %q, want Y", data)
	}

	// The chain — the whole point of CAS. The matched id is now stale and no longer matches; the id
	// the replace produced does. A second writer holding the old id is detected; one that re-read
	// succeeds.
	if _, err := s.Create(ctx, "cas", bytesMeta, store.PutOpts{IfMatch: x.Generation}); !errors.Is(err, clip.ErrPreconditionFailed) {
		t.Errorf("stale If-Match after replace = %v, want ErrPreconditionFailed", err)
	}
	// y's id matches now; mustPutOpts fatals if the CAS were refused, so this also proves the chain
	// holds.
	mustPutOpts(t, s, "cas", []byte("W"), store.PutOpts{IfMatch: y.Generation})
	// "*" matches the present current too — "replace only if something is there," which now there is.
	v := mustPutOpts(t, s, "cas", []byte("V"), store.PutOpts{IfMatch: "*"})
	if _, data := mustGet(t, s, "cas"); string(data) != "V" {
		t.Errorf("after star CAS read %q, want V", data)
	}

	// Precedence: a finalized current (V) plus a held-open live writer. A stale precondition is 412
	// — the value moved, re-read — evaluated before the busy gate; a matching one is ErrBusy — it
	// still holds, but a write cannot land while another is in flight. Telling the two apart is the
	// precedence's point.
	live, err := s.Create(ctx, "cas", bytesMeta, store.PutOpts{})
	if err != nil {
		t.Fatalf("held-open live Create: %v", err)
	}
	if _, err := s.Create(ctx, "cas", bytesMeta, store.PutOpts{IfMatch: x.Generation}); !errors.Is(err, clip.ErrPreconditionFailed) {
		t.Errorf("stale If-Match with a live incumbent = %v, want ErrPreconditionFailed (CAS before busy)", err)
	}
	if _, err := s.Create(ctx, "cas", bytesMeta, store.PutOpts{IfMatch: v.Generation}); !errors.Is(err, clip.ErrBusy) {
		t.Errorf("matching If-Match with a live incumbent = %v, want ErrBusy", err)
	}
	if err := live.Abort(); err != nil {
		t.Fatalf("Abort held-open live: %v", err)
	}

	// A consume-once mid-delivery is genConsumed, not the readable value, so it matches no If-Match
	// — the same "not the current finalized value" arm as an absent or live current. "*" is the
	// strongest probe: it would match any present finalized clip, so its 412 shows a claimed secret
	// is not "present" to a CAS. No bespoke mid-delivery handling is needed; the predicate refuses it
	// by construction.
	mustPutOpts(t, s, "secret", []byte("s"), store.PutOpts{ConsumeOnce: true})
	claim, _, err := s.Open(ctx, "secret", store.GetOpts{}) // claim flips it to genConsumed, held open
	if err != nil {
		t.Fatalf("Open consume-once: %v", err)
	}
	if _, err := s.Create(ctx, "secret", bytesMeta, store.PutOpts{IfMatch: "*"}); !errors.Is(err, clip.ErrPreconditionFailed) {
		t.Errorf("If-Match on a mid-delivery consume-once = %v, want ErrPreconditionFailed", err)
	}
	if err := claim.Close(); err != nil {
		t.Fatalf("Close consume-once claim: %v", err)
	}
}

func testReadAfterSupersede(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustPut(t, s, "doc", []byte("AAAA"))

	// Open a reader on the finalized A, then replace it with B before reading.
	rcA, cA, err := s.Open(ctx, "doc", store.GetOpts{})
	if err != nil {
		t.Fatalf("Open A: %v", err)
	}
	mustPut(t, s, "doc", []byte("BBBBBB"))

	// A's reader still delivers A's full bytes to EOF: the open handle pins them even though the
	// generation is no longer the name's current.
	dataA, err := io.ReadAll(rcA)
	if cerr := rcA.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatalf("read A after supersede: %v", err)
	}
	if string(dataA) != "AAAA" {
		t.Errorf("superseded reader read %q, want AAAA", dataA)
	}

	cB, dataB := mustGet(t, s, "doc")
	if string(dataB) != "BBBBBB" {
		t.Errorf("fresh read = %q, want BBBBBB", dataB)
	}
	if cB.Generation == cA.Generation {
		t.Error("replacement kept the same generation id")
	}
}

func testDeleteOfLive(t *testing.T, s store.Store) {
	ctx := context.Background()
	w, err := s.Create(ctx, "wip", bytesMeta, store.PutOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("partial")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := s.Delete(ctx, "wip"); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Delete of a live-only name = %v, want ErrNotFound", err)
	}
	// The writer's own generation is untouched and finalizes normally.
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, data := mustGet(t, s, "wip"); string(data) != "partial" {
		t.Errorf("read %q, want partial", data)
	}
}

func testLiveFollow(t *testing.T, s store.Store) {
	ctx := context.Background()
	w, err := s.Create(ctx, "stream", bytesMeta, store.PutOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("chunk-1;")); err != nil {
		t.Fatalf("Write 1: %v", err)
	}

	// Attach the follower to the live generation before the writer closes — this is what makes the
	// case deterministic without a fake clock.
	rc, c, err := s.Open(ctx, "stream", store.GetOpts{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if c.Finalized {
		t.Error("clip view says finalized while following a live write")
	}
	drained := make(chan string, 1)
	go func() {
		data, _ := io.ReadAll(rc)
		_ = rc.Close()
		drained <- string(data)
	}()

	if _, err := w.Write([]byte("chunk-2")); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := <-drained; got != "chunk-1;chunk-2" {
		t.Errorf("follower drained %q, want chunk-1;chunk-2", got)
	}
}

// testRendezvousWait is the black-box smoke for the consumer-before-producer ordering, on either
// medium: a reader Opens with Wait set on a name that does not exist, a writer then creates and
// finalizes it, and the reader must receive the full value through the public Store interface
// alone. It deliberately does not — and cannot — isolate either wake. Blind to the lease the parked
// reader holds (that is white-box), it cannot force the reader to park before the write, and even
// parked a plain reader has two ways to be served, the live follow or the finalized section, so a
// single lost wake is masked by the other path or by the write simply completing first. Per-wake
// isolation is owned by the deterministic tests instead: the memory synctests in wait_test.go and
// the disk lease- barrier tests in rendezvous_test.go, one per wake per medium. Here the value
// is only that the public rendezvous resolves to the same bytes on both media; the ceiling guards
// against a wedge.
func testRendezvousWait(t *testing.T, s store.Store) {
	ctx := context.Background()
	type result struct {
		data []byte
		err  error
	}
	res := make(chan result, 1)
	go func() {
		rc, _, err := s.Open(ctx, "late", store.GetOpts{Wait: true})
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

	// Write the clip the waiter is blocked on. The wake-on-create and wake-on-finalize transitions
	// both land while it is parked (or it resolves the live generation outright if it lost the race) —
	// either way the full value reaches it.
	w, err := s.Create(ctx, "late", bytesMeta, store.PutOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("arrived")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case r := <-res:
		if r.err != nil {
			t.Fatalf("waiting Open: %v", r.err)
		}
		if string(r.data) != "arrived" {
			t.Errorf("waiting reader drained %q, want arrived", r.data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waiting Open did not wake within 3s")
	}
}

// testFollowNext is the cross-medium proof of follow-next: a read that skips the value current at
// entry and resolves the next generation written to the name instead. A finalized v1 is seeded as
// the baseline; a live v2 is opened and held; a follow-next reader must then skip v1 and attach to
// v2, which it follows to a clean end — drained as v2's bytes, never v1's.
//
// Holding v2 live and waiting on the attached signal before finalizing it is what makes the
// resolution deterministic on either medium, without a clock or a lease peek: the reader finds
// v2 already in flight and resolves it at once, and because the resolve happens while v1 is still
// the current value, the baseline it captures is v1 — so the skip is genuinely exercised, with no
// chance for a v2 that finalized first to become the baseline and wedge the read. The park-then-
// wake path the skip also serves is pinned deterministically for memory in follow_test.go; here the
// value is that the skip resolves the next write's bytes identically across media.
func testFollowNext(t *testing.T, s store.Store) {
	ctx := context.Background()
	v1 := mustPut(t, s, "slot", []byte("v1-current"))

	// The replacement, opened live and held: with the finalized v1 still current, a follow-next reader
	// must step past v1 and attach to this v2.
	wB, err := s.Create(ctx, "slot", bytesMeta, store.PutOpts{})
	if err != nil {
		t.Fatalf("Create v2: %v", err)
	}
	v2gen := wB.Clip().Generation

	type result struct {
		data []byte
		gen  string
		live bool
		err  error
	}
	res := make(chan result, 1)
	attached := make(chan struct{}) // closed once the reader has resolved v2, so v1 was its baseline
	go func() {
		rc, c, err := s.Open(ctx, "slot", store.GetOpts{FollowNext: true})
		close(attached)
		if err != nil {
			res <- result{err: err}
			return
		}
		data, err := io.ReadAll(rc)
		if cerr := rc.Close(); cerr != nil && err == nil {
			err = cerr
		}
		res <- result{data: data, gen: c.Generation, live: !c.Finalized, err: err}
	}()

	// Only finalize v2 once the reader has attached, so it captured v1 — not a just-finalized v2 — as
	// the baseline it skips.
	<-attached
	if _, err := wB.Write([]byte("v2-next")); err != nil {
		t.Fatalf("Write v2: %v", err)
	}
	if err := wB.Close(); err != nil {
		t.Fatalf("Close v2: %v", err)
	}

	select {
	case r := <-res:
		if r.err != nil {
			t.Fatalf("follow-next Open: %v", r.err)
		}
		if string(r.data) != "v2-next" {
			t.Errorf("follow-next drained %q, want v2-next (the next write, never v1)", r.data)
		}
		if r.gen == v1.Generation {
			t.Errorf("follow-next resolved the baseline generation %q, want the next write's", v1.Generation)
		}
		if r.gen != v2gen {
			t.Errorf("follow-next resolved generation %q, want v2 %q", r.gen, v2gen)
		}
		if !r.live {
			t.Error("follow-next attached to a finalized section, want a live follow of the in-flight next write")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("follow-next did not complete within 3s")
	}
}

func testEmptyClip(t *testing.T, s store.Store) {
	ctx := context.Background()
	written := mustPut(t, s, "empty", nil)
	if written.Size != 0 {
		t.Errorf("written size = %d, want 0", written.Size)
	}

	rc, _, err := s.Open(ctx, "empty", store.GetOpts{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var buf [8]byte
	n, err := rc.Read(buf[:])
	_ = rc.Close()
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Errorf("Read of empty clip = (%d, %v), want (0, EOF)", n, err)
	}

	st, err := s.Stat(ctx, "empty")
	if err != nil || st.Size != 0 {
		t.Errorf("Stat = %+v, %v; want size 0", st, err)
	}
	if list, _ := s.List(ctx); len(list) != 1 {
		t.Errorf("List length = %d, want 1", len(list))
	}
}

func testAbortDiscards(t *testing.T, s store.Store) {
	ctx := context.Background()
	w, err := s.Create(ctx, "tmp", bytesMeta, store.PutOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("partial")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// A follower attached before the abort must end torn, never at a clean EOF.
	rc, _, err := s.Open(ctx, "tmp", store.GetOpts{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	torn := make(chan error, 1)
	go func() {
		_, readErr := io.ReadAll(rc)
		_ = rc.Close()
		torn <- readErr
	}()

	if err := w.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if readErr := <-torn; !errors.Is(readErr, clip.ErrAborted) {
		t.Errorf("follower after Abort = %v, want ErrAborted", readErr)
	}
	if _, _, err := s.Open(ctx, "tmp", store.GetOpts{}); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Open after Abort = %v, want ErrNotFound", err)
	}
}

func testWriteAfterClose(t *testing.T, s store.Store) {
	ctx := context.Background()
	w, err := s.Create(ctx, "x", bytesMeta, store.PutOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("data")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := w.Write([]byte("more")); !errors.Is(err, clip.ErrClosed) {
		t.Errorf("Write after Close = %v, want ErrClosed", err)
	}
	if err := w.Close(); !errors.Is(err, clip.ErrClosed) {
		t.Errorf("second Close = %v, want ErrClosed", err)
	}
}

func testBadNames(t *testing.T, s store.Store) {
	ctx := context.Background()
	if _, err := s.Stat(ctx, "ghost"); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Stat unknown = %v, want ErrNotFound", err)
	}
	if _, _, err := s.Open(ctx, "ghost", store.GetOpts{}); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Open unknown = %v, want ErrNotFound", err)
	}
	if err := s.Delete(ctx, "ghost"); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Delete unknown = %v, want ErrNotFound", err)
	}

	const bad = "has/slash"
	if _, err := s.Stat(ctx, bad); !errors.Is(err, clip.ErrNameInvalid) {
		t.Errorf("Stat invalid = %v, want ErrNameInvalid", err)
	}
	if _, _, err := s.Open(ctx, bad, store.GetOpts{}); !errors.Is(err, clip.ErrNameInvalid) {
		t.Errorf("Open invalid = %v, want ErrNameInvalid", err)
	}
	if _, err := s.Create(ctx, bad, bytesMeta, store.PutOpts{}); !errors.Is(err, clip.ErrNameInvalid) {
		t.Errorf("Create invalid = %v, want ErrNameInvalid", err)
	}
}

// testReplacementInvisibleWhileLive proves the core no-torn-read rule: while a replacement is still
// being written, readers keep seeing the prior finalized value, and the replacement becomes visible
// only when it finalizes — never mid-write.
func testReplacementInvisibleWhileLive(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustPut(t, s, "doc", []byte("A-value"))

	// Begin a replacement and leave it live (unfinalized).
	wB, err := s.Create(ctx, "doc", bytesMeta, store.PutOpts{})
	if err != nil {
		t.Fatalf("Create B: %v", err)
	}
	if _, err := wB.Write([]byte("B-value")); err != nil {
		t.Fatalf("Write B: %v", err)
	}

	// While B is live, every read still resolves the finalized A.
	if c, data := mustGet(t, s, "doc"); string(data) != "A-value" {
		t.Errorf("read during live replacement = %q (gen %s), want A-value", data, c.Generation)
	}
	if st, err := s.Stat(ctx, "doc"); err != nil || !st.Finalized {
		t.Errorf("Stat during live replacement = %+v, %v; want finalized A", st, err)
	}

	// B becomes visible only once it finalizes, and not before.
	if err := wB.Close(); err != nil {
		t.Fatalf("Close B: %v", err)
	}
	if _, data := mustGet(t, s, "doc"); string(data) != "B-value" {
		t.Errorf("read after B finalized = %q, want B-value", data)
	}
}

// testListExcludesLive proves List reports finalized clips only: a name carrying just a live, in-
// flight generation is absent from the listing until it finalizes.
func testListExcludesLive(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustPut(t, s, "done", []byte("done"))

	w, err := s.Create(ctx, "wip", bytesMeta, store.PutOpts{})
	if err != nil {
		t.Fatalf("Create wip: %v", err)
	}
	if _, err := w.Write([]byte("partial")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	list, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0].Name != "done" {
		t.Errorf("List = %+v, want only the finalized 'done'", list)
	}

	// The live clip finalizes normally afterwards.
	if err := w.Close(); err != nil {
		t.Fatalf("Close wip: %v", err)
	}
}

// testPerClipCap proves the per-clip byte cap rejects the offending chunk whole, with no partial
// write to make it fit: a write that would cross the cap returns ErrTooLarge and leaves the size
// unchanged, while a write reaching the cap exactly is admitted.
func testPerClipCap(t *testing.T, factory func(*testing.T, store.Config) store.Store) {
	ctx := context.Background()
	s := factory(t, store.Config{MaxClip: 10})

	w, err := s.Create(ctx, "capped", bytesMeta, store.PutOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write(make([]byte, 8)); err != nil {
		t.Fatalf("Write 8: %v", err)
	}
	if _, err := w.Write(make([]byte, 5)); !errors.Is(err, clip.ErrTooLarge) {
		t.Errorf("Write 5 over cap 10 = %v, want ErrTooLarge", err)
	}
	if sz := w.Clip().Size; sz != 8 {
		t.Errorf("after a rejected chunk, size = %d, want 8 (no partial write)", sz)
	}
	if _, err := w.Write(make([]byte, 2)); err != nil {
		t.Fatalf("Write 2 reaching the cap exactly: %v", err)
	}
	if _, err := w.Write(make([]byte, 1)); !errors.Is(err, clip.ErrTooLarge) {
		t.Errorf("Write 1 over a full cap = %v, want ErrTooLarge", err)
	}
	if err := w.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
}

// testTotalCap proves the total-byte cap and its release on delete: a full store refuses a further
// byte with ErrNoSpace, and deleting the clip that filled it frees the budget for the next write.
func testTotalCap(t *testing.T, factory func(*testing.T, store.Config) store.Store) {
	ctx := context.Background()
	s := factory(t, store.Config{MaxTotal: 10})

	wA, err := s.Create(ctx, "a", bytesMeta, store.PutOpts{})
	if err != nil {
		t.Fatalf("Create a: %v", err)
	}
	if _, err := wA.Write(make([]byte, 10)); err != nil {
		t.Fatalf("Write a filling the budget: %v", err)
	}
	if err := wA.Close(); err != nil {
		t.Fatalf("Close a: %v", err)
	}

	wB, err := s.Create(ctx, "b", bytesMeta, store.PutOpts{})
	if err != nil {
		t.Fatalf("Create b: %v", err)
	}
	if _, err := wB.Write(make([]byte, 1)); !errors.Is(err, clip.ErrNoSpace) {
		t.Errorf("Write against a full store = %v, want ErrNoSpace", err)
	}
	if err := wB.Abort(); err != nil {
		t.Fatalf("Abort b: %v", err)
	}

	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatalf("Delete a: %v", err)
	}
	wC, err := s.Create(ctx, "c", bytesMeta, store.PutOpts{})
	if err != nil {
		t.Fatalf("Create c: %v", err)
	}
	if _, err := wC.Write(make([]byte, 10)); err != nil {
		t.Fatalf("Write c after freeing the budget: %v", err)
	}
	if err := wC.Close(); err != nil {
		t.Fatalf("Close c: %v", err)
	}
}

// testTotalCapConcurrent proves the cap holds with no overshoot under writers racing against
// different names: the finalized bytes never exceed the budget, and the cap is genuinely exercised
// — at least one writer is refused.
func testTotalCapConcurrent(t *testing.T, factory func(*testing.T, store.Config) store.Store) {
	ctx := context.Background()
	const total = 100
	s := factory(t, store.Config{MaxTotal: total})

	var finalized, refusals atomic.Int64
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w, err := s.Create(ctx, fmt.Sprintf("w%d", i), bytesMeta, store.PutOpts{})
			if err != nil {
				return
			}
			for range 3 { // try to write 30 bytes; the budget admits at most three writers
				if _, err := w.Write(make([]byte, 10)); errors.Is(err, clip.ErrNoSpace) {
					refusals.Add(1)
					_ = w.Abort()
					return
				}
			}
			if err := w.Close(); err == nil {
				finalized.Add(w.Clip().Size)
			}
		}(i)
	}
	wg.Wait()

	if got := finalized.Load(); got > total {
		t.Errorf("finalized %d bytes, over the cap of %d", got, total)
	}
	if refusals.Load() == 0 {
		t.Error("no writer was refused; the concurrent cap was never exercised")
	}
}

// testClipCountCap proves the generation-count cap is enforced at Create and released on delete: a
// third create over a cap of two is refused, and freeing a slot admits it.
func testClipCountCap(t *testing.T, factory func(*testing.T, store.Config) store.Store) {
	ctx := context.Background()
	s := factory(t, store.Config{MaxClips: 2})

	mustPut(t, s, "a", []byte("a"))
	mustPut(t, s, "b", []byte("b"))

	if _, err := s.Create(ctx, "c", bytesMeta, store.PutOpts{}); !errors.Is(err, clip.ErrNoSpace) {
		t.Errorf("third Create over count cap 2 = %v, want ErrNoSpace", err)
	}
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatalf("Delete a: %v", err)
	}
	w, err := s.Create(ctx, "c", bytesMeta, store.PutOpts{})
	if err != nil {
		t.Fatalf("Create c after freeing a slot: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close c: %v", err)
	}
}

// testTTLExpires proves how a write's retention choice resolves to the clip's absolute expiry,
// observed through Stat: an explicit TTL and a store default each land exactly that span past
// finalize, Keep overrides a default to never expire, and a store with no default leaves an
// unspecified clip with no expiry at all — never "expire immediately".
func testTTLExpires(t *testing.T, factory func(*testing.T, store.Config) store.Store) {
	ctx := context.Background()
	statExpiry := func(t *testing.T, c store.Config, o store.PutOpts) (finalized, expires time.Time) {
		t.Helper()
		s := factory(t, c)
		mustPutOpts(t, s, "k", []byte("x"), o)
		st, err := s.Stat(ctx, "k")
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		return st.FinalizedAt, st.ExpiresAt
	}

	if fin, exp := statExpiry(t, store.Config{}, store.PutOpts{TTL: time.Hour}); !exp.Equal(fin.Add(time.Hour)) {
		t.Errorf("explicit TTL: ExpiresAt %v, want FinalizedAt+1h %v", exp, fin.Add(time.Hour))
	}
	if _, exp := statExpiry(t, store.Config{DefaultTTL: time.Hour}, store.PutOpts{Keep: true}); !exp.IsZero() {
		t.Errorf("keep over a default: ExpiresAt %v, want zero", exp)
	}
	if fin, exp := statExpiry(t, store.Config{DefaultTTL: time.Hour}, store.PutOpts{}); !exp.Equal(fin.Add(time.Hour)) {
		t.Errorf("store default: ExpiresAt %v, want FinalizedAt+1h %v", exp, fin.Add(time.Hour))
	}
	if _, exp := statExpiry(t, store.Config{}, store.PutOpts{}); !exp.IsZero() {
		t.Errorf("no default: ExpiresAt %v, want zero", exp)
	}
}

// testConsumeOnceAtMostOnce proves the delivery guarantee under contention: when many readers race
// to open one finalized consume-once clip, exactly one receives the bytes and every other is denied
// — by ErrConsumed if it arrives mid-delivery or ErrNotFound if after cleanup — and none receives
// the secret a second time.
func testConsumeOnceAtMostOnce(t *testing.T, factory func(*testing.T, store.Config) store.Store) {
	ctx := context.Background()
	s := factory(t, store.Config{})
	mustPutOpts(t, s, "secret", []byte("top-secret"), store.PutOpts{ConsumeOnce: true})

	const readers = 16
	var delivered, denied atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range readers {
		wg.Go(func() {
			<-start
			rc, _, err := s.Open(ctx, "secret", store.GetOpts{})
			if err != nil {
				if errors.Is(err, clip.ErrConsumed) || errors.Is(err, clip.ErrNotFound) {
					denied.Add(1)
				}
				return
			}
			data, _ := io.ReadAll(rc)
			_ = rc.Close()
			if string(data) == "top-secret" {
				delivered.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()

	if delivered.Load() != 1 {
		t.Errorf("readers that got the secret = %d, want exactly 1", delivered.Load())
	}
	if denied.Load() != readers-1 {
		t.Errorf("readers denied = %d, want %d (every other reader)", denied.Load(), readers-1)
	}
}

// testConsumeOnceInvisibleWhileLive proves a consume-once clip cannot be seen until it finalizes:
// while its upload is in flight neither Open nor Stat reveals it — which both preserves at-most-
// once (no two followers could attach) and avoids confirming a secret exists mid-upload — and only
// after Close can it be claimed and read.
func testConsumeOnceInvisibleWhileLive(t *testing.T, factory func(*testing.T, store.Config) store.Store) {
	ctx := context.Background()
	s := factory(t, store.Config{})

	w, err := s.Create(ctx, "secret", bytesMeta, store.PutOpts{ConsumeOnce: true})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("shh")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, _, err := s.Open(ctx, "secret", store.GetOpts{}); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Open of a live consume-once = %v, want ErrNotFound", err)
	}
	if _, err := s.Stat(ctx, "secret"); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Stat of a live consume-once = %v, want ErrNotFound", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, data := mustGet(t, s, "secret"); string(data) != "shh" {
		t.Errorf("read after finalize = %q, want shh", data)
	}
}

// testConsumeOnceClaimCleanup proves the claim-then-cleanup window: a second open while the claim
// is held reports ErrConsumed (the mid-delivery 410), and once the claiming reader drains and
// closes, the clip is gone and a later open reports ErrNotFound (404).
func testConsumeOnceClaimCleanup(t *testing.T, factory func(*testing.T, store.Config) store.Store) {
	ctx := context.Background()
	s := factory(t, store.Config{})
	mustPutOpts(t, s, "secret", []byte("payload"), store.PutOpts{ConsumeOnce: true})

	rc, _, err := s.Open(ctx, "secret", store.GetOpts{})
	if err != nil {
		t.Fatalf("first Open (the claim): %v", err)
	}
	if _, _, err := s.Open(ctx, "secret", store.GetOpts{}); !errors.Is(err, clip.ErrConsumed) {
		t.Errorf("second Open mid-delivery = %v, want ErrConsumed", err)
	}
	if _, err := s.Stat(ctx, "secret"); !errors.Is(err, clip.ErrConsumed) {
		t.Errorf("Stat mid-delivery = %v, want ErrConsumed", err)
	}

	data, err := io.ReadAll(rc)
	if cerr := rc.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatalf("read the claimed clip: %v", err)
	}
	if string(data) != "payload" {
		t.Errorf("claimed read = %q, want payload", data)
	}
	if _, _, err := s.Open(ctx, "secret", store.GetOpts{}); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Open after the consume completed = %v, want ErrNotFound", err)
	}
}

// testConsumeOnceStatNeverClaims proves Stat is non-destructive: it reports a finalized consume-
// once clip as many times as asked without claiming it, leaving the bytes for a later Open to
// consume.
func testConsumeOnceStatNeverClaims(t *testing.T, factory func(*testing.T, store.Config) store.Store) {
	ctx := context.Background()
	s := factory(t, store.Config{})
	mustPutOpts(t, s, "secret", []byte("payload"), store.PutOpts{ConsumeOnce: true})

	for i := range 2 {
		st, err := s.Stat(ctx, "secret")
		if err != nil {
			t.Fatalf("Stat %d: %v", i, err)
		}
		if !st.ConsumeOnce || !st.Finalized {
			t.Errorf("Stat %d = %+v, want a finalized consume-once clip", i, st)
		}
	}
	if _, data := mustGet(t, s, "secret"); string(data) != "payload" {
		t.Errorf("read after two stats = %q, want payload (Stat must not have claimed)", data)
	}
}

// testConsumeOnceDeleteDestroys proves an explicit delete of an unclaimed consume-once clip
// destroys it with zero delivery: a later open finds nothing.
func testConsumeOnceDeleteDestroys(t *testing.T, factory func(*testing.T, store.Config) store.Store) {
	ctx := context.Background()
	s := factory(t, store.Config{})
	mustPutOpts(t, s, "secret", []byte("payload"), store.PutOpts{ConsumeOnce: true})

	if err := s.Delete(ctx, "secret"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, _, err := s.Open(ctx, "secret", store.GetOpts{}); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Open after delete of consume-once = %v, want ErrNotFound", err)
	}
}

// testConsumeOnceCanceledOpenNeverClaims proves the one delivery is not spent on a request that
// is already gone: an Open whose context is canceled claims nothing and returns the cancellation,
// leaving the clip claimable for a later, live reader. The claim is irreversible, so declining to
// make it for a reader that cannot receive is what keeps the secret for one that can.
func testConsumeOnceCanceledOpenNeverClaims(t *testing.T, factory func(*testing.T, store.Config) store.Store) {
	s := factory(t, store.Config{})
	mustPutOpts(t, s, "secret", []byte("payload"), store.PutOpts{ConsumeOnce: true})

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := s.Open(canceled, "secret", store.GetOpts{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open with a canceled context = %v, want context.Canceled", err)
	}
	// The secret was not claimed: a live reader still receives it in full.
	if _, data := mustGet(t, s, "secret"); string(data) != "payload" {
		t.Errorf("after a canceled Open, read %q, want payload (the secret must stay claimable)", data)
	}
}
