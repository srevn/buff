package store

import (
	"context"
	"errors"
	"io"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/srevn/buff/clip"

	"github.com/srevn/buff/store/internal/buffer"
)

// These are the white-box tests: they reach into the store's unexported machinery — the id
// allocator, the leased registry, and a fault-injecting medium — to prove the parts the behavioural
// contract suite cannot observe from outside. They run under the race detector, where the orphan
// race and any lock-order slip would surface.

var hexID = regexp.MustCompile(`^[0-9a-f]{32}$`)

// handleCount and hasHandle read the registry map under its lock. They live in the test so the
// store carries no accessor that exists only for tests; white-box tests may reach the fields
// directly.
func handleCount(r *registry) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.handles)
}

func hasHandle(r *registry, name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.handles[name]
	return ok
}

// TestGenIDMonotonic proves a name's ids strictly increase across allocations, even when the clock
// jumps backwards, and that the rendered form is 32 lowercase hex characters that sort the same way
// the underlying counters do.
func TestGenIDMonotonic(t *testing.T) {
	// allocate needs no name; built through the one constructor, armed like every handle.
	h := newHandle("")
	base := time.Unix(1_700_000_000, 0) // a realistic wall clock; a backward jump stays post-epoch

	id1, err := h.allocate(base)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	id2, err := h.allocate(base.Add(time.Second)) // clock advances
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	id3, err := h.allocate(base.Add(-time.Hour)) // clock jumps backwards
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}

	if !(id1.String() < id2.String() && id2.String() < id3.String()) {
		t.Fatalf("ids not strictly increasing: %s, %s, %s", id1, id2, id3)
	}
	for _, id := range []genID{id1, id2, id3} {
		if !hexID.MatchString(id.String()) {
			t.Errorf("id %q is not 32 lowercase hex chars", id)
		}
	}
	// The backward clock must not regress the counter: it clamps to one past the last.
	if id3.prefix() != id2.prefix()+1 {
		t.Errorf("backward clock did not clamp: id2 prefix %d, id3 prefix %d", id2.prefix(), id3.prefix())
	}
	// The first prefix is the wall clock itself, and it round-trips through the encoding.
	if id1.prefix() != uint64(base.UnixNano()) {
		t.Errorf("first prefix = %d, want %d", id1.prefix(), uint64(base.UnixNano()))
	}
}

// TestGenStateString pins the rendering of every lifecycle state, including the consumed state
// store.Open reaches, so the set stays complete and named.
func TestGenStateString(t *testing.T) {
	cases := map[genState]string{
		genLive:      "live",
		genFinalized: "finalized",
		genConsumed:  "consumed",
		genState(99): "unknown",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Errorf("genState(%d).String() = %q, want %q", s, got, want)
		}
	}
}

// TestRegistryLeaseEviction proves the eviction rule directly: a handle survives as long as a lease
// is outstanding or it carries a generation, and is dropped exactly when neither holds.
func TestRegistryLeaseEviction(t *testing.T) {
	r := newRegistry()

	h := r.acquire("x")
	if h2 := r.acquire("x"); h2 != h {
		t.Fatal("acquire returned a different handle for the same name")
	}
	if h.leases != 2 {
		t.Fatalf("leases = %d, want 2", h.leases)
	}

	// One lease still outstanding: the empty handle must not be evicted.
	r.release(h)
	if !hasHandle(r, "x") {
		t.Fatal("handle evicted while a lease was still held")
	}

	// A generation present pins the handle even at zero leases.
	h.current = &generation{state: genFinalized}
	r.release(h)
	if !hasHandle(r, "x") {
		t.Fatal("handle with a current generation was evicted")
	}

	// Empty and unleased: now it goes. A fresh acquire/release of an empty handle evicts it.
	h.current = nil
	h2 := r.acquire("x")
	r.release(h2)
	if hasHandle(r, "x") {
		t.Fatal("empty, unleased handle was not evicted")
	}
}

// TestOrphanRace is the orphan-race stress: many goroutines pound a few shared names with the
// whole operation mix at once. Under the race detector it would catch an unsynchronized field or a
// lost generation; the final drain proves no handle leaks — every name, once deleted and quiesced,
// leaves the registry empty — and that the quota balances exactly, every reserve across the
// create/write/abort/delete churn matched by its release, back to zero.
func TestOrphanRace(t *testing.T) {
	s := newStore(memMedium{}, time.Now, Config{})
	ctx := context.Background()
	names := []string{"a", "b", "c"}
	meta := clip.Meta{Kind: clip.KindBytes}

	var wg sync.WaitGroup
	for g := range 24 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 50 {
				name := names[(g+i)%len(names)]
				switch (g + i) % 5 {
				case 0, 1: // create + finalize
					if w, err := s.Create(ctx, name, meta, PutOpts{}); err == nil {
						_, _ = w.Write([]byte("payload"))
						_ = w.Close()
					}
				case 2: // create + abort
					if w, err := s.Create(ctx, name, meta, PutOpts{}); err == nil {
						_, _ = w.Write([]byte("partial"))
						_ = w.Abort()
					}
				case 3: // open + drain
					if rc, _, err := s.Open(ctx, name, GetOpts{}); err == nil {
						_, _ = io.Copy(io.Discard, rc)
						_ = rc.Close()
					}
				case 4: // stat then delete
					_, _ = s.Stat(ctx, name)
					_ = s.Delete(ctx, name)
				}
			}
		}(g)
	}
	wg.Wait()

	// Drain: with every writer and reader closed, deleting each name must empty the registry.
	for _, name := range names {
		_ = s.Delete(ctx, name)
	}
	if n := handleCount(s.reg); n != 0 {
		t.Fatalf("registry leaked %d handles after drain", n)
	}
	if b, c := s.quota.bytes.Load(), s.quota.clips.Load(); b != 0 || c != 0 {
		t.Fatalf("quota leaked after drain: bytes=%d clips=%d, want 0/0", b, c)
	}
}

// errMedium is the fault the faulty medium injects; identity is all the tests match on.
var errMedium = errors.New("medium failure")

// faultyMedium is the medium analogue of the buffer's fake backing: it serves real in-memory
// buffers but can be told to fail any of the four lifecycle steps. It exercises the store's error
// handling, including the paths the infallible memory medium never reaches.
type faultyMedium struct {
	createErr          error
	finalizeErr        error
	claimErr           error
	claimCommitted     bool // when claimErr is set, whether the claim's irreversible step took effect
	unpublishErr       error
	unpublishCommitted bool // when unpublishErr is set, whether the retire's irreversible step took effect
}

func (m *faultyMedium) create(id genID) (*buffer.Buffer, error) {
	if m.createErr != nil {
		return nil, m.createErr
	}
	return buffer.NewMemory(), nil
}
func (m *faultyMedium) finalize(g *generation) (committed bool, err error) {
	if m.finalizeErr != nil {
		// The rename-never-took arm: the in-memory abort, identical for committed and !committed alike.
		// The committed arm's one distinct effect is the durable retire — observable on disk only, proven
		// by TestFinalizeAbortDurableNoResurrection — so it is not reached through this in-memory fake,
		// and faultyMedium earns no finalizeCommitted field the way claim and unpublish (whose arms
		// diverge in memory) earn theirs.
		return false, m.finalizeErr
	}
	return true, nil
}
func (m *faultyMedium) claim(g *generation) (committed bool, err error) {
	if m.claimErr != nil {
		return m.claimCommitted, m.claimErr
	}
	return true, nil
}
func (m *faultyMedium) unpublish(g *generation) (committed bool, err error) {
	if m.unpublishErr != nil {
		return m.unpublishCommitted, m.unpublishErr
	}
	return true, nil
}
func (m *faultyMedium) abortPublish(*generation) {} // never invoked here: finalize never reports committed
func (m *faultyMedium) remove(g *generation)     {}

// TestCreateFailureEvicts proves a medium that cannot make a home leaves no handle behind: the
// lease is released and the empty handle evicted, so a failed create never leaks.
func TestCreateFailureEvicts(t *testing.T) {
	s := newStore(&faultyMedium{createErr: errMedium}, time.Now, Config{})
	ctx := context.Background()

	_, err := s.Create(ctx, "x", clip.Meta{Kind: clip.KindBytes}, PutOpts{})
	if !errors.Is(err, errMedium) {
		t.Fatalf("Create error = %v, want wrap of %v", err, errMedium)
	}
	if n := handleCount(s.reg); n != 0 {
		t.Fatalf("create failure leaked %d handles", n)
	}
}

// TestCountCapRejectionEvicts proves a Create the clip-count cap refuses leaves no handle behind.
// The reserve fails only after the handle was acquired, so the count-cap path must run the same
// release-and-evict the create- and allocate-failure paths already rely on.
func TestCountCapRejectionEvicts(t *testing.T) {
	s := newStore(memMedium{}, time.Now, Config{MaxClips: 1})
	ctx := context.Background()

	finalize(t, s, "a", PutOpts{}, []byte("a")) // fills the single slot
	if _, err := s.Create(ctx, "b", clip.Meta{Kind: clip.KindBytes}, PutOpts{}); !errors.Is(err, clip.ErrNoSpace) {
		t.Fatalf("Create over the count cap = %v, want ErrNoSpace", err)
	}
	if hasHandle(s.reg, "b") {
		t.Error("a count-cap-rejected Create leaked its handle")
	}
}

// TestFinalizeFailureAborts proves a finalize that fails behaves exactly like an abort: the live
// generation is torn so a follower reads an aborted error rather than EOF, the handle is left empty
// and evicted, and Close returns the wrapped cause.
func TestFinalizeFailureAborts(t *testing.T) {
	s := newStore(&faultyMedium{finalizeErr: errMedium}, time.Now, Config{})
	ctx := context.Background()

	w, err := s.Create(ctx, "x", clip.Meta{Kind: clip.KindBytes}, PutOpts{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// A follower attached to the still-live generation must end torn, never clean.
	rc, _, err := s.Open(ctx, "x", GetOpts{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got := make(chan error, 1)
	go func() {
		_, readErr := io.ReadAll(rc)
		_ = rc.Close()
		got <- readErr
	}()

	if err := w.Close(); !errors.Is(err, errMedium) {
		t.Fatalf("Close error = %v, want wrap of %v", err, errMedium)
	}
	if readErr := <-got; !errors.Is(readErr, clip.ErrAborted) {
		t.Fatalf("follower error = %v, want %v", readErr, clip.ErrAborted)
	}
	if n := handleCount(s.reg); n != 0 {
		t.Fatalf("finalize failure leaked %d handles", n)
	}
}

// TestFinalizeFailureKeepsPrevious proves a failed replacement does not destroy the value it would
// have replaced: after a clean A and a finalize-failing B, the name still reads A.
func TestFinalizeFailureKeepsPrevious(t *testing.T) {
	m := &faultyMedium{}
	s := newStore(m, time.Now, Config{})
	ctx := context.Background()

	wA, err := s.Create(ctx, "x", clip.Meta{Kind: clip.KindBytes}, PutOpts{})
	if err != nil {
		t.Fatalf("Create A: %v", err)
	}
	if _, err := wA.Write([]byte("A-value")); err != nil {
		t.Fatalf("Write A: %v", err)
	}
	if err := wA.Close(); err != nil {
		t.Fatalf("Close A: %v", err)
	}

	m.finalizeErr = errMedium // arm the failure for B
	wB, err := s.Create(ctx, "x", clip.Meta{Kind: clip.KindBytes}, PutOpts{})
	if err != nil {
		t.Fatalf("Create B: %v", err)
	}
	if _, err := wB.Write([]byte("B-value")); err != nil {
		t.Fatalf("Write B: %v", err)
	}
	if err := wB.Close(); !errors.Is(err, errMedium) {
		t.Fatalf("Close B error = %v, want wrap of %v", err, errMedium)
	}

	rc, _, err := s.Open(ctx, "x", GetOpts{})
	if err != nil {
		t.Fatalf("Open after failed replace: %v", err)
	}
	data, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != "A-value" {
		t.Fatalf("after failed replace, read %q, want %q", data, "A-value")
	}
}

// TestClaimFailureRevert proves one of the two ways a consume-once claim can fail: a claim that
// never takes effect — its durable marker was not written, so it has no side effect — reverts the
// generation to finalized rather than stranding it as consumed. The Open fails with the wrapped
// cause, and the clip is left claimable for the next reader. (Its committed-but-undurable sibling
// is TestClaimFailureCommittedDestroys.) It exercises the path the infallible memory medium never
// reaches, through a medium that fails the claim step with no side effect.
func TestClaimFailureRevert(t *testing.T) {
	s := newStore(&faultyMedium{claimErr: errMedium}, time.Now, Config{})
	ctx := context.Background()

	w, err := s.Create(ctx, "secret", clip.Meta{Kind: clip.KindBytes}, PutOpts{ConsumeOnce: true})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, _, err := s.Open(ctx, "secret", GetOpts{}); !errors.Is(err, errMedium) {
		t.Fatalf("Open with a failing claim = %v, want wrap of %v", err, errMedium)
	}

	// The clip is reverted to finalized — still claimable — never stuck consumed.
	h := s.reg.acquire("secret")
	var state genState
	h.peek(func() { state = h.current.state })
	s.reg.release(h)
	if state != genFinalized {
		t.Errorf("after a failed claim, state = %v, want finalized (still claimable)", state)
	}
}

// TestClaimFailureCommittedDestroys proves the other way a consume-once claim can fail: the claim's
// irreversible step took effect but could not be made durable — the marker was written, its flush
// was not (committed, with an error). The clip can no longer resolve, so reverting it to claimable
// would strand a clip every later Open re-fails; instead the store destroys it in place. Open
// returns the wrapped cause, the quota is fully released, the handle is evicted, and a later Open
// finds nothing. At-most-once holds with zero delivery. This is the disk medium's rename-succeeded-
// but-fsync-failed window, reached deterministically here through a medium that reports a committed
// claim failure without the real fsync.
func TestClaimFailureCommittedDestroys(t *testing.T) {
	s := newStore(&faultyMedium{claimErr: errMedium, claimCommitted: true}, time.Now, Config{})
	ctx := context.Background()

	w, err := s.Create(ctx, "secret", clip.Meta{Kind: clip.KindBytes}, PutOpts{ConsumeOnce: true})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, _, err := s.Open(ctx, "secret", GetOpts{}); !errors.Is(err, errMedium) {
		t.Fatalf("Open with a committed-but-undurable claim = %v, want wrap of %v", err, errMedium)
	}

	if hasHandle(s.reg, "secret") {
		t.Error("destroy-in-place left the handle behind")
	}
	if b, c := s.quota.bytes.Load(), s.quota.clips.Load(); b != 0 || c != 0 {
		t.Errorf("quota after destroy-in-place: bytes=%d clips=%d, want 0/0", b, c)
	}
	if _, _, err := s.Open(ctx, "secret", GetOpts{}); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Open after destroy = %v, want ErrNotFound", err)
	}
}

// TestCommitReadDeclinesCanceledClaim pins the claim-time ctx guard directly: the irreversible
// finalized→consumed flip is declined when ctx is already canceled, so the one delivery is not
// spent on a reader gone. The window it guards — a cancel landing after await's post-park check
// but before the commit — is synchronous and unreachable through public Open, whose entry guard
// catches a pre- canceled ctx before commitRead ever runs; so this drives commitRead directly. The
// guard returns before any state mutation, so the bare call (no gate hold) has nothing to serialize
// against, and it is medium-agnostic — a ctx check before any medium call — so the memory medium
// covers it. The non- trivial half is the assertion that the delivery survived: a live Open still
// claims and reads it.
func TestCommitReadDeclinesCanceledClaim(t *testing.T) {
	s := newStore(memMedium{}, time.Now, Config{})
	finalize(t, s, "secret", PutOpts{ConsumeOnce: true}, []byte("payload"))

	h := s.reg.acquire("secret")
	var g *generation
	h.peek(func() { g = h.current })

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	res, wake, err := s.commitRead(canceled, h, g, "secret")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("commitRead with a canceled ctx = %v, want context.Canceled", err)
	}
	if wake {
		t.Error("a declined claim moved nothing, yet reported a wake")
	}
	if res.rc != nil || res.cleanup != nil {
		t.Error("a declined claim returned a reader or a cleanup obligation, want neither")
	}
	if g.state != genFinalized {
		t.Errorf("after a declined claim, state = %v, want finalized (no flip, no claim)", g.state)
	}
	s.reg.release(h)

	// The delivery was preserved: a live Open still claims the secret and reads it in full.
	rc, _, err := s.Open(context.Background(), "secret", GetOpts{})
	if err != nil {
		t.Fatalf("Open after a declined claim = %v, want the secret still claimable", err)
	}
	defer rc.Close()
	if data, err := io.ReadAll(rc); err != nil {
		t.Fatalf("ReadAll: %v", err)
	} else if string(data) != "payload" {
		t.Errorf("read %q, want payload (the declined claim must leave the secret intact)", data)
	}
}

// TestDeleteUnpublishFailRevert proves one of the two ways a durable delete can fail: the retire's
// rename never takes effect — no on-disk side effect — so the clip is left standing rather than
// reported gone. Delete returns the wrapped cause, current is still the finalized generation (Stat
// finds it), and the quota is unchanged. It is the delete twin of TestClaimFailureRevert, reaching
// through a faulty medium the !committed arm the infallible memory medium never can.
func TestDeleteUnpublishFailRevert(t *testing.T) {
	s := newStore(&faultyMedium{unpublishErr: errMedium}, time.Now, Config{})
	ctx := context.Background()

	finalize(t, s, "doc", PutOpts{}, []byte("payload"))

	if err := s.Delete(ctx, "doc"); !errors.Is(err, errMedium) {
		t.Fatalf("Delete with a failing unpublish = %v, want wrap of %v", err, errMedium)
	}

	// The clip is left standing — still finalized, still readable — never reported gone, and its
	// footprint is untouched because nothing was reclaimed.
	if _, err := s.Stat(ctx, "doc"); err != nil {
		t.Errorf("after a failed retire, Stat = %v, want the clip still present", err)
	}
	if b, c := s.quota.bytes.Load(), s.quota.clips.Load(); b != int64(len("payload")) || c != 1 {
		t.Errorf("quota after a failed retire: bytes=%d clips=%d, want %d/1", b, c, len("payload"))
	}
}

// TestDeleteUnpublishCommittedRetires proves the other way a durable delete can fail: the retire's
// rename took effect but could not be flushed (committed, with an error). meta.json is already
// gone, so the clip can no longer resurrect; the store clears current and reclaims rather than
// reverting to a readable state disk no longer backs. Delete still returns the wrapped cause — the
// operation was not made durable on its own terms — yet the handle is evicted, the quota released,
// and a later Open finds nothing. It is the delete twin of TestClaimFailureCommittedDestroys.
func TestDeleteUnpublishCommittedRetires(t *testing.T) {
	s := newStore(&faultyMedium{unpublishErr: errMedium, unpublishCommitted: true}, time.Now, Config{})
	ctx := context.Background()

	finalize(t, s, "doc", PutOpts{}, []byte("payload"))

	if err := s.Delete(ctx, "doc"); !errors.Is(err, errMedium) {
		t.Fatalf("Delete with a committed-but-undurable retire = %v, want wrap of %v", err, errMedium)
	}

	if hasHandle(s.reg, "doc") {
		t.Error("a committed retire left the handle behind")
	}
	if b, c := s.quota.bytes.Load(), s.quota.clips.Load(); b != 0 || c != 0 {
		t.Errorf("quota after a committed retire: bytes=%d clips=%d, want 0/0", b, c)
	}
	if _, _, err := s.Open(ctx, "doc", GetOpts{}); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Open after a committed retire = %v, want ErrNotFound", err)
	}
}

// TestRegistrySnapshotDoesNotPinRegistry proves snapshot's load-bearing property directly: it
// copies the handle set under registry.mu and releases the lock before the caller touches any
// handle, so a slow per-handle operation cannot stall the registry. With one handle's mutex held
// — standing in for a create or claim mid-fsync — snapshot still returns at once and an operation
// on another name acquires its handle without waiting, the cross-name non-contention the leased
// registry promises. The lock order holds: snapshot takes handle.mu nowhere, and the caller takes
// it only after registry.mu is released.
func TestRegistrySnapshotDoesNotPinRegistry(t *testing.T) {
	r := newRegistry()
	slow := r.acquire("slow")
	slow.mu.Lock() // a per-handle op (a create/claim fsync) holding its lock for the duration

	got := r.snapshot()
	if len(got) != 1 || got[0] != slow {
		t.Fatalf("snapshot = %v, want exactly the one held handle", got)
	}

	// registry.mu must be free now: another name's acquire/release completes while slow.mu is held.
	done := make(chan struct{})
	go func() {
		r.release(r.acquire("other"))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("acquire on another name stalled while a handle lock was held — snapshot pinned registry.mu")
	}

	slow.mu.Unlock()
	r.release(slow)
}

// blockingMedium wraps memMedium and parks the first create it sees until released. The store
// calls create under that name's handle lock, so while the create is parked that lock is held —
// exactly the shape the disk medium's makeGenDir mkdir+fsync (and a claim's rename+fsync) has under
// handle.mu. It lets a test hold one handle's lock across a slow operation and prove other names
// proceed.
//
// The medium is name-agnostic — a generation's home is keyed by its id alone — so it cannot select
// which create to park by name; instead it parks the first, and a test that wants one specific name
// parked simply issues that create before any other. sync.Once both signals entered and blocks, so
// exactly the first create parks and a stray second one would not deadlock waiting on release.
type blockingMedium struct {
	memMedium
	entered chan struct{} // closed when the parked create enters — its handle lock is held by then
	release chan struct{} // the parked create returns once this is closed
	once    sync.Once     // parks the first create only
}

func (m *blockingMedium) create(id genID) (*buffer.Buffer, error) {
	m.once.Do(func() {
		close(m.entered)
		<-m.release
	})
	return m.memMedium.create(id)
}

// TestSlowCreateDoesNotStallOtherNames is the executable form of "operations on different names
// never contend," on the disk medium's terms — the property F1 found advertised but not delivered,
// and the test it lacked. A create on one name parks while holding that name's handle lock, a burst
// of List walks reach across the locked handle, and a Stat on a different name must still complete
// promptly. Against the prior each(), a List that reached the parked handle held registry.mu while
// blocked on that handle's lock, so the Stat — needing registry.mu to acquire its own handle —
// stalled behind it for the whole slow create. Snapshotting the handle set releases registry.mu
// before any handle lock, so the walks block only on the one handle and the Stat proceeds. Run
// under -race.
func TestSlowCreateDoesNotStallOtherNames(t *testing.T) {
	bm := &blockingMedium{entered: make(chan struct{}), release: make(chan struct{})}
	s := newStore(bm, time.Now, Config{})
	ctx := context.Background()

	// A create on "slow" parks under its handle lock, holding it for the whole test.
	createDone := make(chan Writer, 1)
	go func() {
		w, err := s.Create(ctx, "slow", clip.Meta{Kind: clip.KindBytes}, PutOpts{})
		if err != nil {
			t.Errorf("parked Create: %v", err)
		}
		createDone <- w
	}()
	<-bm.entered // "slow" is now in the registry with its handle lock held

	// A burst of concurrent List walks, each forced to reach the locked handle. Under the prior each()
	// one of them pins registry.mu while blocked on slow's lock; under snapshot none does.
	var walkers sync.WaitGroup
	for range 8 {
		walkers.Go(func() { _, _ = s.List(ctx) })
	}

	// The probe: a Stat on a different name needs only registry.mu (to acquire its handle), nothing
	// "slow" holds. It must complete while "slow" is parked — proving no walk pinned registry.mu.
	probed := make(chan struct{})
	go func() {
		_, _ = s.Stat(ctx, "other")
		close(probed)
	}()
	select {
	case <-probed:
	case <-time.After(2 * time.Second):
		t.Fatal("Stat on another name stalled behind a parked create — registry.mu pinned across a handle lock")
	}

	close(bm.release) // let the parked create finish; the walks drain past the now-unlocked handle
	walkers.Wait()
	if w := <-createDone; w != nil {
		_ = w.Abort() // the generation was never written; discard it so the handle evicts cleanly
	}
}

// TestDeleteRacingFinalize pins the prev-identity flip Delete and a finalizing replacement contend
// over. Delete reads prev := h.current and a finalizing Close reads its own prev and flips current;
// the handle lock serialises them into a deterministic last-writer-wins, so the two orderings
// are logical, not racy, and the test drives each explicitly. Either way the original is released
// exactly once — never twice (quota underflow) — and current ends pointing at whichever operation
// ran last.
func TestDeleteRacingFinalize(t *testing.T) {
	ctx := context.Background()
	// setup gives a name a finalized v1 and a live, written-but-unclosed replacement v2 — the two
	// generations Delete and Close fight over. Create succeeds despite v1 because v1 is finalized, not
	// live, so the name admits a new live generation beside it.
	setup := func(t *testing.T) (*store, Writer) {
		t.Helper()
		s := newStore(memMedium{}, time.Now, Config{})
		finalize(t, s, "k", PutOpts{}, []byte("v1"))
		w, err := s.Create(ctx, "k", clip.Meta{Kind: clip.KindBytes}, PutOpts{})
		if err != nil {
			t.Fatalf("Create replacement: %v", err)
		}
		if _, err := w.Write([]byte("vv2")); err != nil {
			t.Fatalf("Write replacement: %v", err)
		}
		return s, w
	}

	t.Run("delete then close installs the replacement", func(t *testing.T) {
		s, w := setup(t)
		if err := s.Delete(ctx, "k"); err != nil { // drops v1, releasing its quota
			t.Fatalf("Delete: %v", err)
		}
		if err := w.Close(); err != nil { // installs v2 over the now-empty current
			t.Fatalf("Close: %v", err)
		}
		assertContent(t, s, "k", "vv2")         // the replacement stands as current
		assertQuota(t, s, int64(len("vv2")), 1) // v1 released once by Delete, only v2 remains
	})

	t.Run("close then delete drops both", func(t *testing.T) {
		s, w := setup(t)
		if err := w.Close(); err != nil { // installs v2 and reclaims v1
			t.Fatalf("Close: %v", err)
		}
		if err := s.Delete(ctx, "k"); err != nil { // then drops the installed v2
			t.Fatalf("Delete: %v", err)
		}
		if _, _, err := s.Open(ctx, "k", GetOpts{}); !errors.Is(err, clip.ErrNotFound) {
			t.Errorf("Open = %v, want ErrNotFound (delete cleared the installed replacement)", err)
		}
		assertQuota(t, s, 0, 0) // v1 released by Close, v2 by Delete — each exactly once
		if n := handleCount(s.reg); n != 0 {
			t.Errorf("registry holds %d handles, want 0 (both generations gone)", n)
		}
	})
}

// TestSupersedeConsumedWhileDraining pins the split ownership of a consumed generation's
// reclamation. When a replacement finalizes over a consume-once generation whose reader is still
// draining, the supersede must skip reclaiming that consumed prev (the prevConsumed branch in
// Close) and leave it to the reader's Close (cleanupConsumed, guarded by current == g). The two run
// in either order under the handle lock; both must release the consumed generation exactly once and
// leave the replacement standing. Drop either guard and this double-releases (quota underflow) or
// clears a live current.
func TestSupersedeConsumedWhileDraining(t *testing.T) {
	ctx := context.Background()
	// setup finalizes a consume-once v1, claims it with an open reader (so v1 is consumed with its
	// bytes pinned), then creates a live, written-but-unclosed plain replacement v2.
	setup := func(t *testing.T) (*store, io.ReadCloser, Writer) {
		t.Helper()
		s := newStore(memMedium{}, time.Now, Config{})
		finalize(t, s, "secret", PutOpts{ConsumeOnce: true}, []byte("v1"))
		reader, _, err := s.Open(ctx, "secret", GetOpts{}) // claims v1: now consumed, bytes pinned
		if err != nil {
			t.Fatalf("Open (claim): %v", err)
		}
		w, err := s.Create(ctx, "secret", clip.Meta{Kind: clip.KindBytes}, PutOpts{})
		if err != nil {
			t.Fatalf("Create replacement: %v", err)
		}
		if _, err := w.Write([]byte("vv2")); err != nil {
			t.Fatalf("Write replacement: %v", err)
		}
		return s, reader, w
	}

	t.Run("supersede then reader close", func(t *testing.T) {
		s, reader, w := setup(t)
		if err := w.Close(); err != nil { // supersede: skips the consumed prev, installs v2
			t.Fatalf("Close: %v", err)
		}
		if data, err := io.ReadAll(reader); err != nil || string(data) != "v1" {
			t.Fatalf("drained reader = %q (err %v), want v1 — the consumed bytes outlive the supersede", data, err)
		}
		if err := reader.Close(); err != nil { // releases the consumed v1, leaves v2 (current != v1)
			t.Fatalf("reader Close: %v", err)
		}
		assertContent(t, s, "secret", "vv2")
		assertQuota(t, s, int64(len("vv2")), 1) // v1 released once by the reader, v2 stands
	})

	t.Run("reader close then supersede", func(t *testing.T) {
		s, reader, w := setup(t)
		if data, err := io.ReadAll(reader); err != nil || string(data) != "v1" {
			t.Fatalf("drained reader = %q (err %v), want v1", data, err)
		}
		if err := reader.Close(); err != nil { // releases the consumed v1 and clears current (current == v1)
			t.Fatalf("reader Close: %v", err)
		}
		if err := w.Close(); err != nil { // installs v2 over the cleared current, reclaiming nothing
			t.Fatalf("Close: %v", err)
		}
		assertContent(t, s, "secret", "vv2")
		assertQuota(t, s, int64(len("vv2")), 1) // v1 released once by the reader, v2 never reclaimed
	})
}
