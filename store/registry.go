package store

import "sync"

// registry is the store's map of live, in-use clip names to their handles. It exists to satisfy two
// demands at once: every operation on a name must be serialized, and the map must not grow without
// bound as names come and go. The map is bounded by the number of names with a generation or an in-
// flight operation right now, never by historical traffic, because a handle is dropped the moment
// it is both idle and empty.
//
// The hazard that shapes the design is eviction racing acquisition. Without a guard, a reaper could
// drop a handle as empty in the very window after a Create took the handle but before it installed
// its generation, stranding that generation where no later lookup would find it. The lease counter
// closes that window: a handle is evicted only when no operation holds a lease on it and it carries
// no generation.
//
// Lock hierarchy, held as an invariant across the whole store: registry.mu is always taken before
// any handle.mu, and never the reverse. The registry uses a plain Mutex — lease churn writes the
// counter on every acquire and release, so there is no read-mostly path a RWMutex would help.
// release is the one site that nests handle.mu inside registry.mu, in that canonical order, so it
// cannot deadlock against any handle-only critical section.
type registry struct {
	mu      sync.Mutex
	handles map[string]*clipHandle
}

// newRegistry returns an empty registry ready to acquire handles.
func newRegistry() *registry {
	return &registry{handles: make(map[string]*clipHandle)}
}

// clipHandle is the per-name coordination point: one handle per name with an operation outstanding,
// holding that name's generations and the gate that serializes work on them.
//
// Two locks guard its fields, deliberately. leases is guarded by the registry's mutex, because
// acquisition and eviction are registry-wide decisions made while that mutex is held. Everything
// else — the current and live generation pointers and the monotonic id seed — is guarded by the
// embedded gate's mutex, so that operations on different names never contend, and operations on one
// name serialize without touching the registry. The gate owns that mutex and the lifecycle notifier
// together and exposes the only three ways the lock is taken (peek/transition/await); the handle
// holds the state the gate coordinates but never reaches for the lock itself.
type clipHandle struct {
	gate                   // serializes work on this name and wakes its waiters; guards the fields below
	name       string      // the clip's logical name; set once at creation, never a path component
	leases     int         // guarded by registry.mu; >0 pins the handle against eviction
	current    *generation // the readable finalized generation, or nil; guarded by gate.mu
	live       *generation // the single in-flight generation, or nil; guarded by gate.mu
	lastPrefix uint64      // monotonic id seed for this name; guarded by gate.mu
}

// newHandle is the one place a clipHandle is built, so every handle is born through newGate with
// its notifier armed — production and the white-box tests alike — making "every clipHandle has an
// armed notifier" a structural fact rather than something each call site must remember. It is the
// inter-generation mirror of buffer.newBuffer, which arms every Buffer's notifier through one
// site for the same reason. The wake mechanism, its liveness asymmetry against the buffer, and the
// conditional-wake discipline now live with the gate that owns them.
func newHandle(name string) *clipHandle {
	return &clipHandle{name: name, gate: newGate()}
}

// acquire returns the handle for name, creating it if absent, with a lease taken. While the caller
// holds that lease the handle cannot be evicted, whatever its generations — which is what lets a
// Create work on a handle across the window before it installs a generation, and lets an Open keep
// a handle alive across a lock-free stream that outlives the handle lock.
func (r *registry) acquire(name string) *clipHandle {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := r.handles[name]
	if h == nil {
		h = newHandle(name) // the registry's one creation path; newHandle arms the notifier
		r.handles[name] = h
	}
	h.leases++
	return h
}

// snapshot copies the current set of handle pointers under registry.mu, then releases the lock
// before returning — so a caller locks each handle with registry.mu NOT held. That decoupling
// is load-bearing. The operations that run under handle.mu include a first-generation create's
// mkdir+fsync chain and a consume-once claim's rename+fsync; holding registry.mu across a walk that
// locked one of those handles would block every acquire and release — the start of every operation
// on every name — behind that one slow disk op. Snapshotting the set and locking each handle off
// the registry lock is what keeps "operations on different names never contend" true on disk, not
// only on the memory medium where create and claim are instant.
//
// The pointers are stable: a handle's address never changes once created, so a caller reads
// its fields under the handle's own mutex regardless of how the pointer was obtained. A handle
// evicted between the snapshot and the caller's lock is harmless — eviction requires it be empty
// (no current, no live), so the caller reads nil from both and skips it. It is the read-only set
// the store's List and the reaper's candidate sweep each walk; the lock order (registry.mu before
// handle.mu) is honoured because registry.mu is already released before any handle.mu is taken.
func (r *registry) snapshot() []*clipHandle {
	r.mu.Lock()
	defer r.mu.Unlock()
	hs := make([]*clipHandle, 0, len(r.handles))
	for _, h := range r.handles {
		hs = append(hs, h)
	}
	return hs
}

// release drops one lease and evicts the handle if that was the last lease and the handle carries
// no generation. The emptiness check runs under a nested gate.peek, taken inside registry.mu in
// the canonical order; the map delete stays in release proper, outside the peek, because mutating
// the registry is a registry concern and the peek is a pure handle-state reader. Splitting the
// old single critical section into "peek the bool, then delete" is safe because registry.mu spans
// both: every mutator must lease the handle first (which needs registry.mu), so while release holds
// registry.mu across the peek and the delete, no mutator can interleave to change current or live.
//
// Why the read of current and live is race-free is load-bearing, so state it exactly. Every mutator
// of those fields — Create, the Close flip, discard, Delete and the reaper's retire, and a consumed
// clip's cleanup — holds a lease across the mutation and releases it only afterwards. So when this
// call drives the lease count to zero, no mutator is or can be in flight: mutual exclusion here
// comes from that lease invariant, not from the gate. registry.mu then supplies the visibility —
// the last mutator's write precedes its own release, which precedes this decrement — so the read
// sees the final values. The nested peek is kept regardless of that argument: it makes the read
// locally, obviously correct, and stays safe even if some future mutator path were to touch the
// handle without first leasing it.
func (r *registry) release(h *clipHandle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h.leases--
	if h.leases == 0 {
		empty := false
		h.peek(func() { empty = h.current == nil && h.live == nil })
		if empty {
			delete(r.handles, h.name)
		}
	}
}
