package store

import (
	"context"
	"sync"
)

// gate is a clipHandle's lifecycle coordinator: the lock that serializes work on one name, the
// close-and-replace notify channel that lock guards, and the only ways that lock is ever taken.
// peek reads under it; transition mutates under it and wakes, or transitionResult does the same
// while handing a result back to the caller; await parks under it until the name becomes readable
// and then commits under the same hold. Promoting the lock into a type with exactly these roles
// is what makes the wake structural: wakeLocked is gate-private, fired only by transition,
// transitionResult, and await, so a readable-state change cannot escape without a wake and nothing
// outside the gate can fire a stray one. The hand-written "remember to wake" that was smeared
// across every mutator becomes one owner with one job.
//
// The gate is content-agnostic — it names no generation and knows nothing of the resolution
// rule. The state it coordinates (current, live, the id seed) lives on the handle and is touched
// only inside the closures peek/transition/transitionResult/await run under the lock — save once
// at single-threaded startup, where restore seeds a recovered name's fields directly, with no
// goroutine yet serving to contend with or to wake. That agnosticism is deliberate: it lets the
// gate be proven in isolation against a fake predicate exactly as the followable buffer is proven
// against a fake backing, and it is why await and transitionResult are free functions generic over
// their result rather than methods, Go forbidding generic methods. The cost of guarding foreign
// fields is that one discipline cannot be made structural — a closure must touch only handle
// state, leaf-lock quota ops, and the bounded medium IO that already ran under this lock, never
// registry.mu (which sits above the gate in the lock hierarchy) nor a re-entry of the gate itself.
// Those two fail unalike, and the dangerous one is quiet: a re-entered gate self-deadlocks on the
// spot, every run, caught by any test that walks the path, while a misplaced registry.mu inverts
// the hierarchy into a deadlock that surfaces only under contention — so it is the residue a new
// mutator's author must consciously guard, the loud one advertising itself. The wake, the part
// every mutator used to get wrong, now cannot be.
//
// It is the inter-generation twin of the buffer's notifier (store/internal/buffer): that one wakes
// a follower when one generation's byte log grows or ends, this one wakes a waiter when a name's
// set of generations changes — the same mechanism one scale out. The two are mirrored, not shared,
// because the state each backs differs in kind. The buffer's is a strictly monotonic, guaranteed-
// to-terminate byte log, so a follower is always eventually woken and its three mutators wake
// unconditionally; this handle's is a non-monotonic set of pointers a write may never populate,
// so a waiter's only guaranteed unblock is ctx-cancel, and its mutators wake conditionally — only
// when readable state actually moved. transition's bool is that mirror adjusted for a domain with
// conditional transitions: an always-wake form would broadcast on every rejected admission and
// every no-op guard, measurably worse churn on the semi-hot reject paths, for a wakeup no waiter is
// parked to receive.
type gate struct {
	mu     sync.Mutex
	notify chan struct{} // closed-and-replaced under mu on every readable transition; guarded by mu
}

// newGate returns a gate with its notifier armed. wakeLocked closes-and-replaces notify on each
// transition, and a nil channel would panic on the first one, so every gate must be born armed.
// clipHandle embeds the gate by value and constructs it through this one site — the inter-
// generation mirror of buffer.newBuffer arming every Buffer's notifier through one site, so "every
// gate is armed" is structural rather than a constructor's obligation to remember. Returning by
// value is copylocks- clean: a freshly minted gate is moved into the handle once, before any lock
// is ever taken on it, and never copied afterward.
func newGate() gate { return gate{notify: make(chan struct{})} }

// wakeLocked releases every waiter parked on this name's lifecycle and arms the next wait. Closing
// the current notify channel wakes all waiters at once — a single send would wake only one and
// strand the rest — and a fresh channel arms the next wait. The caller holds mu, the same lock
// under which a waiter captures notify and reads the handle's state; that shared lock is what makes
// a wakeup impossible to lose between a waiter resolving the read state and beginning to wait. It
// is private to the gate by intent: only transition and await reach it, so "every readable change
// wakes" needs no per-site vigilance and no caller can fire a wake the gate did not sanction.
func (g *gate) wakeLocked() {
	close(g.notify)
	g.notify = make(chan struct{})
}

// peek runs fn under the lock and wakes nothing — the read role, for an op that only snapshots the
// handle's state: a stat, a list, a writer's header view, the reaper's candidate scan, a release's
// emptiness check. It exists so those sites need not lock the promoted mu directly, which would
// puncture the gate's ownership of the lock; with peek, every critical section in the store is
// exactly one of peek/transition/transitionResult/await and mu is taken nowhere else.
func (g *gate) peek(fn func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	fn()
}

// transition runs fn under the lock and wakes iff fn reports the readable state moved — the
// void mutate-and-wake role. fn returns true when it changed what a read resolves (an install,
// a finalize flip, a cleared pointer) and false when it touched nothing observable (a rejected
// admission, a guard whose target was already gone, a medium rename that never took). The gate
// owns the wake, so a mutator cannot forget it and cannot fire a stray one; it can only declare
// the bool, which is true on every genuine change and false only on a provable no-op — and the
// dangerous direction, returning false when state changed, is the unnatural one to write. This is
// the form for a mutator whose only output is the wake; one that must also hand a result back to
// its caller uses transitionResult, so a result never has to be smuggled out through a captured
// local.
func (g *gate) transition(fn func() bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if fn() {
		g.wakeLocked()
	}
}

// transitionResult is transition's typed sibling: it runs fn under the lock, wakes iff fn reports
// the readable state moved, and returns fn's result and error to the caller. It is the mutate role
// for an op that must hand something back — the generation it installed, the superseded one to
// reclaim off the lock, a rejection error — where the void transition would force that result out
// through a captured local. fn declares the same three independent outputs await's commit does: a
// result, a wake, and an error, none derived from another. The wake is honored before the unlock
// and regardless of the error, so a mutator that both moved readable state and then failed still
// wakes — the mutate role reporting a transition exactly as the wait-then-commit role does. It is
// await without the wait, and a free function for the same reason: Go forbids generic methods. The
// one closure convention the gate doc names binds fn here exactly as it binds a transition closure.
func transitionResult[T any](g *gate, fn func() (T, bool, error)) (T, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	t, wake, err := fn()
	if wake {
		g.wakeLocked()
	}
	return t, err
}

// await is the resolve-or-wait role: park until the name becomes readable, then commit under the
// same uninterrupted hold the successful resolve ran under. It is the one path that both waits
// and mutates, and the hold must not break between them — a consume-once claim's atomicity depends
// on no second reader slipping in between the resolve that picks a generation and the commit that
// claims it, so the claim lives in commit, run inside await, rather than back in the caller after
// a park.
//
// resolve runs under the lock and yields the readable generation or an error. waitable classifies
// a resolve error: a still-not-here-yet error means park and re-resolve on the next wake; anything
// else (a consumed clip, say) surfaces at once — the same "gone" a non-waiting caller gets. commit
// runs on a successful resolve, under the SAME hold, and returns its result, a wake bool, and an
// error. The wake is honored before unlock and INDEPENDENT of that error: a commit may legitimately
// return (result, wake=true, err≠nil) — a claim that stuck (a real finalized→consumed flip that
// must wake) and then failed to open its reader — so await fires the wake, then surfaces the error.
// The notify capture is the no-lost-wakeup hinge: it reads notify under the very lock acquisition
// that ran resolve, so a wake landing after the resolve but before the park closes the exact
// channel about to be selected on and cannot be missed.
//
// It is a free function, not a method, because Go forbids generic methods and the gate must stay
// agnostic over the resolved value R and the result T.
func await[R, T any](g *gate, ctx context.Context,
	resolve func() (R, error),
	waitable func(error) bool,
	commit func(R) (T, bool, error),
) (T, error) {
	var zero T
	for {
		g.mu.Lock()
		r, err := resolve()
		if err != nil {
			if waitable(err) {
				notify := g.notify // captured under the SAME lock as resolve — the no-lost-wakeup hinge
				g.mu.Unlock()
				select {
				case <-notify: // a transition landed — re-resolve
				case <-ctx.Done():
				}
				// One ctx check after the select covers a pure cancel and a wake that raced a cancel alike: a
				// select with both ready picks at random, so without this a request canceled during a finalize
				// could re-resolve and claim a consume-once secret for a client already gone.
				if cerr := ctx.Err(); cerr != nil {
					return zero, cerr
				}
				continue
			}
			g.mu.Unlock()
			return zero, err
		}
		t, wake, cerr := commit(r)
		if wake {
			g.wakeLocked()
		}
		g.mu.Unlock()
		return t, cerr
	}
}
