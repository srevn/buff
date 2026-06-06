package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/srevn/buff/api"
	"github.com/srevn/buff/store"
)

// drainTimeout is the default for runtime.drainTimeout: how long graceful shutdown waits for in-
// flight finalized work before it forces the remaining connections closed. It is a constant, not
// a configuration knob — the configuration surface names no shutdown-grace variable, and inventing
// one would silently extend it. The per-runtime field it seeds is an internal seam a test can
// shorten to drive the forced-close path, and the single place a future BUFF_DRAIN_TIMEOUT would
// feed if a demonstrated need ever arrived.
const drainTimeout = 15 * time.Second

// runtime is one running server: the tuned HTTP server, the bound listener, the store it relays to,
// the data root that store writes through, the reaper interval, the graceful-drain window, and the
// logger. newRuntime builds it with every fallible step already done — directory opened, recovery
// replayed, port bound — so Run is pure scheduling and Addr is observable before Run, which is what
// lets a test bind an ephemeral port and drive the whole stack.
type runtime struct {
	httpSrv      *http.Server
	listener     net.Listener
	store        store.Store
	root         *os.Root
	reapInterval time.Duration
	drainTimeout time.Duration // graceful-drain window; seeded from the drainTimeout default, shortenable by a test
	log          *slog.Logger
}

// newRuntime performs all of serving's fallible setup and returns a runtime ready to Run, or the
// first error that setup cannot proceed past. It creates and opens the data directory, constructs
// the disk store (which replays recovery before returning), builds the HTTP edge over it, and
// binds the listener — binding here, not in Run, so a port clash fails synchronously and the
// chosen port is known before the run loop starts. Once the root is open a single disarm-on-success
// cleanup closes it on every error path, so the no-leak-on-failed-construction guarantee holds by
// construction rather than by remembering to close on each branch.
func newRuntime(c config, log *slog.Logger) (_ *runtime, err error) {
	// The data directory is the os.Root boundary itself — operator configuration, never a request-
	// influenced name — so it is created and opened with plain os. That is the one correct place
	// outside the root for raw filesystem access; the all-IO-through-os.Root invariant governs names
	// inside the root, not the root's own location.
	if err := os.MkdirAll(c.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("buff: data dir: %w", err)
	}
	root, err := os.OpenRoot(c.DataDir)
	if err != nil {
		return nil, fmt.Errorf("buff: open data dir: %w", err)
	}
	// The root is open, so every error from here must release it. One disarm-on-success cleanup,
	// keyed off the named return, closes it on whichever branch fails — a future fallible step cannot
	// reintroduce the descriptor leak by forgetting its own close. On success err is nil and this is a
	// no-op: the runtime then owns the root, closed once by Run or by Close.
	defer func() {
		if err != nil {
			_ = root.Close()
		}
	}()
	// NewDisk runs recovery (scan + restore) before it returns, so an error here is a boot the store
	// cannot run on — an unreadable or unpreparable root. A single corrupt generation never reaches
	// it: recovery isolates and quarantines that, so one bad clip cannot brick the boot.
	st, err := store.NewDisk(root, c.storeConfig(), c.diskOpts(log))
	if err != nil {
		return nil, err
	}
	srv := api.New(st, c.apiOptions(log))
	ln, err := net.Listen("tcp", c.Addr)
	if err != nil {
		return nil, fmt.Errorf("buff: listen %s: %w", c.Addr, err)
	}
	return &runtime{
		httpSrv:      srv.HTTPServer(c.Addr),
		listener:     ln,
		store:        st,
		root:         root,
		reapInterval: c.ReapInterval,
		drainTimeout: drainTimeout,
		log:          log,
	}, nil
}

// Addr reports the address the listener actually bound, resolving an ephemeral :0 to its chosen
// port. A caller reads it after newRuntime and before Run to learn where to connect.
func (rt *runtime) Addr() net.Addr { return rt.listener.Addr() }

// Close releases the listener and the data root that newRuntime acquired — the symmetric teardown
// for a runtime built but, on some early-return path, never Run. Run otherwise closes both itself
// (the listener when Serve returns, the root via its own defer), so a Close after a completed Run
// is a harmless second close: its already-closed errors are not real faults and are dropped, and
// only a genuine close error — possible solely on the first close — is returned. That idempotence
// is what lets a construction site defer Close unconditionally without double-faulting against
// Run's own teardown.
func (rt *runtime) Close() error {
	return errors.Join(ignoreClosed(rt.listener.Close()), ignoreClosed(rt.root.Close()))
}

// ignoreClosed drops the "already closed" error a redundant second close returns — net.ErrClosed
// from the listener, fs.ErrClosed from the data root — so a best-effort Close after Run reports
// success rather than a teardown that already happened. Any other error is a real fault and passes
// through untouched.
func ignoreClosed(err error) error {
	if errors.Is(err, net.ErrClosed) || errors.Is(err, os.ErrClosed) {
		return nil
	}
	return err
}

// Run serves until ctx is cancelled or a fatal fault stops it, then drains and returns. A clean
// signal-triggered stop returns nil; a real Serve or listen fault returns through the group. Two
// concerns errgroup.WithContext would fuse are kept apart on purpose: an errgroup cancels its own
// context with the first sibling's error as the cause — the right answer to "why did the group
// stop", but the wrong answer to the question a handler asks of its own context, "did the server
// stop me, or did the client vanish?", which must read 503 and is never the client's to answer for.
// So the request context hangs off an owned stopCtx whose cause this layer controls, and the group
// is a plain join that only collects the first fault for the exit code.
func (rt *runtime) Run(ctx context.Context) error {
	// Close the data root once the server has fully stopped. A read descriptor a handler opened
	// through the root is an independent *os.File, not a child of the root descriptor, so a reader
	// still draining one survives this close, and no new store operation can begin because the server
	// has stopped accepting. The single residue is on the forced-close path: a consume-once reader's
	// deferred reclamation may run its root.RemoveAll after this close and no-op with ErrClosed —
	// never a panic, as os.Root reports a closed root as an error — and the next boot's recovery
	// reclaims that orphaned claim marker. So the close is safe either way, and it matters for an
	// embedding or a test that builds many runtimes in one process, where leaked root descriptors
	// would accumulate.
	defer rt.root.Close()

	// stopCtx is the parent of every request context and the one place a request's cancellation cause
	// is set — always ErrServerStopping, whichever event begins the stop. A delivered signal cancels
	// the root with that cause and stopCtx inherits it; a fatal Serve fault has the serve goroutine
	// set it below. Keeping the cause off the group's own context is the whole point: were BaseContext
	// the group context, an errgroup would stamp the first sibling's error as the cause onto every
	// in-flight request context synchronously, the instant Serve faulted — and a context's cause is
	// write-once, so no later cancel could correct it, billing the server's own fault to the client as
	// a 400. This single field is still the whole of the selective shutdown: a live follower watches
	// this context and unwinds at once, while a finalized read or a consume delivery watches none
	// and keeps draining under Shutdown. No second cancel tree; the read-path framing supplies the
	// selectivity.
	stopCtx, beginStop := context.WithCancelCause(ctx)
	// vet's lostcancel guard, and a no-op for the cause in practice: it runs after Wait, which returns
	// only once the watcher has — and the watcher returns only after stopCtx is already cancelled.
	defer beginStop(nil)
	rt.httpSrv.BaseContext = func(net.Listener) context.Context { return stopCtx }

	// A plain group joins the lifecycle members and surfaces the first fault to Run's caller for the
	// exit code; it owns no context, because the shutdown signal is stopCtx, set deliberately rather
	// than as a side effect of which member failed first. The one fault-capable member is Serve, and
	// it begins the stop itself — a plain group does not cancel its siblings, so any future fault-
	// capable member must do the same, or Wait would hang with the others still parked on stopCtx.
	var g errgroup.Group

	g.Go(func() error {
		// Serve until Shutdown or Close stops it; both report ErrServerClosed, the one clean stop. Any
		// other error is a genuine serving fault: begin the stop with the cause a signal carries — so
		// an upload it cuts is an honest 503, not blamed on the client — then return the fault so Run
		// surfaces it and the watcher drains the rest.
		err := rt.httpSrv.Serve(rt.listener)
		if err != nil && err != http.ErrServerClosed {
			beginStop(api.ErrServerStopping)
			return err
		}
		return nil
	})

	// Schedule background reaping only when the concrete store actually reaps and an interval was set.
	// The capability assertion is where the operational sweep stays off the Store seam: a backing that
	// is not a Reaper is simply never scheduled, rather than the seam growing a method every fake must
	// carry. The interval gate is an optimisation, not a correctness requirement — RunReaper no-ops
	// on a non-positive interval on its own — but gating here means a disabled reaper spawns no group
	// member at all, rather than one that wakes only to return.
	if r, ok := rt.store.(store.Reaper); ok && rt.reapInterval > 0 {
		g.Go(func() error {
			store.RunReaper(stopCtx, r, rt.reapInterval)
			return nil
		})
	}

	// The watcher drains once the stop begins — a signal cancelling the root that stopCtx inherits,
	// or the serve goroutine's beginStop on a fatal fault. Either trigger stops the server, so a fatal
	// Serve fault tears the whole runtime down as surely as a signal does.
	g.Go(func() error {
		<-stopCtx.Done()
		rt.shutdown()
		return nil
	})

	rt.log.Info("buff serving", "addr", rt.listener.Addr().String(), "drain", rt.drainTimeout)
	return g.Wait()
}

// shutdown stops accepting connections and drains in-flight work within a bounded window. It runs
// a fresh timeout context, not the already-cancelled group context, because the drain must outlive
// the cancellation that triggered it: by now live followers and still-sending uploads are unwinding
// through their request contexts, and what Shutdown waits on is the finalized reads and consume
// deliveries that watch no context and finish on their own. If the window elapses with work still
// active, Close forces the remaining connections shut — a torn upload's body read then errors and
// its deferred Abort discards the generation; a cut consume delivery's reader Close reclaims it —
// so at-most-once delivery holds either way and nothing is left half-finalized.
func (rt *runtime) shutdown() {
	rt.log.Info("buff shutting down")
	dctx, cancel := context.WithTimeout(context.Background(), rt.drainTimeout)
	defer cancel()
	if err := rt.httpSrv.Shutdown(dctx); err != nil {
		// The drain window elapsed with handlers still active. Force the remaining connections shut, and
		// say so: a forced close cuts in-flight finalized reads and consume deliveries mid-stream, unlike
		// a clean drain, so the distinction is something an operator needs to see in the log.
		rt.log.Warn("graceful drain window exceeded; forcing connections closed", "after", rt.drainTimeout)
		_ = rt.httpSrv.Close()
	}
}
