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

// drainTimeout bounds how long graceful shutdown waits for in-flight finalized work before it forces
// the remaining connections closed. It is a constant rather than a configuration knob on purpose: the
// configuration surface names no shutdown-grace variable, and inventing one would silently extend
// that surface. Promoting it to an environment variable is a clean future addition if an operator
// ever needs a longer or shorter window.
const drainTimeout = 15 * time.Second

// runtime is one running server: the tuned HTTP server, the bound listener, the store it relays to,
// the data root that store writes through, the reaper interval, and the logger. newRuntime builds it
// with every fallible step already done — directory opened, recovery replayed, port bound — so Run
// is pure scheduling and Addr is observable before Run, which is what lets a test bind an ephemeral
// port and drive the whole stack.
type runtime struct {
	httpSrv      *http.Server
	listener     net.Listener
	store        store.Store
	root         *os.Root
	reapInterval time.Duration
	log          *slog.Logger
}

// newRuntime performs all of serving's fallible setup and returns a runtime ready to Run, or the
// first error that setup cannot proceed past. It creates and opens the data directory, constructs
// the disk store (which replays recovery before returning), builds the HTTP edge over it, and binds
// the listener — binding here, not in Run, so a port clash fails synchronously and the chosen port
// is known before the run loop starts. Every error after the root is opened closes it, so a failed
// construction leaks no descriptor.
func newRuntime(c config, log *slog.Logger) (*runtime, error) {
	// The data directory is the os.Root boundary itself — operator configuration, never a
	// request-influenced name — so it is created and opened with plain os. That is the one correct
	// place outside the root for raw filesystem access; the all-IO-through-os.Root invariant governs
	// names inside the root, not the root's own location.
	if err := os.MkdirAll(c.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("buff: data dir: %w", err)
	}
	root, err := os.OpenRoot(c.DataDir)
	if err != nil {
		return nil, fmt.Errorf("buff: open data dir: %w", err)
	}
	// NewDisk runs recovery (scan + restore) before it returns, so an error here is a boot the store
	// cannot run on — an unreadable or unpreparable root. A single corrupt generation never reaches
	// it: recovery isolates and quarantines that, so one bad clip cannot brick the boot.
	st, err := store.NewDisk(root, c.storeConfig(), c.diskOpts(log))
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	srv := api.New(st, c.apiOptions(log))
	ln, err := net.Listen("tcp", c.Addr)
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("buff: listen %s: %w", c.Addr, err)
	}
	return &runtime{
		httpSrv:      srv.HTTPServer(c.Addr),
		listener:     ln,
		store:        st,
		root:         root,
		reapInterval: c.ReapInterval,
		log:          log,
	}, nil
}

// Addr reports the address the listener actually bound, resolving an ephemeral :0 to its chosen
// port. A caller reads it after newRuntime and before Run to learn where to connect.
func (rt *runtime) Addr() net.Addr { return rt.listener.Addr() }

// Close releases the listener and the data root that newRuntime acquired — the symmetric teardown
// for a runtime built but, on some early-return path, never Run. Run otherwise closes both itself
// (the listener when Serve returns, the root via its own defer), so a Close after a completed Run is
// a harmless second close: its already-closed errors are not real faults and are dropped, and only a
// genuine close error — possible solely on the first close — is returned. That idempotence is what
// lets a construction site defer Close unconditionally without double-faulting against Run's own
// teardown.
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

// Run serves until ctx is cancelled or a fatal error stops it, then drains and returns. It binds
// three members to one group context: the HTTP server, the retention reaper (when the store can reap
// and an interval is set), and a watcher that drives graceful shutdown. A clean signal-triggered
// stop returns nil; a real Serve or listen fault returns through the group.
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

	g, gctx := errgroup.WithContext(ctx)
	// BaseContext makes every request context a child of the group context, and that single field is
	// the whole of the selective shutdown. Cancelling the root context — a delivered signal — cancels
	// gctx, and through it every in-flight request context: a live follower watches that context and
	// unwinds at once, while a finalized read or a consume delivery watches no context and keeps
	// draining under Shutdown. No second cancel tree; the read-path framing supplies the selectivity.
	rt.httpSrv.BaseContext = func(net.Listener) context.Context { return gctx }

	g.Go(func() error {
		// Serve until Shutdown or Close stops it; both report ErrServerClosed, the one clean stop.
		// Any other error is a genuine serving fault and propagates through the group.
		if err := rt.httpSrv.Serve(rt.listener); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	// Schedule background reaping only when the concrete store actually reaps and an interval was set.
	// The capability assertion is where the operational sweep stays off the Store seam: a backing that
	// is not a Reaper is simply never scheduled, rather than the seam growing a method every fake must
	// carry. The interval gate is an optimisation, not a correctness requirement — RunReaper no-ops on
	// a non-positive interval on its own — but gating here means a disabled reaper spawns no group
	// member at all, rather than one that wakes only to return.
	if r, ok := rt.store.(store.Reaper); ok && rt.reapInterval > 0 {
		g.Go(func() error {
			store.RunReaper(gctx, r, rt.reapInterval)
			return nil
		})
	}

	// The watcher fires on either trigger the group context carries: a signal cancelling the root, or
	// a sibling member returning an error. Either way it drains and stops the server, so a fatal Serve
	// fault tears the whole runtime down as surely as a signal does.
	g.Go(func() error {
		<-gctx.Done()
		rt.shutdown()
		return nil
	})

	rt.log.Info("buff serving", "addr", rt.listener.Addr().String(), "drain", drainTimeout)
	return g.Wait()
}

// shutdown stops accepting connections and drains in-flight work within a bounded window. It runs a
// fresh timeout context, not the already-cancelled group context, because the drain must outlive the
// cancellation that triggered it: by now live followers and still-sending uploads are unwinding
// through their request contexts, and what Shutdown waits on is the finalized reads and consume
// deliveries that watch no context and finish on their own. If the window elapses with work still
// active, Close forces the remaining connections shut — a torn upload's body read then errors and
// its deferred Abort discards the generation; a cut consume delivery's reader Close reclaims it — so
// at-most-once delivery holds either way and nothing is left half-finalized.
func (rt *runtime) shutdown() {
	rt.log.Info("buff shutting down")
	dctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	if err := rt.httpSrv.Shutdown(dctx); err != nil {
		// The drain window elapsed with handlers still active. Force the remaining connections shut,
		// and say so: a forced close cuts in-flight finalized reads and consume deliveries mid-stream,
		// unlike a clean drain, so the distinction is something an operator needs to see in the log.
		rt.log.Warn("graceful drain window exceeded; forcing connections closed", "after", drainTimeout)
		_ = rt.httpSrv.Close()
	}
}
