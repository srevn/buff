package api

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"
)

// aLongTimeAgo is an instant far enough in the past that arming it as a read deadline unblocks a
// parked read at once. It mirrors the sentinel net/http uses internally to cancel a connection's
// pending IO: the runtime poller returns a blocked read the moment its deadline is at or before
// now, which is the only way to interrupt a body read otherwise waiting on the socket with no
// regard for context.
var aLongTimeAgo = time.Unix(1, 0)

// abortOnCancel unblocks a parked request-body read when ctx is canceled, and returns a stop
// function the caller defers to retire it. A blocked Read on the request body waits on the socket
// and does not observe context cancellation on its own; on graceful shutdown the request context —
// a child of the server's, canceled when the signal fires — or on a vanished client, the read would
// otherwise stay parked until the connection is force-closed. The watcher arms a read deadline
// in the past on cancel, so the read returns at once, the body copy stops, and the PUT's deferred
// Abort discards the live generation promptly. It is the upload's half of context-as-disconnect-
// signal, the symmetric counterpart to the follower's context-aware Read on the download side.
//
// The lifecycle is kept airtight because a leaked or late-firing watcher would hide in exactly
// the place the concurrency baseline warns about. The goroutine blocks until ctx is canceled or
// stop is called, so it never outlives the request. disarm, taken under the same lock fire holds,
// prevents any arming after stop returns — but it does not by itself beat a fire already in flight:
// the two contend for the lock with no ordering, so a cancellation landing just as the handler
// finishes can arm the past deadline before disarm records it. What makes that harmless is the
// connection, not the guard. A request whose ctx was canceled mid-handler means the peer is gone
// (client disconnect) or the server is draining (doKeepAlives is false on shutdown), so net/http
// never recycles that connection for a later request: a stale past deadline lands on a socket that
// will serve no further read. The guard is kept as cheap, local insurance, holding even should
// net/http's reuse behaviour ever change.
//
// When an idle deadline is also in force, this watcher and idleResetReader both set the same
// connection read deadline, yet they never fight — the watcher only ever arms one in the past, and
// the reader re-arms a future one only on shouldReset's half-window cadence. That cadence is a no-
// op in the moment just after a read, which is exactly when a parked read is waiting on the socket
// for the poke, so a cancel landing on a parked read unblocks it at once. The one window where a
// re-arm could overwrite the poke needs more than half the idle bound to elapse between two reads,
// and even there the bounded Close backstop on shutdown closes the connection, so the read never
// blocks unboundedly either way. This is load-bearing: it is what lets the past-deadline poke
// win against a live idle deadline, the property the upload-cancel end-to-end case pins on the
// production config.
func abortOnCancel(ctx context.Context, ctl *http.ResponseController) func() {
	g := &cancelGuard{}
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			g.fire(ctl)
		case <-stopped:
		}
	}()
	return func() {
		g.disarm()
		close(stopped)
	}
}

// cancelGuard serialises the watcher's deadline arming against the handler's disarm, so the read
// deadline is armed on cancel only while the upload is still running, never after it has finished.
type cancelGuard struct {
	mu       sync.Mutex
	disarmed bool
}

// fire arms the past read deadline that unblocks the parked read, unless the guard was already
// disarmed by a handler that finished first.
func (g *cancelGuard) fire(ctl *http.ResponseController) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.disarmed {
		_ = ctl.SetReadDeadline(aLongTimeAgo)
	}
}

// disarm marks the guard so a later fire is a no-op. It takes the lock fire holds, so once disarm
// returns the watcher cannot arm a deadline.
func (g *cancelGuard) disarm() {
	g.mu.Lock()
	g.disarmed = true
	g.mu.Unlock()
}

// shouldReset decides whether to push the idle deadline forward on this read or write. It resets
// on the first call, when no deadline is set yet, and thereafter only once more than half the idle
// window has elapsed since the last reset, so a high-throughput transfer resets on a coarse cadence
// rather than per chunk. The half-window slack keeps a steady transfer from ever tripping while
// still bounding a true stall to under one and a half idle windows. It is pure so the cadence is
// testable without a clock.
func shouldReset(last, now time.Time, idle time.Duration) bool {
	return last.IsZero() || now.Sub(last) > idle/2
}

// deadline turns an optional maximum duration into an absolute instant from now, or the zero
// instant when the duration is not positive (no maximum). It is read once at the start of an upload
// so the absolute cap does not drift forward as the upload proceeds.
func deadline(max time.Duration) time.Time {
	if max <= 0 {
		return time.Time{}
	}
	return time.Now().Add(max)
}

// idleResetReader wraps a request body so a stalled upload trips a deadline while a long but active
// one does not. Before each read it arms the connection read deadline, folding an optional idle
// bound and an optional absolute maximum into one timer. It also remembers the first non-EOF read
// error, which lets the PUT handler tell a client-side truncation apart from a server-side write
// fault. With neither bound set, no deadline is armed; the error is recorded either way.
type idleResetReader struct {
	r       io.Reader
	ctl     *http.ResponseController
	idle    time.Duration
	max     time.Time
	last    time.Time
	readErr error
}

// Read arms the deadline if due, then reads, recording any non-EOF error.
func (d *idleResetReader) Read(p []byte) (int, error) {
	d.arm()
	n, err := d.r.Read(p)
	if err != nil && err != io.EOF {
		d.readErr = err
	}
	return n, err
}

// arm sets or pushes the connection read deadline. With an idle bound it slides the deadline
// forward on the coarse cadence of shouldReset, capping it at the absolute maximum if one is also
// set — the earlier of the two wins. With only an absolute maximum it sets that deadline once, on
// the first read, since an absolute cap does not slide; the maximum must hold even when the idle
// bound is disabled. With neither, it does nothing. last doubles as the cadence marker and, in the
// maximum-only case, the "armed once" marker.
func (d *idleResetReader) arm() {
	switch {
	case d.idle > 0:
		now := time.Now()
		if !shouldReset(d.last, now, d.idle) {
			return
		}
		dl := now.Add(d.idle)
		if !d.max.IsZero() && dl.After(d.max) {
			dl = d.max
		}
		_ = d.ctl.SetReadDeadline(dl)
		d.last = now
	case !d.max.IsZero() && d.last.IsZero():
		_ = d.ctl.SetReadDeadline(d.max)
		d.last = time.Now()
	}
}

// idleResetWriter wraps a response body symmetrically: before each write it pushes the connection
// write deadline forward on the same coarse cadence, so a client that stalls while reading a
// clip trips the deadline, and for a live stream it flushes after each write so a follower sees
// bytes promptly rather than waiting for the transport buffer to fill. It has no absolute maximum
// — a read carries no total-duration cap. An idle of zero disables the deadline; flushing still
// happens. A flush failure on an otherwise-good write is surfaced as the write's error so the copy
// stops at once rather than on the next, doomed, write.
type idleResetWriter struct {
	w     io.Writer
	ctl   *http.ResponseController
	idle  time.Duration
	flush bool
	last  time.Time
}

// Write pushes the deadline if due, writes, then flushes a live stream.
func (d *idleResetWriter) Write(p []byte) (int, error) {
	if d.idle > 0 {
		now := time.Now()
		if shouldReset(d.last, now, d.idle) {
			_ = d.ctl.SetWriteDeadline(now.Add(d.idle))
			d.last = now
		}
	}
	n, err := d.w.Write(p)
	if d.flush && err == nil {
		if ferr := d.ctl.Flush(); ferr != nil {
			err = ferr
		}
	}
	return n, err
}
