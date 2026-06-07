package api

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/wire"
)

// errBadRequest is the api-internal sentinel for a malformed request the store never sees:
// an unknown clip kind, a TTL that is not a non-negative duration, a filename that will
// not percent-decode. It maps to the generic bad_request row. A bad filename keeps its own
// clip.ErrFilenameInvalid identity — which also maps to bad_request — so the one place that
// validates a filename need not translate its error first.
var errBadRequest = errors.New("api: malformed request")

// ErrServerStopping is the cancellation cause that turns an upload cut by shutdown into an honest
// 503 rather than a 400. A body read aborted by a canceled request context looks identical at the
// socket whether the server is stopping or the client vanished; the only out-of-band signal is the
// cause carried on the context. The wiring layer that owns the server lifecycle sets this as the
// cause that reaches every request context when it begins a graceful stop — whether a delivered
// signal or a fatal serving fault triggers it; the put and get handlers consult it to tell the two
// cases apart. It lives here, not in the wiring layer, because deciding a canceled-by-shutdown read
// is a 503 is exactly the domain-to-HTTP mapping this layer owns, and the wiring layer opts in by
// using it. With no cause set — an embedder that never stops with it — classification is unchanged,
// so this is additive.
var ErrServerStopping = errors.New("buff: server stopping")

// errMap pairs each domain sentinel with its wire row. mapErr walks it with errors.Is, so a wrapped
// store error still resolves to the right row and the status and sentinel come from the canonical
// table rather than being typed by hand. An error matching no row is an unexpected internal fault.
var errMap = []struct {
	err  error
	info wire.ErrInfo
}{
	{clip.ErrNotFound, wire.ErrNotFound},
	{clip.ErrConsumed, wire.ErrConsumed},
	{clip.ErrBusy, wire.ErrBusy},
	{clip.ErrClosed, wire.ErrClosed},
	{clip.ErrPreconditionFailed, wire.ErrPrecondition},
	{clip.ErrTooLarge, wire.ErrTooLarge},
	{clip.ErrNoSpace, wire.ErrNoSpace},
	{clip.ErrNameInvalid, wire.ErrNameBad},
	{clip.ErrFilenameInvalid, wire.ErrBadReq},
	{errBadRequest, wire.ErrBadReq},
}

// mapErr resolves a domain error to its wire row — the single forward mapping from a clip sentinel
// to an HTTP status and Buff-Error string, and the one place a clip sentinel becomes a status:
// the put and get classifiers route their store and request errors through here rather than re-
// deciding. The statuses set elsewhere — a shutdown 503, a client-gone reset, the internal row
// written directly on a finalize or marshal fault — are transport or internal dispositions, none
// keyed on a clip sentinel. An unrecognised error — a wrapped backing fault, a finalize failure
// — falls through to the internal row, so an unexpected error is reported as 500 and never
// misclassified as a client error.
func mapErr(err error) wire.ErrInfo {
	for _, m := range errMap {
		if errors.Is(err, m.err) {
			return m.info
		}
	}
	return wire.ErrInternal
}

// writeErr sends an error response before any body has started: the Buff-Error sentinel header, the
// status, and a one-line body that is the sentinel itself — never the underlying cause, which may
// carry a clip name or an internal detail. The cause of a genuine internal failure is logged here
// instead, the one piece a constant body omits that an operator needs; every other status is self-
// describing, so its cause is not logged here; the per-request access log records the rest. A HEAD
// response carries the header and status but no body. This is for the pre-stream path only: once a
// body has started, an error is a panic, not a status that can no longer be changed.
func (s *Server) writeErr(w http.ResponseWriter, r *http.Request, info wire.ErrInfo, cause error) {
	if info == wire.ErrInternal && cause != nil {
		s.opt.Logger.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", cause)
	}
	h := w.Header()
	h.Set(wire.HeaderError, info.Sentinel)
	h.Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(info.Status)
	if r.Method != http.MethodHead {
		fmt.Fprintln(w, info.Sentinel)
	}
}
