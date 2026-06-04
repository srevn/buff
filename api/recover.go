package api

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/srevn/buff/wire"
)

// statusRecorder wraps the ResponseWriter to remember what became of a request, for two readers in
// the same frame: the recover backstop, which needs to know whether the response has started to
// choose between a clean error reply and an abrupt reset; and the access log, which needs the final
// status, the byte count, and whether the stream was torn. It implements Unwrap so
// http.NewResponseController still reaches the real connection beneath it; without that, setting a
// deadline or flushing a live stream would silently report "not supported" and the streaming
// wrappers would quietly do nothing.
type statusRecorder struct {
	rw      http.ResponseWriter
	status  int   // captured status; defaults to 200, Go's implicit status for a bare Write
	n       int64 // response bytes written to the client
	wrote   bool  // whether the response has started
	aborted bool  // whether the stream was torn (an ErrAbortHandler reset), so the access log marks it
}

// statusClientClosed is the status the access log records for a response aborted before any header
// was sent — only ever get's reset of a client that vanished before a byte shipped. It is nginx's
// "client closed request" convention and is log-only: the connection is reset with no status line,
// so the value never reaches the wire and cannot collide with a real one. It distinguishes a
// pre-header reset from a 200-then-torn in the log, where the 200 default would otherwise mislead.
const statusClientClosed = 499

// Header exposes the underlying response headers.
func (s *statusRecorder) Header() http.Header { return s.rw.Header() }

// WriteHeader captures the status, records that the response has started, then forwards the status.
func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.wrote = true
	s.rw.WriteHeader(code)
}

// Write records that the response has started, counts the bytes, then forwards them.
func (s *statusRecorder) Write(b []byte) (int, error) {
	s.wrote = true
	n, err := s.rw.Write(b)
	s.n += int64(n)
	return n, err
}

// Unwrap exposes the underlying writer so http.NewResponseController can drill through to the
// connection for SetReadDeadline, SetWriteDeadline, and Flush.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.rw }

// size reports the clip size for the access log. On a clean response the Buff-Size header is
// authoritative when set — a finalized GET/HEAD, or a PUT's stored size — else the bytes written to
// the client, the honest fallback for a live follow whose size is in flux and carries no header. On
// a torn response that header, if any, declares more than was delivered, so the bytes actually
// shipped are the truthful count: a torn finalized read logs what reached the client, not the size
// it had promised.
func (s *statusRecorder) size() int64 {
	if !s.aborted {
		if v := s.Header().Get(wire.HeaderSize); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				return n
			}
		}
	}
	return s.n
}

// recoverer is the last-resort backstop around every handler, and the frame the access log shares.
// Two defers run in LIFO order. The access-log defer is registered first so it runs last: it sees
// whatever final state the request reached and — because a re-panic keeps unwinding the
// earlier-registered defers — still runs while the abort below propagates, recording a torn stream
// without itself recovering, so the connection reset survives the logging seam.
//
// The recover defer is registered second so it runs first. The one intentional panic in the request
// path is http.ErrAbortHandler, the signal that tears down a torn stream; it is re-panicked untouched
// so Go's server still resets the connection without a terminating chunk — swallowing it would let a
// truncated stream look complete. Any other panic is unexpected: its cause is logged once, and if
// nothing has been written yet a real 500 is sent; otherwise, a panic mid-body where the status is
// already gone, it too becomes the abrupt reset rather than a misleading clean end. aborted is set
// only on the two reset paths, never on the clean 500 — in the access log it means "the client saw a
// torn stream," which a well-formed 500 reply is not.
func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sr := &statusRecorder{rw: w, status: http.StatusOK}
		start := time.Now()
		defer func() {
			if s.opt.AccessLog {
				s.logAccess(r, sr, time.Since(start))
			}
		}()
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			if rec == http.ErrAbortHandler {
				sr.aborted = true
				if !sr.wrote {
					// The only pre-header abort is get's reset of a client that vanished before any
					// byte shipped; record that honestly rather than leaving the 200 default for a
					// response whose status line never went out.
					sr.status = statusClientClosed
				}
				panic(rec)
			}
			s.opt.Logger.Error("panic serving request", "method", r.Method, "path", r.URL.Path, "panic", rec)
			if sr.wrote {
				sr.aborted = true
				panic(http.ErrAbortHandler)
			}
			s.writeErr(sr, r, wire.ErrInternal, nil)
		}()
		next.ServeHTTP(sr, r)
	})
}

// logAccess emits one structured access line per request — metadata only, never content. mode is the
// HTTP method (the server's honest fact; "copy" vs "paste" is a client concept it never sees); name
// is the slot; status, size, and the torn-stream flag come from the recorder; kind comes from the
// response header on a read, or for a write — whose response carries none — the request header. dur is
// elapsed wall time, which time.Since reads from the monotonic clock; a duration in a log is benign
// and needs no injected clock, unlike the store's id and expiry clock.
func (s *Server) logAccess(r *http.Request, sr *statusRecorder, d time.Duration) {
	s.opt.Logger.LogAttrs(r.Context(), slog.LevelInfo, "request",
		slog.String("mode", r.Method),
		slog.String("name", r.PathValue("name")),
		slog.Int("status", sr.status),
		slog.Int64("size", sr.size()),
		slog.String("kind", firstNonEmpty(sr.Header().Get(wire.HeaderKind), r.Header.Get(wire.HeaderKind))),
		slog.Bool("aborted", sr.aborted),
		slog.Duration("dur", d),
	)
}

// firstNonEmpty returns a when it is set, else b — the access log's kind comes from whichever side
// carries it: the response header on a read, the request header on a write.
func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
