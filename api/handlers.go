package api

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"

	"github.com/srevn/buff/store"
	"github.com/srevn/buff/wire"
)

// put creates or replaces a clip from the request body. It is framing-agnostic — a
// Content-Length or a chunked body both work — because only how the body read ends matters: a
// clean end finalizes, anything else aborts. Exactly one terminal ends the generation: a
// deferred abort runs unless the finalize committed, so an early return, a cap rejection, or
// even an unexpected panic still discards the generation rather than leaking a live one. The
// 200 is sent only after Close returns, so a client is told "stored" only once the bytes are
// durable, never before.
func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	meta, opts, err := parsePut(r)
	if err != nil {
		s.writeErr(w, r, mapErr(err), nil)
		return
	}
	wr, err := s.store.Create(r.Context(), r.PathValue("name"), meta, opts)
	if err != nil {
		s.writeErr(w, r, mapErr(err), err)
		return
	}
	committed := false
	defer func() {
		if !committed {
			_ = wr.Abort()
		}
	}()

	ctl := http.NewResponseController(w)
	body := &idleResetReader{r: r.Body, ctl: ctl, idle: s.opt.UploadIdle, max: deadline(s.opt.UploadMax)}
	// A parked body read does not observe context cancellation on its own, so on graceful shutdown
	// or a vanished client it would block until the connection is force-closed. Watch the request
	// context and unblock the read at once on cancel, so the deferred Abort above discards the live
	// generation promptly — the upload's half of context-as-disconnect-signal, symmetric with the
	// follower's context-aware read on the download side.
	stop := abortOnCancel(r.Context(), ctl)
	defer stop()
	bp := s.bufPool.Get().(*[]byte)
	defer s.bufPool.Put(bp)
	if _, err := io.CopyBuffer(wr, body, *bp); err != nil {
		info, cause := classifyPut(r.Context(), err, body)
		s.writeErr(w, r, info, cause)
		return
	}
	if err := wr.Close(); err != nil {
		s.writeErr(w, r, wire.ErrInternal, err)
		return
	}
	committed = true

	c := wr.Clip()
	h := w.Header()
	h.Set(wire.HeaderGeneration, c.Generation)
	h.Set(wire.HeaderSize, itoa(c.Size))
	w.WriteHeader(http.StatusOK)
}

// classifyPut decides which side a failed body copy blames — the read-vs-write router that is the
// upload mirror of classifyGet. A read error means the body ended early: usually the client's
// truncated upload or a tripped idle/max deadline, reported best-effort as a bad request, but the
// identical failure arises when the operator stops the server mid-upload and the parked read is
// poked. The cancellation cause on ctx is the only signal that tells the two apart, so a read cut
// by shutdown is an honest 503, not the client's 400. Everything else is the writer's fault, routed
// through mapErr — the single clip-sentinel forward map — so a store cap resolves to its 413/507 and
// an unrecognised backing fault to the internal row, carried as the cause.
//
// The read branch is tested first, and safely: a clip sentinel is raised on the store's write side
// and never flows into body.readErr, which records only what reading the request body returned, so a
// read identity and a writer sentinel are mutually exclusive and ordering cannot confuse them. The
// discriminator is identity, not errors.Is — io.Copy passes the error it chose through unwrapped and
// idleResetReader stored the very value it returned, so err == body.readErr holds exactly when
// io.Copy surfaced the read error. io.Copy also reports a writer error in preference to a reader one,
// so when a truncating read and a backing write fault coincide it returns the writer's fault, a
// different value from body.readErr, which therefore routes through mapErr and is logged rather than
// mistaken for a client truncation. writeErr logs only the internal row, so a cap rides its sentinel
// along as a now-non-nil cause but stays unlogged, exactly as a relay's routine caps and truncations
// always have — and a store sentinel added later is routed to its true status for free, never the
// 500 a hand-listed subset would have defaulted it to.
func classifyPut(ctx context.Context, err error, body *idleResetReader) (wire.ErrInfo, error) {
	if body.readErr != nil && err == body.readErr { // io.Copy surfaced the read error → the body ended early
		if stoppingCut(ctx) {
			return wire.ErrUnavailable, nil // the operator stopped the server, not the client
		}
		return wire.ErrBadReq, nil
	}
	return mapErr(err), err // writer's fault: caps → 413/507, unrecognised → internal (logged)
}

// stoppingCut reports whether ctx was cut by graceful shutdown rather than by the client — the
// cause cmd/buff sets at the root when it begins to stop, which propagates to every request
// context. It is the one bit that tells an operator-initiated cut from a vanished client, and the
// upload and read paths consult it identically: an upload cut by shutdown is reported 503 rather
// than blamed on the client as 400, and a read cut by shutdown is a 503 rather than a connection
// reset. With no such cause set — a live client cancelling, or an embedder that never stops with
// it — context.Cause is the plain context.Canceled and this is false.
func stoppingCut(ctx context.Context) bool {
	return errors.Is(context.Cause(ctx), ErrServerStopping)
}

// isCancel reports whether err is a context cancellation — a transport event, never a domain
// error. It is how the read path tells the store's pre-stream cancellation guard apart from a clip
// sentinel: Open declines an already-cancelled request by returning its ctx.Err(), which matches no
// domain row. Both a plain cancellation (a vanished client) and a deadline are treated alike, since
// each means the request is no longer worth serving; errors.Is unwraps either however it was
// wrapped on the way out.
func isCancel(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// get reads a clip, following a still-being-written one to its clean end. The framing is
// decided once, here, from whether the target is already finalized: a finalized clip is sent
// with an exact Content-Length and no trailer, so any client detects a short read; a live clip
// is sent chunked with a Buff-Status trailer declared before the body and set to complete only
// if the follow reaches a clean end. The reader is always closed, including during a panic
// unwind, which is what releases the lease and, for a consume-once clip, destroys it after its
// single delivery.
func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	rc, c, err := s.store.Open(r.Context(), r.PathValue("name"), store.GetOpts{})
	if err != nil {
		info, cause, reset := classifyGet(r.Context(), err)
		if reset {
			panic(http.ErrAbortHandler) // reset like a torn stream; the recover backstop re-raises it
		}
		s.writeErr(w, r, info, cause)
		return
	}
	defer rc.Close()

	writeHeaders(w, c)
	ctl := http.NewResponseController(w)
	if c.Finalized {
		w.Header().Set("Content-Length", itoa(c.Size))
		w.WriteHeader(http.StatusOK)
		s.stream(ctl, w, rc, false)
		return
	}
	w.Header().Set("Trailer", wire.HeaderStatus)
	w.WriteHeader(http.StatusOK)
	s.stream(ctl, w, rc, true)
}

// classifyGet maps a failed Open to its pre-stream disposition — the read-side twin of classifyPut.
// Open's guard declines an already-cancelled request before it claims a consume-once clip or ships a
// byte, so any disposition decided here is safe: nothing has been delivered. A non-cancel error is a
// domain sentinel or a backing fault, kept on mapErr's status and carried as the cause — so a
// genuine internal fault is still logged, while a sentinel's cause is passed but never logged
// (writeErr logs only the internal row). A context cancellation is a transport event, not a domain
// fault, and splits two ways. A read cut by graceful shutdown is an honest 503 to a client that may
// still be present and can retry, mirroring the upload path. A cancellation without that cause is a
// vanished client: there is no status to send a reader that is gone, so the read path resets the
// connection exactly as a torn live stream does — reset says so to the caller, which raises the same
// http.ErrAbortHandler that stream does, so a client-gone read aborts identically whether it fails
// before the body or during it, and the access log records one uniform torn-GET signal either way.
func classifyGet(ctx context.Context, err error) (info wire.ErrInfo, cause error, reset bool) {
	if !isCancel(err) {
		return mapErr(err), err, false
	}
	if stoppingCut(ctx) {
		return wire.ErrUnavailable, nil, false // 503: a real reply to a client that may still be there
	}
	return wire.ErrInfo{}, nil, true // client gone: no row, no log, just the reset
}

// head returns a clip's metadata with no body and without ever claiming it. It resolves through
// Stat, never Open, so a metadata probe of a consume-once clip does not spend its one delivery —
// routing HEAD into the GET handler would do exactly that, which is why HEAD has its own route
// and its own handler.
func (s *Server) head(w http.ResponseWriter, r *http.Request) {
	c, err := s.store.Stat(r.Context(), r.PathValue("name"))
	if err != nil {
		s.writeErr(w, r, mapErr(err), err)
		return
	}
	writeHeaders(w, c)
	if c.Finalized {
		w.Header().Set("Content-Length", itoa(c.Size))
	}
	w.WriteHeader(http.StatusOK)
}

// delete removes a clip's finalized generation. It never disturbs a generation still being
// written — that one belongs to the PUT writing it — so deleting a name that has only a live
// generation, or no generation at all, is a not-found.
func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Delete(r.Context(), r.PathValue("name")); err != nil {
		s.writeErr(w, r, mapErr(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// list returns every finalized clip as a JSON envelope. The clip array is always present, so an
// empty store renders as [] rather than null, and the entries are sorted by name to give the
// store's unordered snapshot a stable, friendly presentation.
func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	cs, err := s.store.List(r.Context())
	if err != nil {
		s.writeErr(w, r, mapErr(err), err)
		return
	}
	out := listEnvelope{Clips: make([]wireClip, 0, len(cs))}
	for _, c := range cs {
		out.Clips = append(out.Clips, toWire(c))
	}
	slices.SortFunc(out.Clips, func(a, b wireClip) int { return cmp.Compare(a.Name, b.Name) })
	s.writeJSON(w, r, out)
}

// health reports liveness and the server's static capabilities. It is unversioned and stable
// so deploy tooling never has to track it, and its feature list is the seam a client checks
// before relying on an optional capability.
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	doc := healthDoc{
		Status:   "ok",
		Version:  s.opt.Version,
		API:      []string{"v1"},
		Features: []string{"follow", "consume-once"},
	}
	s.writeJSON(w, r, doc)
}

// writeJSON sends v as a JSON response under the same torn-response contract the streaming paths
// keep — the success twin of writeErr. It marshals into a buffer first so the two failure modes are
// told apart: a marshal fault (which the list and health shapes cannot in fact produce) means
// nothing has reached the wire, so it is a clean 500 — never a phantom client-gone reset the recover
// backstop would then log as a pre-header 499. A write fault means the client vanished mid-body, the
// one way a started JSON body can tear, so it raises the same http.ErrAbortHandler the streaming
// paths do and the access log marks the response aborted. Without this a torn list or health would
// return normally and log aborted=false, the lone gap in the otherwise-universal torn-response rule.
func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, v any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil { // the buffer keeps Encode's trailing newline
		s.writeErr(w, r, wire.ErrInternal, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if _, err := buf.WriteTo(w); err != nil {
		panic(http.ErrAbortHandler)
	}
}
