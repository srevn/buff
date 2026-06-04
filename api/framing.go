package api

import (
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/wire"
)

// writeHeaders sets the Buff-* response metadata shared by GET and HEAD, plus an octet-stream
// content type and a nosniff guard so the relay's opaque bytes are never content-sniffed into a
// guessed type — octet-stream alone only states the intent, while X-Content-Type-Options: nosniff
// is what makes a browser honour it instead of sniffing the body anyway. It must be called before
// WriteHeader. Size and the absolute expiry are sent only for a finalized generation — a live one
// has a size still in flux and no expiry yet — and a filename is percent-encoded on the way out,
// the mirror of the decode on the way in.
func writeHeaders(w http.ResponseWriter, c clip.Clip) {
	h := w.Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set(wire.HeaderGeneration, c.Generation)
	h.Set(wire.HeaderKind, string(c.Meta.Kind))
	h.Set(wire.HeaderFinalized, strconv.FormatBool(c.Finalized))
	h.Set(wire.HeaderConsume, strconv.FormatBool(c.ConsumeOnce))
	if c.Meta.Filename != "" {
		h.Set(wire.HeaderFilename, url.PathEscape(c.Meta.Filename))
	}
	if c.Finalized {
		h.Set(wire.HeaderSize, itoa(c.Size))
		if !c.ExpiresAt.IsZero() {
			h.Set(wire.HeaderExpires, c.ExpiresAt.UTC().Format(time.RFC3339))
		}
	}
}

// stream copies a clip's bytes to the client and signals completion through framing alone. The
// followable buffer returns a clean io.EOF only when a generation finished cleanly, so a nil
// copy error is the one and only success: for a live target it then sets the Buff-Status
// trailer, declared up front by the caller; for a finalized target the exact Content-Length the
// caller set already frames it, and no trailer is needed. Any copy error means a torn stream —
// a truncated upload behind the buffer, an abort, a vanished or stalled client — and becomes a
// panic with http.ErrAbortHandler, which Go turns into an abrupt connection close with no
// terminating chunk and no trailer, so a truncated read can never reach the client looking
// complete. A live response flushes its headers on attach and then each chunk, so a follower sees
// the response — and then the bytes — as soon as it attaches and they are written; a finalized
// bulk read withholds nothing yet and lets the transport batch.
func (s *Server) stream(ctl *http.ResponseController, w http.ResponseWriter, src io.Reader, live bool) {
	bp := s.bufPool.Get().(*[]byte)
	defer s.bufPool.Put(bp)
	dst := &idleResetWriter{w: w, ctl: ctl, idle: s.opt.UploadIdle, flush: live}
	if live {
		// Flush the already-written status and metadata headers now, before the first byte. A
		// follower blocks for bytes that may not arrive for a while, so without this an attach to a
		// clip still being written would withhold the whole response — even its metadata — until the
		// producer happens to write, and a consumer could not tell "attached, waiting" from "still
		// connecting". Flushing on attach makes the live response observable at once, the bytes
		// following as they are produced. A flush failure means the client is already gone: treat it
		// as the torn stream a failed first write would, so the abort contract is identical either way.
		if err := ctl.Flush(); err != nil {
			panic(http.ErrAbortHandler)
		}
	}
	if _, err := io.CopyBuffer(dst, src, *bp); err != nil {
		panic(http.ErrAbortHandler)
	}
	if live {
		w.Header().Set(wire.HeaderStatus, wire.StatusComplete)
	}
}
