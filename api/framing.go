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
// the mirror of the decode on the way in. The executable bit rides like the filename — present
// only when set, so its absence is read as not executable.
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
	// "true" mirrors the FormatBool the other response booleans use, but set only when executable —
	// the present-when-set shape Buff-Filename keeps, deliberately not Buff-Consume's always-present
	// "true"/"false", since the vast majority of clips carry no runnable bit to announce.
	if c.Meta.Executable {
		h.Set(wire.HeaderExecutable, "true")
	}
	if c.Finalized {
		h.Set(wire.HeaderSize, itoa(c.Size))
		if !c.ExpiresAt.IsZero() {
			h.Set(wire.HeaderExpires, c.ExpiresAt.UTC().Format(time.RFC3339))
		}
	}
}

// stream emits a clip's bytes under the framing that proves their completeness, and owns that whole
// decision: it declares the framing header, writes the 200, copies the body, and on the live arm
// seals the stream with the completion trailer. Keeping both completion proofs here — the finalized
// arm's exact Content-Length and the live arm's Buff-Status trailer — is what stops them drifting
// apart; writeHeaders, its sibling, sets only the Buff-* metadata that GET and HEAD share.
//
// The followable buffer returns a clean io.EOF only when a generation finalized cleanly, so a nil
// io.CopyBuffer error is the one and only success. Any copy error is a torn stream — a truncated
// upload behind the buffer, an abort, a vanished or stalled client — and becomes
// panic(http.ErrAbortHandler), which Go turns into an abrupt connection close with no terminating
// chunk and no trailer, so a truncated read can never reach a client looking complete. The framing
// is chosen once, from the c.Finalized snapshot taken under the store handle lock at Open: finalized
// is terminal, and a live→finalized transition is absorbed by the follower reaching a clean EOF, so
// deciding once never goes stale.
func (s *Server) stream(w http.ResponseWriter, src io.Reader, c clip.Clip) {
	bp := s.bufPool.Get().(*[]byte)
	defer s.bufPool.Put(bp)
	ctl := http.NewResponseController(w)
	live := !c.Finalized

	if c.Finalized {
		// Load-bearing, not redundant. Without it Go buffers the body in its fixed response buffer
		// (2048 bytes) and, the instant a copy write overflows that buffer while the handler is still
		// running, commits the response to chunked transfer. This arm declares no trailer, so a
		// wholly-delivered clip larger than 2048 bytes would then arrive chunked with no completion
		// proof and every client would read it as torn. Declaring the exact length forces fixed-length
		// framing at every size — the finalized arm's completion proof, and what keeps a finalized read
		// plain length-delimited bytes for any third-party client.
		w.Header().Set("Content-Length", itoa(c.Size))
	} else {
		// The live arm's proof is an end-of-stream trailer, which Go emits only when its name was
		// announced before the body. A live clip's length is unknown at header time, so fixed-length
		// framing is impossible; chunked plus a trailer is the only framing that can prove the
		// completeness of a stream whose end is not known in advance.
		w.Header().Set("Trailer", wire.HeaderStatus)
	}
	w.WriteHeader(http.StatusOK)

	dst := &idleResetWriter{w: w, ctl: ctl, idle: s.opt.UploadIdle, flush: live}
	if live {
		// Flush the status and metadata now, before the first byte. A follower blocks for bytes that
		// may not arrive for a while, so without this an attach to a clip still being written would
		// withhold the whole response — even its metadata — until the producer happens to write, and a
		// consumer could not tell "attached, waiting" from "still connecting". Flushing on attach makes
		// the live response observable at once, the bytes following as they are produced. A flush
		// failure means the client is already gone: treat it as the torn stream a failed first write
		// would, so the abort contract is identical either way.
		if err := ctl.Flush(); err != nil {
			panic(http.ErrAbortHandler)
		}
	}
	if _, err := io.CopyBuffer(dst, src, *bp); err != nil {
		panic(http.ErrAbortHandler)
	}
	if live {
		// The clean copy reached the buffer's clean EOF — the one success — so promote the announced
		// trailer to complete. An aborted follow never arrives here: its copy error panicked above.
		w.Header().Set(wire.HeaderStatus, wire.StatusComplete)
	}
}
