package client

import (
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/wire"
)

// encodeHeaders builds the Buff-* request headers for a Put. The kind is always concrete by the
// time it reaches here — Put defaults an absent one to bytes before calling this — so the header
// carries the real kind the clip will be stored under and never an empty value the server would
// have to default in its place. The optional fields are sent only when set, and only ever as the
// exact values the server's strict parse accepts — wire.FlagOn for the boolean flags, a Go duration
// for the TTL — so the client never emits a value the server would reject for a flag it does send.
// The filename is percent-encoded here, the mirror of the decode the server applies on the way
// in; the pair is PathEscape/PathUnescape, never the query codec, which would corrupt a '+' into
// a space.
func encodeHeaders(m clip.Meta, o PutOpts) http.Header {
	h := http.Header{}
	h.Set(wire.HeaderKind, string(m.Kind))
	if m.Filename != "" {
		h.Set(wire.HeaderFilename, url.PathEscape(m.Filename))
	}
	if m.Executable {
		h.Set(wire.HeaderExecutable, wire.FlagOn)
	}
	if o.TTL > 0 {
		h.Set(wire.HeaderTTL, o.TTL.String())
	}
	if o.Keep {
		h.Set(wire.HeaderKeep, wire.FlagOn)
	}
	if o.ConsumeOnce {
		h.Set(wire.HeaderConsume, wire.FlagOn)
	}
	return h
}

// parseClip reads the Buff-* response headers of a GET or HEAD into a clip.Clip. Size and the
// absolute expiry are present only for a finalized generation, so they are read only then; a live
// one reports neither. CreatedAt and FinalizedAt are not GET or HEAD headers at all — they appear
// only in the list JSON — so they stay zero here, which is honest: this snapshot genuinely does not
// know them. The filename is percent-decoded, the mirror of the encode the send path applies. The
// three booleans are matched against wire.BoolTrue, the value the server formats a response boolean
// as — deliberately not the wire.FlagOn the request side sends for the same Buff-Consume and Buff-
// Executable flags: the two directions encode a boolean differently and only the round-trip test
// guards their agreement, so neither side may be normalised to the other alone. The metadata fields
// are parsed leniently: a malformed size, expiry, or filename is dropped rather than failing the
// call, because none of them decides whether a read is complete — that is the body's job, where a
// wrong answer would corrupt data, not merely lose a label.
func parseClip(name string, h http.Header) clip.Clip {
	c := clip.Clip{
		Name:        name,
		Generation:  h.Get(wire.HeaderGeneration),
		Meta:        clip.Meta{Kind: clip.Kind(h.Get(wire.HeaderKind)), Executable: h.Get(wire.HeaderExecutable) == wire.BoolTrue},
		Finalized:   h.Get(wire.HeaderFinalized) == wire.BoolTrue,
		ConsumeOnce: h.Get(wire.HeaderConsume) == wire.BoolTrue,
	}
	if v := h.Get(wire.HeaderFilename); v != "" {
		if d, err := url.PathUnescape(v); err == nil {
			c.Meta.Filename = d
		}
	}
	// Normalize against a foreign or hostile server: a non-buff peer can echo a file-scoped field
	// on a kind that does not carry it — a bytes clip announcing an executable bit — and a -o paste
	// would honour it on apply. Cleaning it here, where the response becomes a domain clip, keeps that
	// illegal shape from ever reaching a sink or a renderer; it is the read-side mirror of the store's
	// admission normalize, and shares the lenient posture above — a bad field is dropped, never fatal.
	c.Meta = c.Meta.Normalized()
	if c.Finalized {
		c.Size = atoi64(h.Get(wire.HeaderSize))
		if t := parseTime(h.Get(wire.HeaderExpires)); !t.IsZero() {
			c.ExpiresAt = t
		}
	}
	return c
}

// atoi64 parses a byte-count header value, yielding zero for an empty or malformed one. The server
// always sends a well-formed size on a finalized response, so the lenient zero is a defensive floor
// for a foreign or broken peer, never the normal path.
func atoi64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// parseTime parses an RFC 3339 instant, yielding the zero time for an empty or malformed value. An
// absent expiry is the empty string — a clip that never expires — so the zero time it returns is
// the correct "no expiry" sentinel, and a malformed one degrades to the same rather than failing a
// call over a cosmetic field.
func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}
