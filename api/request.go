package api

import (
	"net/http"
	"net/url"
	"time"

	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
	"github.com/srevn/buff/wire"
)

// parsePut reads the Buff-* request headers of a PUT into the metadata and options the store needs.
// A missing kind defaults to bytes — the domain type validates exactly and never defaults, so
// defaulting an absent wire value is this layer's job. Every malformed value is a bad request: an
// unknown kind, an undecodable or unsafe filename, a TTL that is not a non-negative Go duration,
// or a Keep, Consume, or Executable flag present but not wire.FlagOn. A filename arrives percent-
// encoded and is decoded at this boundary, never deeper, the mirror of the encode the read path
// applies. A TTL of zero asks for the store default; a positive one is taken as given. Unrecognised
// Buff-* headers are ignored, so a newer client may send headers this server does not know.
func parsePut(r *http.Request) (clip.Meta, store.PutOpts, error) {
	kind := clip.KindBytes
	if v := r.Header.Get(wire.HeaderKind); v != "" {
		kind = clip.Kind(v)
		if !kind.Valid() {
			return clip.Meta{}, store.PutOpts{}, errBadRequest
		}
	}

	var filename string
	if v := r.Header.Get(wire.HeaderFilename); v != "" {
		decoded, err := url.PathUnescape(v)
		if err != nil {
			return clip.Meta{}, store.PutOpts{}, errBadRequest
		}
		if err := clip.ValidFilename(decoded); err != nil {
			return clip.Meta{}, store.PutOpts{}, err
		}
		filename = decoded
	}

	var o store.PutOpts
	if v := r.Header.Get(wire.HeaderTTL); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return clip.Meta{}, store.PutOpts{}, errBadRequest
		}
		o.TTL = d
	}
	var err error
	if o.Keep, err = boolHeader(r, wire.HeaderKeep); err != nil {
		return clip.Meta{}, store.PutOpts{}, err
	}
	if o.ConsumeOnce, err = boolHeader(r, wire.HeaderConsume); err != nil {
		return clip.Meta{}, store.PutOpts{}, err
	}
	// If-Match carries an opaque generation token, not a strict flag, so it is read straight through
	// with no validation here: the store is the sole authority on whether it matches the current
	// value, and a malformed or stale token simply fails the CAS as a 412 rather than a 400. Absent
	// reads as empty, which the store takes as an unconditional write.
	o.IfMatch = r.Header.Get(wire.HeaderIfMatch)
	// Executable is metadata, not a write option, so it lands on Meta beside the filename rather
	// than in PutOpts — but it is the same strict on/off flag, parsed through the one boolHeader so a
	// typo'd value fails as loudly as a typo'd Buff-Consume.
	executable, err := boolHeader(r, wire.HeaderExecutable)
	if err != nil {
		return clip.Meta{}, store.PutOpts{}, err
	}

	return clip.Meta{Kind: kind, Filename: filename, Executable: executable}, o, nil
}

// boolHeader reads a strict on/off Buff-* flag: absent is off and wire.FlagOn is on, while any
// other value is a malformed request rather than a silent off. Rejecting a bad value keeps these
// flags as strict as the TTL parse above, so a typo'd Buff-Keep fails cleanly instead of quietly
// letting a clip the caller meant to keep forever expire on the default retention. It rejects a
// bad value of a header it knows, not an unknown header — a newer client's unrecognised headers are
// still ignored upstream.
func boolHeader(r *http.Request, name string) (bool, error) {
	switch r.Header.Get(name) {
	case "":
		return false, nil
	case wire.FlagOn:
		return true, nil
	default:
		return false, errBadRequest
	}
}
