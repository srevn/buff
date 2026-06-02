package api

import (
	"time"

	"github.com/srevn/buff/clip"
)

// wireClip is the JSON shape of a clip in a list response, kept deliberately separate from
// clip.Clip so the wire format is decoupled from the domain type: a field added or renamed in
// the domain cannot silently change these bytes. Times are RFC 3339 strings, matching the
// Buff-Expires header's encoding, and an absent expiry is the empty string so omitempty drops
// it — a zero time.Time would not omit and would render as a misleading zero instant, so the
// formatting is done here rather than left to the struct.
type wireClip struct {
	Name        string    `json:"name"`
	Generation  string    `json:"generation"`
	Kind        clip.Kind `json:"kind"`
	Filename    string    `json:"filename,omitempty"`
	Size        int64     `json:"size"`
	CreatedAt   string    `json:"created_at"`
	FinalizedAt string    `json:"finalized_at"`
	ExpiresAt   string    `json:"expires_at,omitempty"`
	ConsumeOnce bool      `json:"consume_once"`
}

// listEnvelope wraps the clip array so a list response is an object rather than a bare array,
// leaving room for a pagination cursor and top-level metadata without breaking clients. Clips
// is always non-nil so an empty store marshals to [] rather than null.
type listEnvelope struct {
	Clips []wireClip `json:"clips"`
	Next  string     `json:"next"`
}

// healthDoc is the /health body: liveness plus the static capability lists a client can probe
// before relying on an optional feature.
type healthDoc struct {
	Status   string   `json:"status"`
	Version  string   `json:"version"`
	API      []string `json:"api"`
	Features []string `json:"features"`
}

// toWire projects a finalized clip onto its wire shape. Only finalized clips are listed, so
// CreatedAt and FinalizedAt are always present; ExpiresAt is empty, and so omitted, for a clip
// that never expires. Times are normalised to UTC so the output is canonical.
func toWire(c clip.Clip) wireClip {
	wc := wireClip{
		Name:        c.Name,
		Generation:  c.Generation,
		Kind:        c.Meta.Kind,
		Filename:    c.Meta.Filename,
		Size:        c.Size,
		CreatedAt:   c.CreatedAt.UTC().Format(time.RFC3339),
		FinalizedAt: c.FinalizedAt.UTC().Format(time.RFC3339),
		ConsumeOnce: c.ConsumeOnce,
	}
	if !c.ExpiresAt.IsZero() {
		wc.ExpiresAt = c.ExpiresAt.UTC().Format(time.RFC3339)
	}
	return wc
}
