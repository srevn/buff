// Package clip is buff's pure domain vocabulary: the clip value types, the
// lifecycle sentinel errors, and the two name validators. It does no IO, speaks no
// HTTP, and touches no filesystem; it imports only errors, time, regexp, and unicode/utf8.
//
// Both the server and the client depend on it for their shared types. Because it is
// pure data and pure functions, that shared dependency couples no behaviour between
// the two sides — which is the whole reason the domain vocabulary lives in a leaf
// package instead of being re-spelled, and left to drift, on each side.
package clip

import "time"

// Kind is a presentation hint for how a clip's bytes are meant to be rendered. It
// never affects how bytes are stored or relayed — content always passes through
// verbatim — so it is advisory metadata, not a storage mode.
type Kind string

// The three clip kinds: text is an opaque byte stream, file is a single named file,
// and archive is a tar stream a consumer may extract.
const (
	KindText    Kind = "text"
	KindFile    Kind = "file"
	KindArchive Kind = "archive"
)

// Valid reports whether k is one of the three known kinds. The check is exact: no
// case folding and no defaulting. Interpreting an absent or unknown wire value — for
// instance defaulting a missing kind to text — is the HTTP layer's job, deliberately
// kept out of the domain type.
func (k Kind) Valid() bool {
	return k == KindText || k == KindFile || k == KindArchive
}

// Meta is the small descriptive record carried alongside a clip's bytes: the kind,
// and for file and archive clips the basename to remember and later restore.
type Meta struct {
	Kind     Kind
	Filename string // file/archive clips only; a validated basename with no separators
}

// Clip is the runtime view of a clip's current state, returned by stat and list
// operations and reflected onto response headers. The durable on-disk metadata
// record is a superset of these fields.
//
// It deliberately carries no struct tags. The wire representation is a separate
// data-transfer type owned by the HTTP layer, so that adding or renaming a domain
// field here can never silently change the bytes on the wire.
type Clip struct {
	Name        string
	Generation  string // opaque, time-sortable id; assigned at creation
	Meta        Meta
	Size        int64 // exact byte count once finalized; bytes-so-far while still live
	CreatedAt   time.Time
	FinalizedAt time.Time // zero while live
	ExpiresAt   time.Time // zero means no expiry; otherwise an absolute instant
	ConsumeOnce bool
	Finalized   bool // true once durably committed and no longer live; a runtime convenience
}
