// Package clip is buff's pure domain vocabulary: the clip value types, the lifecycle sentinel
// errors, and the two name validators. It does no IO, speaks no HTTP, and touches no filesystem; it
// imports only errors, time, regexp, and unicode/utf8.
//
// Both the server and the client depend on it for their shared types. Because it is pure data and
// pure functions, that shared dependency couples no behaviour between the two sides — which is the
// whole reason the domain vocabulary lives in a leaf package instead of being re-spelled, and left
// to drift, on each side.
package clip

import "time"

// Kind records the gesture that produced a clip — and so the shape its bytes arrived in: a
// bare byte stream, a single named file, or a tar of a tree. That shape is the clip's default
// disposition at a consumer's terminal (show, save, extract), but Kind is provenance, never a
// content claim: the relay never inspects bytes, so the kind says how a clip was made, not what
// its bytes are. It never affects how bytes are stored or relayed — content always passes through
// verbatim — so it is advisory metadata, not a storage mode.
type Kind string

// The three kinds name the shape of a producing gesture, not a content type: bytes is an opaque,
// nameless byte stream (piped stdin), file is one named file, and archive is a tar of a tree a
// consumer may extract. The first is bytes, deliberately not text: it is just bytes — no name,
// no structure — and the content-blind relay never reads them to claim more, so a piped PNG is an
// honest bytes clip, never a text clip its own bytes contradict.
const (
	KindBytes   Kind = "bytes"
	KindFile    Kind = "file"
	KindArchive Kind = "archive"
)

// Valid reports whether k is one of the three known kinds. The check is exact: no case folding and
// no defaulting. Interpreting an absent or unknown wire value — for instance defaulting a missing
// kind to bytes — is the HTTP layer's job, deliberately kept out of the domain type.
func (k Kind) Valid() bool {
	return k == KindBytes || k == KindFile || k == KindArchive
}

// Meta is the small descriptive record carried alongside a clip's bytes: the kind, for file and
// archive clips the basename to remember and later restore, and for a file clip whether it was
// runnable.
type Meta struct {
	Kind     Kind
	Filename string // file/archive clips only; a validated basename with no separators
	// Executable is whether a file clip's source carried an executable bit, carried so a paste
	// can restore that one runnable bit of the file's identity. It is a bool, not a full mode, and
	// orthogonal to Kind exactly as Filename is: runnable-or-not is the only permission bit intrinsic
	// to the content, so it is the only one that travels a relay; group/other and the special bits
	// are the consumer's local policy, re-derived from its umask at paste rather than dictated by the
	// producer. Meaningful only for KindFile — an archive carries per-entry modes in its tar, and a
	// bare byte stream has no file to run.
	Executable bool
}

// Normalized returns m with the file-scoped fields cleared on a kind that does not carry them. Meta
// is a flat product, but the domain it models is a sum: a bytes stream carries neither a name nor a
// runnable bit, a file carries both, an archive carries a name (its entries' own modes ride inside
// the tar). The struct cannot hold that cross-field shape, so this re-imposes it — Executable
// survives only on a file clip, Filename only on a file or an archive. An empty or unknown kind
// carries neither, the safe reading of a label this build cannot interpret; the kind itself is left
// untouched, so a stray or unknown label is never silently rewritten to a known one and its routing
// stays advisory.
//
// It is total and idempotent, and a no-op on every shape buff's own producer makes — so it can only
// clear an illegal combination a non-conforming peer or a corrupt record introduced, never alter a
// legal one. That is what makes it safe to apply at every boundary where a wire or disk record
// becomes a domain Meta: the illegal state cannot survive the seam, so no sink, renderer, or durable
// record downstream has to remember the cross-field rule.
func (m Meta) Normalized() Meta {
	if m.Kind != KindFile {
		m.Executable = false
	}
	if m.Kind != KindFile && m.Kind != KindArchive {
		m.Filename = ""
	}
	return m
}

// Clip is the runtime view of a clip's current state, returned by stat and list operations and
// reflected onto response headers. The durable on-disk metadata record is a superset of these
// fields.
//
// It deliberately carries no struct tags. The wire representation is a separate data-transfer type
// owned by the HTTP layer, so that adding or renaming a domain field here can never silently change
// the bytes on the wire.
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
