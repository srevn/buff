// Package wire is the byte-for-byte protocol contract that the server and the client must agree
// on exactly: the /v1 path prefix and routes, the Buff-* header names, the live-stream completion
// sentinel, and the canonical error table.
//
// It is pure data with zero imports — not even the domain package or net/http — so neither side
// can drift from the other, and the client never couples to the server by importing it. The error
// statuses are plain integers for the same reason: importing net/http for its status constants
// would break the zero-import rule this package exists to hold.
package wire

// V1 is the path prefix for the versioned content API. The health route below is deliberately left
// unversioned so deploy tooling never has to track its version.
const V1 = "/v1"

// Route prefixes both sides build from, so a route is never spelled two different ways. The server
// forms its router pattern by appending a "/{name}" wildcard to PathClips; the client forms a
// request URL by appending an escaped name — both from the one prefix here. PathHealth is the
// unversioned liveness route.
const (
	PathClips  = V1 + "/clips"
	PathHealth = "/health"
)

// The Buff-* header names, plus two reserved names — the standard If-Match and the custom Buff-
// Force — that make up the v1 framing. The server reads them from requests and sets them on
// responses; the client does the reverse. Spelling each exactly once here is what stops the two
// sides drifting. These are the literal on-the-wire names, frozen within /v1.
const (
	HeaderKind       = "Buff-Kind"       // clip kind; set on a PUT, echoed on GET and HEAD
	HeaderFilename   = "Buff-Filename"   // percent-encoded UTF-8 basename; by convention only for file and archive clips
	HeaderExecutable = "Buff-Executable" // file clips: the runnable bit; "1" on a PUT, "true" on GET and HEAD, absent means not executable
	HeaderTTL        = "Buff-TTL"        // PUT: retention as a Go duration, or "0" for the server default
	HeaderKeep       = "Buff-Keep"       // PUT: "1" never expires, overriding any TTL
	HeaderConsume    = "Buff-Consume"    // "1" for consume-once; set on a PUT, reported on GET and HEAD
	HeaderGeneration = "Buff-Generation" // response: the opaque id of the generation served
	HeaderFinalized  = "Buff-Finalized"  // response: "true" or "false" — whether the served generation is durable
	HeaderSize       = "Buff-Size"       // response: byte count; sent only when finalized
	HeaderExpires    = "Buff-Expires"    // response: absolute expiry instant; sent only when finalized
	HeaderStatus     = "Buff-Status"     // trailer on a live chunked GET; its only value is StatusComplete
	HeaderError      = "Buff-Error"      // response: the machine-readable error sentinel from the table below
	HeaderIfMatch    = "If-Match"        // reserved; accepted but not interpreted in v1
	HeaderForce      = "Buff-Force"      // reserved; accepted but not interpreted in v1
)

// FlagOn and BoolTrue are the two halves of the request/response boolean encode-split the header
// comments above describe. A PUT carries a present-when-set flag as FlagOn, absent meaning off, so
// only the on-value ever travels (Buff-Keep, Buff-Consume, Buff-Executable); a GET or HEAD response
// spells a boolean as BoolTrue, which the decode side matches exactly. The two directions differ
// deliberately and only the round-trip test guards their agreement — so spelling each exactly once
// here is what keeps two hand-typed literals from drifting on opposite sides of the wire, the same
// service StatusComplete does for the trailer. BoolTrue must equal what strconv.FormatBool(true)
// emits: the always-present response booleans (Buff-Finalized, Buff-Consume) are formatted with it
// and decoded against BoolTrue, so the constant names the value they already share.
const (
	FlagOn   = "1"
	BoolTrue = "true"
)

// StatusComplete is the sole value of the Buff-Status trailer. A live chunked GET sets it only
// after the writer finalizes cleanly, so a client that never observes it knows the stream was
// truncated rather than completed.
const StatusComplete = "complete"

// The capability strings the server advertises at /health, single-spelled here like every Buff-*
// header and error sentinel rather than as bare literals in the handler. A feature string is now
// protocol vocabulary both sides may read: the server offers it, and a client that gates on an
// optional capability checks for it, so the two must share one symbol and never drift apart as two
// hand-typed strings would.
const (
	FeatureFollow      = "follow"       // a reader may follow a live clip to its clean end
	FeatureConsumeOnce = "consume-once" // a clip may be delivered to one reader, then destroyed
	FeatureWait        = "wait"         // a GET blocks for an absent clip to appear, bounded by its context
)

// Features is the capability set the server advertises verbatim at /health. Listing it here, not in
// the handler, single-sources the advertisement the way Rows single-sources the error table: the
// server sends exactly this slice and a gating client checks membership in it, so neither side re-
// spells a feature literal the other could drift from. Treat it as immutable, like Rows.
var Features = []string{FeatureFollow, FeatureConsumeOnce, FeatureWait}

// ErrInfo is one row of the canonical error table: the machine-readable sentinel a response carries
// in its Buff-Error header, paired with its HTTP status code. The server derives its domain-error-
// to-row map from these rows and the client derives the reverse, so the two directions cannot
// disagree.
type ErrInfo struct {
	Sentinel string
	Status   int
}

// The canonical error rows, each pairing a Buff-Error sentinel with its HTTP status. There is
// deliberately no row for an aborted live stream: that condition resets the connection rather than
// producing a status response, so it has no place in the table.
var (
	ErrNotFound = ErrInfo{Sentinel: "not_found", Status: 404}
	ErrConsumed = ErrInfo{Sentinel: "consumed", Status: 410}
	ErrBusy     = ErrInfo{Sentinel: "busy", Status: 409}
	ErrClosed   = ErrInfo{Sentinel: "closed", Status: 409}
	ErrTooLarge = ErrInfo{Sentinel: "too_large", Status: 413}
	ErrNoSpace  = ErrInfo{Sentinel: "no_space", Status: 507}
	ErrNameBad  = ErrInfo{Sentinel: "name_invalid", Status: 400}
	ErrBadReq   = ErrInfo{Sentinel: "bad_request", Status: 400}
	ErrInternal = ErrInfo{Sentinel: "internal", Status: 500}
	// ErrUnavailable marks a request the server could not complete because it is stopping or otherwise
	// temporarily unable — not the client's fault. Its one current use is an upload cut short by
	// graceful shutdown: the body read ends like a client truncation, but the cause is the operator
	// stopping the server, so reporting bad_request would misattribute the fault. A client with no
	// reverse-map row for it reads a generic 503 and can retry, which is the right advice.
	ErrUnavailable = ErrInfo{Sentinel: "unavailable", Status: 503}
)

// Rows enumerates every error row above, in declaration order, so the translations to and from
// clip errors can be tested for completeness instead of hand-audited: api/ ranges it to prove every
// row is emittable, client/ to prove every row is either reverse-mapped or deliberately a generic
// HTTPError. Without it nothing could range "all wire rows," so each side re-listed the set by
// hand and an added row could slip through unmapped and untested. It names the vars rather than
// re-spelling their values, so it cannot drift from them, and a test parses this file to prove it
// lists exactly the declared ErrInfo vars. A var, not const, because ErrInfo is a struct; treat it
// as immutable, like the rows themselves.
var Rows = []ErrInfo{
	ErrNotFound,
	ErrConsumed,
	ErrBusy,
	ErrClosed,
	ErrTooLarge,
	ErrNoSpace,
	ErrNameBad,
	ErrBadReq,
	ErrInternal,
	ErrUnavailable,
}
