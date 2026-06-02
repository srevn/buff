package store

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"time"
)

// genID is a generation's opaque, time-sortable identity. The first eight bytes are a
// per-name monotonic counter seeded from the wall clock; the last eight are random. The
// two halves serve different jobs: the counter orders a name's generations (and lets a
// human read creation order off a listing), while the random tail guarantees that two
// generations minted in different process lifetimes — across a crash, even within one
// clock tick — can never share a directory name on disk.
//
// Because the counter is big-endian and the whole id is rendered as fixed-width lowercase
// hex, a lexical compare of two ids equals a numeric compare of their counters: the
// greatest id string is the most recent generation. That equality is what lets the store
// pick the current generation, and recovery pick the survivor, by string comparison alone.
type genID [16]byte

// allocate mints the next id for a name and advances the name's monotonic seed. It is a
// clipHandle method, not a free function, precisely because the seed it advances —
// lastPrefix — is per-name state living on the handle, so the counter and its memory are
// kept in one place; the caller must hold handle.mu.
//
// The counter is the wall clock in nanoseconds, but clamped to strictly exceed the last
// one issued for this name. A clock that jumps backwards — NTP correction, a manual reset —
// therefore cannot mint an id that sorts before an earlier generation of the same name,
// which would otherwise make the older generation masquerade as current. Only one
// generation of a name is live at a time, so a name's generations are minted one after
// another and the counter alone disambiguates them; the random tail never has to break a
// tie.
func (h *clipHandle) allocate(now time.Time) (genID, error) {
	prefix := uint64(now.UnixNano())
	if prefix <= h.lastPrefix {
		prefix = h.lastPrefix + 1
	}
	var id genID
	binary.BigEndian.PutUint64(id[:8], prefix)
	if _, err := rand.Read(id[8:]); err != nil {
		return genID{}, err
	}
	h.lastPrefix = prefix
	return id, nil
}

// String renders the id as 32 lowercase hex characters — its form both on the wire as the
// opaque generation token and, on disk later, as the generation's directory name.
func (id genID) String() string { return hex.EncodeToString(id[:]) }

// prefix decodes the monotonic counter from the id's first eight bytes. Recovery uses it to
// reseed a name's lastPrefix from its surviving directory names, so a restart resumes the
// monotonic sequence where it left off rather than risking a lower id after a backward clock.
func (id genID) prefix() uint64 { return binary.BigEndian.Uint64(id[:8]) }

// after reports whether id names a more recent generation than other. It compares the two ids by
// their big-endian halves — the monotonic counter first, the random tail only to break a tie that
// one name's strictly increasing counters never actually produce — which yields the same ordering
// as a lexical compare of their hex form, reached without rendering either to a string. Recovery
// uses it to pick a name's survivor from its surviving directories.
func (id genID) after(other genID) bool {
	if a, b := id.prefix(), other.prefix(); a != b {
		return a > b
	}
	return binary.BigEndian.Uint64(id[8:]) > binary.BigEndian.Uint64(other[8:])
}
