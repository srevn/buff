package store

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/srevn/buff/clip"
)

// metaVersion is the schema version stamped into every metadata record this build writes. Recovery
// reads it first: a record whose version is newer than this is uninterpretable, so recovery
// quarantines it rather than guessing at fields it does not understand. That makes the field the
// forward-compatibility escape hatch — the on-disk format can evolve without a silent misread by an
// older binary. This build writes and understands version 1.
const metaVersion = 1

// metaFile is a finalized generation's durable record: the source of truth for its identity, size,
// and retention once the process that wrote it is gone. It is written exactly once, at finalize, as
// JSON — greppable on disk, and tolerant of fields a later version adds.
//
// The presence of this file is itself the finalize marker. A generation directory without one
// is, by definition, an unfinished or aborted write — garbage a crash left behind — so recovery
// reclaims any such directory and a reader never resolves it. Size is the byte count taken from the
// buffer, which equals the data file's length because a short write aborts rather than finalizes;
// recovery cross-checks the two and quarantines a generation where they disagree, catching a data
// file a crash truncated after the bytes but before this record.
type metaFile struct {
	Version     int       `json:"version"`
	Name        string    `json:"name"`                 // the logical clip name, so the record is self-describing for recovery and forensics
	Generation  string    `json:"generation"`           // the generation id, equal to the directory name; cross-checked on load
	Kind        clip.Kind `json:"kind"`                 // the producing gesture; advisory only, bytes pass through verbatim
	Filename    string    `json:"filename,omitempty"`   // remembered basename for a file or archive clip
	Executable  bool      `json:"executable,omitempty"` // file clips: the source's runnable bit, restored at paste
	Size        int64     `json:"size"`                 // exact finalized byte count; equals the data file's length
	CreatedAt   time.Time `json:"created_at"`           // when the generation was opened
	FinalizedAt time.Time `json:"finalized_at"`         // the retention clock's origin
	ExpiresAt   time.Time `json:"expires_at,omitzero"`  // absolute deadline; zero means never expires
	ConsumeOnce bool      `json:"consume_once"`         // delivered to one reader, then destroyed
	Checksum    string    `json:"checksum,omitempty"`   // optional content checksum, e.g. "crc32c:ab12cd34"; empty when checksums are off
}

// loadMeta decodes a metadata record this build can act on, or reports why it cannot. A record that
// is not valid JSON, or whose version is newer than this build understands, is uninterpretable:
// recovery can trust not one field of it — not even its size — so loadMeta rejects both as errors
// and recovery quarantines the generation rather than guess at fields it does not understand. A
// version this build understands (at most metaVersion) is returned for the record-versus-bytes
// checks in validate. Folding the version gate in here, rather than into validate, is deliberate:
// it lets recovery reject an uninterpretable record before it ever stats the data file, so the
// order in which the on-disk states are judged stays "can I read this record at all?" before "does
// it match the bytes?".
func loadMeta(b []byte) (metaFile, error) {
	var mf metaFile
	if err := json.Unmarshal(b, &mf); err != nil {
		return metaFile{}, fmt.Errorf("decode meta.json: %w", err)
	}
	if mf.Version > metaVersion {
		return metaFile{}, fmt.Errorf("meta.json version %d is newer than supported version %d", mf.Version, metaVersion)
	}
	return mf, nil
}

// validate checks an interpretable record against the directory it was found in and the data
// file beside it: the recorded generation must equal the directory name, and the recorded size
// must equal the data file's actual length. A generation mismatch means a record in the wrong
// place — tampering or corruption. A size mismatch means a data file a crash truncated after the
// bytes landed but before this record was made durable; because finalize syncs the data before it
// publishes the record, a present record can only ever sit beside complete data, so a disagreement
// is real corruption — the gap this always-on cross-check exists to catch. Either way the record
// cannot be trusted and recovery quarantines it. The caller has already confirmed via loadMeta that
// the version is interpretable, and that the data file exists.
func (mf metaFile) validate(id genID, dataSize int64) error {
	if mf.Generation != id.String() {
		return fmt.Errorf("meta.json generation %q does not match its directory %q", mf.Generation, id.String())
	}
	if mf.Size != dataSize {
		return fmt.Errorf("meta.json size %d does not match data file size %d", mf.Size, dataSize)
	}
	return nil
}
