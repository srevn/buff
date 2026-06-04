// Package archive turns a set of local paths into a deterministic tar and untars an
// untrusted tar back into a confined directory. It is a pure leaf: it imports only the
// standard library and reaches the filesystem for extraction solely through a caller-
// supplied *os.Root, so it knows nothing of buff's store, wire, or HTTP and can be
// tested in isolation against hostile input.
//
// The two directions have deliberately asymmetric trust.
//
//   - Stream reads the caller's OWN files by ordinary path. Taring your own tree is not
//     a security boundary, so the source side is unconfined: it opens paths directly and
//     walks them with filepath.WalkDir. Its job is fidelity and determinism, not defense.
//
//   - Extract untars UNTRUSTED bytes — an archive that may have come from anywhere — so
//     every file and directory it creates is made through the caller's *os.Root, which
//     makes escape from the destination impossible by construction. On top of that
//     boundary the policy is conservative: regular files and directories only, no-clobber,
//     an entry-count cap, and a path that must localize cleanly.
//
// The same asymmetry decides how each direction treats the entry types it does not
// handle. A symlink, hardlink, device, FIFO or socket in your own tree is something you
// plausibly did not mean to transfer, so Stream skips it (reporting it through
// StreamOpts.OnSkip) and keeps going. The identical entry inside an untrusted archive is
// the abuse surface, so Extract rejects it and fails the whole operation.
package archive

import (
	"errors"
	"io/fs"
)

// The sentinel errors. Following clip's lead they are bare errors.New values, compared
// with errors.Is: an archive entry name or a source path may be hostile, so the error's
// identity is all a caller matches on and the offending bytes are never echoed back
// inside it. An extraction diagnostic that must point at a specific entry wraps a
// sentinel with the entry's ORDINAL — its position in the archive — never its name.
var (
	// ErrUnsafePath means a tar entry's name is not a clean, local, relative path: it is
	// absolute, escapes its root with "..", is "." or empty, or contains a NUL. Such a
	// name is rejected before any byte is written.
	ErrUnsafePath = errors.New("archive: unsafe entry path")

	// ErrUnsupportedEntry means a tar entry is neither a regular file nor a directory — a
	// symlink, hardlink, device, FIFO, socket, or other special type that confined
	// extraction refuses to materialize.
	ErrUnsupportedEntry = errors.New("archive: unsupported entry type")

	// ErrExists means a tar entry's target already exists in the destination. Extraction
	// is no-clobber: an existing name is an error, never an overwrite.
	ErrExists = errors.New("archive: destination entry already exists")

	// ErrTooManyEntries means an archive holds more entries than the extraction cap
	// allows — the tar-bomb backstop for an archive of many tiny entries. Total bytes are
	// already bounded upstream, since buff never compresses.
	ErrTooManyEntries = errors.New("archive: too many entries")

	// ErrDestExists means ExtractNew was asked to publish into a name that already
	// exists. The atomic mode publishes a whole archive into a fresh directory, so a
	// pre-existing destination is refused rather than merged into. The refusal is
	// best-effort under concurrency, not atomic: a name appearing mid-extraction is still
	// refused when the publishing rename can detect it, but os.Root exposes no no-replace
	// rename, so a concurrently-created empty directory may be replaced instead. See
	// ExtractNew for the floor this leaves.
	ErrDestExists = errors.New("archive: destination already exists")

	// ErrDuplicateRoot means two distinct source roots share a basename, which would make
	// their entries collide under one name in the tar. Stream refuses rather than silently
	// merge or overwrite.
	ErrDuplicateRoot = errors.New("archive: duplicate root basename")

	// ErrUnusableRoot means a source root has no usable basename to name its entries — it
	// is ".", "..", or a filesystem root. Resolving such a root to a concrete directory
	// name is a caller (CLI) concern; the package refuses it so its output stays well
	// defined.
	ErrUnusableRoot = errors.New("archive: root has no usable name")

	// ErrNoRoots means Stream was given no source roots to archive.
	ErrNoRoots = errors.New("archive: no roots to archive")

	// ErrEmptyArchive means Stream was given roots that yielded no entries at all — every
	// root resolved to a skipped special file (a device, FIFO, socket, or a symlink, none
	// of which Stream follows). A tar of nothing is not a meaningful clip, so Stream refuses
	// it rather than emit a bare trailer. It is distinct from ErrNoRoots (an empty root
	// list): the roots existed but archived to nothing. A real empty directory is one entry,
	// not zero, so it is unaffected.
	ErrEmptyArchive = errors.New("archive: no entries to archive")
)

// StreamOpts carries the optional knobs of Stream. Its zero value is valid: skips are
// silent (the entry is still skipped).
type StreamOpts struct {
	// OnSkip, if non-nil, is called once for each source entry Stream skips because it is
	// neither a regular file nor a directory. rel is the entry's archive-relative name (a
	// path from the caller's own trusted tree, safe to print) and mode is its file mode.
	// The entry is skipped whether or not OnSkip is set; the hook only reports it. The CLI
	// wires it to a stderr warning.
	OnSkip func(rel string, mode fs.FileMode)
}

// ExtractOpts carries the optional knobs of Extract and ExtractNew. Its zero value is
// valid: the entry cap falls back to the default.
type ExtractOpts struct {
	// MaxEntries caps how many tar entries are extracted — the tar-bomb backstop. A value
	// <= 0 selects defaultMaxEntries.
	MaxEntries int
}

// defaultMaxEntries bounds how many entries a single archive may contain when the caller
// leaves ExtractOpts.MaxEntries unset. It guards only the many-tiny-entries case: an
// archive's total bytes are already bounded by the per-clip size cap upstream, because
// buff never compresses, so extracted size is archive size.
const defaultMaxEntries = 10000

// tempPrefix names the temporary sibling directory ExtractNew untars into before it
// renames the finished tree into place. The leading dot keeps it out of ordinary
// listings; the fixed prefix lets a crashed extraction's leftover be recognized.
const tempPrefix = ".buff-"

// norm returns o with any zero-valued field replaced by its default, so the rest of the
// extractor reads the knobs without repeating the fallback.
func norm(o ExtractOpts) ExtractOpts {
	if o.MaxEntries <= 0 {
		o.MaxEntries = defaultMaxEntries
	}
	return o
}
