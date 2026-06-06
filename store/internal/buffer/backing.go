package buffer

import "io"

// backing is the byte store beneath a Buffer: the place appended bytes are kept and the place
// readers read them from. A Buffer wraps exactly one backing for its whole life, so where bytes
// live — an in-memory slice or a file on disk — is invisible to the followable machinery written
// above it.
//
// The contract every backing must honour, stated here because both implementations and every reader
// lean on it:
//
// - append stores its bytes before it returns. A Buffer publishes the new size only after append
// returns, so any size a reader can observe is always backed by bytes that already exist — a reader
// can never address a byte that is not there.
//
// - openRead is refcounted and lazy. The first outstanding handle opens the underlying source;
// the last handle closed releases it. Open sources therefore scale with the number of generations
// being read, not the number of readers, and a source reopened after the count fell back to zero
// — a fresh read once every earlier reader has gone — must work. It is only ever called on a
// backing whose source still exists: the store opens a read on a generation it still holds, before
// releasing it, so the source is present at the call; a backing whose source has since vanished may
// return an error rather than invent one.
//
// - openRead is a leaf operation. A caller may invoke it while holding a higher lock, so it must
// take only the backing's own brief state and must never reach back out into another lock, which
// could invert an ordering it knows nothing of.
//
// - reads of already-published bytes take no Buffer lock. The bytes in [0,size) are immutable once
// published, so a reader copies them without contending on the lock the writer uses to advance
// size — which is what lets many readers fan out from one writer for free. A backing guards its own
// state (or needs no lock at all); it never reaches for the Buffer's.
type backing interface {
	// append stores p and reports how many bytes were stored. There is a single writer, so calls
	// never overlap, and it never runs after closeWrite has released the append side — the Buffer's
	// terminated() gate refuses a write past a terminal before it reaches here. The bytes are in the
	// backing before it returns; a short count with an error means only that many bytes were stored.
	append(p []byte) (int, error)

	// sync flushes stored bytes to stable storage. On disk this is an fsync; in memory there is
	// nothing to flush and it does nothing. Like append it is a writer-side call and never runs after
	// closeWrite.
	sync() error

	// openRead returns a fresh read view of the backing's bytes, opening the underlying source on the
	// first outstanding handle (see the refcount contract above).
	openRead() (readHandle, error)

	// closeWrite releases the append side. The Buffer calls it once, on a terminal — a clean finish
	// or an abort, the only moment the writer is done. An implementation must still release at most
	// once if it is ever called twice: the same close-once discipline readHandle.Close follows, so
	// a descriptor close that is not idempotent can never fire twice should a terminal be signalled
	// redundantly.
	closeWrite() error
}

// readHandle is one reader's view of a backing's immutable bytes. ReadAt is safe to call
// concurrently on a single handle and across sibling handles of the same backing: it addresses only
// already-published, immutable bytes and carries no shared cursor. Close is idempotent — calling it
// more than once releases the handle exactly once and never double-releases the source beneath it,
// because the unwind of an aborted read can legitimately close the same handle twice.
type readHandle interface {
	io.ReaderAt
	io.Closer
}
