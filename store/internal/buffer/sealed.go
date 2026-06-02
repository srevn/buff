package buffer

import "os"

// sealedBacking is the read-only backing of a finished log: a file on disk with no writer. It is
// the whole of a generation recovered at startup, whose bytes were written and finished in a
// previous process — all that remains is to read them. It has no write side at all; it is just
// the shared, refcounted read descriptor, so it embeds readShare as its entirety and adds only
// the no-op write methods the backing contract names but a finished log never exercises.
//
// Modelling a recovered generation this way, rather than as a disk backing with a nil append
// descriptor, keeps the type honest: there is no append fd to leak and no nil to guard, and the
// read path is the very same readShare a live disk backing uses — so a recovered clip pins its
// unlinked inode for a draining reader exactly as a runtime-finalized one does.
type sealedBacking struct {
	readShare // the shared, refcounted read descriptor; promotes openRead
}

// sealedBacking satisfies backing: openRead from the embedded readShare, the rest no-ops.
var _ backing = (*sealedBacking)(nil)

// NewSealed returns a finished-at-birth Buffer over a file of exactly size bytes, opening no
// descriptor until the first read. open yields a fresh O_RDONLY descriptor for the bytes, shared
// and refcounted across readers like any file-backed buffer's; recovering ten thousand clips
// therefore costs zero read descriptors until each is first read. size is the file's known byte
// count, reported by Size without ever touching the disk, so the count survives even after the
// bytes are unlinked — which is what keeps quota accounting correct when a recovered clip is
// later superseded.
func NewSealed(open func() (*os.File, error), size int64) *Buffer {
	return newSealedBuffer(newSealedBacking(open), size)
}

func newSealedBacking(open func() (*os.File, error)) *sealedBacking {
	return &sealedBacking{readShare: readShare{open: open}}
}

// append is never called: a sealed log is finished at birth and has no writer. It exists only to
// satisfy the backing contract, and reports nothing stored. The receiver is a pointer, like every
// method on a backing that embeds readShare, so the embedded mutex is never copied.
func (*sealedBacking) append([]byte) (int, error) { return 0, nil }

// sync is never called: there is no writer and nothing unflushed in a finished log.
func (*sealedBacking) sync() error { return nil }

// closeWrite is a no-op: a sealed backing has no append descriptor to release. The Buffer signals
// a terminal only when a writer ends a live log, which never happens for one finished at birth.
func (*sealedBacking) closeWrite() error { return nil }
