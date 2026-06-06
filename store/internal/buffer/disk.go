package buffer

import (
	"os"
	"sync"
)

// readShare is the read side every file-backed buffer shares: a single O_RDONLY descriptor, opened
// lazily when the first reader attaches and closed when the last one goes, handed to any number of
// readers at once. Open descriptors therefore scale with the number of generations being read, not
// the number of readers — a hundred followers of one clip share one descriptor.
//
// Reads are preads against that shared descriptor. A pread carries no file offset, so any number
// run concurrently — on one handle or across siblings — without serialising on each other or on a
// writer appending at the end. That is what lets readers fan out from one descriptor for free, and
// why the bytes need no lock: only the descriptor and its reference count do.
//
// The mutex guards readFD and refs, never the bytes. A handle captures the descriptor when it
// opens; while it is open the reference count is at least one, so the descriptor can be neither
// closed nor replaced beneath it. The captured descriptor is thus stable for the handle's whole
// life, which is exactly why ReadAt can run unlocked.
//
// It is shared by composition: a read-write disk backing embeds it for the read half of a file it
// also appends to, and a read-only sealed backing embeds it as its whole self. The subtle refcount-
// and-capture code that keeps an unlinked inode readable for a draining reader thus lives, and is
// proven, once — it cannot drift between the two.
type readShare struct {
	open   func() (*os.File, error) // opens a fresh O_RDONLY descriptor for the bytes
	mu     sync.Mutex               // guards readFD and refs; never the bytes
	readFD *os.File                 // the shared read descriptor, open exactly while refs > 0
	refs   int                      // outstanding read handles sharing readFD
}

// openRead returns a reader's view of the bytes, opening the shared descriptor on the first
// outstanding handle and sharing it with every later one. A failed open leaves the count at zero,
// so the next attempt opens afresh rather than handing out a nil descriptor. It takes only this
// share's lock and reaches for no other, so it is safe to call while the store holds a higher lock.
func (s *readShare) openRead() (readHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refs == 0 {
		f, err := s.open()
		if err != nil {
			return nil, err
		}
		s.readFD = f
	}
	s.refs++
	// Capture the descriptor under the lock. It cannot change while this handle holds its reference,
	// so the handle reads through this captured pointer with no further locking.
	return &shareHandle{s: s, fd: s.readFD}, nil
}

// release drops one reader. The last one out closes the shared descriptor and clears it, so a
// generation read again after every reader has gone reopens cleanly on the next openRead.
func (s *readShare) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refs--
	if s.refs == 0 {
		_ = s.readFD.Close()
		s.readFD = nil
	}
}

// shareHandle is one reader's view of a readShare. It captures the shared descriptor at open and
// reads through that capture: the descriptor is guaranteed open for the handle's life because the
// handle holds a reference to it, so no lock is needed on the read path. Close drops that reference
// exactly once.
type shareHandle struct {
	s    *readShare
	fd   *os.File
	once sync.Once
}

// ReadAt preads into p at off through the captured descriptor. It needs no lock: the descriptor is
// open and fixed while this handle holds its reference, and pread carries no cursor, so concurrent
// reads on one handle or across siblings never interfere. A negative offset is rejected at the
// boundary rather than left to fault inside the syscall.
func (h *shareHandle) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errOffset
	}
	return h.fd.ReadAt(p, off)
}

// Close drops the handle's reference to the shared descriptor, exactly once however many times it
// is called — the abort unwind of a read can close the same handle twice.
func (h *shareHandle) Close() error {
	h.once.Do(h.s.release)
	return nil
}

// diskBacking keeps a Buffer's bytes in a file being appended to. The single writer appends through
// one write descriptor; readers share the embedded readShare's one O_RDONLY descriptor. The two
// sides are separate descriptors on the same inode: the writer appends through one, followers pread
// through the other, and the kernel's page cache keeps them coherent with no fsync, which is what
// lets a follower see live bytes the instant they are written.
type diskBacking struct {
	readShare           // the shared, refcounted read descriptor; promotes openRead
	appendFD  *os.File  // the one write descriptor; closeWrite releases it, once
	fsync     bool      // whether sync flushes appended bytes to stable storage
	wOnce     sync.Once // makes closeWrite release the append descriptor at most once
}

// diskBacking satisfies backing; the read half comes from the embedded readShare.
var _ backing = (*diskBacking)(nil)

// NewDisk returns a Buffer whose bytes live in a file being written. appendFD is the descriptor the
// single writer appends through; open yields a fresh O_RDONLY descriptor for the same bytes, called
// once when the first reader attaches and again only if a later reader attaches after every earlier
// one has gone; fsync reports whether Sync flushes to stable storage. The Buffer owns appendFD from
// here on and releases it on its terminal.
func NewDisk(appendFD *os.File, open func() (*os.File, error), fsync bool) *Buffer {
	return newBuffer(newDiskBacking(appendFD, open, fsync))
}

func newDiskBacking(appendFD *os.File, open func() (*os.File, error), fsync bool) *diskBacking {
	return &diskBacking{readShare: readShare{open: open}, appendFD: appendFD, fsync: fsync}
}

// append writes p at the end of the file. *os.File.Write reports a short count only together with
// a non-nil error, which is exactly the backing contract — a partial disk write (a full disk, say)
// surfaces the count that landed and the error at once, and the Buffer publishes just those bytes.
func (d *diskBacking) append(p []byte) (int, error) { return d.appendFD.Write(p) }

// sync flushes appended bytes to stable storage when durability is on, and does nothing when it
// is off. It gates itself on the backing's own flag, so the Buffer can call Sync without learning
// whether this clip is meant to be durable — the backing alone holds that choice.
func (d *diskBacking) sync() error {
	if !d.fsync {
		return nil
	}
	return d.appendFD.Sync()
}

// closeWrite releases the append descriptor, at most once however many times it is called. A file
// descriptor close is not idempotent, so the once guard is what lets the Buffer signal a terminal
// more than once — which the abort unwind can — without closing the descriptor twice.
func (d *diskBacking) closeWrite() error {
	var err error
	d.wOnce.Do(func() { err = d.appendFD.Close() })
	return err
}
