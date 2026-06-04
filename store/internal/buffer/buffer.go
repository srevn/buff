// Package buffer provides buff's followable byte log: a single-writer, many-reader,
// append-only log whose size only ever grows and whose readers can follow it as it is
// being written. It is the primitive that makes a live stream and a stored clip the
// same thing — a follower is simply a reader of a log the writer has not finished yet.
//
// A Buffer keeps two concerns behind two locks that are never held at once. Its own lock
// guards the small control state — the published size and the terminal flags — and the
// notifier that wakes blocked readers. A backing, with a lock of its own (or none),
// holds the bytes. Published bytes, the region [0,size), are immutable, so readers copy
// them without touching the Buffer's lock; that is what lets many readers fan out from
// one writer at no mutual cost.
//
// Two backings satisfy the same contract: an in-memory slice (here) and, later, a file
// on disk. The followable machinery — the size notifier, the follower's wait loop, the
// finished-log fast path — is written once, above the backing, and does not change
// between them.
package buffer

import (
	"context"
	"errors"
	"io"
	"sync"
)

// errOffset reports a negative read offset — a programmer error, since every in-package
// caller reads from a non-negative position. It is caught at the boundary (Reader) and in
// the backing's ReadAt rather than left to fault deep in a slice or a pread.
var errOffset = errors.New("buffer: negative read offset")

// errClosed reports a writer-side call — Append or Sync — after a terminal ended the log.
// Like errOffset it is programmer error caught at the boundary: the lone writer owns exactly
// one terminal and never writes past it, so this fires only under misuse — and fires the same
// way for every backing, in place of memory's silent size-grow past a finished log, disk's
// cryptic EBADF deep in a write, or a sealed log's silent no-op. One defined failure, at the
// boundary, before the backing is ever touched.
var errClosed = errors.New("buffer: write after terminal")

// Buffer is a followable, append-only byte log with one writer and any number of
// readers. Construct a live one with NewMemory or NewDisk; NewSealed builds one already
// finished, for a log recovered from disk. The writer calls Append to add bytes and exactly
// one of Finish or Fail to end the log; readers call Reader to follow a live log or Section
// to read a finished one. Every method is safe for concurrent use by readers; Append is
// called by the single writer alone, and is refused once a terminal has run.
type Buffer struct {
	mu      sync.Mutex
	size    int64
	closed  bool          // set by Finish: a caught-up follower then reads io.EOF
	aborted bool          // set by Fail: a caught-up follower then reads clip.ErrAborted
	notify  chan struct{} // closed and replaced on every state change, to wake all waiters
	back    backing
}

// newBuffer wraps a backing in a fresh Buffer with an armed notifier. Every live constructor
// funnels through here so the control state is initialised in exactly one place; the disk
// constructor differs from NewMemory only in the backing it passes.
func newBuffer(back backing) *Buffer {
	return &Buffer{notify: make(chan struct{}), back: back}
}

// newSealedBuffer wraps a backing in a Buffer that is finished at birth: its size is fixed and
// known up front, and closed is set so the log is already complete with no writer to end it. It
// is the constructor for a generation recovered from disk at startup — a log that was finished
// in a previous process and is only ever read now. The notifier is armed like any other Buffer's
// so a follower (should one ever attach to a finished log) waits correctly and reaches EOF at
// once; the ordinary read path takes Section, which never consults it.
func newSealedBuffer(back backing, size int64) *Buffer {
	return &Buffer{notify: make(chan struct{}), back: back, size: size, closed: true}
}

// NewMemory returns a Buffer whose bytes live in memory, ready to Append to.
func NewMemory() *Buffer {
	return newBuffer(newMemBacking())
}

// wakeLocked wakes every reader waiting for a state change. Closing the current notify
// channel releases all of them at once — a single send could wake only one — and a fresh
// channel is installed to arm the next wait. The caller must hold mu, the same lock under
// which a follower captures the channel; that shared lock is why a wakeup can never be
// lost between a follower reading the size and beginning to wait.
func (b *Buffer) wakeLocked() {
	close(b.notify)
	b.notify = make(chan struct{})
}

// advance publishes n freshly stored bytes and wakes followers. It runs under mu and is
// the only place size grows, so a published size is always backed by bytes that already
// exist.
func (b *Buffer) advance(n int64) {
	b.mu.Lock()
	b.size += n
	b.wakeLocked()
	b.mu.Unlock()
}

// terminated reports whether a terminal — Finish or Fail — has ended the log. It reads the
// very flags a follower reads, under the very lock a follower holds, so it is the writer's
// gate derived from the terminal state rather than stored beside it: there is no fourth bit to
// set when a terminal fires, so the gate can never drift from the closed/aborted it shadows. It
// guards the sequential misuse — a write after a terminal returned; a write truly concurrent
// with a terminal is a single-writer-contract violation no cheap gate can mend, and not this
// gate's job.
func (b *Buffer) terminated() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed || b.aborted
}

// Append stores p and then publishes the stored bytes, in that order, so a reader can never
// observe a size whose bytes are not yet there. It returns the number of bytes stored and any
// error from the backing, advancing the published size by exactly the count stored. Only the
// single writer calls Append, and only before a terminal: a call after one returns errClosed
// before the backing is touched, refusing the write that would otherwise grow a finished log
// past the size a follower was already told was complete.
func (b *Buffer) Append(p []byte) (int, error) {
	if b.terminated() {
		return 0, errClosed
	}
	n, err := b.back.append(p)
	if n > 0 {
		b.advance(int64(n))
	}
	return n, err
}

// Sync flushes stored bytes to stable storage (an fsync on disk, nothing in memory). It is
// separate from Finish because the writer interleaves store-owned metadata IO between flushing
// the bytes and marking the log finished, and only the writer can own that whole sequence. Like
// Append it runs only before a terminal; a call after one returns errClosed.
func (b *Buffer) Sync() error {
	if b.terminated() {
		return errClosed
	}
	return b.back.sync()
}

// Finish ends the log cleanly: a follower that has read every byte then reads io.EOF. It
// wakes any waiting followers and releases the append side. It must be called at most
// once and never together with Fail — the writer that owns the log calls exactly one
// terminal, exactly once.
func (b *Buffer) Finish() error {
	b.mu.Lock()
	b.closed = true
	b.wakeLocked()
	b.mu.Unlock()
	return b.back.closeWrite()
}

// Fail ends the log as torn: a follower that has read every buffered byte then reads
// clip.ErrAborted, never io.EOF, so a truncated stream can never be mistaken for a
// complete one. It wakes any waiting followers and releases the append side. Like Finish
// it is called at most once and never together with Finish.
func (b *Buffer) Fail() error {
	b.mu.Lock()
	b.aborted = true
	b.wakeLocked()
	b.mu.Unlock()
	return b.back.closeWrite()
}

// Size reports the number of bytes published so far: growing while the log is live, fixed
// once it is finished. It is the single source of truth for a log's size in both states.
func (b *Buffer) Size() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size
}

// Reader returns a follower that streams the log from off, blocking for more bytes until
// the log ends and delivering io.EOF only on a clean Finish. Reads stop promptly when ctx
// is cancelled. off must be non-negative; a caller following from the start passes 0. The
// returned reader must be closed to release its read handle.
func (b *Buffer) Reader(ctx context.Context, off int64) (io.ReadCloser, error) {
	if off < 0 {
		return nil, errOffset
	}
	h, err := b.back.openRead()
	if err != nil {
		return nil, err
	}
	return &follower{b: b, ctx: ctx, off: off, h: h}, nil
}

// Section returns a reader over a finished log's bytes, [0,size). It captures the size
// once and reads that fixed range, so it needs no notifier and never blocks — the fast
// path for an already-finished log. It is meant for finished logs only; the caller
// guarantees the log will not grow further. The returned reader must be closed to release
// its read handle.
func (b *Buffer) Section() (io.ReadCloser, error) {
	b.mu.Lock()
	size := b.size
	b.mu.Unlock()
	h, err := b.back.openRead()
	if err != nil {
		return nil, err
	}
	return &sectionReader{SectionReader: io.NewSectionReader(h, 0, size), h: h}, nil
}

// sectionReader adds Close to an io.SectionReader so that closing the reader releases the
// read handle beneath it; io.SectionReader does not close its source. Close releases the
// handle exactly once.
type sectionReader struct {
	*io.SectionReader
	h    readHandle
	once sync.Once
}

func (s *sectionReader) Close() error {
	var err error
	s.once.Do(func() { err = s.h.Close() })
	return err
}
