package buffer

import (
	"io"
	"sync"
)

// memBacking keeps a Buffer's bytes in a single growing slice. It is the backing used for tests and
// for ephemeral, in-process clips; the disk backing keeps bytes in a file instead. It guards the
// slice with its own mutex, deliberately separate from the Buffer's size lock, so readers copying
// bytes never contend with the writer advancing the size — the separation that lets readers fan out
// from one writer.
type memBacking struct {
	mu   sync.Mutex
	data []byte
}

func newMemBacking() *memBacking { return &memBacking{} }

// append copies p onto the end of the slice and returns len(p). A slice append cannot partially
// fail, so it never reports a short count or an error.
func (m *memBacking) append(p []byte) (int, error) {
	m.mu.Lock()
	m.data = append(m.data, p...)
	m.mu.Unlock()
	return len(p), nil
}

// sync does nothing: bytes held in memory have no stable storage to flush to.
func (m *memBacking) sync() error { return nil }

// closeWrite does nothing: there is no descriptor to release for an in-memory slice.
func (m *memBacking) closeWrite() error { return nil }

// openRead hands back a handle that shares the backing. There is no descriptor to open or to
// refcount, so every handle is independent and opening one after all earlier ones have closed is
// free.
func (m *memBacking) openRead() (readHandle, error) { return &memHandle{m: m}, nil }

// memHandle is a reader's view of a memBacking. It holds the backing by pointer, which is what
// keeps the bytes readable after the store has dropped the generation: as long as a reader holds a
// handle the garbage collector cannot reclaim the slice — the in-memory stand-in for an open file
// descriptor pinning an unlinked inode on disk.
type memHandle struct{ m *memBacking }

// ReadAt copies bytes at off into p. It takes the backing lock only long enough to capture the
// current slice header, then copies outside the lock from the immutable [0,size) region, so
// concurrent readers serialise neither on each other nor on the writer. It honours io.ReaderAt
// fully: a negative offset returns errOffset, and a read that reaches the end of the stored bytes
// returns what it could and io.EOF. Every in-package caller passes a non-negative offset, so that
// guard is defence in depth.
func (h *memHandle) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errOffset
	}
	h.m.mu.Lock()
	data := h.m.data
	h.m.mu.Unlock()
	if off >= int64(len(data)) {
		return 0, io.EOF
	}
	n := copy(p, data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Close releases the handle. A memory handle owns no descriptor, so there is nothing to release and
// repeated calls are safe.
func (h *memHandle) Close() error { return nil }
