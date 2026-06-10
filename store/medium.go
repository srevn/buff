package store

import "github.com/srevn/buff/store/internal/buffer"

// medium is where a generation physically lives and how it is published and reclaimed. One store
// binds to one medium for its whole life: an in-memory medium for ephemeral, test, and embedded
// use, and a disk medium rooted at a data directory. The store consumes only the medium, never
// the byte-log backing beneath it — the medium builds the buffer and hands it back, so the choice
// of where bytes live stays the medium's private business and the store's concurrency machinery
// is written once, above both.
//
// The five methods are the generation's lifecycle seam, and the only place medium-specific IO sits:
//
// - create makes a fresh, empty home for a new generation and returns its byte log. It runs under
// the per-name handle lock, just after the id is allocated there, so it must stay bounded — a disk
// medium makes the generation directory, creates the data file, and fsyncs the one new directory
// entry; an in-memory medium just allocates a buffer. It takes the id alone, not the name: the id
// is the generation's whole on-disk address — the directory is clips/<genid>, with the name living
// only in the metadata record and the RAM registry — so the medium needs nothing else to place it.
// The byte log must exist before the generation that owns it does, which is why create takes the
// id rather than a generation. It does not write metadata: a generation is published at finalize,
// not create.
//
// - finalize durably publishes the generation's metadata — a disk medium writes and commits a
// metadata file; an in-memory medium has nothing to persist. It runs after the byte log is synced
// and before the in-memory current-pointer flip, and outside the handle lock, so its fsyncs never
// stall other operations on the same name. The generation already carries the finalized time the
// published record needs. Like claim and unpublish it forwards commit's committed flag — whether
// the publish rename took effect — but the writer reads it for a different fork. Not an in-memory
// one: a failed finalize always discards the live generation, the previous current standing,
// whichever way committed falls. An on-disk one: a publish that renamed its meta.json in and then
// could not flush it (committed, with an error) leaves a present marker a crash could let recovery
// reinstate, so the writer durably retires it via abortPublish before discarding the home; a
// publish whose rename never took (not committed) left nothing to retire. committed with no error
// is a clean publish.
//
// - claim durably marks a finalized consume-once generation as claimed, so a crash cannot resurrect
// a secret already handed out — a disk medium renames meta.json to meta.consumed, which both stops
// the generation resolving as a readable current and, with the directory fsync, makes the claim
// durable; an in-memory medium has nothing to persist and does nothing. Unlike the others it runs
// under the handle lock: the store's state flip from finalized to consumed is the serialization
// point that lets exactly one reader win, and the durable rename must land inside that same
// window, before any byte is shipped. The brief under-lock fsync is the deliberate cost of that
// guarantee, and nothing at all for memory. It returns whether the claim is committed — whether
// its irreversible step (the rename) took effect — so the store can tell apart the two ways it can
// fail: a claim that never took (the rename failed, no side effect) is reverted to finalized and
// stays claimable, while a claim that took but could not be made durable (the rename succeeded, its
// fsync did not) is forfeit — meta.json is already gone — so the store destroys it in place rather
// than reverting to a claimable state it can no longer honour. A claim that reports committed with
// no error has fully succeeded.
//
// - unpublish durably retires a finalized generation a Delete or a reap removes — a disk medium
// renames meta.json to meta.deleted, the removal twin of claim's rename, which stops the generation
// resolving and, with the directory fsync, makes the retirement survive a crash; an in-memory
// medium has nothing to persist and does nothing. Like claim it runs under the handle lock —
// current still points at the generation while it renames, so the lock is what keeps a concurrent
// supersede from reclaiming that same home underneath the in-flight rename — and it forwards
// whether its irreversible rename committed, so the store tells a retire that never took (revert:
// leave the clip standing) from one that took but could not flush (forfeit: clear it to match a
// disk where meta.json is already gone). It is what gives a removal the crash-atomicity a finalize
// already has, where a best-effort remove alone would let a failed reclaim resurrect a deleted clip
// at the next boot.
//
// - abortPublish durably retires the present meta.json of a generation whose publish rename
// took but whose finalize then failed — a disk medium renames meta.json to meta.aborted, the
// finalize-abort sibling of claim's and unpublish's rename-aside; an in-memory medium has nothing
// to persist. The writer calls it on exactly the committed-but-failed finalize arm, before the
// best-effort RemoveAll that discards the home, so a crash GCs a markerless leftover rather than
// resurrecting a write the client was told failed. It is alone among the rename-aside methods in
// returning nothing and running off the handle lock: off the lock because the generation is the
// writer's own live one, never a name's current, so no supersede, delete, or reap can race its home
// the way they could a retire of current; returns-nothing because the upload has already failed —
// there is no in-memory fork to drive and no success to report — and because the store core carries
// no logger, a failed retire is the medium's own to log, the durability twin of remove, not the
// caller's to silently discard.
//
// - remove reclaims a generation's home — a disk medium deletes its directory; an in-memory medium
// does nothing and lets the dropped generation be collected once its last reader releases it. It is
// best-effort and runs outside the handle lock: a reader still holding the generation's bytes keeps
// reading them to completion, and the operation that triggered the reclaim has already succeeded —
// so remove returns nothing, because there is no outcome a caller may act on. It and abortPublish
// are the two that return nothing: the rest gate the operation that calls them, these two never
// can. A disk medium that cannot delete the home records it rather than failing the caller; the
// bytes then linger until something reclaims them, and the medium promises no more than that.
type medium interface {
	create(id genID) (*buffer.Buffer, error)
	finalize(g *generation) (committed bool, err error)
	claim(g *generation) (committed bool, err error)
	unpublish(g *generation) (committed bool, err error)
	abortPublish(g *generation)
	remove(g *generation)
}

// memMedium keeps generations in memory. create hands back a memory-backed buffer; finalize,
// abortPublish, and remove have nothing durable to do.
//
// remove must not touch the buffer's bytes. A superseded or deleted memory generation is reclaimed
// by dropping its pointer and letting the garbage collector free it once the last reader lets go —
// the in-process stand-in for an open file descriptor pinning an unlinked inode on disk. Zeroing or
// reusing the slice instead would tear a reader still draining the old value, which is exactly the
// read-after-supersede guarantee the shared contract proves.
type memMedium struct{}

// Interface conformance, checked at compile time — the value-receiver twin of diskMedium's pointer
// assertion. A drifting method set is a build error here, not only at the newStore call site that
// happens to pass a memMedium.
var _ medium = memMedium{}

// create returns a fresh in-memory byte log. It ignores the id: that addresses an on-disk home a
// memory generation does not have.
func (memMedium) create(genID) (*buffer.Buffer, error) { return buffer.NewMemory(), nil }

// finalize has nothing to persist for an in-memory generation. It reports the publish committed
// with no error for symmetry with claim and unpublish — there is no durable publish step to half-
// complete — so the writer never reaches the on-disk retire a committed-with-error arm would gate.
func (memMedium) finalize(*generation) (committed bool, err error) { return true, nil }

// claim has nothing to persist for an in-memory generation: the store's under-lock flip to the
// consumed state is memory's entire claim, since there is no on-disk marker to rename. It reports
// the claim committed and never errors — there is no durable step to half-complete — so the store
// neither reverts nor destroys on its account.
func (memMedium) claim(*generation) (committed bool, err error) { return true, nil }

// unpublish has nothing to persist for an in-memory generation: the store's under-lock clear of the
// current pointer is memory's entire retire, since there is no on-disk marker to rename. Like claim
// it reports committed with no error — there is no durable step to half-complete — so the store
// neither reverts nor forfeits on its account.
func (memMedium) unpublish(*generation) (committed bool, err error) { return true, nil }

// abortPublish has nothing to retire for an in-memory generation: a memory medium never published
// a durable marker, so a failed finalize leaves nothing on disk to rename aside. Its finalize never
// reports the committed-with-error arm that would reach here, so this is a no-op kept for interface
// completeness, the in-memory twin of the disk medium's real rename.
func (memMedium) abortPublish(*generation) {}

// remove drops an in-memory generation without touching its bytes, leaving any live reader to keep
// them alive by holding the buffer. Nothing physically lingers and nothing can fail, so there is
// never anything to record.
func (memMedium) remove(*generation) {}
