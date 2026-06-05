package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/srevn/buff/store/internal/buffer"
)

// The on-disk layout vocabulary. Every path the disk medium builds is composed from these, so
// the shape of the tree — and the names a later recovery pass will look for — is defined in one
// place:
//
//	clips/<genid>/
//	  data           the byte log: the append target and the ReadAt source
//	  meta.json      the finalize marker and durable record; present iff the generation finalized
//	  meta.json.tmp  the half-written record, present only mid-finalize
//	  meta.consumed  the durable consume-once claim marker
//
// <genid> is the generation id's string form — fixed-width lowercase hex, globally unique across
// names and process lifetimes by its random tail — so it alone names a generation's directory,
// directly under one flat clips/. The caller's name appears in no path: it lives only in
// meta.json and the RAM registry. Two consequences follow, both load-bearing. Traversal is
// impossible by construction, and no future name shape — Unicode, hierarchical — can change the
// layout, because the name is never a path component. And a generation is one whole directory
// created, published, claimed, and reclaimed as a unit, with no per-name parent left behind once
// its last generation is gone — so high-cardinality, never-reused names leave nothing behind.
//
// Beside clips/ sits quarantine/, a flat home for generations recovery could not interpret —
// a record from a newer version, a truncated or mismatched data file. A quarantined generation
// is moved there whole, as quarantine/<genid>/, and never auto-deleted: it is preserved for an
// operator to inspect, the deliberate opposite of discarding data we do not understand.
const (
	dirClips      = "clips"
	dirQuarantine = "quarantine"
	fileData      = "data"
	fileMeta      = "meta.json"
	fileMetaTmp   = "meta.json.tmp"
	fileConsumed  = "meta.consumed"
)

// diskMedium stores generations as directories beneath one os.Root, the boundary nothing
// escapes. It is stateless per generation — every path is derived from the generation id on
// demand — so it holds only the root it writes through and whether to flush each durable step.
//
// fsync and checksum are parameters of the medium, not fields of Config. Config is
// medium-agnostic policy whose zero value disables each knob; durability must instead default on,
// and both flushing and checksumming are meaningful only on disk. Making them construction
// arguments of the one constructor that has a disk keeps Config pure and makes them explicit
// choices where the disk is chosen. The logger is injected for the same reason — no global
// mutable state — and recovery is its one user: it logs loudly when it quarantines, and a boot
// summary when it had work to do.
type diskMedium struct {
	root     *os.Root
	fsync    bool
	checksum bool
	log      *slog.Logger
}

// Interface conformance, checked at compile time so a drifting method signature is a build
// error rather than a runtime surprise.
var _ medium = (*diskMedium)(nil)

// DiskOpts carries the disk medium's knobs: whether to flush each durable step to stable storage,
// whether to store and verify a content checksum, and the logger recovery reports through. The
// zero value is the safe-but-fast choice for tests and embedding — no flushing, no checksum, the
// default logger; a server maps its environment into it. It is a struct rather than positional
// booleans so each knob reads at the call site and so a future disk knob is an additive field,
// not a signature change.
type DiskOpts struct {
	Fsync    bool         // flush data, metadata, and directory entries to stable storage
	Checksum bool         // compute a CRC32C at finalize and verify it at recovery
	Logger   *slog.Logger // recovery's logger; nil uses slog.Default()
}

// NewDisk returns a store that keeps clips as directories beneath root, bounded and aged by c and
// configured by o. The caller owns root and closes it once the store is done. Before serving, it
// replays whatever the last process left on disk: ensureDirs prepares the shared parents, scan
// classifies every generation directory — keeping survivors, reclaiming garbage, quarantining the
// uninterpretable — and restore rebuilds the in-memory registry, the per-name id seed, and the
// quota from exactly what survived. The error return reports a construction failure the store
// cannot run without: an unreadable or unpreparable root structure. A single bad generation never
// reaches it — recovery isolates and logs that — so one corrupt clip cannot brick the boot.
func NewDisk(root *os.Root, c Config, o DiskOpts) (Store, error) {
	log := o.Logger
	if log == nil {
		log = slog.Default()
	}
	m := &diskMedium{root: root, fsync: o.Fsync, checksum: o.Checksum, log: log}
	if err := m.ensureDirs(); err != nil {
		return nil, err
	}
	rec, err := m.scan(o.Checksum)
	if err != nil {
		return nil, err
	}
	s := newStore(m, time.Now, c)
	s.restore(rec)
	// Surface what recovery did, but only when it did something: a fresh or cleanly-shut store
	// recovers nothing and stays silent, so the common case adds no noise, while a boot that kept,
	// quarantined, or reclaimed anything leaves one summary line. A non-zero quarantined is the
	// operator's signal that corruption was preserved under quarantine/ and awaits inspection.
	if n := len(rec.survivors); n > 0 || rec.quarantined > 0 || rec.reclaimed > 0 {
		log.Info("recovery complete", "recovered", n, "quarantined", rec.quarantined, "reclaimed", rec.reclaimed)
	}
	return s, nil
}

// ensureDirs makes the shared top-level directories — clips/ and quarantine/ — exist, and durable
// when durability is on, once at construction before any request. Establishing them here rather
// than lazily means no per-name or recovery path ever has to create a shared parent, so two
// first-ever operations can never race over whether a parent's entry in the data root is durable:
// by the time any generation is written or quarantined, both parents are already durable. One
// directory sync of the data root makes both new entries durable at once.
func (m *diskMedium) ensureDirs() error {
	clipsNew, err := mk(m.root, dirClips)
	if err != nil {
		return err
	}
	quarNew, err := mk(m.root, dirQuarantine)
	if err != nil {
		return err
	}
	if m.fsync && (clipsNew || quarNew) {
		return m.syncDir(".") // one sync makes both new entries in the data root durable
	}
	return nil
}

// genPath is the directory holding one generation: clips/<genid>. The generation id alone names
// it — globally unique across names and process lifetimes by its random tail — so a clip's name
// appears in no path and lives only in the metadata record and the RAM registry. That uniqueness
// is what lets every generation share one flat clips/ with no name-derived parent: a generation
// is created, published, claimed, and reclaimed as one whole directory keyed by its id.
func genPath(id genID) string {
	return dirClips + "/" + id.String()
}

// create makes a fresh home for a new generation and returns its byte log. On any failure it
// removes this generation's own directory, so a half-made generation never lingers; it touches no
// sibling, so a prior finalized generation is never at risk. It runs under the per-name handle
// lock, so it stays bounded: one directory creation, the data file, and the single directory
// fsync that makes the new entry durable.
func (m *diskMedium) create(id genID) (*buffer.Buffer, error) {
	genDir := genPath(id)
	buf, err := m.openGen(genDir)
	if err != nil {
		_ = m.root.RemoveAll(genDir) // best-effort: undo this generation's own half-made directory
		return nil, err
	}
	return buf, nil
}

// openGen creates the generation directory and opens its data file as the single append target,
// handing the buffer an opener that reopens that file O_RDONLY for readers. The append and read
// sides are separate descriptors on the same inode: the writer appends through one, followers
// pread through the other, and the kernel's page cache keeps them coherent with no fsync, which
// is what lets a follower see live bytes the instant they are written.
func (m *diskMedium) openGen(genDir string) (*buffer.Buffer, error) {
	if err := m.makeGenDir(genDir); err != nil {
		return nil, err
	}
	dataPath := genDir + "/" + fileData
	f, err := m.root.OpenFile(dataPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	open := func() (*os.File, error) { return m.root.Open(dataPath) }
	return buffer.NewDisk(f, open, m.fsync), nil
}

// makeGenDir creates clips/<genid>/ and, when durability is on, makes its new entry durable. A
// directory fsync makes the entries within that directory durable, not the directory's own entry
// in its parent — so to make the new <genid> entry durable, its parent clips/ is fsynced. clips/
// exists durably from construction, so this is the whole chain: one Mkdir, one parent fsync, with
// no per-name level to build or to leave behind.
//
// The Mkdir is strict — any error, including that the directory already exists, fails the create.
// Because clips/ is flat, this guards global id uniqueness across every name, not just one name's
// generations: the astronomically unlikely collision of two random generation ids surfaces loudly
// here instead of silently reusing a directory.
func (m *diskMedium) makeGenDir(genDir string) error {
	if err := m.root.Mkdir(genDir, 0o700); err != nil {
		return err
	}
	if !m.fsync {
		return nil
	}
	return m.syncDir(dirClips) // the new <genid> entry's parent
}

// mk creates one directory, reporting whether it was the creator. An already-existing directory
// is not an error — a restart over a populated data root finds clips/ and quarantine/ already
// there — but every other failure is returned. The created flag lets ensureDirs fsync the data
// root only when it actually added an entry.
func mk(root *os.Root, path string) (created bool, err error) {
	if err = root.Mkdir(path, 0o700); err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrExist) {
		return false, nil
	}
	return false, err
}

// finalize writes the generation's durable record and publishes it atomically. It marshals the
// metadata, streams it to meta.json.tmp and flushes that, then commits the tmp file to its final
// name — the single rename whose durable appearance is what makes the clip recoverable. It runs
// off the handle lock, after the bytes are synced and before the in-memory current pointer
// flips, so its fsyncs never stall another operation on the same name.
func (m *diskMedium) finalize(g *generation) error {
	genDir := genPath(g.id)
	mf := metaFile{
		Version:     metaVersion,
		Name:        g.name,
		Generation:  g.id.String(),
		Kind:        g.meta.Kind,
		Filename:    g.meta.Filename,
		Executable:  g.meta.Executable,
		Size:        g.buf.Size(),
		CreatedAt:   g.created,
		FinalizedAt: g.finalized,
		ExpiresAt:   g.expires,
		ConsumeOnce: g.consume,
	}
	// With checksums on, hash the data into the record before publishing it. The writer has
	// already synced the bytes, so this re-read is page-cache-hot and streamed — no clip bytes
	// land on the heap — and recovery verifies the same hash the same way. Off by default: the
	// trade is doubling finalize's read IO, declined unless asked for.
	if m.checksum {
		sum, err := checksumData(m.root, genDir+"/"+fileData)
		if err != nil {
			return err
		}
		mf.Checksum = sum
	}
	if err := m.writeMeta(genDir, mf); err != nil {
		return err
	}
	// A finalize that fails — whether the rename never happened or happened but could not be
	// made durable — routes through the writer's abort, which removes the whole generation
	// directory, so the half-published meta.json cannot survive either way; the committed flag
	// the rename reports matters only to the consume claim, not here.
	_, err := m.commit(genDir, fileMetaTmp, fileMeta)
	return err
}

// writeMeta marshals mf and writes it to meta.json.tmp, flushing the bytes to stable storage
// when durability is on so the record's data is durable before the commit makes its name
// durable. Marshalling happens before the file is touched, so a marshal failure leaves no stray
// tmp; the file is closed on every path. The tmp bears the final name only after the caller's
// commit, so a half-written record is never mistaken for a finalized one.
func (m *diskMedium) writeMeta(genDir string, mf metaFile) error {
	b, err := json.Marshal(mf)
	if err != nil {
		return err
	}
	f, err := m.root.OpenFile(genDir+"/"+fileMetaTmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if m.fsync {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
	}
	return f.Close()
}

// claim durably marks a finalized consume-once generation as claimed by renaming meta.json to
// meta.consumed. The rename does double duty: it removes the only file that makes the generation
// resolve as a readable current, so a racing reader finds nothing to claim, and — made durable
// by the directory fsync inside commit — it ensures a crash cannot resurrect a secret already
// handed out. The store calls it under the handle lock, the window in which exactly one reader
// wins the claim, before any byte ships.
//
// It forwards commit's committed flag: the rename is the irreversible step. If the rename fails
// the claim never took and the store reverts it; if the rename succeeds but its fsync does not,
// the claim took but is not durable — meta.json is already gone — and the store destroys the
// generation in place rather than reverting to a claimable state it can no longer honour.
func (m *diskMedium) claim(g *generation) (committed bool, err error) {
	return m.commit(genPath(g.id), fileMeta, fileConsumed)
}

// remove reclaims a generation's home by deleting its directory. It is best-effort and runs off
// the handle lock; a reader that opened the data file before the delete keeps reading the
// now-nameless inode to completion. The operation that triggered the reclaim has already
// succeeded, so a failed delete must never become its error — hence no return value.
//
// But a failure is recorded, not swallowed: this is the runtime counterpart to recovery's
// removeGenDir, which logs its own reclaim failures, and without a log here a failed delete would
// be invisible until the next boot. The bytes stay on disk until something reclaims them, and the
// medium does not promise when — the path that triggered the reclaim decides whether the next
// startup treats the leftover as garbage to GC or as a finalized survivor to reinstate. A
// consume-once clip's plaintext is the case that earns a distinct, greppable line: a deliver-once
// secret kept past its one delivery is the lingering byte an operator most needs to see.
func (m *diskMedium) remove(g *generation) {
	err := m.root.RemoveAll(genPath(g.id))
	if err == nil {
		return
	}
	msg := "failed to reclaim a generation's home; its bytes remain on disk"
	if g.consume {
		msg = "failed to reclaim a consume-once clip's home; its plaintext remains on disk"
	}
	m.log.Warn(msg, "name", g.name, "generation", g.id.String(), "err", err)
}

// commit renames from to to within genDir and, when durability is on, fsyncs genDir so the
// renamed-in entry is durable. It is the one helper every durability-critical rename passes
// through — the finalize publish and the consume-once claim alike — so the rename-then-fsync
// discipline cannot be forgotten at a call site.
//
// It reports whether the rename took effect, separately from any error, because the rename is
// the point of no return: once it succeeds the old name is gone whether or not the following
// fsync does. A caller that must undo a failed commit can revert only when committed is false;
// once it is true the rename has happened and only forward cleanup is correct. committed is
// false only when the rename itself failed.
func (m *diskMedium) commit(genDir, from, to string) (committed bool, err error) {
	if err := m.root.Rename(genDir+"/"+from, genDir+"/"+to); err != nil {
		return false, err
	}
	if !m.fsync {
		return true, nil
	}
	if err := m.syncDir(genDir); err != nil {
		return true, err
	}
	return true, nil
}

// syncDir flushes a directory to stable storage by opening it and syncing it, making the entries
// within it — a renamed-in file, a freshly created subdirectory — durable. On darwin the Go
// runtime issues a full-device flush inside Sync, falling back automatically on filesystems that
// reject it, so this is the genuine durability primitive on every supported platform with no
// platform-specific code of buff's own.
func (m *diskMedium) syncDir(path string) error {
	d, err := m.root.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// crc32cTable is the Castagnoli polynomial table the content checksum hashes against, computed
// once and shared. It is immutable lookup data, not mutable global state — Castagnoli is chosen
// because its CRC is hardware-accelerated on common CPUs.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// checksumData streams the file at dataPath through a CRC32C and returns it as "crc32c:<8 hex>".
// One helper serves both directions of the optional content checksum: finalize calls it to stamp
// the record, recovery to verify it, so the two can never compute the hash differently. The read
// is streamed through io.Copy, so however large the clip, no bulk bytes accumulate on the heap.
// The algorithm prefix names the hash, leaving room for a future one to be added without
// ambiguity; under this schema version only "crc32c:" is ever written.
func checksumData(root *os.Root, dataPath string) (string, error) {
	f, err := root.Open(dataPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := crc32.New(crc32cTable)
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("crc32c:%08x", h.Sum32()), nil
}
