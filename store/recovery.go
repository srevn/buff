package store

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/srevn/buff/clip"

	"github.com/srevn/buff/store/internal/buffer"
)

// Startup recovery is the disk store's first act, before it serves a single request: replay
// whatever the last process left on disk and rebuild from it exactly the in-memory state that
// survived. It is split across two seams, mirroring the create→Create division the rest of the
// store already follows — the medium does the disk work and returns facts, the store turns facts
// into installed generations:
//
// - diskMedium.scan walks clips/, classifies every generation by one ordered rule, groups
// the survivors by the name in each record, and performs every disk mutation recovery makes —
// reclaiming garbage, quarantining the uninterpretable, GC-ing superseded survivors. It returns the
// survivors as plain facts. All of recovery's disk-layout knowledge lives here.
//
// - store.restore takes those facts and rebuilds the RAM state: a finished-at-birth sealed buffer
// per survivor, the registry handle that holds it, the per-name monotonic id seed, and the quota's
// recomputed footprint. It knows lifecycle and registry, not disk layout.
//
// Neither is a medium-interface method: the memory medium has nothing to recover, and forcing it to
// return store-core types would couple the seam for nothing. They are concrete methods called only
// from NewDisk.
//
// Two properties make recovery safe to interrupt. It is idempotent: its only mutations are
// RemoveAll and rename-into-quarantine, each of which leaves a state the next boot's scan
// classifies identically — a crash mid-recovery needs no recovery of its own, so no step needs
// durability. And it is fault-tolerant: a single bad generation is quarantined or reclaimed and
// isolated, never fatal; scan returns an error only when the root structure itself is unreadable,
// the one failure the store genuinely cannot run past.

// recovered is a finalized generation that survived the scan: the facts rebuilt from its metadata
// record, plus an opener for its validated data file. scan has done all the disk work by the time
// it produces one; restore turns it into an installed, readable generation with no further IO.
type recovered struct {
	id        genID
	name      string
	meta      clip.Meta
	created   time.Time
	finalized time.Time
	expires   time.Time
	consume   bool
	size      int64
	open      func() (*os.File, error) // opens the data file O_RDONLY for the sealed buffer
}

// candidate is one generation directory that classified as a valid finalized survivor, before the
// per-name contest that keeps only the greatest id. It carries just what toRecovered needs.
type candidate struct {
	id   genID
	mf   metaFile
	size int64
}

// recovery is the outcome of a scan: the survivors restore installs, and the disposition tallies
// a boot summary reports. The two counts are operator-facing — how many generations were preserved
// under quarantine/ for inspection, and how many were reclaimed as unrecoverable garbage — and each
// is incremented at the single site its disposition actually reaches the disk, so the summary can
// never claim more than recovery truly did.
type recovery struct {
	survivors   []recovered
	quarantined int // moved whole under quarantine/: corruption an operator must inspect
	reclaimed   int // RemoveAll'd: an unfinalized, interrupted-destroy, or superseded generation
}

// maxMetaBytes bounds the metadata record recovery will read into memory. A real meta.json is well
// under a kilobyte; a wildly larger one is corruption or tampering, and reading it whole would be
// the one place recovery could be made to exhaust memory. Capping the read turns that into a parse
// failure — and so a quarantine — instead.
const maxMetaBytes = 1 << 20

// scan replays the on-disk state once at startup, through the same os.Root every request uses.
// It walks clips/, classifies each generation directory, groups the valid survivors by the name
// in their records, and returns the recovery: the survivors for restore to install, plus the
// quarantined and reclaimed tallies a boot summary reports. verifyChecksum turns on the content-
// checksum check for generations that recorded one. It errors only when clips/ cannot be listed
// — a root structure the store cannot run past; every finer-grained failure is isolated within
// classify.
func (m *diskMedium) scan(verifyChecksum bool) (recovery, error) {
	entries, err := m.listDir(dirClips)
	if err != nil {
		return recovery{}, fmt.Errorf("recovery: list %s: %w", dirClips, err)
	}
	var rec recovery
	// The flat clips/ holds every name's generations together, so grouping is by the name each record
	// carries, not by a parent directory. Classify each generation once; a valid candidate joins its
	// name's group, while the uninterpretable and the garbage are quarantined or reclaimed inside
	// classify and never reach byName.
	byName := make(map[string][]candidate)
	for _, e := range entries {
		if !e.IsDir() {
			continue // clips/ holds <genid>/ directories; a stray file is left untouched
		}
		if c, ok := m.classify(e.Name(), verifyChecksum, &rec); ok {
			byName[c.mf.Name] = append(byName[c.mf.Name], c)
		}
	}
	// Among one name's finalized candidates keep the greatest id as its current generation; the rest
	// are a supersede that crashed before it could reclaim the loser — pure garbage at boot, where
	// there are no readers. The grouping above, by the name each record carries, is what scopes this
	// contest to one name: a flat clips/ has no directory structure to do it.
	for _, cands := range byName {
		keep := cands[0]
		for _, c := range cands[1:] {
			if c.id.after(keep.id) {
				keep = c
			}
		}
		for _, c := range cands {
			if c.id != keep.id {
				m.removeGenDir(genPath(c.id), &rec)
			}
		}
		rec.survivors = append(rec.survivors, m.toRecovered(keep))
	}
	return rec, nil
}

// classify decides one generation directory's fate by one ordered tree, cheapest and most decisive
// checks first. It quarantines only what it cannot interpret, GCs only unrecoverable garbage, and
// returns a candidate only for a record that fully validates against the bytes beside it. Every
// disk mutation a single generation occasions happens here.
func (m *diskMedium) classify(dirname string, verifyChecksum bool, rec *recovery) (candidate, bool) {
	id, ok := parseGenID(dirname)
	if !ok {
		// Not a generation id we ever minted — never delete the unknown.
		m.quarantine(dirname, "unparseable generation directory name", rec)
		return candidate{}, false
	}
	genDir := genPath(id)

	b, err := m.readMeta(genDir + "/" + fileMeta)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// No finalize marker: a crash mid-write, an aborted partial, a consumed survivor (meta.json
			// was renamed to meta.consumed), or an interrupted destroy past the meta unlink. All garbage;
			// RemoveAll takes the whole directory, transient files and all. v1 writes no meta.partial, so
			// there is never an in-progress upload to keep here.
			m.removeGenDir(genDir, rec)
			return candidate{}, false
		}
		// The marker is present but unreadable — an IO error we cannot see past. Preserve it.
		m.quarantine(dirname, "meta.json unreadable: "+err.Error(), rec)
		return candidate{}, false
	}

	mf, err := loadMeta(b)
	if err != nil {
		// Unparseable, or a version newer than this build understands — uninterpretable.
		m.quarantine(dirname, err.Error(), rec)
		return candidate{}, false
	}
	// The clip's name is the record's own, and the flat clips/<genid> layout gives it no second
	// on- disk encoding to cross-check against — but it needs none: lifecycle ops derive every path
	// from the id, so a record's name can never disagree with its location the way the old key tree
	// allowed. What still must hold is that the name is one the namespace admits. A record naming
	// a clip Create would have rejected is quarantined before the data is even stat'd — installing
	// it would seat a generation List and the quota counted yet no Open/Stat/Delete (each gated on
	// ValidName) could ever reach: a phantom. Quarantining keeps the invariant that every name in the
	// registry is one Create admitted, and a broken name earns preservation even were its data gone, a
	// stronger signal than the GC a sound-named dataless remnant would get.
	if err := clip.ValidName(mf.Name); err != nil {
		m.quarantine(dirname, "meta.json name is not a valid clip name: "+err.Error(), rec)
		return candidate{}, false
	}

	fi, err := m.root.Stat(genDir + "/" + fileData)
	switch {
	case errors.Is(err, os.ErrNotExist):
		// An interpretable record but no data file: an interrupted destroy left the marker behind with
		// nothing to preserve, so complete the GC rather than accumulate a dataless quarantine entry.
		// This is the deliberate counterpart to a truncated data file below — a missing file means no
		// bytes to keep; a short one means corruption worth keeping.
		m.removeGenDir(genDir, rec)
		return candidate{}, false
	case err != nil:
		// Cannot even stat the data, so cannot verify it — preserve rather than guess.
		m.quarantine(dirname, "data file unstattable: "+err.Error(), rec)
		return candidate{}, false
	}

	if err := mf.validate(id, fi.Size()); err != nil {
		// A generation mismatch, or a size mismatch from a crash-truncated data file — corruption.
		m.quarantine(dirname, err.Error(), rec)
		return candidate{}, false
	}
	if verifyChecksum && mf.Checksum != "" {
		// A recorded checksum and verification on: rehash the bytes and compare. Both a read error (the
		// bytes cannot be confirmed) and a true mismatch (silent corruption) are things we must not
		// serve, but they are logged apart so the forensic reason recorded is the honest one.
		sum, err := checksumData(m.root, genDir+"/"+fileData)
		if err != nil {
			m.quarantine(dirname, "data checksum unreadable: "+err.Error(), rec)
			return candidate{}, false
		}
		if sum != mf.Checksum {
			m.quarantine(dirname, "data checksum mismatch", rec)
			return candidate{}, false
		}
	}
	return candidate{id: id, mf: mf, size: fi.Size()}, true
}

// toRecovered turns a winning candidate into the facts restore installs, building the opener that
// will lazily reopen its data file O_RDONLY. The name is the record's own — the only place a clip's
// name lives on disk — and the size is the data file's actual length, already confirmed equal to
// the record's. dataPath is captured fresh per call, so the closure binds this generation's path
// and no other.
func (m *diskMedium) toRecovered(c candidate) recovered {
	dataPath := genPath(c.id) + "/" + fileData
	// Disk is a trust boundary like any decode: a hand-edited or corrupt meta.json can carry a file-
	// scoped field on a kind that does not own it, so the metadata is normalized as it re-enters the
	// domain, the recovery mirror of the admission normalize in Create. The Kind itself is left raw
	// — deliberately not validated or quarantined the way a bad Name is. A bad name seats a phantom
	// no ValidName-gated op could ever reach; a weird-but-readable kind is fully reachable and routes
	// safely (an unknown kind falls through to raw bytes), so quarantining over an advisory label
	// would deny access to good bytes — the opposite of recovery's preserve-readable-data stance,
	// where quarantine is reserved for the genuinely uninterpretable.
	return recovered{
		id:        c.id,
		name:      c.mf.Name,
		meta:      clip.Meta{Kind: c.mf.Kind, Filename: c.mf.Filename, Executable: c.mf.Executable}.Normalized(),
		created:   c.mf.CreatedAt,
		finalized: c.mf.FinalizedAt,
		expires:   c.mf.ExpiresAt,
		consume:   c.mf.ConsumeOnce,
		size:      c.size,
		open:      func() (*os.File, error) { return m.root.Open(dataPath) },
	}
}

// quarantine moves a generation we could not interpret out of clips/ and under quarantine/, whole
// and never deleted, then logs loudly — the deliberate opposite of discarding data we do not
// understand. It keeps the generation's own directory name under quarantine/, which cannot collide
// with a live clip path; on the astronomically unlikely chance the same name was quarantined
// before, it uniquifies rather than clobber the prior forensic copy. A failed move is logged, not
// fatal.
func (m *diskMedium) quarantine(dirname, reason string, rec *recovery) {
	src := dirClips + "/" + dirname
	base := dirQuarantine + "/" + dirname
	dst := base
	for i := 1; ; i++ {
		if _, err := m.root.Stat(dst); err != nil {
			break // free (not-exist), or unstattable — let Rename surface the latter
		}
		dst = fmt.Sprintf("%s.%d", base, i) // a prior quarantine holds the name; pick the next
	}
	if err := m.root.Rename(src, dst); err != nil {
		m.log.Error("recovery: failed to quarantine a generation", "src", src, "reason", reason, "err", err)
		return
	}
	rec.quarantined++ // counted only on a move that took effect, so the summary matches the disk
	m.log.Warn("recovery: quarantined an uninterpretable generation", "from", src, "to", dst, "reason", reason)
}

// removeGenDir reclaims one generation's directory whole — its data, any transient marker, and
// the directory itself. It is the GC arm of recovery: a generation with no finalize marker, an
// interrupted destroy, or a superseded survivor. A failure is logged but never fatal, and is
// reclaimed again on the next boot, since the classification that reached here is unchanged.
func (m *diskMedium) removeGenDir(genDir string, rec *recovery) {
	if err := m.root.RemoveAll(genDir); err != nil {
		m.log.Warn("recovery: failed to reclaim a generation", "dir", genDir, "err", err)
		return
	}
	rec.reclaimed++ // counted only on a reclamation that took effect
}

// listDir reads a directory's entries through the root. os.Root has no ReadDir of its own, so
// it opens the directory and reads it as a file — the entries' IsDir separates the <genid>/
// directories recovery walks from any stray file beside them.
func (m *diskMedium) listDir(path string) ([]os.DirEntry, error) {
	d, err := m.root.Open(path)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	return d.ReadDir(-1)
}

// readMeta reads a metadata record through the root, bounded so a corrupt or hostile file cannot
// exhaust memory (see maxMetaBytes). A not-exist error is the caller's signal that the generation
// was never finalized; every other error means the present record could not be read.
func (m *diskMedium) readMeta(path string) ([]byte, error) {
	f, err := m.root.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, maxMetaBytes))
}

// parseGenID decodes a generation directory name back into the id that produced it: exactly the 32
// lowercase hex characters genID.String writes. A name of any other length, any non-hex character,
// or uppercase hex is not one we minted — it is rejected so recovery quarantines it rather than
// treat a foreign or tampered directory as a generation.
func parseGenID(s string) (genID, bool) {
	var id genID
	if len(s) != hex.EncodedLen(len(id)) {
		return genID{}, false
	}
	if _, err := hex.Decode(id[:], []byte(s)); err != nil {
		return genID{}, false
	}
	if id.String() != s { // hex.Decode also accepts uppercase; require the canonical form we wrote
		return genID{}, false
	}
	return id, true
}

// restore rebuilds the in-memory store from the survivors scan found, mirroring create→Create:
// a finished-at-birth sealed buffer per survivor, installed as a name's current generation. It
// runs once, single-threaded, before any goroutine serves, which is why it needs no handle lock
// and fires no lifecycle wake — no other operation can observe a handle while restore is the only
// thing running, so seeding current is not a transition any waiter could be parked on. This is the
// one current assignment in the store that does not wakeLocked, and deliberately so: acquire still
// arms the notifier, but restore holds no handle lock to wake it under, and there is no observer
// to wake. acquire/release reuse the registry's one creation path and respect the lease invariant,
// so the handle is created and then kept (current is not nil, so release does not evict it) with no
// new registry API. The quota is set last, absolute, to exactly what survived.
func (s *store) restore(rec recovery) {
	var bytes int64
	for _, r := range rec.survivors {
		g := &generation{
			id:        r.id,
			name:      r.name,
			meta:      r.meta,
			created:   r.created,
			finalized: r.finalized,
			expires:   r.expires,
			consume:   r.consume,
			state:     genFinalized,
			buf:       buffer.NewSealed(r.open, r.size),
			// ttl is deliberately left zero. It is the pre-finalize retention span, resolved at Create and
			// consumed at Close to set expires; once a generation is finalized it is never read again. A
			// recovered generation is finalized, so expires — restored from the record above — is its sole
			// retention authority, and the reaper keys on that.
		}
		h := s.reg.acquire(r.name)
		h.current = g
		// Reseed the name's monotonic id from the survivor it kept. The survivor is the greatest id among
		// this name's finalized generations, so seeding lastPrefix from its prefix guarantees the next
		// id this name mints outsorts it — even if the wall clock has since jumped backwards, which would
		// otherwise let a newer write masquerade as older.
		h.lastPrefix = r.id.prefix()
		s.reg.release(h)
		bytes += r.size
	}
	s.quota.restore(bytes, int64(len(rec.survivors)))
}
