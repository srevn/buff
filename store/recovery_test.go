package store

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/srevn/buff/clip"
)

// This is the kill-between-every-IO-step harness — the project's highest crash-risk surface. Each
// test manufactures one on-disk state a power loss could leave by driving the real medium and
// buffer helpers to a chosen stop point, then "crashes" by abandoning that store and recovering a
// fresh one over the same bytes, and asserts recovery lands in the one right place: kept, GC'd, or
// quarantined, with the quota rebuilt to the byte.
//
// Durability is off throughout, deliberately: an in-process crash keeps the page cache, so recovery
// sees every issued write whether or not it was flushed — recovery's logic is fsync-independent.
// Real media corruption a crash cannot fake (a truncated or byte-flipped data file) is produced
// by hand. The whole table drives the writer's finalize order (the stop points below), so covering
// every gap in it is covering "between every IO step".

// stop names how far driveGen drives the real write path before the simulated crash. The points
// are exactly the steps of the writer's finalize order — create, append, sync, write-meta, commit
// (the consts below) — so stopping at each in turn enumerates every interruption a crash mid-write
// can leave.
type stop int

const (
	stopCreate    stop = iota // data file created, empty, no metadata
	stopAppend                // bytes appended, not synced, no metadata
	stopSync                  // bytes synced, no metadata
	stopWriteMeta             // meta.json.tmp written, not yet committed (renamed)
	stopFinalize              // committed: meta.json present — a clean finalized generation
)

// mintID allocates a real generation id for clock, via a throwaway handle so the monotonic seed
// starts fresh — two calls with increasing clocks therefore yield ids that sort in that order.
func mintID(t *testing.T, clock time.Time) genID {
	t.Helper()
	id, err := (&clipHandle{}).allocate(clock)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// driveGen drives the real create → append → Sync → writeMeta → commit helpers under name to the
// given stop point, in that exact order, and returns the id it minted so the test can find the
// directory. It is how the harness manufactures each interrupted on-disk state from the genuine
// write path rather than a hand-built imitation, so a drift between the writer and recovery would
// surface here. The append descriptor is deliberately left open at the stop: a crash abandons it,
// and recovery must cope regardless.
func driveGen(t *testing.T, m *diskMedium, name string, data []byte, consume bool, to stop, clock time.Time) genID {
	t.Helper()
	id := mintID(t, clock)
	buf, err := m.create(id)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if to == stopCreate {
		return id
	}
	if _, err := buf.Append(data); err != nil {
		t.Fatalf("append: %v", err)
	}
	if to == stopAppend {
		return id
	}
	if err := buf.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if to == stopSync {
		return id
	}
	genDir := genPath(id)
	mf := metaFile{
		Version: metaVersion, Name: name, Generation: id.String(), Kind: clip.KindText,
		Size: buf.Size(), CreatedAt: clock, FinalizedAt: clock, ConsumeOnce: consume,
	}
	if m.checksum {
		sum, err := checksumData(m.root, genDir+"/"+fileData)
		if err != nil {
			t.Fatalf("checksum: %v", err)
		}
		mf.Checksum = sum
	}
	if err := m.writeMeta(genDir, mf); err != nil {
		t.Fatalf("writeMeta: %v", err)
	}
	if to == stopWriteMeta {
		return id
	}
	if _, err := m.commit(genDir, fileMetaTmp, fileMeta); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return id
}

// --- assertions on a recovered store -------------------------------------------------------

// assertGone fails unless name resolves to nothing — recovery either reclaimed or quarantined it.
func assertGone(t *testing.T, s *store, name string) {
	t.Helper()
	if _, _, err := s.Open(context.Background(), name, GetOpts{}); !errors.Is(err, clip.ErrNotFound) {
		t.Errorf("Open %q = %v, want ErrNotFound", name, err)
	}
}

// assertContent opens name, reads it whole, and checks the bytes, returning the clip view so a
// caller can assert its metadata too. It is how a test confirms a survivor came back readable.
func assertContent(t *testing.T, s *store, name, want string) clip.Clip {
	t.Helper()
	rc, c, err := s.Open(context.Background(), name, GetOpts{})
	if err != nil {
		t.Fatalf("Open %q: %v", name, err)
	}
	got, err := io.ReadAll(rc)
	if cerr := rc.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatalf("read %q: %v", name, err)
	}
	if string(got) != want {
		t.Errorf("read %q = %q, want %q", name, got, want)
	}
	return c
}

// assertQuota fails unless the recomputed footprint matches to the byte and the count — the leak
// detector carried from the policy phase, now pointed at recovery's accounting.
func assertQuota(t *testing.T, s *store, bytes, clips int64) {
	t.Helper()
	if b, c := s.quota.bytes.Load(), s.quota.clips.Load(); b != bytes || c != clips {
		t.Errorf("quota = %d bytes / %d clips, want %d / %d", b, c, bytes, clips)
	}
}

// assertReclaimed fails unless a generation's directory is gone from clips/.
func assertReclaimed(t *testing.T, m *diskMedium, id genID) {
	t.Helper()
	assertAbsent(t, m.root, genPath(id))
}

// assertQuarantined fails unless a generation was moved whole under quarantine/, data and all.
func assertQuarantined(t *testing.T, m *diskMedium, id genID) {
	t.Helper()
	dst := dirQuarantine + "/" + id.String()
	if _, err := m.root.Stat(dst + "/" + fileData); err != nil {
		t.Errorf("quarantine of %s: Stat %s/data = %v, want present", id, dst, err)
	}
	assertReclaimed(t, m, id) // and gone from clips/
}

// --- disk-mutation helpers (manufacture corruption a crash cannot) -------------------------

// writeRaw overwrites a file within the root, the way a crash or tamperer might leave it.
func writeRaw(t *testing.T, root *os.Root, path string, b []byte) {
	t.Helper()
	f, err := root.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// truncateData shortens a generation's data file, standing in for a crash that lost its tail after
// the bytes were counted into the record but before — impossibly, since finalize syncs data first —
// that could happen for real; so this models media corruption, which earns quarantine.
func truncateData(t *testing.T, m *diskMedium, id genID, size int64) {
	t.Helper()
	f, err := m.root.OpenFile(genPath(id)+"/"+fileData, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// rewriteMeta reads a generation's metadata record, mutates it, and writes it back — the way to
// forge a record from a newer version, or one whose name no longer matches its directory.
func rewriteMeta(t *testing.T, m *diskMedium, id genID, mutate func(*metaFile)) {
	t.Helper()
	p := genPath(id) + "/" + fileMeta
	var mf metaFile
	if err := json.Unmarshal(readRoot(t, m.root, p), &mf); err != nil {
		t.Fatal(err)
	}
	mutate(&mf)
	b, err := json.Marshal(mf)
	if err != nil {
		t.Fatal(err)
	}
	writeRaw(t, m.root, p, b)
}

// --- the truth table -----------------------------------------------------------------------

// TestRecoverDiscardsUnfinalized covers every interruption before the meta.json commit, each of
// which leaves a generation with no finalize marker, which recovery reclaims whole — the data file,
// any stray temp marker, and the generation directory itself — leaving the name absent and the
// quota at zero. The flat layout has no per-name parent, so nothing is left behind. crash == abort.
func TestRecoverDiscardsUnfinalized(t *testing.T) {
	for _, tc := range []struct {
		name string
		to   stop
	}{
		{"create only", stopCreate},
		{"appended, not synced", stopAppend},
		{"synced, no meta", stopSync},
		{"meta tmp, not committed", stopWriteMeta},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, m := newDiskStore(t, Config{}, DiskOpts{})
			id := driveGen(t, m, "doc", []byte("partial bytes"), false, tc.to, time.Unix(1000, 0))

			s, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
			assertGone(t, s, "doc")
			assertQuota(t, s, 0, 0)
			assertReclaimed(t, m, id) // the whole clips/<genid>/ directory is gone, nothing left over
		})
	}
}

// TestRecoverFinalizedSurvivor proves a cleanly committed generation comes back readable, at its
// right generation id, with the quota recomputed to its size.
func TestRecoverFinalizedSurvivor(t *testing.T) {
	s1, m := newDiskStore(t, Config{}, DiskOpts{})
	c1 := finalize(t, s1, "doc", PutOpts{}, []byte("durable bytes"))

	s2, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
	c2 := assertContent(t, s2, "doc", "durable bytes")
	if c2.Generation != c1.Generation {
		t.Errorf("recovered generation %q, want %q", c2.Generation, c1.Generation)
	}
	assertQuota(t, s2, int64(len("durable bytes")), 1)
}

// TestRecoverEmptyClip is the off-by-one trap: a finalized 0-byte clip has a data file that exists
// with size 0 and a record whose size is 0. Recovery keeps it because the stat succeeds and 0 == 0
// — the present-but-empty case is distinguished from the absent-data case (which is reclaimed). A
// naive "size <= 0 is garbage" would wrongly delete a valid empty clip.
func TestRecoverEmptyClip(t *testing.T) {
	s1, m := newDiskStore(t, Config{}, DiskOpts{})
	finalize(t, s1, "empty", PutOpts{}, nil)

	s2, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
	c := assertContent(t, s2, "empty", "")
	if c.Size != 0 {
		t.Errorf("recovered empty clip size = %d, want 0", c.Size)
	}
	assertQuota(t, s2, 0, 1) // zero bytes, but it counts as one clip
}

// TestRecoverPreservesMetadata proves restore rebuilds the whole durable record, not just the
// bytes: kind, filename, the executable bit, the finalized instant, and the absolute expiry all
// survive the round trip through meta.json unchanged — the disk-persistence half of the executable
// feature, the counterpart to the contract suite's in-memory proof. The struct-equality check on
// Meta below is what pins the executable bit: it must come back exactly as written. The filename
// is deliberately non-ASCII (café.pdf): meta.json serializes it as a JSON string, so this also pins
// that a valid multi-byte UTF-8 basename survives the encoding/json round trip byte-for-byte — the
// fidelity ValidFilename's UTF-8 gate guarantees by refusing the non-UTF-8 names that would not.
func TestRecoverPreservesMetadata(t *testing.T) {
	s1, m := newDiskStore(t, Config{}, DiskOpts{})
	w, err := s1.Create(context.Background(), "report", clip.Meta{Kind: clip.KindFile, Filename: "café.pdf", Executable: true}, PutOpts{TTL: time.Hour})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("PDF")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	c1 := w.Clip()

	s2, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
	c2, err := s2.Stat(context.Background(), "report")
	if err != nil {
		t.Fatalf("Stat recovered: %v", err)
	}
	if c2.Meta != c1.Meta || c2.Generation != c1.Generation || c2.Size != c1.Size {
		t.Errorf("recovered meta = %+v, want %+v", c2, c1)
	}
	if !c2.FinalizedAt.Equal(c1.FinalizedAt) || !c2.ExpiresAt.Equal(c1.ExpiresAt) {
		t.Errorf("recovered times finalized=%v expires=%v, want %v / %v", c2.FinalizedAt, c2.ExpiresAt, c1.FinalizedAt, c1.ExpiresAt)
	}
	if c2.ExpiresAt.IsZero() {
		t.Error("recovered clip lost its expiry")
	}
}

// TestRecoverConsumedSurvivorReclaimed proves a consume-once clip claimed just before a crash does
// not resurrect: the claim renamed meta.json to meta.consumed, so recovery finds no finalize marker
// and reclaims the directory. At-most-once holds with zero delivery — the secret is simply gone.
func TestRecoverConsumedSurvivorReclaimed(t *testing.T) {
	_, m := newDiskStore(t, Config{}, DiskOpts{})
	id := driveGen(t, m, "secret", []byte("payload"), true, stopFinalize, time.Unix(1000, 0))
	if _, err := m.claim(&generation{id: id}); err != nil { // claim by id, then "crash"
		t.Fatalf("claim: %v", err)
	}

	s, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
	assertGone(t, s, "secret")
	assertQuota(t, s, 0, 0)
	assertReclaimed(t, m, id)
}

// TestRecoverConsumeOnceSurvivorClaimable proves the other consume-once arm: a finalized but
// unclaimed consume-once clip survives a crash as an ordinary finalized generation, and a reader
// after recovery can still claim and drain it exactly once.
func TestRecoverConsumeOnceSurvivorClaimable(t *testing.T) {
	s1, m := newDiskStore(t, Config{}, DiskOpts{})
	finalize(t, s1, "secret", PutOpts{ConsumeOnce: true}, []byte("payload"))

	s2, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
	assertContent(t, s2, "secret", "payload") // the claim fires on this Open, then cleanup destroys it
	assertGone(t, s2, "secret")               // a second reader finds it gone — delivered at most once
}

// TestRecoverKeepsMaxIDGeneration proves the keep-the-greatest-id resolution: two committed
// generations under one name (a supersede that crashed before it could reclaim the loser) resolve
// to the greater id as current, the lesser reclaimed, the quota counting exactly one.
func TestRecoverKeepsMaxIDGeneration(t *testing.T) {
	_, m := newDiskStore(t, Config{}, DiskOpts{})
	idOld := driveGen(t, m, "doc", []byte("old"), false, stopFinalize, time.Unix(1000, 0))
	idNew := driveGen(t, m, "doc", []byte("newer"), false, stopFinalize, time.Unix(2000, 0))
	if !idNew.after(idOld) {
		t.Fatalf("test setup: idNew %s does not sort after idOld %s", idNew, idOld)
	}

	s, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
	c := assertContent(t, s, "doc", "newer")
	if c.Generation != idNew.String() {
		t.Errorf("current generation = %q, want the greater id %q", c.Generation, idNew)
	}
	assertReclaimed(t, m, idOld)
	assertQuota(t, s, int64(len("newer")), 1)
}

// TestRecoverReclaimsMetaWithoutData is the sharpening of the validate-or-quarantine rule: an
// interpretable record beside a vanished data file is an interrupted destroy with nothing to
// preserve, so recovery completes the GC rather than accumulate a dataless quarantine entry.
// Distinct from a truncated data file (corruption, quarantined) — a missing file means no bytes
// to keep.
func TestRecoverReclaimsMetaWithoutData(t *testing.T) {
	s1, m := newDiskStore(t, Config{}, DiskOpts{})
	c := finalize(t, s1, "doc", PutOpts{}, []byte("bytes"))
	id, _ := parseGenID(c.Generation)
	if err := m.root.Remove(genPath(id) + "/" + fileData); err != nil {
		t.Fatalf("remove data: %v", err)
	}

	s2, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
	assertGone(t, s2, "doc")
	assertQuota(t, s2, 0, 0)
	assertReclaimed(t, m, id) // GC'd, not quarantined
	assertAbsent(t, m.root, dirQuarantine+"/"+id.String())
}

// TestRecoverQuarantines covers the integrity arms plus the corruption shapes the always-on cross-
// checks catch: a crash-truncated data file (a size mismatch), a record from a newer version, a
// record naming a clip the namespace forbids (which would otherwise seat a phantom), and — with
// checksums on — a byte-flipped but same-length data file. Each is preserved under quarantine/,
// never deleted, and leaves the name absent and the quota at zero. The forbidden-name arm is the
// guard that remains after the flat layout retired the old name-hashes-to-its-directory cross-
// check: the path no longer encodes the name, so ValidName is the sole record-name check, and it
// must still refuse a name Create would have rejected.
func TestRecoverQuarantines(t *testing.T) {
	for _, tc := range []struct {
		name     string
		checksum bool
		corrupt  func(t *testing.T, m *diskMedium, id genID)
	}{
		{"truncated data", false, func(t *testing.T, m *diskMedium, id genID) {
			truncateData(t, m, id, 2) // record says 5 bytes, file now holds 2
		}},
		{"future version", false, func(t *testing.T, m *diskMedium, id genID) {
			rewriteMeta(t, m, id, func(mf *metaFile) { mf.Version = metaVersion + 1 })
		}},
		{"name the namespace forbids", false, func(t *testing.T, m *diskMedium, id genID) {
			rewriteMeta(t, m, id, func(mf *metaFile) { mf.Name = "has/slash" }) // ValidName rejects it
		}},
		{"checksum mismatch", true, func(t *testing.T, m *diskMedium, id genID) {
			writeRaw(t, m.root, genPath(id)+"/"+fileData, []byte("XXXXX")) // same length, wrong bytes
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := DiskOpts{Checksum: tc.checksum}
			s1, m := newDiskStore(t, Config{}, opts)
			c := finalize(t, s1, "doc", PutOpts{}, []byte("bytes"))
			id, _ := parseGenID(c.Generation)
			tc.corrupt(t, m, id)

			s2, _ := recoverDiskStore(t, m.root, time.Now, Config{}, opts)
			assertGone(t, s2, "doc")
			assertQuota(t, s2, 0, 0)
			assertQuarantined(t, m, id)
		})
	}
}

// TestRecoverNameSourcedFromMeta pins the flat layout's deliberate trade: with the clip name no
// longer encoded in the path, recovery sources it solely from the record. A meta.json edited to a
// different but still-valid name comes back under that name — there is no second on-disk encoding
// to disagree with it, and none is needed, because lifecycle paths derive from the id. This is
// the one behaviour change from retiring the name-hashes-to-its-directory cross-check, and it cuts
// two ways. A name-only corruption that does not collide — the case here — merely mislabels: the
// bytes come back under the wrong name, none lost. One corrupted into another clip's name instead
// collides in byName and is resolved by the same greatest-id contest a real crashed supersede
// uses, so the loser's directory is reclaimed — within the data-dir trust boundary the lone path to
// silent data loss, and it takes a corruption that both forms a valid colliding name and preserves
// the size match (a checksum would not catch it: the data is untouched, only the name field
// changed). That collision case is pinned by TestRecoverNameCollisionReclaimsLoser.
func TestRecoverNameSourcedFromMeta(t *testing.T) {
	s1, m := newDiskStore(t, Config{}, DiskOpts{})
	c := finalize(t, s1, "original", PutOpts{}, []byte("payload"))
	id, _ := parseGenID(c.Generation)
	rewriteMeta(t, m, id, func(mf *metaFile) { mf.Name = "renamed" }) // still a valid name

	s2, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
	assertContent(t, s2, "renamed", "payload") // installed under the record's name
	assertGone(t, s2, "original")              // and not the directory-implied one — there is none
	assertQuota(t, s2, int64(len("payload")), 1)
}

// TestRecoverNameCollisionReclaimsLoser pins the dark side of sourcing the name from the record:
// a corruption that rewrites one clip's name to another's makes the two collide under one name,
// and recovery's greatest-id contest — the same one a crashed supersede relies on — keeps only the
// greater id and reclaims the loser's directory, taking its bytes with it. This is the documented
// lone path to silent data loss inside the data-dir trust boundary; pinning it keeps a future
// change from quietly turning it into a leak (two clips left on disk under one name) or a panic.
// The ids are minted under an advancing clock, so which generation wins is deterministic rather
// than a clock race between two quick Creates.
func TestRecoverNameCollisionReclaimsLoser(t *testing.T) {
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	m := newDiskMedium(t, root, DiskOpts{})
	s1 := newStore(m, advancingClock(time.Unix(1_700_000_000, 0), time.Second), Config{})

	loser := finalize(t, s1, "alpha", PutOpts{}, []byte("alpha-data"))
	winner := finalize(t, s1, "beta", PutOpts{}, []byte("beta-payload"))
	idLoser, _ := parseGenID(loser.Generation)
	idWinner, _ := parseGenID(winner.Generation)
	if !idWinner.after(idLoser) {
		t.Fatal("setup: the second clip must hold the greater id for the contest precondition")
	}
	// Corrupt beta's record to claim alpha's name: still a valid name, its data and size untouched.
	rewriteMeta(t, m, idWinner, func(mf *metaFile) { mf.Name = "alpha" })

	s2, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
	assertContent(t, s2, "alpha", "beta-payload") // the greater id won the collided name
	assertGone(t, s2, "beta")                     // beta's name no longer resolves
	assertReclaimed(t, m, idLoser)                // and the loser's directory — alpha's bytes — is gone
	assertQuota(t, s2, int64(len("beta-payload")), 1)
}

// TestRecoverChecksumRoundTrip proves the happy path of the checksum feature: a clip finalized
// with checksums on stores a crc32c in its record, and recovery with verification on recomputes it,
// matches, and keeps the clip. Without this, the quarantine-on-mismatch test could pass by always
// quarantining.
func TestRecoverChecksumRoundTrip(t *testing.T) {
	s1, m := newDiskStore(t, Config{}, DiskOpts{Checksum: true})
	c := finalize(t, s1, "doc", PutOpts{}, []byte("checked bytes"))
	id, _ := parseGenID(c.Generation)

	var mf metaFile
	if err := json.Unmarshal(readRoot(t, m.root, genPath(id)+"/"+fileMeta), &mf); err != nil {
		t.Fatal(err)
	}
	if mf.Checksum == "" {
		t.Fatal("finalize with checksums on stored no checksum")
	}

	s2, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{Checksum: true})
	assertContent(t, s2, "doc", "checked bytes")
	assertQuota(t, s2, int64(len("checked bytes")), 1)
}

// TestRecoverChecksumMixed proves both directions of the feature toggle are graceful: a clip
// finalized without a checksum recovers under verification (nothing to verify, so it is kept),
// and a clip finalized with one recovers without verification (the check is skipped, the stored
// checksum left intact). Neither mismatched setting can quarantine a sound clip.
func TestRecoverChecksumMixed(t *testing.T) {
	t.Run("finalized off, recovered on", func(t *testing.T) {
		s1, m := newDiskStore(t, Config{}, DiskOpts{Checksum: false})
		finalize(t, s1, "doc", PutOpts{}, []byte("bytes"))
		s2, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{Checksum: true})
		assertContent(t, s2, "doc", "bytes") // empty checksum: nothing to verify, kept
	})
	t.Run("finalized on, recovered off", func(t *testing.T) {
		s1, m := newDiskStore(t, Config{}, DiskOpts{Checksum: true})
		finalize(t, s1, "doc", PutOpts{}, []byte("bytes"))
		s2, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{Checksum: false})
		assertContent(t, s2, "doc", "bytes") // verification off: check skipped, kept
	})
}

// TestRecoverQuarantinesUnparseableDir proves recovery never deletes the unknown: a directory under
// clips/ whose name is not a generation id we ever minted is moved to quarantine/ whole, while a
// valid sibling recovers normally.
func TestRecoverQuarantinesUnparseableDir(t *testing.T) {
	s1, m := newDiskStore(t, Config{}, DiskOpts{})
	finalize(t, s1, "doc", PutOpts{}, []byte("good"))

	// A foreign directory with a data file, directly under clips/ beside a valid generation.
	stray := dirClips + "/" + "not-a-generation-id"
	if err := m.root.Mkdir(stray, 0o700); err != nil {
		t.Fatal(err)
	}
	writeRaw(t, m.root, stray+"/"+fileData, []byte("mystery"))

	s2, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
	assertContent(t, s2, "doc", "good") // the valid generation still recovers
	if _, err := m.root.Stat(dirQuarantine + "/not-a-generation-id/" + fileData); err != nil {
		t.Errorf("unparseable directory not quarantined: %v", err)
	}
}

// TestRecoverReseedsLastPrefix is the backward-clock danger recovery closes: recovery seeds a
// name's monotonic id from its survivor, so the next generation outsorts it even when the wall
// clock has jumped backwards across the restart. Without the reseed, a lower id would be minted and
// "current = greatest id" would point at the stale survivor forever.
func TestRecoverReseedsLastPrefix(t *testing.T) {
	_, m := newDiskStore(t, Config{}, DiskOpts{})
	idOld := driveGen(t, m, "doc", []byte("first"), false, stopFinalize, time.Unix(2_000_000_000, 0))

	// Recover under a clock set well before the survivor was written, then write a replacement.
	backward := time.Unix(1_000_000_000, 0)
	s, _ := recoverDiskStore(t, m.root, fixedClock(backward), Config{}, DiskOpts{})
	cNew := finalize(t, s, "doc", PutOpts{}, []byte("second"))
	idNew, _ := parseGenID(cNew.Generation)

	if !idNew.after(idOld) {
		t.Errorf("replacement id %s does not outsort survivor %s under a backward clock — reseed failed", idNew, idOld)
	}
	assertContent(t, s, "doc", "second") // the replacement is current, not the stale survivor
}

// TestRecoverSealedInodePinSurvivesSupersede proves a recovered generation is followed and
// superseded exactly like a live one: a reader holding its sealed read handle drains to EOF even
// after a replacement RemoveAll's the recovered directory out from under it. It is the read-after-
// supersede guarantee, for a clip that never had a live writer in this process.
func TestRecoverSealedInodePinSurvivesSupersede(t *testing.T) {
	_, m := newDiskStore(t, Config{}, DiskOpts{})
	driveGen(t, m, "doc", []byte("recovered bytes"), false, stopFinalize, time.Unix(1000, 0))

	s, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
	rc, _, err := s.Open(context.Background(), "doc", GetOpts{}) // opens the sealed read fd, pins the inode
	if err != nil {
		t.Fatalf("Open recovered: %v", err)
	}
	defer rc.Close()

	finalize(t, s, "doc", PutOpts{}, []byte("replacement")) // supersede: RemoveAll the recovered directory

	got, err := io.ReadAll(rc) // the held handle drains the now-nameless inode
	if err != nil {
		t.Fatalf("read after supersede: %v", err)
	}
	if string(got) != "recovered bytes" {
		t.Errorf("read after supersede = %q, want the recovered bytes", got)
	}
}

// TestRecoverIsolatesBadGeneration is the fault-tolerance guarantee: one generation recovery cannot
// interpret is quarantined and isolated, while every other name recovers and the boot does not
// abort. A single corrupt clip must never brick startup.
func TestRecoverIsolatesBadGeneration(t *testing.T) {
	s1, m := newDiskStore(t, Config{}, DiskOpts{})
	finalize(t, s1, "good1", PutOpts{}, []byte("aaa"))
	finalize(t, s1, "good2", PutOpts{}, []byte("bb"))
	cBad := finalize(t, s1, "bad", PutOpts{}, []byte("xxxx"))
	idBad, _ := parseGenID(cBad.Generation)
	writeRaw(t, m.root, genPath(idBad)+"/"+fileMeta, []byte("{ not valid json")) // unreadable record

	s2, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
	assertContent(t, s2, "good1", "aaa")
	assertContent(t, s2, "good2", "bb")
	assertGone(t, s2, "bad")
	assertQuarantined(t, m, idBad)
	assertQuota(t, s2, int64(len("aaa")+len("bb")), 2) // only the two survivors counted
}

// TestRecoverIsIdempotent proves recovery is safe to interrupt: running it twice over the same
// root — the second time over the state the first left — yields the same survivors, reclaims and
// quarantines nothing further, and leaves the quarantine recovery never walks untouched. A crash
// mid-recovery therefore re-classifies identically on the next boot.
func TestRecoverIsIdempotent(t *testing.T) {
	s1, m := newDiskStore(t, Config{}, DiskOpts{})
	finalize(t, s1, "keep", PutOpts{}, []byte("survivor"))
	garbage := driveGen(t, m, "garbage", []byte("partial"), false, stopSync, time.Unix(1000, 0)) // no meta → GC
	cBad := finalize(t, s1, "bad", PutOpts{}, []byte("zzzz"))
	idBad, _ := parseGenID(cBad.Generation)
	rewriteMeta(t, m, idBad, func(mf *metaFile) { mf.Version = metaVersion + 1 }) // → quarantine

	// First recovery: keep the survivor, reclaim the garbage, quarantine the bad one.
	s2, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
	assertContent(t, s2, "keep", "survivor")
	assertReclaimed(t, m, garbage)
	assertQuarantined(t, m, idBad)
	assertQuota(t, s2, int64(len("survivor")), 1)

	// Second recovery over the state the first left: identical outcome, nothing further changed.
	s3, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
	assertContent(t, s3, "keep", "survivor")
	assertGone(t, s3, "bad")
	assertQuota(t, s3, int64(len("survivor")), 1)
	// The quarantined copy from the first pass is untouched — recovery never walks quarantine/.
	if _, err := m.root.Stat(dirQuarantine + "/" + idBad.String() + "/" + fileData); err != nil {
		t.Errorf("second recovery disturbed the quarantine: %v", err)
	}
}

// TestRecoverQuarantineUniquifiesOnCollision exercises the quarantine target-collision branch: when
// quarantine/<genid> already holds an earlier forensic copy, a fresh quarantine of the same id must
// land at the .1 suffix and leave the prior copy untouched, never clobbering data an operator was
// meant to inspect. The collision is astronomically unlikely in the wild — it needs the same id
// quarantined twice — so it is staged by hand here.
func TestRecoverQuarantineUniquifiesOnCollision(t *testing.T) {
	s1, m := newDiskStore(t, Config{}, DiskOpts{})
	c := finalize(t, s1, "doc", PutOpts{}, []byte("bytes"))
	id, _ := parseGenID(c.Generation)

	// Pre-occupy the target this generation would move to, standing in for a prior pass's copy.
	base := dirQuarantine + "/" + id.String()
	if err := m.root.Mkdir(base, 0o700); err != nil {
		t.Fatal(err)
	}
	writeRaw(t, m.root, base+"/"+fileData, []byte("prior forensic copy"))

	// Corrupt the live generation so recovery must quarantine it — into base.1, not over base.
	truncateData(t, m, id, 2)

	s2, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
	assertGone(t, s2, "doc")
	if got := readRoot(t, m.root, base+"/"+fileData); string(got) != "prior forensic copy" {
		t.Errorf("prior quarantine clobbered: data = %q, want it untouched", got)
	}
	if _, err := m.root.Stat(base + ".1/" + fileData); err != nil {
		t.Errorf("collision not uniquified: Stat %s.1/data = %v, want present", base, err)
	}
}

// TestRecoverQuarantinesUnreadableMeta exercises the present-but-unreadable record arm: a meta.json
// that exists — so it is not the no-marker GC case — but cannot be read as a file is preserved
// under quarantine, never deleted. Replacing it with a directory makes the read fail with neither a
// not-exist nor a JSON error, the IO-error branch a corrupt inode would reach.
func TestRecoverQuarantinesUnreadableMeta(t *testing.T) {
	s1, m := newDiskStore(t, Config{}, DiskOpts{})
	c := finalize(t, s1, "doc", PutOpts{}, []byte("bytes"))
	id, _ := parseGenID(c.Generation)

	genDir := genPath(id)
	if err := m.root.Remove(genDir + "/" + fileMeta); err != nil {
		t.Fatalf("remove meta.json: %v", err)
	}
	if err := m.root.Mkdir(genDir+"/"+fileMeta, 0o700); err != nil { // present, but unreadable as a file
		t.Fatalf("mkdir meta.json: %v", err)
	}

	s2, _ := recoverDiskStore(t, m.root, time.Now, Config{}, DiskOpts{})
	assertGone(t, s2, "doc")
	assertQuarantined(t, m, id)
}
