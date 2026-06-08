package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/srevn/buff/archive"
	"github.com/srevn/buff/clip"
)

// Sink is the paste counterpart of Source: somewhere a clip's bytes go. Like Source it is the seam
// a future output bridge would implement, so it is exported even though the v1 sinks are unexported
// and selected by chooseSink. Write streams the reader to the destination and returns any error;
// cancellation reaches a byte-copying sink through the reader, which is the completion-checked GET
// body, while the archive sinks also honour ctx between entries.
type Sink interface {
	Write(ctx context.Context, r io.Reader, m clip.Meta) error
}

// salvager is the capability a terminal gesture sink adds when it can rescue a consume-once
// delivery whose normal landing collided. Only the two sinks buff itself picks a name for —
// saveSink and newDirSink — implement it, and that name is the whole boundary: buff chose it, so
// buff makes a collision on it good. The -o sinks are excluded not because they cannot collide
// irrecoverably — extractSink never clobbers and refuses a spent delivery exactly as newDirSink
// does (only fileSink genuinely clobbers, so it alone never refuses) — but because the user named
// that destination, so a failure there is theirs to see and to retry. The sink contributes only
// the mechanism — name a sibling, land beside yourself — and never learns why: the consume-once
// knowledge stays in the flow (divertConsumeOnce), one policy in one place for every sink.
//
// That centralization is a convenience, not a guard. The rescue dispatches through a runtime
// sink.(salvager) assertion, so a new buff-named no-clobber sink that omits salvager compiles
// and is silently skipped — what is centralized is the gate (a salvager need not re-derive the
// consumed+pristine+collision decision, the flow owns it), not the obligation to participate (the
// internal_test anchors guard the existing two against signature drift, not a missing third). A
// forgotten salvager is still no silent loss: the flow reports every unlanded consume-once delivery
// (see paste), so the worst case is a delivery lost-but-unrescued, never one gone without a word.
//
// The rescue is two steps, split on purpose. siblingName decides WHAT the sibling beside the
// colliding target is called, purely — it reads no byte and touches no disk — so the flow can
// vet that one name (clip.ValidFilename, the basename rule both naming schemes answer to) before
// any IO, while the spent body is provably still whole. landBeside then does the IO: it writes
// the pristine body to that already-validated name and narrates the diversion. Keeping the name
// decision out of the write is what makes "no usable sibling" one signal scored in one place —
// rather than whatever low-level error each sink's own open happened to raise — and it strengthens
// the precondition: because siblingName reads nothing, the body is untouched between the flow's
// pristine check and landBeside by construction, not by a convention to read nothing first.
type salvager interface {
	// siblingName formats the sibling beside the sink's colliding target from the delivery's
	// generation id. Pure: no IO, no byte read, so the flow can validate the result before any write.
	siblingName(m clip.Meta, gen string) string
	// landBeside writes the whole pristine body to name — a flow-validated basename beside the sink's
	// colliding target — and narrates the diversion. name is known-good, so a failure here is a real
	// IO fault, a torn copy, a genuinely malformed delivery, or an astronomically-unlikely O_EXCL
	// race, never an unusable name.
	landBeside(ctx context.Context, r io.Reader, m clip.Meta, name string) error
}

// divertConsumeOnce is the paste flow's one rescue point for a consume-once delivery the server
// spent at Open and the chosen sink then could not land. The flow calls it only once it has
// established the delivery is recoverable — the clip is consume-once and the body is pristine (no
// byte consumed, no tear) — so the single question left is whether this refusal is a recoverable
// collision on a sink that knows how to land beside itself.
//
// Two filters, each necessary and neither sufficient: the sink must be a salvager (the -o sinks are
// not — the user named that destination, so its failure is theirs to see), and the refusal must be
// a collision rather than a permission or space fault an alternate name in the same directory would
// hit identically. A refusal that passes neither is returned unchanged, leaving the original error
// — and its exit code — to stand.
//
// The generation id is the sibling's uniqueness, and it is a wire value a foreign or buggy
// peer controls, so the flow vets it on the two axes the sink no longer has to. An ABSENT id
// cannot form a distinct name — and a degenerate sibling (report.pdf. / slot-) is itself a VALID
// basename, so the ValidFilename check below cannot catch it — so it is refused on its own, as
// the uniqueness floor. A PRESENT id is spliced into the sibling and the whole name run through
// clip.ValidFilename: a hostile (separator, control) or over-long id cannot splice into a usable
// basename, and rejecting it here — before landBeside reads a byte — keeps "no usable sibling"
// one signal that wraps the collision (scoring the same 6 as the absent-id floor) for either
// sink, instead of the assorted low-level errors each sink's own open would otherwise raise. Both
// refusals leave the body untouched and keep the collision identity (so a script still reads the
// collision's exit code); each names only why no sibling could form, and the flow names the loss
// itself, once, for every path (see paste).
func divertConsumeOnce(ctx context.Context, sink Sink, r io.Reader, cl clip.Clip, refusal error) error {
	sv, ok := sink.(salvager)
	if !ok || !isCollision(refusal) {
		return refusal
	}
	if cl.Generation == "" {
		return fmt.Errorf("%w; no generation id to form a unique sibling", refusal)
	}
	beside := sv.siblingName(cl.Meta, cl.Generation)
	if clip.ValidFilename(beside) != nil {
		return fmt.Errorf("%w; the delivery's name cannot form a usable sibling", refusal)
	}
	return sv.landBeside(ctx, r, cl.Meta, beside)
}

// isCollision reports whether err is a no-clobber landing collision on a still-PRISTINE body — the
// only sink refusal a consume-once salvage diverts. It lists the two sentinels a salvager raises
// before reading a byte: os.ErrExist, from saveSink's O_EXCL open, and archive.ErrDestExists, from
// newDirSink's up-front Lstat in ExtractNew.
//
// archive.ErrExists is deliberately absent — but not because it cannot reach a salvager.
// newDirSink's ExtractNew untars through extractReg, whose O_EXCL rejects a duplicate-entry tar
// with ErrExists, and a foreign peer can send such a tar. It is excluded because it cannot reach
// one PRISTINE: a duplicate is detectable only after a prior entry was extracted, so bytes are
// already read and the pristine gate (paste.go) has ruled the divert out before isCollision is ever
// consulted. (That gate also covers ErrDestExists's other raise site — the publish-rename race —
// which likewise arrives after the tar is drained; only its up-front Lstat raise is a pristine,
// divertable collision.) The pristine gate, not this list, is the load-bearing guard against an
// empty-tree salvage; this list only names the two pre-read shapes a divert may legitimately act
// on.
func isCollision(err error) bool {
	return errors.Is(err, os.ErrExist) || errors.Is(err, archive.ErrDestExists)
}

// chooseSink resolves where a paste's bytes go, drawing only on the clip's kind, whether output is
// a terminal, and the -o flag — never on the bytes themselves. The kind is the gesture that made
// the clip: a piped stream is bytes, a single file is file, a tree is archive. Routing by kind
// restores that gesture without inspecting content — at a terminal buff saves a file, extracts an
// archive, and shows the bytes; to a pipe it is cat; -o forces the destination. Routing draws only
// on metadata and environment, never on the bytes, so the whole relay is content-blind, the same
// stance the server holds.
//
// -o is the explicit target and wins. -o - is the Unix spelling of "raw bytes to stdout" for any
// kind, which also keeps -o - from being read as a file literally named "-". Without -o, a terminal
// restores the gesture by kind and a pipe or redirect always gets raw bytes, so piping a tar to
// tar and redirecting to a file behave the way the shell leads one to expect. Routing never reads
// cl.Finalized: a still-being-written file saves exactly as a finalized one (a live archive already
// extracts ungated), so a clip's disposition cannot change as it finalizes. The trade the gesture
// model makes is that the producer chooses the gesture and the consumer bears it — a binary stream
// piped in as a bytes clip will garble a terminal on paste, recovered with -o - or a pipe.
func chooseSink(cl clip.Clip, inv invocation, std IO) Sink {
	if inv.outputSet {
		if inv.output == "-" {
			return stdoutSink{w: std.Out} // explicit raw stdout, any kind
		}
		if cl.Meta.Kind == clip.KindArchive {
			return extractSink{target: inv.output, errw: std.Err}
		}
		return fileSink{target: inv.output, errw: std.Err}
	}
	if std.OutIsTTY {
		// newDirSink and saveSink are the no-clobber landings buff names itself, so each implements
		// salvager (above): when one collides on a consume-once delivery the server has already spent,
		// the flow lands the spent body on a sibling rather than losing it. A new buff-named no-clobber
		// terminal sink wired here owes the same — without salvager its colliding consume-once delivery
		// is still reported lost (the flow's uniform tail) but never rescued.
		switch cl.Meta.Kind {
		case clip.KindArchive:
			// ExtractNew publishes one new directory and requires its name to be a single path component,
			// so reduce the slot to its last component. A no-op while names are single-component; what keeps
			// a future hierarchical slot extracting into its leaf rather than tripping ExtractNew's single-
			// component guard. path (not filepath) because a slot is the logical "/"-namespace, not an OS
			// path.
			return newDirSink{name: path.Base(inv.slot), errw: std.Err}
		case clip.KindFile:
			return saveSink{errw: std.Err, slot: inv.slot}
		}
		// A bytes clip — and any kind a foreign peer left unset or unknown — falls through: with no name
		// to save under and no structure to extract, its bytes go raw to the terminal, not to a file.
	}
	return stdoutSink{w: std.Out}
}

// stdoutSink writes the clip's bytes straight to standard output: the raw bytes of a bytes or
// file clip, or the raw tar of an archive bound for a pipe or redirect, and a bytes clip shown
// at a terminal. A truncated read surfaces as the copy error from the completion-checked body;
// some bytes may already have reached output before the truncation is known, which is inherent to
// streaming to a stream that cannot be unwound.
type stdoutSink struct{ w io.Writer }

func (s stdoutSink) Write(ctx context.Context, r io.Reader, m clip.Meta) error {
	_, err := io.Copy(s.w, r)
	return buffErr(err)
}

// fileSink writes a bytes or file clip's bytes to a -o target. If the target is an existing
// directory the clip is saved inside it under its remembered filename — a name drawn from clip
// metadata, not from the command, so the landing path is narrated (through narrateSave) exactly as
// a terminal save is; otherwise the target names the file to write, clobbering an existing one the
// way a shell redirect does, and staying silent because the user spelled that whole path out. errw
// carries the directory arm's one notice and is unused by the file arm.
type fileSink struct {
	target string
	errw   io.Writer
}

// Write resolves the target and writes the bytes. The directory case needs a remembered filename
// and saves under it through openInDir, the shared filename boundary that re-validates the peer-
// supplied name and confines the write to the directory. The plain-file case writes the user's
// own literal path directly, clobbering like a redirect — their choice to make. Both cases set the
// file's run bit to the clip's — on for a runnable clip, cleared otherwise — so a copied binary
// lands runnable and a plain file lands inert even when it clobbers an executable predecessor.
func (s fileSink) Write(ctx context.Context, r io.Reader, m clip.Meta) error {
	if fi, err := os.Stat(s.target); err == nil && fi.IsDir() {
		if m.Filename == "" {
			return errors.New("buff: clip has no filename; specify -o <path>")
		}
		f, err := openInDir(s.target, m.Filename, false, m.Executable) // clobber, like a shell redirect
		if err != nil {
			return buffErr(err)
		}
		// openInDir succeeded, so m.Filename was a valid single component: the joined path is the real
		// destination, and naming it now — before the copy — mirrors saveSink, the one other sink that
		// lands a metadata-chosen filename the user never typed.
		narrateSave(s.errw, filepath.Join(s.target, m.Filename))
		return copyClose(f, r)
	}
	f, err := os.Create(s.target)
	if err != nil {
		return fmt.Errorf("buff: %w", err)
	}
	// The user named this literal path, so setting the clip's run bit — owner-exec on, or all
	// exec cleared when the clip is not runnable — is the cp-like behaviour they asked for. Both
	// directions, so a non-exec clip clobbering an existing executable does not inherit its run bit.
	// This arm is unconfined by design — the user's own path, not a peer-supplied name — so it applies
	// straight to the os.Create fd rather than through openInDir.
	if err := applyExecutable(f, m.Executable); err != nil {
		f.Close()
		return buffErr(err)
	}
	return copyClose(f, r)
}

// newDirSink extracts an archive into a fresh directory named for the slot, in the working
// directory — the behaviour for an archive pasted at a terminal. It is always the atomic whole-
// archive form, so a pre-existing directory of that name is refused rather than merged into: the
// terminal default did not ask to merge, and refusing avoids silently mixing a new tree into an
// old one. On success it narrates the landing directory, the archive counterpart of saveSink's file
// note: a paste that lands a whole tree on disk is exactly the disk landing worth confirming, and
// errw carries that one line.
type newDirSink struct {
	name string
	errw io.Writer
}

func (s newDirSink) Write(ctx context.Context, r io.Reader, m clip.Meta) error {
	root, err := os.OpenRoot(".")
	if err != nil {
		return fmt.Errorf("buff: %w", err)
	}
	defer root.Close()
	if err := archive.ExtractNew(ctx, root, s.name, r, archive.ExtractOpts{}); err != nil {
		return buffErr(err) // a torn or colliding extract rolls back and is never narrated
	}
	narrateExtract(s.errw, "./"+s.name)
	return nil
}

// siblingName names the free sibling directory beside the slot name Write could not claim: the slot
// with the delivery's generation id appended (project -> project-<gen>), unique per delivery. The
// hyphen-suffix is the directory scheme, deliberately not the file sink's dot-infix (besideName): a
// directory name has no extension to keep last, and splicing before a trailing dot would mis-read a
// dotted slot (a slot may be named "my.app") as if its last segment were an extension.
func (s newDirSink) siblingName(m clip.Meta, gen string) string {
	return s.name + "-" + gen
}

// landBeside extracts the spent consume-once archive into the flow-validated sibling directory
// — the archive half of saveSink.landBeside, called under the same whole-body precondition. The
// sibling is unique per delivery, so the publish needs no probe and no loop: a single ExtractNew
// reads the body exactly once and renames the finished tree into place.
//
// That single commit is load-bearing, not a simplification. The body is spent by the first
// extraction, so a retry on a different name would re-extract an already-drained reader — which
// succeeds and silently plants a bogus empty tree. One commit means a late collision on this name
// — now only a concurrent creator, since the flow already screened out a hostile or over-long name
// — is the reported loss it is, never a silent empty directory. The narration is past tense and
// follows the publish, as narrateExtract's: ExtractNew is atomic, so reaching the line means the
// tree is wholly there.
func (s newDirSink) landBeside(ctx context.Context, r io.Reader, m clip.Meta, name string) error {
	root, err := os.OpenRoot(".")
	if err != nil {
		return fmt.Errorf("buff: %w", err)
	}
	defer root.Close()
	if err := archive.ExtractNew(ctx, root, name, r, archive.ExtractOpts{}); err != nil {
		// Names only why the sibling landing failed; the flow appends the loss once for every path.
		// Keeping err under %w preserves the collision identity (a concurrent creator's ErrDestExists),
		// so the exit code stays the 6 it would be without the salvage attempt.
		return fmt.Errorf("buff: %s exists; salvaging beside it also failed: %w", "./"+s.name, err)
	}
	narrateDivertedExtract(s.errw, "./"+s.name, "./"+name)
	return nil
}

// extractSink extracts an archive into an explicit -o target. An existing directory is merged
// into, with the archiver's per-entry no-clobber as the safety net; an absent target is published
// atomically as a new directory, which requires its parent to exist; a target that is an existing
// file is an error, since an archive needs a directory. Merging into an existing directory cannot
// be atomic, so a failed merge may leave some entries behind — the one weaker guarantee, accepted
// because the user named that directory explicitly. Either way a clean extraction narrates the
// landing directory through errw, the disk-landing confirmation the file sinks already give; only
// success reaches that line, so a failed merge's partial state is reported as the error it is,
// never as a landing.
type extractSink struct {
	target string
	errw   io.Writer
}

func (s extractSink) Write(ctx context.Context, r io.Reader, m clip.Meta) error {
	target := filepath.Clean(s.target)
	fi, err := os.Stat(target)
	switch {
	case err == nil && fi.IsDir():
		root, err := os.OpenRoot(target)
		if err != nil {
			return fmt.Errorf("buff: %w", err)
		}
		defer root.Close()
		if err := archive.Extract(ctx, root, r, archive.ExtractOpts{}); err != nil {
			return buffErr(err)
		}
	case err == nil:
		return fmt.Errorf("buff: -o %s is a file; an archive extracts into a directory", s.target)
	case errors.Is(err, os.ErrNotExist):
		parent, err := os.OpenRoot(filepath.Dir(target))
		if err != nil {
			return fmt.Errorf("buff: %w", err)
		}
		defer parent.Close()
		if err := archive.ExtractNew(ctx, parent, filepath.Base(target), r, archive.ExtractOpts{}); err != nil {
			return buffErr(err)
		}
	default:
		return fmt.Errorf("buff: %w", err) // a real stat failure (permission) is neither "directory" nor "free"
	}
	narrateExtract(s.errw, target)
	return nil
}

// saveSink is the disposition for a file clip pasted at a terminal with no -o: it saves the clip
// into the working directory under its remembered filename, refusing to clobber an existing file.
// It is a pure disk-saver — its name is finally exact — touching the terminal only to narrate where
// the bytes landed.
//
// It narrates because the bytes land on disk where the terminal cannot show them: buff @doc saves
// ./report.pdf rather than printing it, so confirming the landing — and the metadata-drawn name the
// user never typed — is worth a line. Every disk-landing sink does the same (fileSink's directory
// arm for a file, newDirSink and extractSink for a tree); only an -o <file> the user spelled out in
// full, and bytes the terminal or a pipe already shows, stay silent.
//
// It is one of the two sinks that implement salvager. Its no-clobber save is the only file
// landing whose open can collide with a delivery the server has already spent: the server claims
// a consume-once delivery at Open, before any byte ships, so a refused save would lose the only
// copy. Rather than lose it, the salvage lands the untouched body on a free sibling beside the
// colliding name (./report.<gen>.pdf). The decision to do so is the flow's, not the sink's (see
// divertConsumeOnce); saveSink only contributes the mechanism — its no-clobber save here, and the
// salvager pair (siblingName, landBeside) below. fileSink's directory arm clobbers, so it never
// collides and never needs either.
type saveSink struct {
	errw io.Writer
	slot string
}

// filename is the basename a save lands under: the clip's remembered filename, falling back to the
// slot's last component for a clip that carries none — now only a malformed peer, since buff's own
// file copies always remember a basename and a bytes clip never reaches this sink. The normal save
// and the salvage land under names built from the same basename, so they share this one resolver.
func (s saveSink) filename(m clip.Meta) string {
	if m.Filename != "" {
		return m.Filename
	}
	return path.Base(s.slot)
}

// Write saves the clip's bytes to a no-clobber file in the working directory. A collision surfaces
// as os.ErrExist before any byte is read — openInDir's pre-commit boundary, where the chmod and the
// O_EXCL open both sit ahead of the copy — which is exactly what lets the flow tell a recoverable
// consume-once collision (body still whole, divertConsumeOnce salvages it) from a replaceable one
// (the collision stands as exit 6 via os.ErrExist).
func (s saveSink) Write(ctx context.Context, r io.Reader, m clip.Meta) error {
	name := s.filename(m)
	f, err := openInDir(".", name, true, m.Executable) // save, no-clobber
	if err != nil {
		return buffErr(err) // a collision is os.ErrExist; the flow may divert a spent consume-once delivery
	}
	narrateSave(s.errw, "./"+name)
	return copyClose(f, r) // a mid-copy failure leaves a partial file and the error, with no rewind
}

// siblingName names the free sibling beside the file Write could not claim: besideName splices the
// delivery's generation id in front of the extension (report.pdf -> report.<gen>.pdf), unique per
// delivery, so a concurrent second salvage of the same name lands its own distinct sibling rather
// than racing this one. Keeping the extension last leaves the rescued file usable by an extension-
// aware tool without a rename.
func (s saveSink) siblingName(m clip.Meta, gen string) string {
	return besideName(s.filename(m), gen)
}

// landBeside lands a spent consume-once delivery on the flow-validated free sibling beside the
// colliding name — the flow calls it only when the body is still whole. The open is still O_EXCL:
// the gen makes a real collision impossible for a buff peer, and on the astronomical chance
// of one it fails closed to a reported loss rather than clobbering. The name itself is already
// flow-validated, so an error here is that race or a real IO fault, never an unusable name. The
// narration is present tense and precedes the non-atomic copy, exactly as narrateSave's: the copy
// can still tear after the line, so it promises the attempt and names where the bytes are going,
// not a finished file.
func (s saveSink) landBeside(ctx context.Context, r io.Reader, m clip.Meta, name string) error {
	existing := s.filename(m)
	f, err := openInDir(".", name, true, m.Executable)
	if err != nil {
		// Names only why the sibling landing failed; the flow appends the loss once for every path.
		// Keeping err under %w preserves the collision identity (an astronomical O_EXCL race on the gen-
		// suffixed name), so the exit code stays the 6 it would be without the salvage attempt.
		return fmt.Errorf("buff: %s exists; salvaging beside it also failed: %w", existing, err)
	}
	narrateDivertedSave(s.errw, "./"+existing, "./"+name)
	return copyClose(f, r)
}

// narrateSave and narrateExtract are the disk-landing notices: a paste that writes bytes somewhere
// the terminal cannot show them says where they went. The rule across every sink is one line —
// confirm a landing unless the user spelled the exact final path (-o <file>) or the bytes are
// already visible (a terminal show, a pipe, -o -). Both write to errw, never stdout, so a paste
// whose stdout is redirected keeps the notice out of its data, and each caller passes its own
// honest path so the shared piece is the wording, not the path.
//
// narrateSave reports a file landing under a name buff resolved from clip metadata — saveSink's
// cwd ./name or fileSink's -o <dir> joined target, a name the user never typed. Its tense is
// deliberate: a streamed, possibly still-live, non-atomic write can tear after this line, so it
// promises the attempt, not a finished file.
func narrateSave(errw io.Writer, path string) {
	fmt.Fprintf(errw, "buff: saving to %s\n", path)
}

// narrateExtract reports a tree landing in a directory buff placed on disk — newDirSink's ./slot
// or extractSink's -o target. Its tense is the opposite of narrateSave's and just as deliberate:
// an extraction is atomic (it builds a temp tree and renames it into place, or rolls back leaving
// nothing), so the caller reaches this line only once the directory is wholly there, and a past-
// tense line can promise the result rather than the attempt. The trailing slash marks the landing
// a directory.
func narrateExtract(errw io.Writer, dir string) {
	fmt.Fprintf(errw, "buff: extracted to %s/\n", dir)
}

// narrateDivertedSave and narrateDivertedExtract are the consume-once salvage's landing notices —
// the diverted-landing counterparts of narrateSave and narrateExtract, and they keep the very same
// tense discipline, because diverting the landing does not change what each sink can promise. A
// salvaged file is a non-atomic copy that can tear after the line, so its notice is present tense
// and printed before the copy; a salvaged tree is an atomic ExtractNew already published, so its
// notice is past tense and printed after. Each names the collision that forced the diversion and
// the sibling the bytes went to, so even a torn file salvage still tells the user where the partial
// bytes are. (One shared neutral line would have to choose a single tense, mislabelling one sink —
// present tense lies about the published tree, past tense lies about the half-written file — so the
// two stay split, the same reason narrateSave and narrateExtract are two functions.)
func narrateDivertedSave(errw io.Writer, existing, landed string) {
	fmt.Fprintf(errw, "buff: %s exists; saving the consume-once delivery to %s instead\n", existing, landed)
}

func narrateDivertedExtract(errw io.Writer, existing, landed string) {
	fmt.Fprintf(errw, "buff: %s exists; extracted the consume-once delivery to %s/ instead\n", existing, landed)
}

// besideName forms the consume-once file salvage's sibling by splicing the delivery's
// generation id in front of name's extension: report.pdf -> report.<gen>.pdf, archive.tar.gz ->
// archive.tar.<gen>.gz. Keeping the extension last leaves the rescued file usable by an extension-
// aware tool without a rename — the one identity worth preserving on this rare path. A name with no
// extension to keep last takes the gen at the end instead: Makefile -> Makefile.<gen>, and a pure
// dotfile (.bashrc -> .bashrc.<gen>) likewise, since its leading dot is part of the name, not an
// extension separator.
//
// It is a pure formatter and validates nothing. The flow runs the whole spliced result through
// clip.ValidFilename before it reaches any open (see divertConsumeOnce), so a hostile or over-long
// gen that would form an unusable basename — a separator, a control, a name past the length bound —
// is caught there, uniformly with the directory sink, rather than here or deep in the write.
func besideName(name, gen string) string {
	ext := path.Ext(name)
	stem := name[:len(name)-len(ext)]
	if stem == "" {
		// A leading-dot name with no interior dot (".bashrc", ".gitignore") is all "extension" to
		// path.Ext, which would splice the gen before the leading dot (.<gen>.bashrc). A dotfile has no
		// extension to keep last, so the gen belongs at the end, exactly as the no-extension case.
		return name + "." + gen
	}
	return stem + "." + gen + ext
}

// openInDir opens name for writing inside dir, confined to an os.Root and re-validating name —
// the receiving half of the filename boundary. The name we are about to write to a consumer's disk
// arrived over the wire from a peer that may be hostile, foreign, or buggy, so it is never trusted
// on the peer's word: "../escape" is rejected here, and the os.Root makes it unwriteable outside
// dir regardless. excl chooses no-clobber — O_EXCL refuses an existing file; without it the open
// truncates one, the way a shell redirect does. executable sets the clip's run bit on the freshly-
// opened file with fchmod (see applyExecutable): owner-exec on for a runnable clip, all exec
// cleared otherwise, so a clobbered file reflects the clip and not the file it replaced.
//
// It is the setup half of a directory save, deliberately apart from the byte copy (copyClose),
// so a caller can tell a save that never began — a rejected name, an unwritable dir, a no-clobber
// collision, a failed chmod — from one that failed mid-write. Folding the chmod in here, before any
// byte is copied, keeps every such case on the pre-commit boundary a consume-once paste needs in
// order to know it can still salvage a delivery the server has already spent.
//
// The root is closed before the file is returned: an os.Root confines name resolution, which is
// finished once the file is open, and an open fd is an independent kernel object that outlives the
// directory handle it was opened through. The error is returned unwrapped — the caller owns the
// user-facing line and adds the buff: marker where it surfaces, so a cause folded into a larger
// diagnostic carries no stray prefix of its own.
func openInDir(dir, name string, excl, executable bool) (*os.File, error) {
	if err := clip.ValidFilename(name); err != nil {
		return nil, fmt.Errorf("refusing unsafe filename %q", name)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	flag := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if excl {
		flag = os.O_RDWR | os.O_CREATE | os.O_EXCL
	}
	f, err := root.OpenFile(name, flag, 0o644)
	if err != nil {
		return nil, err
	}
	// The 0o644 base above is only the create default for a fresh file; applyExecutable then sets the
	// run bit to the clip's with fchmod, both directions. fchmod, not the create mode, is what closes
	// the clobber gap either way: O_TRUNC over an existing file keeps that file's old mode and ignores
	// the create perm, so a create-perm approach would leave a clobbered file runnable when the clip
	// is not, or non-runnable when it is.
	if err := applyExecutable(f, executable); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// applyExecutable sets f's run bits to the clip's intent, in both directions. When want is true it
// mirrors `chmod +x`: owner-exec is forced and group/other gain exec only where the matching read
// bit is already present — derived from the file's on-disk mode, which the kernel already filtered
// through the consumer's umask at create, so a tight umask is honoured without buff reading umask
// itself (no syscall, portable, a clean no-op of effect where a platform has no such bits). When
// want is false it clears all exec bits.
//
// The clear arm is what makes the restore faithful rather than additive. On a clobber (O_TRUNC
// over an existing file — the -o arms) the opened file keeps its predecessor's mode, so a non-
// executable clip landing over an executable file would inherit a run bit that is not the clip's,
// leaking the old file's identity into the new one. Clearing makes the landed file match the clip
// both ways: runnable iff the clip was. It is a no-op on a fresh 0o644 O_EXCL base (no exec bit to
// clear), so it is safe to call unconditionally; running fchmod on the fd, it stays confined to the
// file already opened and works on the unconfined os.Create fd of a plain -o path exactly as on an
// os.Root fd.
func applyExecutable(f *os.File, want bool) error {
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	m := fi.Mode().Perm()
	if want {
		m |= 0o100 | ((m & 0o444) >> 2)
	} else {
		m &^= 0o111
	}
	return f.Chmod(m)
}

// copyClose streams r into f and then closes f, returning the first error. The copy error wins
// when there is one — it is the truncation or read failure the caller reports — and a close error
// surfaces only on an otherwise-clean write, where it means the bytes may not have reached the
// disk.
func copyClose(f *os.File, r io.Reader) error {
	_, cerr := io.Copy(f, r)
	if err := f.Close(); cerr == nil {
		cerr = err
	}
	return buffErr(cerr)
}
