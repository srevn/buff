package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/srevn/buff/client"
)

// action is what a parsed invocation resolves to: one of the six things buff does.
type action int

const (
	actionCopy    action = iota // producer side: stream a source into a Put
	actionPaste                 // consumer side: stream a Get into a sink
	actionList                  // -l: list finalized clips
	actionDelete                // -d @slot: delete a clip
	actionStat                  // -s @slot: report a clip's metadata without reading it
	actionVersion               // --version: print the client version, no network
	actionHelp                  // -h/--help: print usage, no network
)

// invocation is the fully resolved command: an action, its target slot, and the inputs that
// action needs. It is the sole product of parsing — execution past this point never
// re-examines the raw arguments.
type invocation struct {
	act       action
	slot      string         // the @name, or "default" for copy/paste with no @; the target for delete/stat
	paths     []string       // copy sources (bare path arguments), in the order given
	output    string         // -o value (paste only)
	outputSet bool           // whether -o was given
	put       client.PutOpts // --ttl/--keep/--consume (copy only)
	server    string         // --server override of the configured URL
	serverSet bool           // whether --server was given
}

// usageError is a malformed-invocation error: a bad flag, a conflicting mode, a slot where a
// path belongs. It is its own type so the exit map routes every one of them to the generic
// usage exit and a caller can tell a grammar mistake from a network or domain failure. The
// message is the whole diagnostic, printed to stderr as-is.
type usageError struct{ msg string }

func (e *usageError) Error() string { return e.msg }

// usagef builds a usageError with the "buff: " prefix every diagnostic cli originates leads with.
// (An error rendered by the client carries the marker within its sentinel text rather than leading;
// Run prints that as-is.)
func usagef(format string, a ...any) error {
	return &usageError{msg: "buff: " + fmt.Sprintf(format, a...)}
}

// tokens is the lexical result of one scan over the arguments: the slots (each the text
// after an '@'), the bare paths in order, and the flag values. Classification here is purely
// syntactic — an '@' prefix, a leading '-', or neither — and never touches the filesystem.
type tokens struct {
	slots []string
	paths []string
	flags flags
}

// flags holds the whole fixed flag surface. Booleans default off; the value-bearing flags
// carry a "set" companion so an explicitly empty or zero value is distinguishable from an
// unset one.
type flags struct {
	copy, paste        bool // -c / -p
	list, delete, stat bool // -l / -d / -s
	version            bool // --version
	help               bool // -h / --help

	output    string // -o / --output (paste target)
	outputSet bool

	ttl     time.Duration // --ttl (copy retention)
	ttlSet  bool
	keep    bool // --keep (copy: never expire)
	consume bool // --consume (copy: consume-once)

	server    string // --server (URL override)
	serverSet bool
}

// flagSpec describes one flag: whether it consumes a following value, and the setter for its
// kind. Each flag has exactly one of setBool/setValue, matched to takesValue, so a value is
// never read into a boolean flag or vice versa.
type flagSpec struct {
	takesValue bool
	setBool    func(*flags)
	setValue   func(*flags, string) error
}

func boolFlag(set func(*flags)) flagSpec { return flagSpec{setBool: set} }
func valueFlag(set func(*flags, string) error) flagSpec {
	return flagSpec{takesValue: true, setValue: set}
}

// flagSpecs is the entire flag vocabulary, short and long spellings pointing at one setter.
// There is no -f and no -n: files are bare words and slots are @name, so the only flags are
// the mode forces, the management actions, the paste output, the copy write-options, and the
// server override. The server override is --server (long-only): BUFF_URL covers the common
// case, so a per-invocation override need not spend a short letter, and -s stays with the
// -l/-d/-s management family. An argument starting with '-' that is not in this table is a
// usage error, never a silent path.
var flagSpecs = map[string]flagSpec{
	"-c":        boolFlag(func(f *flags) { f.copy = true }),
	"--copy":    boolFlag(func(f *flags) { f.copy = true }),
	"-p":        boolFlag(func(f *flags) { f.paste = true }),
	"--paste":   boolFlag(func(f *flags) { f.paste = true }),
	"-l":        boolFlag(func(f *flags) { f.list = true }),
	"--list":    boolFlag(func(f *flags) { f.list = true }),
	"-d":        boolFlag(func(f *flags) { f.delete = true }),
	"--delete":  boolFlag(func(f *flags) { f.delete = true }),
	"-s":        boolFlag(func(f *flags) { f.stat = true }),
	"--stat":    boolFlag(func(f *flags) { f.stat = true }),
	"--keep":    boolFlag(func(f *flags) { f.keep = true }),
	"--consume": boolFlag(func(f *flags) { f.consume = true }),
	"--version": boolFlag(func(f *flags) { f.version = true }),
	"-h":        boolFlag(func(f *flags) { f.help = true }),
	"--help":    boolFlag(func(f *flags) { f.help = true }),

	"-o":       valueFlag(setOutput),
	"--output": valueFlag(setOutput),
	"--server": valueFlag(func(f *flags, v string) error { f.server = v; f.serverSet = true; return nil }),
	"--ttl":    valueFlag(setTTL),
}

// setOutput records the paste destination, rejecting an empty value at the grammar where it is a
// plain mistake rather than at a sink where it misbehaves. An empty -o is never meaningful and is
// the surprising one: filepath.Clean("") is ".", so an empty -o would silently extract an archive
// into the working directory instead of failing. Caught here, like a negative --ttl, it never
// reaches extractSink. "-o ." for the working directory is explicit and fine.
func setOutput(f *flags, v string) error {
	if v == "" {
		return usagef("-o requires a non-empty path")
	}
	f.output, f.outputSet = v, true
	return nil
}

// setTTL parses a --ttl value as a Go duration and rejects a negative one. The negative case
// is caught here rather than left to slip through: the client omits a non-positive TTL header
// and the server then applies its default, so without this guard "--ttl -1h" would silently
// keep the clip for the default retention instead of reporting the mistake. Zero is allowed
// and asks for the server default explicitly.
func setTTL(f *flags, v string) error {
	d, err := time.ParseDuration(v)
	if err != nil {
		return usagef("invalid --ttl %q: %v", v, err)
	}
	if d < 0 {
		return usagef("--ttl must not be negative: %q", v)
	}
	f.ttl, f.ttlSet = d, true
	return nil
}

// scan walks the arguments once, classifying each by syntax alone. "--" ends flag parsing, so
// a later argument that looks like a flag (a file named "-x") is taken as a path; everything
// after it is positional. A token starting with '@' is a slot — the non-empty text after the
// '@'. A token starting with '-' (other than a lone "-") is a flag, looked up in the fixed
// vocabulary, consuming the next argument as its value when it takes one and no "=value" was
// attached. Anything else — including a lone "-" — is a bare path. @-slots and paths may
// appear in any order; an unknown flag is a usage error, never a silent path.
func scan(args []string) (tokens, error) {
	var t tokens
	flagsEnded := false
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case flagsEnded:
			if err := t.addPositional(a); err != nil {
				return tokens{}, err
			}
		case a == "--":
			flagsEnded = true
		case len(a) > 1 && a[0] == '-':
			consumed, err := t.flags.set(a, args, i)
			if err != nil {
				return tokens{}, err
			}
			i += consumed
		default:
			if err := t.addPositional(a); err != nil {
				return tokens{}, err
			}
		}
	}
	return t, nil
}

// parseArgs runs the two front-end passes in order: the lexical scan that classifies each
// argument by syntax, then the grammar that resolves the scanned tokens into an invocation.
// It is the single front end Run drives; an error from either pass is the usage error the
// run reports.
func parseArgs(args []string, inIsTTY bool) (invocation, error) {
	t, err := scan(args)
	if err != nil {
		return invocation{}, err
	}
	return parse(t, inIsTTY)
}

// addPositional files a non-flag token as a slot or a path. A '@' prefix marks a slot; the
// remainder is its name, which must not be empty — a bare "@" is a sigil with nothing to
// address, rejected here rather than sent to the server as an empty, confusing request.
func (t *tokens) addPositional(a string) error {
	if len(a) > 0 && a[0] == '@' {
		name := a[1:]
		if name == "" {
			return usagef("a slot needs a name after @, e.g. @work")
		}
		t.slots = append(t.slots, name)
		return nil
	}
	t.paths = append(t.paths, a)
	return nil
}

// set applies one flag token to the flag set and reports how many following arguments it
// consumed (0 or 1). A "name=value" form supplies the value inline; otherwise a value-taking
// flag consumes the next argument, an error if there is none. A value attached to a boolean
// flag, or a flag not in the vocabulary, is a usage error.
func (f *flags) set(a string, args []string, i int) (int, error) {
	name, inline, hasInline := splitFlag(a)
	spec, ok := flagSpecs[name]
	if !ok {
		return 0, usagef("unknown flag %q", a)
	}
	if !spec.takesValue {
		if hasInline {
			return 0, usagef("flag %q takes no value", name)
		}
		spec.setBool(f)
		return 0, nil
	}
	if hasInline {
		return 0, spec.setValue(f, inline)
	}
	if i+1 >= len(args) {
		return 0, usagef("flag %q requires a value", name)
	}
	if err := spec.setValue(f, args[i+1]); err != nil {
		return 0, err
	}
	return 1, nil
}

// splitFlag splits a flag token at the first '=', so "--ttl=1h" and "-o=out" supply their
// value inline. With no '=' the value, if the flag takes one, comes from the next argument.
func splitFlag(a string) (name, val string, hasVal bool) {
	if before, after, ok := strings.Cut(a, "="); ok {
		return before, after, true
	}
	return a, "", false
}

// parse turns a scanned token set into a resolved invocation, applying the grammar in a fixed
// order: at most one slot, then a single management action if a flag selects one, otherwise
// copy-vs-paste by mode detection. Every violation is a usage error naming the conflict;
// nothing here probes the filesystem.
func parse(t tokens, inIsTTY bool) (invocation, error) {
	f := t.flags

	// A help request short-circuits the grammar: -h/--help wins over a slot-count, management, or
	// mode error on the same line, so "buff -l -h" prints help rather than "conflicting actions" — a
	// user asking for help has by definition not got the line right yet, and surfacing help is the
	// useful answer. It does not override a *scan* error: an unknown flag earlier on the line
	// ("buff --bogus -h") still fails first, because scan runs before parse and a malformed flag is
	// worth reporting. Help answers offline, exactly like --version.
	if f.help {
		return invocation{act: actionHelp}, nil
	}

	if len(t.slots) > 1 {
		return invocation{}, usagef("too many slots (%s); a command addresses one slot", strings.Join(reslot(t.slots), " "))
	}

	switch n, act := managementAction(f); {
	case n > 1:
		return invocation{}, usagef("conflicting actions; use only one of --list, --delete, --stat, --version")
	case n == 1:
		return parseManage(act, t, f)
	}

	act, err := mode(len(t.paths) > 0, !inIsTTY, f.copy, f.paste)
	if err != nil {
		return invocation{}, err
	}
	inv := invocation{act: act, server: f.server, serverSet: f.serverSet}
	if act == actionPaste {
		if f.ttlSet || f.keep || f.consume {
			return invocation{}, usagef("--ttl/--keep/--consume apply only when copying")
		}
		inv.output, inv.outputSet = f.output, f.outputSet
	} else {
		if f.outputSet {
			return invocation{}, usagef("-o/--output applies only when pasting")
		}
		if f.keep && f.ttlSet {
			return invocation{}, usagef("--keep and --ttl conflict; --keep keeps the clip indefinitely")
		}
		inv.paths = t.paths
		inv.put = client.PutOpts{TTL: f.ttl, Keep: f.keep, ConsumeOnce: f.consume}
	}
	if len(t.slots) == 1 {
		inv.slot = t.slots[0]
	} else {
		inv.slot = "default"
	}
	return inv, nil
}

// managementAction reports how many management flags are set and which action the last one
// names. The caller treats a count above one as a conflict and a count of one as that action;
// the returned action is meaningful only when the count is exactly one.
func managementAction(f flags) (int, action) {
	n, act := 0, actionCopy
	if f.list {
		n, act = n+1, actionList
	}
	if f.delete {
		n, act = n+1, actionDelete
	}
	if f.stat {
		n, act = n+1, actionStat
	}
	if f.version {
		n, act = n+1, actionVersion
	}
	return n, act
}

// parseManage resolves a management invocation and rejects the options that belong only to
// copy or paste. List and version take no slot or path; delete and stat each need exactly one
// slot and no path — a bare word with -d/-s is a path, which is the mistake of writing the
// slot without its '@'.
func parseManage(act action, t tokens, f flags) (invocation, error) {
	if f.copy || f.paste {
		return invocation{}, usagef("-c/-p apply only when copying or pasting")
	}
	if f.ttlSet || f.keep || f.consume {
		return invocation{}, usagef("--ttl/--keep/--consume apply only when copying")
	}
	if f.outputSet {
		return invocation{}, usagef("-o/--output applies only when pasting")
	}
	base := invocation{act: act, server: f.server, serverSet: f.serverSet}
	switch act {
	case actionList, actionVersion:
		if len(t.slots) > 0 || len(t.paths) > 0 {
			return invocation{}, usagef("%s takes no slot or path", actionName(act))
		}
		return base, nil
	default: // actionDelete, actionStat
		if len(t.paths) > 0 {
			return invocation{}, usagef("%s needs a slot (@name), not a path", actionName(act))
		}
		if len(t.slots) != 1 {
			return invocation{}, usagef("%s needs exactly one slot (@name)", actionName(act))
		}
		base.slot = t.slots[0]
		return base, nil
	}
}

// mode resolves copy-vs-paste from whether path arguments are present, whether stdin is piped
// (not a TTY), and the -c/-p forces. The sigil already settled name-vs-path and the stream
// type settles direction, so there is no residual guess: a bare word at a TTY copies that
// file, never silently falling back to a paste. The forces exist for scripts where TTY
// detection is unreliable; -p ignores any incidental piped stdin, and -c with no source is an
// error rather than a guess.
func mode(hasPaths, stdinPiped, forceCopy, forcePaste bool) (action, error) {
	switch {
	case forceCopy && forcePaste:
		return 0, usagef("-c and -p conflict")
	case forcePaste:
		if hasPaths {
			return 0, usagef("paste takes no path arguments")
		}
		return actionPaste, nil
	case hasPaths && stdinPiped:
		return 0, usagef("both stdin and path arguments given; the source must be unambiguous")
	case hasPaths:
		return actionCopy, nil
	case stdinPiped:
		return actionCopy, nil
	case forceCopy:
		return 0, usagef("nothing to copy")
	default:
		return actionPaste, nil
	}
}

// reslot re-attaches the '@' sigil to slot names for a diagnostic, so it shows them as the
// user wrote them.
func reslot(slots []string) []string {
	out := make([]string, len(slots))
	for i, s := range slots {
		out[i] = "@" + s
	}
	return out
}

// actionName is the word a usage message uses for a management action.
func actionName(act action) string {
	switch act {
	case actionList:
		return "--list"
	case actionDelete:
		return "--delete"
	case actionStat:
		return "--stat"
	case actionVersion:
		return "--version"
	default:
		return "command"
	}
}
