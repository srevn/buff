package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/srevn/buff/client"
)

// IO is the process's standard streams and what each one is, injected rather than reached for
// directly so the whole package is testable without a terminal or a subprocess. The stream facts
// are computed once by the binary's main from the real standard files and passed in as plain
// booleans, which is what lets the copy-vs-paste, archive-output, and status-line decisions be
// exercised across every terminal/pipe/file combination from an ordinary test. Foreground is the
// one fact that changes while a run goes on, so it arrives as a function to ask rather than a
// value: a job moves between its terminal's foreground and background at the user's say, so a
// status line asks before every write.
//
// Out carries data only — a pasted clip's bytes, a list table, a stat block. Err carries everything
// else — diagnostics, the source-skip and consume-once warnings, a slow transfer's status line — so
// a consumer redirecting stdout to a file never finds a warning mixed into its data.
type IO struct {
	In         io.Reader   // standard input; the copy source when piped
	Out        io.Writer   // standard output; the default paste destination, data only
	Err        io.Writer   // standard error; diagnostics, warnings, and the status line, never data
	InIsTTY    bool        // input is a terminal: drives copy versus paste
	OutIsTTY   bool        // output is a terminal: drives an archive's extract versus raw tar
	OutIsFile  bool        // output is a regular file: a redirect no other process reads while the run goes on
	ErrIsTTY   bool        // error output is a terminal: someone may be watching a transfer
	Foreground func() bool // whether the process holds error output's terminal's foreground now; nil reads as always
}

// foreground asks the injected Foreground, falling back to always when a caller left it unset — a
// test, or an embedder with no job control to consult. Only a status line asks, and only once the
// gate has found error output a terminal, so the fallback can only ever allow a draw where the
// caller already said a terminal is there.
func (s IO) foreground() bool {
	if s.Foreground != nil {
		return s.Foreground()
	}
	return true
}

// Env is the resolved configuration a run needs from its environment, supplied by the binary's main
// from the process environment. ServerURL already has the binary's full pre-flag precedence folded
// in — the BUFF_URL value, any compiled-in default, then the built-in fallback — and a --server
// flag overrides it for the one invocation. Version is the build-stamped string --version prints.
type Env struct {
	ServerURL string // the server to talk to; the --server flag overrides it per invocation
	Version   string // the client version --version reports
}

// Run parses args, performs the addressed action, and returns the process exit code. It never
// calls os.Exit and never panics on user input — the binary's main turns the returned code into the
// actual exit — so a test drives it directly and reads the code, the streams, and any filesystem
// effect. ctx carries cancellation: the binary's main wires a signal to it, and it threads through
// every network and archive operation so a canceled run stops promptly rather than leaking work.
//
// Every error funnels through one place: diagnostic renders a single line to Err — the error itself
// in the ordinary case, or one honest "canceled" line when a signal stopped the run — then exitCode
// maps the typed error to its code. Success is a silent zero.
func Run(ctx context.Context, args []string, env Env, std IO) int {
	if err := run(ctx, args, env, std); err != nil {
		fmt.Fprintln(std.Err, diagnostic(err))
		return exitCode(err)
	}
	return 0
}

// buffErr marks an error surfaced from below cli — an io.Copy into a sink, an archive extraction,
// a tabwriter flush — with the "buff: " prefix every diagnostic cli originates leads with. cli owns
// the user-facing line for these, but the wrapped library error carries no marker of its own, so
// without this an os, io, or archive error reaches Run's printer as a bare "write …: broken pipe"
// or "archive: …" that does not read as buff's. A nil error stays nil, so a clean pass-through —
// the common case — is returned untouched and never becomes a spurious failure.
func buffErr(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("buff: %w", err)
}

// run is Run's error-returning core: it does the work and hands every failure back as a typed error
// for Run to print and score. Splitting the error handling out keeps the diagnostic and the exit
// mapping in exactly one place rather than at each return.
//
// The client is built lazily, only for an action that talks to the network: --version answers from
// Env alone and --help from a static string, so neither needs a server and a bad or unset URL never
// stops them. A malformed server URL is reported as the construction error it is, which scores as
// the generic usage exit.
func run(ctx context.Context, args []string, env Env, std IO) error {
	inv, err := parseArgs(args, std.InIsTTY)
	if err != nil {
		return err
	}
	if inv.act == actionVersion {
		fmt.Fprintln(std.Out, env.Version)
		return nil
	}
	if inv.act == actionHelp {
		writeUsage(std.Out, env.ServerURL)
		return nil
	}
	c, err := client.New(serverURL(inv, env), nil)
	if err != nil {
		return err
	}
	return dispatch(ctx, c, inv, std)
}

// dispatch performs the resolved action over the wire client. Version and help are already handled
// before the client is built, so they cannot reach here; the remaining five actions each own a
// flow. Delete is a single call with no output — a bare success or a typed error — so it stays
// inline rather than earning its own function.
func dispatch(ctx context.Context, c *client.Client, inv invocation, std IO) error {
	switch inv.act {
	case actionCopy:
		return transfer(ctx, c, inv, std, copyFlow)
	case actionPaste:
		return transfer(ctx, c, inv, std, paste)
	case actionList:
		return list(ctx, c, std)
	case actionStat:
		return stat(ctx, c, inv, std)
	case actionDelete:
		return c.Delete(ctx, inv.slot)
	default:
		// Unreachable: parse yields these five network actions plus version and help, and the latter two
		// are handled in run before dispatch. A defensive internal error is still preferable to a silent
		// misdispatch.
		return fmt.Errorf("buff: internal error: unhandled action %d", inv.act)
	}
}

// flow is the shape the two actions that move a clip's bytes share, copyFlow and paste: each
// resolves its own endpoint from the invocation and the streams, and attaches the stream it moves
// to the transfer's progress.
type flow func(ctx context.Context, c *client.Client, inv invocation, std IO, p *progress) error

// transfer runs move, a copy or a paste, under a progress that the client's connect hook and the
// moved stream report to, and — when the terminal is the run's alone to draw on — a status line
// that shows it. The line is settled before transfer returns, so Run's diagnostic always starts on
// a fresh line beneath whatever the line last said; settling in a defer keeps that true of a flow
// that panics, whose crash report then starts on a clean row as well. IO is a value, so the streams
// the line installs reach exactly this flow — its source and its sink — while Run keeps the real
// ones.
func transfer(ctx context.Context, c *client.Client, inv invocation, std IO, move flow) (err error) {
	p := newProgress(inv, c.Host())
	std, settle := showProgress(ctx, p, std)
	defer func() { settle(err) }()
	return move(client.WithConnectHook(ctx, p.connected), c, inv, std, p)
}

// serverURL applies the configuration precedence for one invocation: the --server flag when
// given, otherwise the environment-resolved default the binary's main supplied. The flag is a per-
// invocation override; the environment carries the usual case.
func serverURL(inv invocation, env Env) string {
	if inv.serverSet {
		return inv.server
	}
	return env.ServerURL
}
