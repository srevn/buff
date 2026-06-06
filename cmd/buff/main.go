// Command buff is the one binary: `buff serve` runs the content-relay server, and bare `buff …` is
// the client. This package is wiring only — the apex main that assembles finished layers and owns
// the few things only a process entry point may: os.Exit, os.Args, the process environment, and the
// real standard files. Everything with behaviour worth testing takes those as parameters, so the
// binary's logic is exercised without a subprocess.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/srevn/buff/api"
	"github.com/srevn/buff/cli"
	"github.com/srevn/buff/cmd/buff/internal/tty"
)

// main is the only os.Exit site and the only reader of the real globals. It hands them to buffMain
// — os.Exit among them, so even the second-signal force-quit is an injected dependency rather than
// a direct global call — and turns buffMain's returned code into the process exit status.
func main() {
	os.Exit(buffMain(os.Args[1:], os.Getenv, os.Stdin, os.Stdout, os.Stderr, os.Exit))
}

// buffMain forks the client grammar's one reserved subcommand from the client path, wires a signal
// into a context both paths share, and returns the process exit code. The environment, the three
// standard files, and os.Exit all arrive as parameters rather than from the globals, so a test
// drives the whole fork — the second-signal force-quit included — without a subprocess.
func buffMain(args []string, getenv func(string) string, in, out, errw *os.File, exit func(int)) int {
	// One signal handler serves both paths: the first SIGINT or SIGTERM cancels ctx, which the
	// server reads as "begin graceful shutdown" and the client as "stop the in-flight operation."
	// The cancellation carries a cause so the server can tell an upload it cut from one the client
	// truncated — a body read aborted by this cancellation is reported 503, not blamed on the
	// client as 400. The cause must be set here, at the root, because a context canceled by its
	// parent inherits the parent's cause: a cause set on some descendant would lose the race to that
	// propagation. The client path shares this context but reads only its Err, never its cause, so a
	// server-named cause is invisible there and harmless. The two-phase escalation over these channels
	// — first signal cancels, second forces exit, done retires the watcher — is watchSignals.
	sigs := make(chan os.Signal, 2) // buffered so a rapid double-tap is queued, not dropped before it is read
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigs)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	done := make(chan struct{})
	defer close(done)
	go watchSignals(sigs, done, cancel, api.ErrServerStopping, exit)

	// serve is the single reserved first token: only `buff serve …` forks to the server. A file
	// literally named serve is copied as ./serve — a bare path, never this token — and any flag or
	// @-slot the client grammar puts first is not the string "serve" either, so the grammar is never
	// shadowed. The server path does not map a signal to 130: a signal there is a graceful stop whose
	// success is exit 0, and runServe has already reported any real fault to errw.
	if len(args) > 0 && args[0] == "serve" {
		if runServe(ctx, args[1:], getenv, errw) != nil {
			return 1
		}
		return 0
	}

	env := cli.Env{ServerURL: resolveServerURL(getenv("BUFF_URL"), bakedURL), Version: buildVersion()}
	stdio := cli.IO{In: in, Out: out, Err: errw, InIsTTY: tty.IsTerminal(in), OutIsTTY: tty.IsTerminal(out), Now: time.Now}
	code := cli.Run(ctx, args, env, stdio)
	return clientExit(code, ctx.Err())
}

// watchSignals turns the signals delivered on sigs into lifecycle actions, retiring when done is
// closed on a clean finish so it never outlives the call. The first signal cancels with cause —
// beginning the server's graceful stop, or the client's operation cancel. A second signal, the
// operator insisting while a drain takes its time, forces an immediate exit rather than being
// swallowed for the whole shutdown window: the conventional "press Ctrl-C again to force-quit."
// The watcher handles that second signal itself rather than restoring the default disposition,
// because signal.Stop does not reliably re-arm a terminating SIGINT once the runtime has trapped
// it. cancel, the cause, and exit are injected, so the two-phase escalation is a unit test over
// channels, not a subprocess delivering real signals: in production exit is os.Exit and never
// returns, ending the process here; under a test it returns and the watcher simply unwinds.
func watchSignals(sigs <-chan os.Signal, done <-chan struct{}, cancel context.CancelCauseFunc, cause error, exit func(int)) {
	select {
	case <-sigs:
		cancel(cause)
	case <-done:
		return // the call finished without a signal
	}
	select {
	case <-sigs:
		exit(130) // second signal: abandon the drain and exit now (128+SIGINT)
	case <-done:
		return // the graceful stop completed before a second signal
	}
}

// clientExit maps a finished client run to a process exit code, translating a signal-canceled
// run to the conventional 128+SIGINT (130). cli.Run sees only the resulting typed error — a mid-
// copy cancel surfaces as 8, a mid-body paste as 7, an archive paste canceled between entries
// as the generic 1 — never the signal itself; only here, where the handler lives, is "the run
// failed because a signal fired" knowable, and all of those cancellation cases normalise alike.
// The handler cancels the context only on a delivered signal, so a non-zero code with a canceled
// context is exactly that case. A clean run despite a late signal stays its own success. SIGTERM
// maps to 130 too — the handler does not record which signal fired, and 130 is the documented
// value. It is split out as a pure function of the code and the context error so the exit-code
// boundary is unit-tested without delivering a real signal.
func clientExit(code int, ctxErr error) int {
	if code != 0 && ctxErr != nil {
		return 130
	}
	return code
}
