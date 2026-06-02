// Command buff is the one binary: `buff serve` runs the content-relay server, and bare `buff …` is
// the client. This package is wiring only — the apex main that assembles finished layers and owns the
// few things only a process entry point may: os.Exit, os.Args, the process environment, and the real
// standard files. Everything with behaviour worth testing takes those as parameters, so the binary's
// logic is exercised without a subprocess.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/srevn/buff/api"
	"github.com/srevn/buff/cli"
)

// defaultServerURL is where the client talks when BUFF_URL is unset: a server on the local machine's
// default port, the friendly default for a single-host relay.
const defaultServerURL = "http://localhost:8080"

// main is the only os.Exit site and the only reader of the real globals, handing them to buffMain
// and turning its returned code into the process exit status.
func main() {
	os.Exit(buffMain(os.Args[1:], os.Getenv, os.Stdin, os.Stdout, os.Stderr))
}

// buffMain forks the client grammar's one reserved subcommand from the client path, wires a signal
// into a context both paths share, and returns the process exit code. The environment and the three
// standard files arrive as parameters rather than from the globals, so a test drives the whole fork
// directly.
func buffMain(args []string, getenv func(string) string, in, out, errw *os.File) int {
	// One signal handler serves both paths. The first SIGINT or SIGTERM cancels ctx — the server
	// reads that as "begin graceful shutdown," the client as "stop the in-flight operation." A second
	// signal, the operator insisting while a drain takes its time, forces an immediate exit rather
	// than being swallowed for the whole shutdown window: the conventional "press Ctrl-C again to
	// force-quit." The watcher reads the second signal explicitly and exits, rather than trying to
	// restore the default disposition — signal.Stop does not reliably re-arm a terminating SIGINT
	// once the runtime has trapped it, so the dependable mechanism is to handle the second signal
	// ourselves. done retires the watcher on a clean finish, so it never outlives the call.
	sigs := make(chan os.Signal, 2) // buffered so a rapid double-tap is queued, not dropped before it is read
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigs)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-sigs:
			// First signal: begin the graceful stop. The cancellation carries a cause so the server
			// can tell an upload it cut from one the client truncated — a body read aborted by this
			// cancellation is reported as 503, not blamed on the client as 400. The cause must be set
			// here, at the root, because a context cancelled by its parent inherits the parent's
			// cause: a cause set on some descendant would lose the race to this propagation. The
			// client path shares this context but reads only its Err, never its cause, so a
			// server-named cause is invisible there and harmless.
			cancel(api.ErrServerStopping)
		case <-done:
			return // the call finished without a signal
		}
		select {
		case <-sigs:
			os.Exit(130) // second signal: abandon the drain and exit now (128+SIGINT)
		case <-done:
			return // the graceful stop completed before a second signal
		}
	}()

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

	env := cli.Env{ServerURL: getenvOr(getenv, "BUFF_URL", defaultServerURL), Version: buildVersion()}
	stdio := cli.IO{In: in, Out: out, Err: errw, InIsTTY: isTTY(in), OutIsTTY: isTTY(out)}
	code := cli.Run(ctx, args, env, stdio)
	return clientExit(code, ctx.Err())
}

// clientExit maps a finished client run to a process exit code, translating a signal-cancelled run to
// the conventional 128+SIGINT (130). cli.Run sees only the resulting typed error — a mid-copy cancel
// surfaces as 8, a mid-paste as 7 — never the signal itself; only here, where the handler lives, is
// "the run failed because a signal fired" knowable. NotifyContext cancels the context only on a
// delivered signal, so a non-zero code with a cancelled context is exactly that case. A clean run
// despite a late signal stays its own success. SIGTERM maps to 130 too — NotifyContext does not
// surface which signal fired, and 130 is the documented value. It is split out as a pure function of
// the code and the context error so the exit-code boundary is unit-tested without delivering a real
// signal.
func clientExit(code int, ctxErr error) int {
	if code != 0 && ctxErr != nil {
		return 130
	}
	return code
}

// isTTY reports whether f is a terminal, using only the standard library: a terminal is a character
// device. The one false positive — /dev/null is a character device too — is exactly why the copy and
// paste forcing flags exist, so detecting the terminal this way needs no x/term dependency to be
// correct everywhere it is relied upon.
func isTTY(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// getenvOr returns the environment value for key, or def when it is unset or empty — the client-side
// mirror of the server's env defaulting, kept here because the client path resolves only this one
// variable.
func getenvOr(getenv func(string) string, key, def string) string {
	if v := getenv(key); v != "" {
		return v
	}
	return def
}
