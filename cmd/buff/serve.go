package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
)

// serveUsage heads `buff serve -h`. The flag list PrintDefaults prints below it already names each
// flag's matching BUFF_* variable, so that list is the environment-variable reference, rendered from
// the one source of truth (the flag definitions) rather than duplicated and left to drift; this text
// supplies only the framing a bare list lacks — the required setting and the precedence rule.
const serveUsage = `buff serve — run the content-relay server.

BUFF_DATA_DIR (or -data-dir) is required: it is the storage root for every clip.
Each flag below has a matching BUFF_* environment variable; an explicit flag
overrides the variable, which overrides the built-in default.

Flags:
`

// runServe is the server entry point: resolve configuration, bind and parse the serve flags, build
// the runtime, and run it until a signal or a fault stops it. It is the single owner of the serve
// path's diagnostics — every failure it produces is written to errw exactly once here — with one
// deliberate exception noted at the flag-parse site, where the flag package writes its own message.
// The caller maps a non-nil return to a non-zero exit and prints nothing more.
//
// Configuration precedence falls out of order alone: configFromEnv resolves defaults and environment
// into a config first, then bindFlags registers flags whose defaults are those resolved values, so
// fs.Parse lets an explicit flag override while an absent one keeps the env-or-default. getenv is
// injected so the resolution is a pure unit test.
func runServe(ctx context.Context, args []string, getenv func(string) string, errw io.Writer) error {
	// report writes a diagnostic to errw and returns the same error, so each non-flag failure is
	// reported in exactly one place and the control flow stays a flat sequence of guarded steps.
	report := func(err error) error {
		if err != nil {
			fmt.Fprintln(errw, err)
		}
		return err
	}

	c, err := configFromEnv(getenv)
	if err != nil {
		return report(err)
	}

	fs := flag.NewFlagSet("buff serve", flag.ContinueOnError)
	fs.SetOutput(errw)
	bindFlags(fs, &c)
	// Head the flag dump with what the server does, the one required setting, and the flag/env
	// precedence — the context a bare list of flags lacks. The per-flag list PrintDefaults emits
	// already names each BUFF_* variable, so it is itself the environment-variable reference; this
	// only frames it. flag invokes this on -h and on a parse error alike.
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), serveUsage)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		// Under ContinueOnError the flag package has already written its diagnostic and the usage to
		// errw, so this returns without reporting again — reporting here would double the message. A
		// -h request arrives as ErrHelp on the same path: usage is printed, and the help request is a
		// clean exit, success.
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	// The data directory is the one configuration value with no usable default — it is the storage
	// boundary itself — so an empty one after env and flags is a hard, named error rather than a
	// silent fallback to some directory the operator did not choose.
	if c.DataDir == "" {
		return report(errors.New("buff: data directory required (set BUFF_DATA_DIR or -data-dir)"))
	}

	// One text logger to errw at Info: the server's structured lines — recovery summary, serving and
	// shutting-down, and one access line per request — alongside its Error-level 5xx causes and
	// recovered panics. stdout stays clear for a client sharing the same terminal.
	log := slog.New(slog.NewTextHandler(errw, &slog.HandlerOptions{Level: slog.LevelInfo}))
	rt, err := newRuntime(c, log)
	if err != nil {
		return report(err)
	}
	// Release the listener and data root even on a path that returns before Run — there is none
	// today, but the defer makes that a non-leak by construction rather than by audit. It is
	// idempotent with Run's own teardown, so the common path (Run ran, it already closed both) is a
	// harmless second close.
	defer rt.Close()
	return report(rt.Run(ctx))
}
