// Package cli is buff's command-line grammar and the flows that execute it. It is the
// first package that composes the others: it reads the @-grammar and the stream types into
// a resolved command, drives that command over the wire client and the tar archiver behind
// a Source/Sink seam, and maps the typed errors that come back to process exit codes.
//
// The grammar is deterministic and never probes the filesystem to classify an argument: a
// slot is @name, a bare argument is a path, a leading '-' is a flag, and the copy-vs-paste
// direction comes from whether stdin is a terminal. Classification depends only on the
// invocation's syntax and stream types, never on what files happen to exist — so the same
// command means the same thing in any directory.
//
// Execution returns a process exit code rather than calling os.Exit, so the whole package
// is testable without a terminal or a subprocess: the binary's main injects the streams and
// their TTY-ness and turns the returned code into the actual exit.
package cli
