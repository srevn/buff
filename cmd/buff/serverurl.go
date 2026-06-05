package main

import "cmp"

// bakedURL is the optional fleet default a build may stamp with
//
//	-ldflags "-X main.bakedURL=https://relay.internal"
//
// It is the client-target mirror of the version stamp: empty in an ordinary build, and a single
// package-level string var because that is the only thing the linker's -X can write. A fleet stamps
// it once so a shipped binary already knows its server and needs no per-container BUFF_URL; the
// server never reads it (buff serve forks before the client path), so baking is harmless to a binary
// run as the server.
var bakedURL string

// fallbackServerURL is where the client talks when nothing is baked and BUFF_URL is unset: a server
// on the local machine's default port, the friendly default for a single-host relay. The server's
// own listen default (":8080", config.go) is a separate literal on purpose — a dial URL and a listen
// address are different things, so coupling them into one constant would be a false economy that
// breaks the moment either wants to move.
const fallbackServerURL = "http://localhost:8080"

// resolveServerURL folds the client's environment-and-build precedence into the one base URL cli.Env
// carries: the BUFF_URL value if set, else the baked default if stamped, else the built-in fallback.
// The per-invocation --server flag sits above this and is applied later by cli; this function owns
// every layer the binary resolves before the flag.
//
// cmp.Or — first non-empty wins — is what makes the empty-stamp case correct by construction rather
// than by a special-case guard: an unset SERVER_URL that still reaches the linker leaves bakedURL "",
// and an empty value must degrade to the next layer, never become an unusable empty base URL handed
// to client.New. The same rule absorbs an empty BUFF_URL. It is pure (both inputs are parameters, so
// every combination is table-tested) and called once on the client path, so unlike the version stamp
// there is nothing impure to isolate and nothing to memoize.
func resolveServerURL(envURL, baked string) string {
	return cmp.Or(envURL, baked, fallbackServerURL)
}
