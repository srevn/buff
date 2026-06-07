package cli

import (
	"fmt"
	"io"
)

// usageText is the client's -h/--help screen: the one grammar rule, copy/paste examples drawn from
// the command matrix, the management and write options, the configuration variable, and the exit
// codes a script reads. It is reference, not a tutorial — the tutorial is the README. Every flag,
// example, and exit code here mirrors the real surface (the flag table and the exit map in this
// package); keeping it in step by hand is the cost of a help screen that does not lie.
const usageText = `buff — content relay client: copy bytes into a named slot, paste them back out.

Usage:
  buff [options] [@slot] [path...]   copy (producer) or paste (consumer)
  buff serve [options]               run the server (see: buff serve -h)

The one rule: @name is a slot; a bare argument is a path. Position is free, so
"buff @work file" and "buff file @work" are the same. No @slot means @default.
Mode follows the streams (override with -c/-p): a path argument or piped stdin
copies; an interactive terminal with no path pastes.

Copy (producer):
  echo hi | buff @msg                a byte stream from stdin into @msg
  buff report.pdf @doc               a file (its basename is remembered)
  buff src/ @proj                    a directory, as an archive
  buff a b c @proj                   several paths, as one archive
  buff --consume @secret < key.pem   deliver to at most one reader, then gone

Paste (consumer):
  buff @msg                          a bytes clip: shown at a terminal, raw bytes to a pipe
  buff @doc                          a file clip at a terminal: saved under its remembered name
  buff @doc -o .                     save under the remembered filename, into cwd
  buff @doc -o out.pdf               save to a specific path
  buff @proj                         an archive at a terminal: extract into a new dir
  buff @proj | tar t                 an archive to a pipe: raw tar bytes
  buff @proj -o dir/                 an archive: extract into dir/

Live follow — read a clip while it is still being written:
  host A:  buff big.iso @x           (still uploading)
  host B:  buff @x                   (attaches to the write in progress, saves bytes as they arrive)
  host B:  buff --follow-next @x     (skip the value there now; wait for and follow the next write)

Options:
  -c, --copy            force copy (scripts where stdin is not a pipe)
  -p, --paste           force paste (scripts where stdout is not a terminal)
  -o, --output <path>   paste destination (a path, a dir for an archive, or - for stdout)
      --follow-next     paste: skip the value there now; wait for and follow the next write
      --ttl <dur>       copy: retention, e.g. 24h or 30m (0 = server default)
      --keep            copy: never expire (overrides --ttl)
      --consume         copy: deliver to at most one reader, then destroy
      --if-match <gen>  copy: replace only if the current generation matches (from buff -s; * = any)
      --server <url>    override BUFF_URL for this invocation

Management:
  -l, --list            list finalized clips
  -d, --delete @slot    delete a clip
  -s, --stat @slot      show a clip's metadata
      --version         print the client version
  -h, --help            print this help

Configuration:
  BUFF_URL              server to talk to (currently %s)

Exit codes:
  0  success                    5  too large / no space
  1  usage / generic error      6  conflict / busy
  3  not found                  7  truncated / incomplete stream
  4  consumed / gone            8  network / connection error
                              130  interrupted (signal)
`

// writeUsage prints the client usage screen to w, rendering serverURL into the configuration block
// so the help tells the truth about where this binary points — the BUFF_URL value, a baked-in
// default, or the built-in fallback, whichever is in effect — rather than a fixed string that would
// lie on a baked binary or under a set BUFF_URL. serverURL is the environment-and-build-resolved
// default, before any --server flag: help describes the binary's configured target, not a one-shot
// override (parse short-circuits help before the flag applies). Like --version it answers offline,
// so the caller routes it to stdout and a clean exit without building a client.
func writeUsage(w io.Writer, serverURL string) {
	fmt.Fprintf(w, usageText, serverURL)
}
