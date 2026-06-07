# buff

Move bytes seamlessly across your machines.

A small, self-hosted content relay: bytes flow `producer → server → consumer` over named **clips**.
On the surface, a network clipboard; the same primitive carries file transfers and live streams.
A clip is an append-only byte log — writers append, readers can follow it while it's still being written.

---

## Install

```sh
go install github.com/srevn/buff/cmd/buff@latest
```

buff is pure Go and the standard library (no cgo), so it builds with **Go 1.26+** and cross-compiles
to any `GOOS` as a single static binary. To build a version-stamped binary from a checkout:

```sh
make dist        # → bin/buff, stamped from `git describe`
buff --version   # v0.1.0
```

`make install-client` writes the binary to `BINDIR` (default `/usr/local/bin`) — all a client needs.
`make install` (alias: `make install-server`) does the same plus lays down a systemd, launchd, or
rc.d unit for the host OS (see [Deployment](#deployment)).

For a fleet, **bake the default server into the client** so it needs no per-host `BUFF_URL`:

```sh
make install-client SERVER_URL=https://relay.internal
```

`make dist SERVER_URL=…` builds the same binary into `bin/` without installing. `BUFF_URL` and
`--server` still override the baked default; an ordinary build bakes nothing.

---

## Quickstart

The one rule: **`@name` is a slot; a bare argument is a path.** Position is free, no `@slot` means
`@default`. Direction follows the streams — a path argument or piped stdin **copies**; an interactive
terminal with no path **pastes**. Force it with `-c`/`-p` when the stream-based default doesn't fit
(cron, CI, scripts without a TTY).

**Copy (producer):**

```sh
echo hi | buff @msg     # a byte stream from stdin into @msg
buff report.pdf @doc    # a file (its basename is remembered)
buff src/ @proj         # a directory, sent as a tar archive
buff a b c @proj        # several paths, as one archive
```

**Paste (consumer):**

```sh
buff @msg               # a bytes clip at a terminal is shown; to a pipe, raw bytes (like cat)
buff @doc               # a file clip at a terminal is saved under its remembered name, not dumped
buff @doc -o .          # save under the remembered filename, into cwd
buff @doc -o out.pdf    # save to a specific path
buff @doc -o -          # force raw bytes to stdout, whatever the kind
buff @proj              # an archive at a terminal: extract into a new ./proj
buff @proj | tar t      # an archive to a pipe: raw tar bytes (like cat)
buff @proj -o dir/      # an archive: extract into dir/
```

> **The producer chose the gesture; at a terminal, buff replays it.** A bytes clip prints, a file
> clip is written under its remembered filename (else the slot), an archive extracts into `./slot`;
> a saved file keeps the source's run bit, so a copied script or binary is restored ready to run.
> The kind is provenance, not inspection — pipe a binary in as a bytes clip (`cat img | buff @x`)
> and paste at a terminal will garble. A pipe or redirect always gets raw bytes; `-o -` forces raw
> bytes even at a terminal.

**Live follow and rendezvous** — attach to a clip already being written, or park on a slot before
anything has been written to it:

```sh
# host A — still uploading a large file:
buff big.iso @x
# host B — attaches at a terminal and streams into ./big.iso as bytes arrive, before A finishes:
buff @x
# host B arrives first — parks until anyone writes to @y; Ctrl-C to detach:
buff @y
# skip whatever is in @log right now; wait for and follow the next write to it:
buff --follow-next @log
```

> A paste of an absent clip parks rather than returning a fast 404, so a consumer can rendezvous with
> a producer that hasn't arrived yet. The wait is bounded only by Ctrl-C or the client disconnecting;
> `buff -s @x` is the instant existence probe when you want "is it there?" without parking.

> **Live-follow interop:** an in-progress follow signals completeness with an HTTP trailer, which `curl`
> and many proxies/libraries silently drop — so following a still-being-written clip needs buff's own
> client. Finalized clips carry a `Content-Length` and are universally interoperable, and reading a clip
> after its upload finished is the common case.

**consume-once** — deliver a secret to exactly one reader, then destroy it:

```sh
buff --consume @secret < key.pem    # producer
buff @secret                        # the one consumer; a second read gets nothing
```

> consume-once is **at-most-once delivery, not confidentiality** — on an untrusted network an attacker
> can race the intended reader for it (see [Trust](#trust-and-security-model)). Add `--keep` to hold the
> secret until it is claimed instead of letting it expire with the default TTL. If the consumer's cwd
> already has the colliding name, buff lands the delivery on a free sibling (`./secret.<gen>` for a file,
> `./slot-<gen>/` for an archive) rather than clobber it or refuse the irreversibly spent delivery.

**Conditional copy** — replace a clip only if it has not moved since you last looked:

```sh
buff -s @config                                            # note its generation: 019477d6c5e1a4b7…
buff --if-match 019477d6c5e1a4b7… new.yaml @config         # refuses unless that gen is still current
buff --if-match '*' new.yaml @config                       # accepts any present clip; refuses if absent
```

> `--if-match` is compare-and-swap on the writer side: a stale token (or an absent clip, without `*`)
> exits **6** (conflict / busy) and the clip is left alone — never silently overwritten. The flag
> gates on the server's `conditional-write` capability; against an older server that would ignore
> the precondition and replace unconditionally, the client refuses to send the write at all.

**Manage:**

```sh
buff -l            # list finalized clips
buff -s @work      # show a clip's metadata (does not consume)
buff -d @work      # delete a clip
buff --version     # client version
buff -h            # full help
```

---

## Recipes

**Anonymous clipboard — `@default`.** No slot name needed for a quick round-trip:

```sh
echo hi | buff           # copies into @default
buff                     # pastes from @default
```

**Bridge to / from the system clipboard.**

```sh
pbpaste | buff @c        # push the macOS clipboard to buff (xclip -o on Linux)
buff @c | pbcopy         # pull a buff clip back into the system clipboard
```

**Live build or log streaming.** Producer relays bytes as they appear; consumer tails to completion.

```sh
# host A — relay a long build's output as it runs:
make 2>&1 | buff @build
# host B — tail it live; Ctrl-C detaches without stopping the producer:
buff @build
```

**`curl` interop for finalized clips.** Finalized clips carry `Content-Length`, so any HTTP client can
read them — a consumer doesn't have to install buff. The wire is `/v1/clips/{name}`; PUTs default to
`Buff-Kind: bytes`, and `Buff-Filename` preserves the file gesture for paste-side replay:

```sh
curl -fsSL http://buff.lan:8080/v1/clips/note         # GET a finalized clip
curl -fT report.pdf -H "Buff-Kind: file" -H "Buff-Filename: report.pdf" \
  http://buff.lan:8080/v1/clips/doc                   # PUT as a file clip
```

A live (still-being-written) clip signals completeness with an HTTP trailer that `curl` and many
proxies silently drop — for those, use buff's own client.

**Inspect a clip without consuming it.** `buff -s @slot` prints the metadata as a key-value block:

```
$ buff -s @report
name:        report
generation:  019477d6c5e1a4b7f3d2e891c5a0b6e4
kind:        file
filename:    report.pdf
size:        2.4MiB
finalized:   true
consume:     false
expires:     in 13h
```

---

## The `@` grammar

`@` is not a valid name character, so it can never collide with content — strip it, validate the rest
(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`, case-sensitive ASCII). The grammar is **syntactic only** — it
never probes the filesystem to guess your intent:

| Syntax | Parses as |
|---|---|
| `buff @work file` ≡ `buff file @work` | slot `@work`, path `file` — position is free |
| `buff @work` | slot `@work`, no path |
| `buff work` | path `work` — a bare word is always a path, so a typo'd slot like `buff wrok` fails cleanly instead of silently mis-pasting |
| `buff` | no args — slot defaults to `@default` |
| `buff @a @b` | usage error — exactly one `@arg` |

Two escapes are the only edge the sigil imposes, both the ordinary "leading `./` means path":

- `buff serve` runs the server — `serve` is the single reserved first token. A file named `serve` is
  copied as `./serve`.
- A file whose name starts with `@` is referenced as `./@foo`.

There is no `-f` and no `-n`: files are bare words, slots are `@name`.

---

## Exit codes

stdout carries **data only**; diagnostics and warnings go to stderr. The client fails loudly on a
truncated read rather than presenting partial data as complete.

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | usage / generic error |
| 3 | not found |
| 4 | consumed / gone |
| 5 | too large / no space |
| 6 | conflict / busy |
| 7 | truncated / incomplete stream |
| 8 | network / connection error |

(An operation interrupted by a signal exits **130**, the conventional `128 + SIGINT`.)

---

## Running the server

```sh
BUFF_DATA_DIR=/var/lib/buff buff serve
```

`BUFF_DATA_DIR` is **required** — it is the storage root (an `os.Root` boundary; nothing escapes it).
Everything else has a default. Every variable below has a matching flag (`-data-dir`, `-addr`,
`-max-clip`, …); **flags override environment variables override defaults**. Run `buff serve -h` for
the flag list (each flag names its variable).

| Variable | Default | Meaning |
|---|---|---|
| `BUFF_DATA_DIR` | *(required)* | storage root for all clips |
| `BUFF_ADDR` | `:8080` | listen address |
| `BUFF_MAX_CLIP` | `1GiB` | per-clip byte cap (`0` = unlimited) |
| `BUFF_MAX_TOTAL` | `10GiB` | total byte cap across all clips (`0` = unlimited) |
| `BUFF_MAX_CLIPS` | `10000` | max live+finalized clip count (`0` = unlimited) |
| `BUFF_TTL` | `24h` | default retention measured from finalize (`0` = keep forever) |
| `BUFF_REAP_INTERVAL` | `60s` | retention reaper tick (`0` = no background reaping) |
| `BUFF_UPLOAD_IDLE` | `30s` | per-request idle deadline (`>0`; cannot be disabled) |
| `BUFF_UPLOAD_MAX` | `0` (off) | absolute cap on one upload's duration (`0` = off) |
| `BUFF_FSYNC` | `on` | durable commit: data + meta + directory fsync (`off` = atomic but not flushed) |
| `BUFF_CHECKSUM` | `off` | store and verify a CRC32C in the durable record |

- **Byte caps** accept a bare byte count or a binary unit: `1G`, `1Gi`, `1GB`, and `1GiB` all mean one
  gibibyte (every unit is `1024`-based).
- **Booleans** accept `on`/`off`, `true`/`false`, `1`/`0`, `yes`/`no`.
- **`BUFF_FSYNC=off`** trades durability for speed: writes stay atomic but unflushed, so a power loss
  may lose recently finalized clips. Fine for clipboard use; leave it `on` for transfers you rely on.

The **client** resolves its server in precedence order: `--server <url>` (per-invocation; long-only,
as `-s` is `--stat`), then `BUFF_URL`, then a default baked in at build time (see [Install](#install)),
then `http://localhost:8080`.

---

## Deployment

The one-step path is **`make install`**: from a checkout it builds the binary, installs it to `BINDIR`
(default `/usr/local/bin`), and lays down the service definition for the detected OS — a systemd unit,
a launchd agent, or an rc.d script — with the binary path and service user substituted in. The
templates live under [`etc/`](etc/); `PREFIX`, `BINDIR`, `DESTDIR`, and `BUFF_USER` relocate the install.

On Linux and FreeBSD the service **runs as the user who ran the install** — resolved through
`SUDO_USER`, so `sudo make install` picks you, not root. It creates no system account; for a dedicated
service user, create one and pass `BUFF_USER=buff`. On macOS the agent simply runs as you, no `sudo`.

### systemd (Linux)

[`etc/systemd/buff.service`](etc/systemd/buff.service) ties `BUFF_DATA_DIR` to a systemd
`StateDirectory` (auto-created and owned by the service user) and maps `systemctl stop` onto buff's
graceful drain (`SIGTERM`, then a stop timeout). Extra `BUFF_*` settings go in `/etc/buff/buff.env`
(read via `EnvironmentFile`).

```sh
sudo make install                          # binary + unit + /etc/buff/buff.env
systemctl daemon-reload && systemctl enable --now buff
```

### launchd (macOS)

[`etc/launchd/io.buff.plist`](etc/launchd/io.buff.plist) is a per-user `LaunchAgent`: it loads at your
login and runs as you — no `sudo`, no service account. It sets `BUFF_DATA_DIR` inline (launchd has no
`EnvironmentFile`) and logs to `~/Library/Logs/buff.log`; `make install` creates the data directory at
`~/.local/share/buff`. Install **without** `sudo` (set `PREFIX` to a writable path if `/usr/local` is
not), then bootstrap it into your GUI domain:

```sh
make install                               # binary + ~/Library/LaunchAgents/io.buff.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/io.buff.plist
```

### rc.d (FreeBSD)

[`etc/freebsd/buff`](etc/freebsd/buff) integrates with `service(8)` via `daemon(8)`. It requires
`buff_data_dir` (failing loudly if unset), creates it owned by the service user, maps `service buff
stop` and shutdown onto the graceful drain, and logs to syslog and `/var/log/buff.log`. `buff_data_dir`
and `buff_addr` are set in `rc.conf`; every other `BUFF_*` knob goes in `/usr/local/etc/buff/buff.env`,
which the script loads into the daemon's environment via rc.subr's `${name}_env_file`.

```sh
sudo make install                          # binary + rc.d script + /usr/local/etc/buff/buff.env
sysrc buff_enable=YES buff_data_dir=/var/db/buff
service buff start
```

### Readiness and health

`GET /health` is unversioned and stable, for liveness/readiness probes:

```json
{ "status": "ok", "version": "buff/v0.1.0", "api": ["v1"],
  "features": ["follow", "consume-once", "wait", "conditional-write", "follow-next"] }
```

`features` is a forward-compatibility seam — a client can check it before relying on an optional
capability.

---

## Trust and security model

buff v1 has **no authentication, no authorization, and no transport or at-rest encryption.** This is
a deliberate, scoped decision — *trust is the deployment* — with hard consequences:

- **Anyone who can reach `BUFF_ADDR` can read, write, overwrite, list, and delete any clip.**
    `buff -l` enumerates every clip name. Slot names are **not secrets** and **are logged**, so never
    put a secret in a slot name.
- **consume-once is a *delivery* guarantee, not a *confidentiality* one.** It delivers a clip's bytes
    to at most one reader, but it does not protect them from an eavesdropper, a faster racing reader on
    the same network, or inspection on disk (clips are plaintext until consumed or expired).
- **DoS is bounded, not prevented.** Size, total-byte, and clip-count caps plus idle deadlines stop a
    single client from exhausting the host, but an unauthenticated peer within those bounds can still
    fill the quota or churn clips.

**Therefore: run buff on a trusted LAN, over an SSH tunnel, or behind a TLS-terminating,
authenticating reverse proxy**. Confidentiality, if you need it, comes
from that layer or from end-to-end encryption the producer and consumer apply themselves.
