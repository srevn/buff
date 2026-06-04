# buff

Move text, files, and directories between machines through one small self-hosted server — a network
clipboard that also handles file transfer and live streaming. All rests on one object — a **clip**,
a named blob you write and read back; a live stream is just a clip you read while it is still being written.

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

`make install-client` installs just the stamped binary to `BINDIR` (default `/usr/local/bin`) — all a
client needs. To run buff as a service, use `make install-server` (≡ `make install`): the binary plus a
systemd, launchd, or rc.d definition for the host OS (see [Deployment](#deployment)).

---

## Quickstart

The one rule: **`@name` is a slot; a bare argument is a path.** Position is free, no `@slot` means
`@default`. Direction follows the streams — a path argument or piped stdin **copies**; an interactive
terminal with no path **pastes**. Force it with `-c`/`-p` where TTY detection is unreliable (cron).

**Copy (producer):**

```sh
echo hi | buff @msg                 # text from stdin into @msg
buff report.pdf @doc                # a file (its basename is remembered)
buff src/ @proj                     # a directory, sent as a tar archive
buff a b c @proj                    # several paths, as one archive
```

**Paste (consumer):**

```sh
buff @msg                           # text at a terminal is shown; to a pipe, raw bytes (like cat)
buff @photo                         # binary at a terminal is saved to ./photo, not dumped as garbage
buff @doc -o .                      # save under the remembered filename, into cwd
buff @doc -o out.pdf                # save to a specific path
buff @doc -o -                      # force raw bytes to stdout, whatever the content
buff @proj                          # an archive at a terminal: extract into a new ./proj
buff @proj | tar t                  # an archive to a pipe: raw tar bytes (like cat)
buff @proj -o dir/                  # an archive: extract into dir/
```

> **At a terminal, buff shows what you can read and saves what you can't** — text prints, a binary
> clip is written to a file (named for its remembered filename, else the slot), and an archive
> extracts into `./slot`. A pipe or redirect always receives the raw bytes unchanged, and `-o -`
> forces raw bytes even at a terminal.

**Live follow** — read a clip while it is still being written:

```sh
# host A — still uploading a large file:
buff < big.iso @x
# host B — attaches and follows it to completion, before A finishes:
buff @x > out.iso
```

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
> secret until it is claimed instead of letting it expire with the default TTL.

**Manage:**

```sh
buff -l            # list finalized clips
buff -s @work      # show a clip's metadata (does not consume)
buff -d @work      # delete a clip
buff --version     # client version
buff -h            # full help
```

---

## The `@` grammar

`@` is not a valid name character, so it can never collide with content — strip it, validate the rest
(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`, case-sensitive ASCII). The grammar is **syntactic only** — it
never probes the filesystem to guess your intent:

| You write | It means |
|---|---|
| `buff @work file` ≡ `buff file @work` | position is free; `@work` is the slot, `file` the source |
| `buff work` | copy the **file** named `work` — a typo'd slot like `buff wrok` fails cleanly instead of silently mis-pasting |
| `buff @work` | paste the **slot** `work` |
| (no `@arg`) | the slot `default` |
| (two `@args`) | a usage error |

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
| `BUFF_UPLOAD_IDLE` | `30s` | per-request idle deadline (`0` = off) |
| `BUFF_UPLOAD_MAX` | `0` (off) | absolute cap on one upload's duration (`0` = off) |
| `BUFF_FSYNC` | `on` | durable commit: data + meta + directory fsync (`off` = atomic but not flushed) |
| `BUFF_CHECKSUM` | `off` | store and verify a CRC32C in the durable record |

- **Byte caps** accept a bare byte count or a binary unit: `1G`, `1Gi`, `1GB`, and `1GiB` all mean one
  gibibyte (every unit is `1024`-based).
- **Booleans** accept `on`/`off`, `true`/`false`, `1`/`0`, `yes`/`no`.
- **`BUFF_FSYNC=off`** trades durability for speed: writes stay atomic but unflushed, so a power loss
  may lose recently finalized clips. Fine for clipboard use; leave it `on` for transfers you rely on.

The **client** reads `BUFF_URL` (default `http://localhost:8080`), overridable per-invocation with
`--server <url>` (long-only — `-s` is `--stat`).

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
stop` and shutdown onto the graceful drain, and logs to syslog and `/var/log/buff.log`.

```sh
sudo make install                          # binary + rc.d script
sysrc buff_enable=YES buff_data_dir=/var/db/buff
service buff start
```

### Readiness and health

`GET /health` is unversioned and stable, for liveness/readiness probes:

```json
{ "status": "ok", "version": "buff/v0.1.0", "api": ["v1"], "features": ["follow", "consume-once"] }
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
