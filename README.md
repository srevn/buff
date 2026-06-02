# buff

A self-hosted **content relay**: bytes flow `producer → server → consumer` over a flat namespace of
named clips. One Go binary — `buff serve` is the server, bare `buff …` is the client.

> **A live stream is just reading a clip that is still being written.**

That one idea is the whole design. "Clipboard," "file transfer," and "live stream" are not three
features — they are three skins over one primitive: a named, append-only clip a reader can follow.

---

## Trust and security model — read this first

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
authenticating reverse proxy** (see [Deployment](#deployment)). Confidentiality, if you need it, comes
from that layer or from end-to-end encryption the producer and consumer apply themselves.

---

## Install

```sh
go install github.com/srevn/buff/cmd/buff@v0.1.0   # or @latest
```

buff is pure Go and the standard library (no cgo), so it builds with **Go 1.26+** and cross-compiles
to any `GOOS` as a single static binary. To build a version-stamped binary from a checkout:

```sh
make dist        # → bin/buff, stamped from `git describe`
buff --version   # v0.1.0
```

A `go install`-ed binary self-identifies too: it reports the module version it was installed at
(`@v0.1.0` → `v0.1.0`), falling back to the embedded VCS revision for a plain `go build`.

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
buff @msg                           # bytes to stdout
buff @doc -o .                      # save under the remembered filename, into cwd
buff @doc -o out.pdf                # save to a specific path
buff @proj                          # an archive at a terminal: extract into a new ./proj
buff @proj | tar t                  # an archive to a pipe: raw tar bytes (like cat)
buff @proj -o dir/                  # an archive: extract into dir/
```

**Live follow** — read a clip while it is still being written:

```sh
# host A — still uploading a large file:
buff < big.iso @x
# host B — attaches and follows it to completion, before A finishes:
buff @x > out.iso
```

> **Interop caveat for live follow.** A *finalized* clip is sent with a `Content-Length` and is
> universally interoperable — `curl`, proxies, any HTTP client can detect truncation. A *concurrent
> live follow* signals completeness with an HTTP **trailer**, which `curl`, many proxies, and many
> HTTP libraries silently drop. Following a still-being-written clip therefore needs a trailer-aware
> client (buff itself); a third-party tool should read finalized clips or accept that it cannot tell a
> complete follow from a truncated one. The common case (read after the upload finished) is unaffected.

**consume-once** — deliver a secret to exactly one reader, then destroy it:

```sh
buff --consume @secret < key.pem    # producer
buff @secret                        # the one consumer; a second read gets nothing
```

> consume-once is an **at-most-once delivery** guarantee, **not confidentiality** (see
> [Trust](#-trust-and-security-model--read-this-first)): on an untrusted network an attacker can list
> the clip and race to consume it. Use it on a trusted network, and add `--keep` if the secret must
> survive until claimed rather than expiring with the default TTL.

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

### systemd (Linux)

[`contrib/buff.service`](contrib/buff.service) is a hardened unit. It ties `BUFF_DATA_DIR` to a
systemd `StateDirectory` (auto-created and owned), maps `systemctl stop` onto buff's graceful drain,
and sandboxes the process (no new privileges, read-only system, restricted syscalls/address families).

```sh
useradd --system --no-create-home --shell /usr/sbin/nologin buff
install -m 0755 bin/buff /usr/local/bin/buff
install -m 0644 contrib/buff.service /etc/systemd/system/buff.service
systemctl daemon-reload && systemctl enable --now buff
```

Add extra `BUFF_*` settings in `/etc/buff/buff.env` (read via `EnvironmentFile`).

### rc.d (FreeBSD)

[`contrib/rc.d/buff`](contrib/rc.d/buff) integrates with `service(8)` via `daemon(8)`. It requires
`buff_data_dir` (failing loudly if unset), creates it owned by an unprivileged user, and maps
`service buff stop` and system shutdown onto the graceful drain.

```sh
pw useradd buff -d /nonexistent -s /usr/sbin/nologin -c "buff content relay"
install -m 0755 bin/buff /usr/local/bin/buff
install -m 0555 contrib/rc.d/buff /usr/local/etc/rc.d/buff
sysrc buff_enable=YES buff_data_dir=/var/db/buff
service buff start
```

### Readiness and health

`GET /healthz` is unversioned and stable, for liveness/readiness probes:

```json
{ "status": "ok", "version": "buff/v0.1.0", "api": ["v1"], "features": ["follow", "consume-once"] }
```

`features` is a forward-compatibility seam — a client can check it before relying on an optional
capability.

### Graceful shutdown

On `SIGINT`/`SIGTERM` the server stops accepting, then drains within a bounded window (~15s): in-flight
finalized reads and consume deliveries complete; **live (unfinalized) uploads are aborted** (identical
to crash recovery — they have no durable record); followers are cancelled. If the window elapses with
work still active, the remaining connections are force-closed (logged as a warning). A **second signal**
during the drain force-quits immediately with exit **130** — the conventional "press Ctrl-C again."

---

## Platform support

- **Server:** any POSIX system — **Linux** and **macOS** are the primary, CI-gated targets. **FreeBSD**
  is a supported community tier: the storage core's assumptions (unlink-while-open inode pinning, a
  correct `fsync`) hold there and it ships an `rc.d` script, and the tree is **built for FreeBSD in CI**
  — but no CI runner exercises it at runtime, so it is built-not-runtime-gated.
- **Client:** works anywhere Go builds, Windows included. A Windows *server* is a future backing.

---

## Limitations and roadmap

These are **deliberately deferred behind named seams**, not gaps — each is additive later without a core
rewrite:

- **Not in v1:** authentication, authorization, TLS, at-rest encryption, multi-user isolation, resumable
  uploads, compression, websockets/SSE, hierarchical names, a config file, clipboard hardware bridges
  (OSC52/pbcopy/wl-copy).
- **Reserved seams:** resumable uploads, conditional writes (`If-Match`) and `--force`, `--follow-live`
  (follow a live *replacement*), `--wait` (block for a not-yet-existing clip), List pagination, and
  hierarchical/Unicode names. The `@team/work` and `@host:work` syntaxes are **reserved, not yet valid**
  names.

---

## Development

The verification gate is one command (it is what CI runs):

```sh
make check        # gofmt · build · vet · test · -race · staticcheck · govulncheck
make fuzz-smoke   # time-boxed fuzz of the validator surfaces
```

The cross-session working agreement, architecture, and decision log live in
[`CLAUDE.md`](CLAUDE.md), [`buff-spec.md`](buff-spec.md), and [`DECISIONS.md`](DECISIONS.md).

---

