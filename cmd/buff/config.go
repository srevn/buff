package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/srevn/buff/api"
	"github.com/srevn/buff/store"
	"github.com/srevn/buff/units"
)

// Server defaults. Each is the value an operator gets with the matching variable unset; they are
// named constants, not literals buried in the parse, so the precedence table and the unit test can
// both refer to the one definition. The byte caps are binary (a gibibyte is 1<<30), the durations
// are real spans, and the two booleans default to the durable-but-checksumless choice a content
// relay wants. A zero default means the policy is off, not unset: an unlimited cap, no background
// reaping, no absolute upload cap — read each comment on config for what its own zero means. The
// idle bound is the exception, defaulting to a real span: it is a standing safety bound, not off.
const (
	defaultAddr         = ":8080"
	defaultMaxClip      = 1 << 30  // 1 GiB per-clip cap
	defaultMaxTotal     = 10 << 30 // 10 GiB total cap
	defaultMaxClips     = 10000
	defaultTTL          = 24 * time.Hour
	defaultReapInterval = 60 * time.Second
	defaultUploadIdle   = 30 * time.Second
	defaultUploadMax    = time.Duration(0) // off: no absolute cap on one upload's duration
	defaultWaitMax      = time.Duration(0) // off: no absolute cap on a waiting GET's park
	defaultFsync        = true
	defaultChecksum     = false
)

// config is the fully-resolved server configuration: the env-and-flag precedence has already been
// applied, so every field holds the value the runtime will use. It is medium-agnostic — it knows
// nothing of os.Root, http.Server, or the store's internals — and projects into the three lower-
// layer option structs through the methods below, so the wiring never reaches past this boundary
// into a constructor's own knobs.
//
// Per-field zero meanings are inherited deliberately from the layer each maps into, never invented
// here: the three caps disable at zero because the quota reads zero as unlimited; TTL zero is "no
// default expiry" (retain forever by default), distinct from the request header's "use the server
// default"; the absolute upload cap and the reaper interval disable at zero because their consumers
// treat zero as off. UploadIdle is the deliberate exception — a standing stall bound that must be
// positive, refused at boot rather than disabled (see validate). Documenting the meaning beside the
// field is what keeps a future reader from "fixing" a zero into a footgun.
type config struct {
	DataDir      string        // BUFF_DATA_DIR: os.Root boundary for all storage; required (no default)
	Addr         string        // BUFF_ADDR: listen address
	MaxClip      int64         // BUFF_MAX_CLIP: per-clip byte cap; 0 = unlimited
	MaxTotal     int64         // BUFF_MAX_TOTAL: total byte cap; 0 = unlimited
	MaxClips     int           // BUFF_MAX_CLIPS: live+finalized count cap; 0 = unlimited
	TTL          time.Duration // BUFF_TTL: default retention from finalize; 0 = no default expiry
	ReapInterval time.Duration // BUFF_REAP_INTERVAL: retention reaper tick; 0 = no background reaping
	UploadIdle   time.Duration // BUFF_UPLOAD_IDLE: per-request idle deadline; must be >0, a standing stall bound (validate refuses a non-positive value at boot)
	UploadMax    time.Duration // BUFF_UPLOAD_MAX: absolute cap on one upload's duration; 0 = off
	WaitMax      time.Duration // BUFF_WAIT_MAX: absolute cap on a waiting GET's park before a 404; 0 = off
	Fsync        bool          // BUFF_FSYNC: durable commit (data+meta+dir fsync); off = atomic-but-not-flushed
	Checksum     bool          // BUFF_CHECKSUM: store and verify a CRC32C in the durable record
}

// storeConfig projects the policy the store quota and default retention need. The caps and the
// default TTL cross the boundary; the disk-only knobs (fsync, checksum) do not — they belong to the
// medium, not to medium-agnostic Config.
func (c config) storeConfig() store.Config {
	return store.Config{
		MaxClip:    c.MaxClip,
		MaxTotal:   c.MaxTotal,
		MaxClips:   c.MaxClips,
		DefaultTTL: c.TTL,
	}
}

// diskOpts projects the disk medium's knobs and hands it the logger recovery reports through. These
// are construction arguments of the one constructor that has a disk, kept off Config so the policy
// struct stays medium-agnostic.
func (c config) diskOpts(log *slog.Logger) store.DiskOpts {
	return store.DiskOpts{
		Fsync:    c.Fsync,
		Checksum: c.Checksum,
		Logger:   log,
	}
}

// apiOptions projects the HTTP edge's options. AccessLog is forced on: the server, unlike a test
// or an embedding, always wants one structured line per request, emitted from the same logger as
// its errors. The streaming bounds carry through — UploadIdle a standing stall bound (boot has
// already guaranteed it positive, so the api's own default never has to fire here), UploadMax the
// one upload opt-out, and WaitMax the waiting-GET park's duration cap; the safety timeouts are left
// zero so the api constructor substitutes its own hardened defaults. Version is the resolved build
// version dressed in the health "buff/" form, distinct from the bare string --version prints.
func (c config) apiOptions(log *slog.Logger) api.Options {
	return api.Options{
		UploadIdle: c.UploadIdle,
		UploadMax:  c.UploadMax,
		WaitMax:    c.WaitMax,
		Logger:     log,
		Version:    "buff/" + buildVersion(),
		AccessLog:  true,
	}
}

// validate rejects a resolved config the runtime cannot honour, after env and flags have both been
// applied. It is the post-parse semantic gate — distinct from the per-field grammar the parsers
// enforce — and a method so it is unit-tested as a pure function, with no flag machinery. Two
// values have no usable resolution.
//
// The data directory is the storage boundary itself, with no sensible default, so an empty one is a
// hard error rather than a silent fallback to some directory the operator did not choose.
//
// UploadIdle is a standing stall bound, not an opt-out: a non-positive value would unbound a
// connected-but-stalled peer on every streaming path — the slowloris the bound exists to stop — so
// it is refused loudly and the operator is pointed at the knob that does mean "no cap". The shared
// grammar already rejects a negative, so in practice this catches the explicit zero, keeping "no
// stall protection" something the configuration can never quietly express; <= 0 rather than == 0 so
// the gate holds whatever value reaches it.
func (c config) validate() error {
	if c.DataDir == "" {
		return errors.New("buff: data directory required (set BUFF_DATA_DIR or -data-dir)")
	}
	if c.UploadIdle <= 0 {
		return errors.New("buff: BUFF_UPLOAD_IDLE (or -upload-idle) must be positive; it is a standing " +
			"stall bound and cannot be disabled — use BUFF_UPLOAD_MAX for no absolute duration cap")
	}
	return nil
}

// configFromEnv resolves the defaults and any set environment variables into a config, before flags
// are bound. getenv is injected rather than reaching for os.Getenv so the whole precedence is a
// pure unit test. The struct literal reads as the precedence table itself — each field names its
// variable and its default in one line — and the first malformed variable surfaces as the returned
// error, the config discarded with it.
func configFromEnv(getenv func(string) string) (config, error) {
	e := envReader{getenv: getenv}
	c := config{
		DataDir:      e.str("BUFF_DATA_DIR", ""),
		Addr:         e.str("BUFF_ADDR", defaultAddr),
		MaxClip:      e.size("BUFF_MAX_CLIP", defaultMaxClip),
		MaxTotal:     e.size("BUFF_MAX_TOTAL", defaultMaxTotal),
		MaxClips:     e.count("BUFF_MAX_CLIPS", defaultMaxClips),
		TTL:          e.dur("BUFF_TTL", defaultTTL),
		ReapInterval: e.dur("BUFF_REAP_INTERVAL", defaultReapInterval),
		UploadIdle:   e.dur("BUFF_UPLOAD_IDLE", defaultUploadIdle),
		UploadMax:    e.dur("BUFF_UPLOAD_MAX", defaultUploadMax),
		WaitMax:      e.dur("BUFF_WAIT_MAX", defaultWaitMax),
		Fsync:        e.boolean("BUFF_FSYNC", defaultFsync),
		Checksum:     e.boolean("BUFF_CHECKSUM", defaultChecksum),
	}
	return c, e.err
}

// bindFlags registers a flag for every knob, each defaulting to the env-resolved value already in c
// and writing back into the same field. This is what makes precedence fall out of stdlib flag with
// no merge step: an explicitly-passed flag overrides the env-resolved default, an absent one leaves
// it untouched, so flags-over-env-over-defaults holds by construction. Every typed knob is bound
// through a flag.Value wrapper below, so a flag and its variable share one grammar — and the same
// non-negative validation: -max-clip 2GiB, -ttl 24h, -max-clips 9, and -fsync=off parse exactly
// as BUFF_MAX_CLIP, BUFF_TTL, BUFF_MAX_CLIPS, and BUFF_FSYNC do, where a stdlib DurationVar/IntVar
// would instead accept a negative the env path rejects.
func bindFlags(fs *flag.FlagSet, c *config) {
	fs.StringVar(&c.DataDir, "data-dir", c.DataDir, "storage root, required (BUFF_DATA_DIR)")
	fs.StringVar(&c.Addr, "addr", c.Addr, "listen address (BUFF_ADDR)")
	fs.Var(sizeFlag{&c.MaxClip}, "max-clip", "per-clip byte cap, 0=unlimited (BUFF_MAX_CLIP)")
	fs.Var(sizeFlag{&c.MaxTotal}, "max-total", "total byte cap, 0=unlimited (BUFF_MAX_TOTAL)")
	fs.Var(countFlag{&c.MaxClips}, "max-clips", "clip-count cap, 0=unlimited (BUFF_MAX_CLIPS)")
	fs.Var(durFlag{&c.TTL}, "ttl", "default retention, 0=none (BUFF_TTL)")
	fs.Var(durFlag{&c.ReapInterval}, "reap-interval", "reaper tick, 0=off (BUFF_REAP_INTERVAL)")
	fs.Var(durFlag{&c.UploadIdle}, "upload-idle", "per-request idle deadline, must be >0 (BUFF_UPLOAD_IDLE)")
	fs.Var(durFlag{&c.UploadMax}, "upload-max", "max upload duration, 0=off (BUFF_UPLOAD_MAX)")
	fs.Var(durFlag{&c.WaitMax}, "wait-max", "max wait-GET park duration, 0=off (BUFF_WAIT_MAX)")
	fs.Var(boolFlag{&c.Fsync}, "fsync", "durable commit on/off (BUFF_FSYNC)")
	fs.Var(boolFlag{&c.Checksum}, "checksum", "store and verify CRC32C (BUFF_CHECKSUM)")
}

// envReader resolves one environment variable per call, defaulting an unset one and recording the
// first malformed one. It carries its own error so configFromEnv stays a single struct literal
// rather than a stutter of parse-and-check blocks: once err is set every later read returns its
// default untouched, and the half-built config is discarded by the caller. A getenv that returns ""
// for an unset variable means an explicitly-empty value is treated as unset — the right call for a
// shell where exporting an empty string is indistinguishable from not exporting it.
type envReader struct {
	getenv func(string) string
	err    error
}

// str resolves a string variable. It cannot fail, so it never touches err.
func (e *envReader) str(key, def string) string {
	if v := e.getenv(key); v != "" {
		return v
	}
	return def
}

// size resolves a byte-cap variable through the shared size grammar.
func (e *envReader) size(key string, def int64) int64 {
	v, ok := e.lookup(key)
	if !ok {
		return def
	}
	n, err := units.ParseSize(v)
	return checkedValue(e, key, v, def, n, err)
}

// count resolves a non-negative integer variable.
func (e *envReader) count(key string, def int) int {
	v, ok := e.lookup(key)
	if !ok {
		return def
	}
	n, err := parseInt(v)
	return checkedValue(e, key, v, def, n, err)
}

// dur resolves a non-negative duration variable.
func (e *envReader) dur(key string, def time.Duration) time.Duration {
	v, ok := e.lookup(key)
	if !ok {
		return def
	}
	d, err := parseDuration(v)
	return checkedValue(e, key, v, def, d, err)
}

// boolean resolves an on/off variable through the shared bool grammar.
func (e *envReader) boolean(key string, def bool) bool {
	v, ok := e.lookup(key)
	if !ok {
		return def
	}
	b, err := parseBool(v)
	return checkedValue(e, key, v, def, b, err)
}

// lookup returns the variable's value and whether it should be parsed: not while an earlier error
// stands, and not when it is unset.
func (e *envReader) lookup(key string) (string, bool) {
	if e.err != nil {
		return "", false
	}
	v := e.getenv(key)
	if v == "" {
		return "", false
	}
	return v, true
}

// checkedValue records the first parse failure on the reader, naming the variable and its offending
// value, and returns the default in its place; otherwise it returns the parsed value. It is a free
// generic function rather than a method because Go has no generic methods, yet one helper over the
// parsed type is what spares each resolver its own copy of the record-or-return branch.
func checkedValue[T any](e *envReader, key, raw string, def, got T, err error) T {
	if err != nil {
		e.err = fmt.Errorf("buff: %s %q: %w", key, raw, err)
		return def
	}
	return got
}

// sizeFlag adapts a byte cap to flag.Value so -max-clip parses through the very same grammar as
// BUFF_MAX_CLIP. String reports the current value as the usage default and must tolerate a nil
// pointer, because flag constructs a zero Value through reflection to detect a flag's zero default.
type sizeFlag struct{ p *int64 }

func (f sizeFlag) String() string {
	if f.p == nil {
		return "0"
	}
	return strconv.FormatInt(*f.p, 10)
}

func (f sizeFlag) Set(s string) error {
	n, err := units.ParseSize(s)
	if err != nil {
		return err
	}
	*f.p = n
	return nil
}

// boolFlag adapts a bool to flag.Value through the on/off grammar, and reports IsBoolFlag so the
// flag works valueless (-fsync), negated (-fsync=off), or with any of the env spellings. As with
// sizeFlag, String tolerates a nil pointer for flag's reflective zero-default probe.
type boolFlag struct{ p *bool }

func (f boolFlag) String() string {
	if f.p == nil {
		return "false"
	}
	return strconv.FormatBool(*f.p)
}

func (f boolFlag) Set(s string) error {
	b, err := parseBool(s)
	if err != nil {
		return err
	}
	*f.p = b
	return nil
}

// IsBoolFlag lets flag accept -fsync with no value, the same as -fsync=true.
func (f boolFlag) IsBoolFlag() bool { return true }

// durFlag adapts a duration to flag.Value so -ttl and the other duration flags parse through the
// very same grammar — and the same non-negative check — as their BUFF_* variables, rather than
// stdlib flag.DurationVar, which accepts a negative the env path rejects. As with sizeFlag, String
// tolerates a nil pointer for flag's reflective zero-default probe.
type durFlag struct{ p *time.Duration }

func (f durFlag) String() string {
	if f.p == nil {
		return "0s"
	}
	return f.p.String()
}

func (f durFlag) Set(s string) error {
	d, err := parseDuration(s)
	if err != nil {
		return err
	}
	*f.p = d
	return nil
}

// countFlag adapts a non-negative count to flag.Value so -max-clips parses through the same grammar
// as BUFF_MAX_CLIPS, including the non-negative check stdlib flag.IntVar would skip. As with
// sizeFlag, String tolerates a nil pointer for flag's reflective zero-default probe.
type countFlag struct{ p *int }

func (f countFlag) String() string {
	if f.p == nil {
		return "0"
	}
	return strconv.Itoa(*f.p)
}

func (f countFlag) Set(s string) error {
	n, err := parseInt(s)
	if err != nil {
		return err
	}
	*f.p = n
	return nil
}

// parseInt reads a non-negative count. Zero means unlimited, matching the clip-count cap.
func parseInt(s string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, errors.New("invalid integer")
	}
	if n < 0 {
		return 0, errors.New("integer must not be negative")
	}
	return n, nil
}

// parseDuration reads a span in the shared human vocabulary — Go's units plus the days and weeks
// a retention is actually written in, so a week-long BUFF_TTL is 7d rather than 168h — and rejects
// a negative one. The grammar itself is signed, as Go's is, because a negative duration is a real
// value; the non-negative rule is a policy of this config and so is applied here, where a negative
// retention or deadline is meaningless and almost always a typo, and is worth failing the boot over
// rather than silently clamping.
func parseDuration(s string) (time.Duration, error) {
	d, err := units.ParseDuration(s)
	if err != nil {
		return 0, errors.New("invalid duration")
	}
	if d < 0 {
		return 0, errors.New("duration must not be negative")
	}
	return d, nil
}

// parseBool reads an on/off knob from the spellings an operator reaches for, rejecting anything
// else rather than coercing it — a misspelled BUFF_FSYNC must fail loudly, not silently disable
// durable commit.
func parseBool(s string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "on", "true", "1", "yes":
		return true, nil
	case "off", "false", "0", "no":
		return false, nil
	default:
		return false, errors.New("invalid boolean (want on/off/true/false/1/0/yes/no)")
	}
}
