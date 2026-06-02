package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/srevn/buff/api"
	"github.com/srevn/buff/store"
)

// Server defaults. Each is the value an operator gets with the matching variable unset; they are
// named constants, not literals buried in the parse, so the precedence table and the unit test can
// both refer to the one definition. The byte caps are binary (a gibibyte is 1<<30), the durations
// are real spans, and the two booleans default to the durable-but-checksumless choice a content
// relay wants. A zero default means the policy is off, not unset: an unlimited cap, no background
// reaping, no idle deadline — read each comment on config for what its own zero means.
const (
	defaultAddr         = ":8080"
	defaultMaxClip      = 1 << 30  // 1 GiB per-clip cap
	defaultMaxTotal     = 10 << 30 // 10 GiB total cap
	defaultMaxClips     = 10000
	defaultTTL          = 24 * time.Hour
	defaultReapInterval = 60 * time.Second
	defaultUploadIdle   = 30 * time.Second
	defaultUploadMax    = time.Duration(0) // off: no absolute cap on one upload's duration
	defaultFsync        = true
	defaultChecksum     = false
)

// config is the fully-resolved server configuration: the env-and-flag precedence has already been
// applied, so every field holds the value the runtime will use. It is medium-agnostic — it knows
// nothing of os.Root, http.Server, or the store's internals — and projects into the three
// lower-layer option structs through the methods below, so the wiring never reaches past this
// boundary into a constructor's own knobs.
//
// Per-field zero meanings are inherited deliberately from the layer each maps into, never invented
// here: the three caps disable at zero because the quota reads zero as unlimited; TTL zero is "no
// default expiry" (retain forever by default), distinct from the request header's "use the server
// default"; the two upload bounds and the reaper interval disable at zero because their consumers
// treat zero as off. Documenting the meaning beside the field is what keeps a future reader from
// "fixing" a zero into a footgun.
type config struct {
	DataDir      string        // BUFF_DATA_DIR: os.Root boundary for all storage; required (no default)
	Addr         string        // BUFF_ADDR: listen address
	MaxClip      int64         // BUFF_MAX_CLIP: per-clip byte cap; 0 = unlimited
	MaxTotal     int64         // BUFF_MAX_TOTAL: total byte cap; 0 = unlimited
	MaxClips     int           // BUFF_MAX_CLIPS: live+finalized count cap; 0 = unlimited
	TTL          time.Duration // BUFF_TTL: default retention from finalize; 0 = no default expiry
	ReapInterval time.Duration // BUFF_REAP_INTERVAL: retention reaper tick; 0 = no background reaping
	UploadIdle   time.Duration // BUFF_UPLOAD_IDLE: per-request idle deadline; 0 = disabled
	UploadMax    time.Duration // BUFF_UPLOAD_MAX: absolute cap on one upload's duration; 0 = off
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

// apiOptions projects the HTTP edge's options. AccessLog is forced on: the server, unlike a test or
// an embedding, always wants one structured line per request, emitted from the same logger as its
// errors. The two upload bounds carry through as policy knobs; the safety timeouts are left zero so
// the api constructor substitutes its own hardened defaults. Version is the resolved build version
// dressed in the healthz "buff/" form, distinct from the bare string --version prints.
func (c config) apiOptions(log *slog.Logger) api.Options {
	return api.Options{
		UploadIdle: c.UploadIdle,
		UploadMax:  c.UploadMax,
		Logger:     log,
		Version:    "buff/" + buildVersion(),
		AccessLog:  true,
	}
}

// configFromEnv resolves the defaults and any set environment variables into a config, before flags
// are bound. getenv is injected rather than reaching for os.Getenv so the whole precedence is a pure
// unit test. The struct literal reads as the precedence table itself — each field names its variable
// and its default in one line — and the first malformed variable surfaces as the returned error,
// the config discarded with it.
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
		Fsync:        e.boolean("BUFF_FSYNC", defaultFsync),
		Checksum:     e.boolean("BUFF_CHECKSUM", defaultChecksum),
	}
	return c, e.err
}

// bindFlags registers a flag for every knob, each defaulting to the env-resolved value already in c
// and writing back into the same field. This is what makes precedence fall out of stdlib flag with
// no merge step: an explicitly-passed flag overrides the env-resolved default, an absent one leaves
// it untouched, so flags-over-env-over-defaults holds by construction. The size and bool flags use
// the flag.Value types below so a flag shares the exact env parser — -max-clip 2GiB and -fsync=off
// read the same grammar BUFF_MAX_CLIP and BUFF_FSYNC do.
func bindFlags(fs *flag.FlagSet, c *config) {
	fs.StringVar(&c.DataDir, "data-dir", c.DataDir, "storage root, required (BUFF_DATA_DIR)")
	fs.StringVar(&c.Addr, "addr", c.Addr, "listen address (BUFF_ADDR)")
	fs.Var(sizeFlag{&c.MaxClip}, "max-clip", "per-clip byte cap, 0=unlimited (BUFF_MAX_CLIP)")
	fs.Var(sizeFlag{&c.MaxTotal}, "max-total", "total byte cap, 0=unlimited (BUFF_MAX_TOTAL)")
	fs.IntVar(&c.MaxClips, "max-clips", c.MaxClips, "clip-count cap, 0=unlimited (BUFF_MAX_CLIPS)")
	fs.DurationVar(&c.TTL, "ttl", c.TTL, "default retention, 0=none (BUFF_TTL)")
	fs.DurationVar(&c.ReapInterval, "reap-interval", c.ReapInterval, "reaper tick, 0=off (BUFF_REAP_INTERVAL)")
	fs.DurationVar(&c.UploadIdle, "upload-idle", c.UploadIdle, "per-request idle deadline, 0=off (BUFF_UPLOAD_IDLE)")
	fs.DurationVar(&c.UploadMax, "upload-max", c.UploadMax, "max upload duration, 0=off (BUFF_UPLOAD_MAX)")
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
	n, err := parseSize(v)
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
	n, err := parseSize(s)
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

// parseSize reads a byte count with an optional binary unit: a bare integer is bytes, and a K, M, G,
// or T suffix multiplies by the matching power of 1024. The suffix is case-insensitive and tolerates
// a trailing i and/or B, so 1G, 1Gi, 1GB, and 1GiB all mean one gibibyte — every unit is binary,
// the only interpretation a byte store needs, with no decimal-versus-binary ambiguity to surprise an
// operator. Zero means unlimited, matching the store's cap semantics; a negative or malformed value,
// or one that overflows int64, is rejected rather than silently coerced.
func parseSize(s string) (int64, error) {
	u := strings.TrimSpace(s)
	if u == "" {
		return 0, errors.New("invalid size")
	}
	// Shed an optional trailing "B"/"iB" so the unit letter is last, then read the multiplier off
	// that letter. The order matters: B before i, so "iB" sheds fully.
	u = strings.TrimSuffix(u, "B")
	u = strings.TrimSuffix(u, "b")
	u = strings.TrimSuffix(u, "i")
	u = strings.TrimSuffix(u, "I")
	mult := int64(1)
	if n := len(u); n > 0 {
		switch u[n-1] {
		case 'K', 'k':
			mult = 1 << 10
		case 'M', 'm':
			mult = 1 << 20
		case 'G', 'g':
			mult = 1 << 30
		case 'T', 't':
			mult = 1 << 40
		}
		if mult != 1 {
			u = u[:n-1]
		}
	}
	v, err := strconv.ParseInt(strings.TrimSpace(u), 10, 64)
	if err != nil {
		return 0, errors.New("invalid size")
	}
	if v < 0 {
		return 0, errors.New("size must not be negative")
	}
	if mult != 1 && v > math.MaxInt64/mult {
		return 0, errors.New("size out of range")
	}
	return v * mult, nil
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

// parseDuration reads a Go duration and rejects a negative one. A negative retention or deadline is
// meaningless and almost always a typo, so it is an error rather than a silent clamp.
func parseDuration(s string) (time.Duration, error) {
	d, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return 0, errors.New("invalid duration")
	}
	if d < 0 {
		return 0, errors.New("duration must not be negative")
	}
	return d, nil
}

// parseBool reads an on/off knob from the spellings an operator reaches for, rejecting anything else
// rather than coercing it — a misspelled BUFF_FSYNC must fail loudly, not silently disable durable
// commit.
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
