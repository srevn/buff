package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// getenvFrom adapts a map to the injected getenv shape, returning "" for an absent key exactly as
// os.Getenv does for an unset variable.
func getenvFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// resolve runs the real precedence pipeline — env-resolved config, then flags whose defaults are
// those resolved values — and returns the config the runtime would use. It is the same sequence
// runServe performs, minus the build-and-run, so a test exercises precedence without a server.
func resolve(t *testing.T, env map[string]string, args ...string) config {
	t.Helper()
	c, err := configFromEnv(getenvFrom(env))
	if err != nil {
		t.Fatalf("configFromEnv: %v", err)
	}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	bindFlags(fs, &c)
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse %v: %v", args, err)
	}
	return c
}

// eq fails unless got equals want, naming the field under test.
func eq[T comparable](t *testing.T, name string, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

// TestConfigPrecedence checks flags > env > defaults for one representative knob of each type: a
// string, a size, an int, a duration, and a bool. Each knob is covered in all three states, so a
// regression in the env-derived-default mechanism surfaces on whichever layer it breaks.
func TestConfigPrecedence(t *testing.T) {
	tests := []struct {
		name  string
		env   map[string]string
		args  []string
		check func(t *testing.T, c config)
	}{
		{"addr default", nil, nil, func(t *testing.T, c config) { eq(t, "addr", c.Addr, defaultAddr) }},
		{"addr env", map[string]string{"BUFF_ADDR": ":9000"}, nil,
			func(t *testing.T, c config) { eq(t, "addr", c.Addr, ":9000") }},
		{"addr flag over env", map[string]string{"BUFF_ADDR": ":9000"}, []string{"-addr", ":7000"},
			func(t *testing.T, c config) { eq(t, "addr", c.Addr, ":7000") }},

		{"max-clip default", nil, nil, func(t *testing.T, c config) { eq(t, "max-clip", c.MaxClip, int64(defaultMaxClip)) }},
		{"max-clip env", map[string]string{"BUFF_MAX_CLIP": "2MiB"}, nil,
			func(t *testing.T, c config) { eq(t, "max-clip", c.MaxClip, int64(2<<20)) }},
		{"max-clip flag over env", map[string]string{"BUFF_MAX_CLIP": "2MiB"}, []string{"-max-clip", "4K"},
			func(t *testing.T, c config) { eq(t, "max-clip", c.MaxClip, int64(4<<10)) }},

		{"max-clips env", map[string]string{"BUFF_MAX_CLIPS": "5"}, nil,
			func(t *testing.T, c config) { eq(t, "max-clips", c.MaxClips, 5) }},
		{"max-clips flag over env", map[string]string{"BUFF_MAX_CLIPS": "5"}, []string{"-max-clips", "9"},
			func(t *testing.T, c config) { eq(t, "max-clips", c.MaxClips, 9) }},

		{"ttl default", nil, nil, func(t *testing.T, c config) { eq(t, "ttl", c.TTL, defaultTTL) }},
		{"ttl env", map[string]string{"BUFF_TTL": "1h"}, nil,
			func(t *testing.T, c config) { eq(t, "ttl", c.TTL, time.Hour) }},
		{"ttl flag over env", map[string]string{"BUFF_TTL": "1h"}, []string{"-ttl", "30m"},
			func(t *testing.T, c config) { eq(t, "ttl", c.TTL, 30*time.Minute) }},

		{"fsync default", nil, nil, func(t *testing.T, c config) { eq(t, "fsync", c.Fsync, true) }},
		{"fsync env off", map[string]string{"BUFF_FSYNC": "off"}, nil,
			func(t *testing.T, c config) { eq(t, "fsync", c.Fsync, false) }},
		{"fsync flag on over env off", map[string]string{"BUFF_FSYNC": "off"}, []string{"-fsync"},
			func(t *testing.T, c config) { eq(t, "fsync", c.Fsync, true) }},
		{"fsync flag =false", nil, []string{"-fsync=false"},
			func(t *testing.T, c config) { eq(t, "fsync", c.Fsync, false) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { tt.check(t, resolve(t, tt.env, tt.args...)) })
	}
}

func TestParseSize(t *testing.T) {
	ok := []struct {
		in   string
		want int64
	}{
		{"0", 0}, {"1024", 1024}, {"1K", 1 << 10}, {"1k", 1 << 10}, {"1Ki", 1 << 10},
		{"1KiB", 1 << 10}, {"1KB", 1 << 10}, {"2M", 2 << 20}, {"3G", 3 << 30}, {"1T", 1 << 40},
		{"1g", 1 << 30}, {"1Gi", 1 << 30}, {"  5K  ", 5 << 10},
	}
	for _, c := range ok {
		got, err := parseSize(c.in)
		if err != nil || got != c.want {
			t.Errorf("parseSize(%q) = %d, %v; want %d, nil", c.in, got, err, c.want)
		}
	}
	for _, in := range []string{"", "-1", "-1K", "abc", "1.5G", "K", "1X", "99999999999999T"} {
		if got, err := parseSize(in); err == nil {
			t.Errorf("parseSize(%q) = %d, nil; want error", in, got)
		}
	}
}

func TestParseInt(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int
	}{{"0", 0}, {"10000", 10000}, {"42", 42}} {
		if got, err := parseInt(c.in); err != nil || got != c.want {
			t.Errorf("parseInt(%q) = %d, %v; want %d, nil", c.in, got, err, c.want)
		}
	}
	for _, in := range []string{"", " ", "-1", "abc", "1.0"} {
		if got, err := parseInt(in); err == nil {
			t.Errorf("parseInt(%q) = %d, nil; want error", in, got)
		}
	}
}

func TestParseDuration(t *testing.T) {
	for _, c := range []struct {
		in   string
		want time.Duration
	}{{"0", 0}, {"24h", 24 * time.Hour}, {"30m", 30 * time.Minute}, {"1h30m", 90 * time.Minute}, {"500ms", 500 * time.Millisecond}} {
		if got, err := parseDuration(c.in); err != nil || got != c.want {
			t.Errorf("parseDuration(%q) = %v, %v; want %v, nil", c.in, got, err, c.want)
		}
	}
	for _, in := range []string{"", "5", "-5s", "abc"} {
		if got, err := parseDuration(in); err == nil {
			t.Errorf("parseDuration(%q) = %v, nil; want error", in, got)
		}
	}
}

func TestParseBool(t *testing.T) {
	for _, c := range []struct {
		in   string
		want bool
	}{{"on", true}, {"off", false}, {"true", true}, {"false", false}, {"1", true}, {"0", false}, {"yes", true}, {"no", false}, {"ON", true}, {"Off", false}} {
		if got, err := parseBool(c.in); err != nil || got != c.want {
			t.Errorf("parseBool(%q) = %v, %v; want %v, nil", c.in, got, err, c.want)
		}
	}
	for _, in := range []string{"", "maybe", "2", "onoff"} {
		if got, err := parseBool(in); err == nil {
			t.Errorf("parseBool(%q) = %v, nil; want error", in, got)
		}
	}
}

// TestConfigFromEnvMalformed checks that each malformed variable fails the resolve and that the
// error names the offending variable, so an operator's diagnostic points at the right knob.
func TestConfigFromEnvMalformed(t *testing.T) {
	for _, k := range []struct{ key, val string }{
		{"BUFF_MAX_CLIP", "abc"},
		{"BUFF_MAX_TOTAL", "-1"},
		{"BUFF_MAX_CLIPS", "-5"},
		{"BUFF_TTL", "-1h"},
		{"BUFF_REAP_INTERVAL", "xyz"},
		{"BUFF_UPLOAD_IDLE", "-30s"},
		{"BUFF_FSYNC", "maybe"},
		{"BUFF_CHECKSUM", "2"},
	} {
		_, err := configFromEnv(getenvFrom(map[string]string{k.key: k.val}))
		if err == nil {
			t.Errorf("configFromEnv(%s=%q) = nil; want error", k.key, k.val)
			continue
		}
		if !strings.Contains(err.Error(), k.key) {
			t.Errorf("error %q does not name %s", err, k.key)
		}
	}
}

// TestDataDirRequired checks the one value with no usable default: an empty data directory after env
// and flags is a hard, named error that reaches errw, not a silent fallback. runServe returns before
// it would build anything, so this never starts a server.
func TestDataDirRequired(t *testing.T) {
	var errb bytes.Buffer
	err := runServe(context.Background(), nil, getenvFrom(nil), &errb)
	if err == nil {
		t.Fatal("runServe with no data dir = nil; want error")
	}
	if !strings.Contains(err.Error(), "data directory required") {
		t.Errorf("error = %q, want data-directory-required", err)
	}
	if !strings.Contains(errb.String(), "data directory required") {
		t.Errorf("errw = %q, want the diagnostic reported", errb.String())
	}
}

// TestServeHelp checks `buff serve -h`: flag's ErrHelp maps to a nil return (a help request is a
// clean exit), and the richer fs.Usage writes the synopsis plus the flag list — naming a BUFF_*
// pairing — to errw. It proves the env/flag documentation reaches the operator, not just a bare
// flag dump, without standing up a server.
func TestServeHelp(t *testing.T) {
	var errb bytes.Buffer
	if err := runServe(context.Background(), []string{"-h"}, getenvFrom(nil), &errb); err != nil {
		t.Fatalf("serve -h returned %v, want nil (a help request is a clean exit)", err)
	}
	out := errb.String()
	for _, want := range []string{"run the content-relay server", "BUFF_DATA_DIR", "-data-dir"} {
		if !strings.Contains(out, want) {
			t.Errorf("serve -h usage missing %q; got:\n%s", want, out)
		}
	}
}

// TestProjections guards the config-to-lower-layer mappings, in particular that apiOptions forces
// AccessLog on and dresses the version in the health form — the wiring the server depends on but no
// other test exercises.
func TestProjections(t *testing.T) {
	c := config{
		MaxClip: 5, MaxTotal: 6, MaxClips: 7, TTL: time.Hour,
		Fsync: true, Checksum: true, UploadIdle: time.Second, UploadMax: time.Minute,
	}
	sc := c.storeConfig()
	eq(t, "store.MaxClip", sc.MaxClip, int64(5))
	eq(t, "store.MaxTotal", sc.MaxTotal, int64(6))
	eq(t, "store.MaxClips", sc.MaxClips, 7)
	eq(t, "store.DefaultTTL", sc.DefaultTTL, time.Hour)

	do := c.diskOpts(nil)
	eq(t, "disk.Fsync", do.Fsync, true)
	eq(t, "disk.Checksum", do.Checksum, true)

	ao := c.apiOptions(nil)
	eq(t, "api.AccessLog", ao.AccessLog, true)
	eq(t, "api.Version", ao.Version, "buff/"+buildVersion())
	eq(t, "api.UploadIdle", ao.UploadIdle, time.Second)
	eq(t, "api.UploadMax", ao.UploadMax, time.Minute)
}

// TestIsTTY checks the two negatives a test can assert portably — a pipe and a regular file are not
// terminals. The character-device positive has no portable fixture under go test, and is the very
// case the copy/paste forcing flags exist to escape, so it is documented here rather than asserted.
func TestIsTTY(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	if isTTY(r) {
		t.Error("pipe read end reported as a TTY")
	}
	if isTTY(w) {
		t.Error("pipe write end reported as a TTY")
	}
	f, err := os.CreateTemp(t.TempDir(), "buff")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if isTTY(f) {
		t.Error("regular file reported as a TTY")
	}
}
