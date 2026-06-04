package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
	"github.com/srevn/buff/wire"
)

// TestShouldReset pins the coarse-cadence predicate: it fires on the first call (no deadline
// yet) and thereafter only once past the half-window, so a steady transfer resets rarely while
// a stall still trips.
func TestShouldReset(t *testing.T) {
	base := time.Unix(1000, 0)
	idle := 30 * time.Second
	cases := []struct {
		name string
		last time.Time
		now  time.Time
		want bool
	}{
		{"first call (zero last)", time.Time{}, base, true},
		{"just set", base, base, false},
		{"within half window", base, base.Add(14 * time.Second), false},
		{"at half window", base, base.Add(15 * time.Second), false},
		{"past half window", base, base.Add(16 * time.Second), true},
		{"well past", base, base.Add(time.Minute), true},
	}
	for _, c := range cases {
		if got := shouldReset(c.last, c.now, idle); got != c.want {
			t.Errorf("%s: shouldReset = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestDeadline checks the absolute-maximum helper: not positive means no maximum (the zero
// instant), positive means an instant the right distance ahead.
func TestDeadline(t *testing.T) {
	if d := deadline(0); !d.IsZero() {
		t.Errorf("deadline(0) = %v, want zero", d)
	}
	if d := deadline(-time.Second); !d.IsZero() {
		t.Errorf("deadline(-1s) = %v, want zero", d)
	}
	before := time.Now()
	d := deadline(time.Hour)
	if !d.After(before.Add(59*time.Minute)) || d.After(before.Add(61*time.Minute)) {
		t.Errorf("deadline(1h) = %v, not ~1h ahead of %v", d, before)
	}
}

// TestWithDefaultsUploadIdle pins UploadIdle as a standing safety bound. The zero Options — the
// frictionless embedding path — must come back with the built-in default, not a disabled bound, and
// any non-positive value is coerced too, so the bound can never resolve to "disabled" at this seam;
// a positive value is preserved untouched. This is the deterministic proof of the reclassification:
// a regression to 0-means-disabled fails right here, without a socket or a clock.
func TestWithDefaultsUploadIdle(t *testing.T) {
	if got := withDefaults(Options{}).UploadIdle; got != defaultUploadIdle {
		t.Errorf("zero Options UploadIdle = %v, want the built-in default %v (a standing bound, never disabled)", got, defaultUploadIdle)
	}
	if got := withDefaults(Options{UploadIdle: -5 * time.Second}).UploadIdle; got != defaultUploadIdle {
		t.Errorf("negative UploadIdle = %v, want the built-in default %v (no disabled state, even via a negative)", got, defaultUploadIdle)
	}
	if got := withDefaults(Options{UploadIdle: 250 * time.Millisecond}).UploadIdle; got != 250*time.Millisecond {
		t.Errorf("positive UploadIdle = %v, want it preserved", got)
	}
}

// TestAbortOnCancel pins the upload's context-cancellation watcher and its disarm guard, the
// critical bit of shutdown infrastructure. On cancel it must arm a past read deadline — the poke
// that unblocks a parked body read; once stopped it must arm nothing, even on a cancellation racing
// the stop, so a finished handler can never poison a connection being recycled for keep-alive.
func TestAbortOnCancel(t *testing.T) {
	t.Run("cancel arms a past read deadline", func(t *testing.T) {
		probe := &deadlineProbe{set: make(chan time.Time, 1)}
		ctl := http.NewResponseController(probe)
		ctx, cancel := context.WithCancel(context.Background())
		stop := abortOnCancel(ctx, ctl)
		defer stop()

		cancel()
		select {
		case dl := <-probe.set:
			if !dl.Before(time.Now()) {
				t.Errorf("armed read deadline %v is not in the past", dl)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("cancel did not unblock the read: no deadline armed")
		}
	})

	t.Run("disarmed before a racing cancel arms nothing", func(t *testing.T) {
		probe := &deadlineProbe{set: make(chan time.Time, 1)}
		ctl := http.NewResponseController(probe)
		ctx, cancel := context.WithCancel(context.Background())
		stop := abortOnCancel(ctx, ctl)

		stop()   // the handler finished cleanly: disarm first
		cancel() // a cancellation arriving after the stop must be a no-op
		select {
		case dl := <-probe.set:
			t.Errorf("deadline %v armed after stop; the disarm guard failed", dl)
		case <-time.After(100 * time.Millisecond):
			// no deadline armed — the guard held
		}
	})
}

// deadlineProbe is a minimal ResponseWriter that reports each SetReadDeadline through a channel, so
// a test can observe the watcher's poke without racing on a shared field.
type deadlineProbe struct {
	set chan time.Time
}

func (p *deadlineProbe) Header() http.Header               { return http.Header{} }
func (p *deadlineProbe) Write(b []byte) (int, error)       { return len(b), nil }
func (p *deadlineProbe) WriteHeader(int)                   {}
func (p *deadlineProbe) SetReadDeadline(t time.Time) error { p.set <- t; return nil }

// TestMapErr pins the one forward mapping: every domain sentinel resolves to its wire row, a
// wrapped sentinel still resolves through errors.Is, and an unrecognised error falls through to
// the internal row rather than being misreported as a client error.
func TestMapErr(t *testing.T) {
	cases := []struct {
		err  error
		want wire.ErrInfo
	}{
		{clip.ErrNotFound, wire.ErrNotFound},
		{clip.ErrConsumed, wire.ErrConsumed},
		{clip.ErrBusy, wire.ErrBusy},
		{clip.ErrClosed, wire.ErrClosed},
		{clip.ErrTooLarge, wire.ErrTooLarge},
		{clip.ErrNoSpace, wire.ErrNoSpace},
		{clip.ErrNameInvalid, wire.ErrNameBad},
		{clip.ErrFilenameInvalid, wire.ErrBadReq},
		{errBadRequest, wire.ErrBadReq},
		{fmt.Errorf("open x: %w", clip.ErrNotFound), wire.ErrNotFound},
		{fmt.Errorf("create x: %w", clip.ErrNoSpace), wire.ErrNoSpace},
		{errors.New("some backing fault"), wire.ErrInternal},
		{nil, wire.ErrInternal},
	}
	for _, c := range cases {
		if got := mapErr(c.err); got != c.want {
			t.Errorf("mapErr(%v) = %+v, want %+v", c.err, got, c.want)
		}
	}
}

// TestClassifyPut pins the by-side classification of a failed upload copy: a cap rejection is a
// real status carried with no cause; a recorded read error is a best-effort bad request unless the
// cancellation cause says the server was stopping, in which case it is an honest 503; only an
// otherwise-unexplained writer error is internal and carries its cause for logging. The two
// read-error cases differ solely in the context cause — the same socket-level truncation — which is
// exactly the distinction the handler must make between a client that vanished and an operator who
// stopped the server. The read-error cases pass err as the very value recorded in readErr, since
// that identity (io.Copy surfaced the read error) is what selects the branch; a distinct writer
// fault coincident with a recorded read error must instead stay internal and logged.
func TestClassifyPut(t *testing.T) {
	readFault := errors.New("connection reset")
	backingFault := errors.New("disk on fire")
	bg := context.Background()

	t.Run("too large", func(t *testing.T) {
		info, cause := classifyPut(bg, clip.ErrTooLarge, &idleResetReader{})
		if info != wire.ErrTooLarge || cause != nil {
			t.Fatalf("got (%+v, %v), want (%+v, nil)", info, cause, wire.ErrTooLarge)
		}
	})
	t.Run("no space", func(t *testing.T) {
		info, cause := classifyPut(bg, clip.ErrNoSpace, &idleResetReader{})
		if info != wire.ErrNoSpace || cause != nil {
			t.Fatalf("got (%+v, %v), want (%+v, nil)", info, cause, wire.ErrNoSpace)
		}
	})
	t.Run("wrapped cap still classified", func(t *testing.T) {
		info, _ := classifyPut(bg, fmt.Errorf("write: %w", clip.ErrTooLarge), &idleResetReader{})
		if info != wire.ErrTooLarge {
			t.Fatalf("got %+v, want %+v", info, wire.ErrTooLarge)
		}
	})
	t.Run("read side truncation", func(t *testing.T) {
		// err is the value io.Copy returned, identical to the one idleResetReader recorded — the
		// signature of io.Copy having surfaced the read error, which is what selects this branch.
		info, cause := classifyPut(bg, readFault, &idleResetReader{readErr: readFault})
		if info != wire.ErrBadReq || cause != nil {
			t.Fatalf("got (%+v, %v), want (%+v, nil)", info, cause, wire.ErrBadReq)
		}
	})
	t.Run("read cut by shutdown is unavailable", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(ErrServerStopping)
		info, cause := classifyPut(ctx, readFault, &idleResetReader{readErr: readFault})
		if info != wire.ErrUnavailable || cause != nil {
			t.Fatalf("got (%+v, %v), want (%+v, nil)", info, cause, wire.ErrUnavailable)
		}
	})
	t.Run("read cut by client disconnect stays bad request", func(t *testing.T) {
		// A vanished client cancels the request context too, but with the plain Canceled cause net/http
		// sets — never ErrServerStopping — so it must still classify as the client's truncation.
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(context.Canceled)
		info, cause := classifyPut(ctx, readFault, &idleResetReader{readErr: readFault})
		if info != wire.ErrBadReq || cause != nil {
			t.Fatalf("got (%+v, %v), want (%+v, nil)", info, cause, wire.ErrBadReq)
		}
	})
	t.Run("backing write fault is internal with cause", func(t *testing.T) {
		info, cause := classifyPut(bg, backingFault, &idleResetReader{})
		if info != wire.ErrInternal || cause != backingFault {
			t.Fatalf("got (%+v, %v), want (%+v, %v)", info, cause, wire.ErrInternal, backingFault)
		}
	})
	t.Run("backing fault coincident with a read error is internal, not a 400", func(t *testing.T) {
		// io.Copy reports the writer fault in preference while idleResetReader has also recorded a
		// coincident read error. The two values differ, so the identity check must not mistake the
		// backing fault for a client truncation — it stays the internal row, logged with its cause.
		// Keying on body.readErr alone (the prior shape) misrouted this to a 400 with no log.
		info, cause := classifyPut(bg, backingFault, &idleResetReader{readErr: readFault})
		if info != wire.ErrInternal || cause != backingFault {
			t.Fatalf("got (%+v, %v), want (%+v, %v)", info, cause, wire.ErrInternal, backingFault)
		}
	})
}

// TestClassifyGet pins the pre-stream disposition of a failed Open, the read-side twin of
// TestClassifyPut. A domain sentinel keeps its row and carries its cause without resetting; an
// unrecognised fault is the internal row with its cause; a cancellation cut by shutdown is a clean
// 503 with no cause and no reset; a cancellation without that cause — a vanished client — resets,
// carrying no row and no cause. A deadline with no stopping cause resets too, so the classifier is
// robust even though the server arms no per-request context deadline today.
func TestClassifyGet(t *testing.T) {
	bg := context.Background()
	backingFault := errors.New("disk on fire")

	t.Run("domain sentinel keeps its row, no reset", func(t *testing.T) {
		info, cause, reset := classifyGet(bg, clip.ErrNotFound)
		if info != wire.ErrNotFound || !errors.Is(cause, clip.ErrNotFound) || reset {
			t.Fatalf("got (%+v, %v, reset=%v), want (%+v, ErrNotFound, false)", info, cause, reset, wire.ErrNotFound)
		}
	})
	t.Run("wrapped sentinel still resolves", func(t *testing.T) {
		info, _, reset := classifyGet(bg, fmt.Errorf("open x: %w", clip.ErrConsumed))
		if info != wire.ErrConsumed || reset {
			t.Fatalf("got (%+v, reset=%v), want (%+v, false)", info, reset, wire.ErrConsumed)
		}
	})
	t.Run("unrecognised fault is internal with cause, no reset", func(t *testing.T) {
		info, cause, reset := classifyGet(bg, backingFault)
		if info != wire.ErrInternal || cause != backingFault || reset {
			t.Fatalf("got (%+v, %v, reset=%v), want (%+v, backingFault, false)", info, cause, reset, wire.ErrInternal)
		}
	})
	t.Run("cancel cut by shutdown is unavailable, no reset, no cause", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(ErrServerStopping)
		info, cause, reset := classifyGet(ctx, context.Canceled)
		if info != wire.ErrUnavailable || cause != nil || reset {
			t.Fatalf("got (%+v, %v, reset=%v), want (%+v, nil, false)", info, cause, reset, wire.ErrUnavailable)
		}
	})
	t.Run("cancel without stopping cause resets", func(t *testing.T) {
		// A vanished client: net/http cancels with the plain Canceled cause, never ErrServerStopping.
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(context.Canceled)
		_, cause, reset := classifyGet(ctx, context.Canceled)
		if !reset || cause != nil {
			t.Fatalf("got (cause=%v, reset=%v), want (nil, true)", cause, reset)
		}
	})
	t.Run("deadline without stopping cause resets", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(context.DeadlineExceeded)
		if _, _, reset := classifyGet(ctx, context.DeadlineExceeded); !reset {
			t.Fatal("a deadline-exceeded cancellation with no stopping cause must reset")
		}
	})
}

// TestForwardCoverage proves the server can emit every row the wire table defines, so a row added
// to the table cannot sit unreachable. The forward direction has three producers: errMap, which
// mapErr walks for store and request errors; classifyPut, which reads the context cause to classify
// a failed upload; and classifyGet, its read-side twin for a failed Open. Two rows come only from
// outside errMap and so are named explicitly — internal, which mapErr falls through to for an
// unrecognised error and either classifier returns for an unexplained fault, and unavailable, which
// either classifier returns for a shutdown-cut transfer — neither having a single clip sentinel to
// key on. classifyGet's other novel outcome, a client-gone reset, deliberately has no row: it resets
// the connection rather than sending a status, exactly as the table omits a row for an aborted live
// stream. Their union must be the whole table. Ranging wire.Rows is what makes this total: a row
// added to the table fails here until a producer covers it.
func TestForwardCoverage(t *testing.T) {
	emittable := make(map[wire.ErrInfo]bool, len(wire.Rows))
	for _, m := range errMap {
		emittable[m.info] = true
	}
	emittable[wire.ErrInternal] = true
	emittable[wire.ErrUnavailable] = true

	for _, row := range wire.Rows {
		if !emittable[row] {
			t.Errorf("wire row %+v is emitted by no forward path (errMap or classifyPut)", row)
		}
	}
	if len(emittable) != len(wire.Rows) {
		t.Errorf("forward paths produce %d distinct rows, but wire.Rows has %d", len(emittable), len(wire.Rows))
	}
}

// TestGetCancelled pins the user-visible half of the fix: a GET whose context is already gone when
// Open's guard runs is no longer the spurious 500-with-Error-log it once was. A clip is finalized so
// Open would succeed but for the cancellation — the guard declines before resolving it, so the
// disposition is the cancellation's alone, never a not-found. A shutdown-caused cut is a clean 503
// and logs nothing at Error; a vanished client resets with http.ErrAbortHandler and logs nothing
// either. The handler is driven directly, so the reset panic surfaces here to be recovered — the
// same one stream raises and the recover backstop re-throws — and the Error-level log sink proves
// neither disposition is misreported as an internal fault.
func TestGetCancelled(t *testing.T) {
	finalized := func(t *testing.T) store.Store {
		t.Helper()
		st := store.NewMemory(store.Config{})
		w, err := st.Create(context.Background(), "doc", clip.Meta{Kind: clip.KindText}, store.PutOpts{})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if _, err := w.Write([]byte("payload")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return st
	}
	// errLogged builds a server whose logger captures only Error-level records, so an empty buffer
	// after a request means the request logged nothing an operator would read as a fault.
	errLogged := func(t *testing.T) (*Server, *bytes.Buffer) {
		var buf bytes.Buffer
		log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))
		return New(finalized(t), Options{Logger: log}), &buf
	}
	// req builds a GET for "doc" bound to ctx, with the path value the router would otherwise set.
	req := func(ctx context.Context) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/v1/clips/doc", nil).WithContext(ctx)
		r.SetPathValue("name", "doc")
		return r
	}

	t.Run("cut by shutdown is a clean 503, not logged at Error", func(t *testing.T) {
		srv, log := errLogged(t)
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(ErrServerStopping)
		rec := httptest.NewRecorder()
		srv.get(rec, req(ctx))
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", rec.Code)
		}
		if got := rec.Header().Get(wire.HeaderError); got != wire.ErrUnavailable.Sentinel {
			t.Errorf("Buff-Error = %q, want %q", got, wire.ErrUnavailable.Sentinel)
		}
		if log.Len() != 0 {
			t.Errorf("emitted Error log %q, want none — a shutdown cut is not an internal fault", log.String())
		}
	})

	t.Run("vanished client resets, not logged at Error", func(t *testing.T) {
		srv, log := errLogged(t)
		ctx, cancel := context.WithCancel(context.Background()) // plain cancel: no stopping cause
		cancel()
		rec := httptest.NewRecorder()
		defer func() {
			if r := recover(); r != http.ErrAbortHandler {
				t.Errorf("recover = %v, want http.ErrAbortHandler (a client-gone read resets)", r)
			}
			if log.Len() != 0 {
				t.Errorf("emitted Error log %q, want none — a vanished client is not an internal fault", log.String())
			}
		}()
		srv.get(rec, req(ctx))
		t.Fatal("get did not reset for a vanished client")
	})
}

// TestParsePut pins header parsing: a missing kind defaults to text, every malformed value is a
// bad request (kind, percent-decode, TTL), a bad filename keeps its own sentinel, an
// encoded-separator filename is rejected as traversal, and the boolean flags are strict "1".
func TestParsePut(t *testing.T) {
	req := func(h map[string]string) *http.Request {
		r := httptest.NewRequest(http.MethodPut, "/v1/clips/x", nil)
		for k, v := range h {
			r.Header.Set(k, v)
		}
		return r
	}

	t.Run("defaults", func(t *testing.T) {
		m, o, err := parsePut(req(nil))
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if m.Kind != clip.KindText || m.Filename != "" {
			t.Errorf("meta = %+v, want {text ,}", m)
		}
		if o != (store.PutOpts{}) {
			t.Errorf("opts = %+v, want zero", o)
		}
	})

	t.Run("kinds", func(t *testing.T) {
		for _, k := range []clip.Kind{clip.KindText, clip.KindFile, clip.KindArchive} {
			m, _, err := parsePut(req(map[string]string{wire.HeaderKind: string(k)}))
			if err != nil || m.Kind != k {
				t.Errorf("kind %q: meta=%+v err=%v", k, m, err)
			}
		}
	})
	t.Run("bad kind", func(t *testing.T) {
		if _, _, err := parsePut(req(map[string]string{wire.HeaderKind: "video"})); !errors.Is(err, errBadRequest) {
			t.Errorf("err = %v, want errBadRequest", err)
		}
	})

	t.Run("filename decode", func(t *testing.T) {
		m, _, err := parsePut(req(map[string]string{wire.HeaderFilename: "caf%C3%A9.pdf"}))
		if err != nil || m.Filename != "café.pdf" {
			t.Errorf("filename = %q err=%v, want café.pdf", m.Filename, err)
		}
	})
	t.Run("bad percent", func(t *testing.T) {
		if _, _, err := parsePut(req(map[string]string{wire.HeaderFilename: "a%ZZ"})); !errors.Is(err, errBadRequest) {
			t.Errorf("err = %v, want errBadRequest", err)
		}
	})
	t.Run("filename traversal", func(t *testing.T) {
		if _, _, err := parsePut(req(map[string]string{wire.HeaderFilename: "../etc/passwd"})); !errors.Is(err, clip.ErrFilenameInvalid) {
			t.Errorf("err = %v, want ErrFilenameInvalid", err)
		}
	})
	t.Run("filename encoded separator", func(t *testing.T) {
		// %2F decodes to '/', which ValidFilename must reject — traversal smuggled through encoding.
		if _, _, err := parsePut(req(map[string]string{wire.HeaderFilename: "a%2Fb"})); !errors.Is(err, clip.ErrFilenameInvalid) {
			t.Errorf("err = %v, want ErrFilenameInvalid", err)
		}
	})
	t.Run("filename invalid utf-8", func(t *testing.T) {
		// %E9 decodes to the lone byte 0xE9 (Latin-1 é), which is byte-faithful through the percent
		// codec but not valid UTF-8. ValidFilename must reject it: encoding/json would silently coerce
		// it to U+FFFD in meta.json and the list response, so the basename would not round-trip.
		if _, _, err := parsePut(req(map[string]string{wire.HeaderFilename: "caf%E9.txt"})); !errors.Is(err, clip.ErrFilenameInvalid) {
			t.Errorf("err = %v, want ErrFilenameInvalid", err)
		}
	})

	t.Run("ttl explicit", func(t *testing.T) {
		_, o, err := parsePut(req(map[string]string{wire.HeaderTTL: "90m"}))
		if err != nil || o.TTL != 90*time.Minute {
			t.Errorf("ttl = %v err=%v", o.TTL, err)
		}
	})
	t.Run("ttl zero is default", func(t *testing.T) {
		_, o, err := parsePut(req(map[string]string{wire.HeaderTTL: "0"}))
		if err != nil || o.TTL != 0 {
			t.Errorf("ttl = %v err=%v, want 0", o.TTL, err)
		}
	})
	t.Run("ttl negative", func(t *testing.T) {
		if _, _, err := parsePut(req(map[string]string{wire.HeaderTTL: "-5m"})); !errors.Is(err, errBadRequest) {
			t.Errorf("err = %v, want errBadRequest", err)
		}
	})
	t.Run("ttl garbage", func(t *testing.T) {
		if _, _, err := parsePut(req(map[string]string{wire.HeaderTTL: "soon"})); !errors.Is(err, errBadRequest) {
			t.Errorf("err = %v, want errBadRequest", err)
		}
	})

	t.Run("keep and consume strict one", func(t *testing.T) {
		_, o, err := parsePut(req(map[string]string{wire.HeaderKeep: "1", wire.HeaderConsume: "1"}))
		if err != nil || !o.Keep || !o.ConsumeOnce {
			t.Errorf("opts = %+v err=%v, want keep+consume", o, err)
		}
		// A present-but-not-"1" flag is malformed, not a silent off — a typo'd --keep fails loudly.
		if _, _, err := parsePut(req(map[string]string{wire.HeaderKeep: "true"})); !errors.Is(err, errBadRequest) {
			t.Errorf("Buff-Keep: true err = %v, want errBadRequest", err)
		}
		if _, _, err := parsePut(req(map[string]string{wire.HeaderConsume: "0"})); !errors.Is(err, errBadRequest) {
			t.Errorf("Buff-Consume: 0 err = %v, want errBadRequest", err)
		}
		// Absent stays off.
		_, o3, err := parsePut(req(nil))
		if err != nil || o3.Keep || o3.ConsumeOnce {
			t.Errorf("absent flags = %+v err=%v, want neither", o3, err)
		}
	})
}

// TestToWire pins the JSON projection: times render as RFC3339, a present expiry is included and
// an absent one omitted (the empty string omitempty drops), and the filename passes through.
func TestToWire(t *testing.T) {
	created := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	finalized := time.Date(2026, 6, 2, 10, 0, 5, 0, time.UTC)
	expires := time.Date(2026, 6, 3, 10, 0, 5, 0, time.UTC)

	t.Run("with expiry and filename", func(t *testing.T) {
		wc := toWire(clip.Clip{
			Name:        "report",
			Generation:  "abc123",
			Meta:        clip.Meta{Kind: clip.KindFile, Filename: "r.pdf"},
			Size:        1234,
			CreatedAt:   created,
			FinalizedAt: finalized,
			ExpiresAt:   expires,
			Finalized:   true,
		})
		if wc.CreatedAt != "2026-06-02T10:00:00Z" || wc.FinalizedAt != "2026-06-02T10:00:05Z" {
			t.Errorf("times = %q / %q", wc.CreatedAt, wc.FinalizedAt)
		}
		if wc.ExpiresAt != "2026-06-03T10:00:05Z" {
			t.Errorf("expires = %q", wc.ExpiresAt)
		}
		if wc.Filename != "r.pdf" || wc.Kind != clip.KindFile || wc.Size != 1234 {
			t.Errorf("wc = %+v", wc)
		}
	})

	t.Run("no expiry omits", func(t *testing.T) {
		wc := toWire(clip.Clip{Name: "kept", CreatedAt: created, FinalizedAt: finalized, Finalized: true})
		if wc.ExpiresAt != "" {
			t.Errorf("expires = %q, want empty (omitted)", wc.ExpiresAt)
		}
		if wc.Filename != "" {
			t.Errorf("filename = %q, want empty", wc.Filename)
		}
	})
}

// TestStatusRecorderUnwrap pins that http.ResponseController reaches the real connection through
// the recorder. Without Unwrap, Flush and the deadline setters would silently report "not
// supported" and the streaming wrappers would do nothing.
func TestStatusRecorderUnwrap(t *testing.T) {
	probe := &ctlProbe{}
	sr := &statusRecorder{rw: probe}
	ctl := http.NewResponseController(sr)
	if err := ctl.Flush(); err != nil {
		t.Errorf("Flush through Unwrap: %v", err)
	}
	if err := ctl.SetWriteDeadline(time.Now()); err != nil {
		t.Errorf("SetWriteDeadline through Unwrap: %v", err)
	}
	if err := ctl.SetReadDeadline(time.Now()); err != nil {
		t.Errorf("SetReadDeadline through Unwrap: %v", err)
	}
	if !probe.flushed {
		t.Error("Flush did not reach the underlying writer")
	}
}

// ctlProbe is a ResponseWriter that also supports the controller features, so a test can confirm
// a controller reaches it through a wrapper.
type ctlProbe struct {
	hdr      http.Header
	flushed  bool
	rdl, wdl time.Time
}

func (c *ctlProbe) Header() http.Header {
	if c.hdr == nil {
		c.hdr = http.Header{}
	}
	return c.hdr
}
func (c *ctlProbe) Write(b []byte) (int, error)        { return len(b), nil }
func (c *ctlProbe) WriteHeader(int)                    {}
func (c *ctlProbe) Flush()                             { c.flushed = true }
func (c *ctlProbe) SetReadDeadline(t time.Time) error  { c.rdl = t; return nil }
func (c *ctlProbe) SetWriteDeadline(t time.Time) error { c.wdl = t; return nil }
