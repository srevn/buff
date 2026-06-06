package api_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/srevn/buff/api"
	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
	"github.com/srevn/buff/wire"
)

// readFullTimeout reads exactly len(buf) bytes or fails — with a ceiling, so a broken live flush
// fails the test instead of hanging it. A timeout leaks the inner goroutine, which is acceptable
// for a failing test.
func readFullTimeout(t *testing.T, r io.Reader, buf []byte, d time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() { _, err := io.ReadFull(r, buf); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("read of %d bytes: %v", len(buf), err)
		}
	case <-time.After(d):
		t.Fatalf("read of %d bytes timed out after %v (live flush broken?)", len(buf), d)
	}
}

// TestGetLiveFraming follows a clip that is still being written. The first chunk must arrive before
// the writer finishes — proving the per-chunk flush — and a clean finalize must set the Buff-
// Status: complete trailer on a chunked, Content-Length-less response. Bytes are produced directly
// on the shared store for precise mid-stream control while the read happens over HTTP.
func TestGetLiveFraming(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	ctx := context.Background()

	part1, part2 := []byte("part-one;"), []byte("part-two")
	wr, err := st.Create(ctx, "live", clip.Meta{Kind: clip.KindBytes}, store.PutOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wr.Write(part1); err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(ts.URL + "/v1/clips/live")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ContentLength != -1 || len(resp.TransferEncoding) == 0 {
		t.Errorf("live response should be chunked with no Content-Length, got len=%d te=%v", resp.ContentLength, resp.TransferEncoding)
	}
	if got := resp.Header.Get(wire.HeaderFinalized); got != "false" {
		t.Errorf("Buff-Finalized = %q, want false", got)
	}
	if got := resp.Header.Get(wire.HeaderSize); got != "" {
		t.Errorf("live response must omit Buff-Size, got %q", got)
	}

	// The first chunk is readable before the writer produces any more — the live promise.
	got1 := make([]byte, len(part1))
	readFullTimeout(t, resp.Body, got1, 3*time.Second)
	if !bytes.Equal(got1, part1) {
		t.Fatalf("first chunk = %q, want %q", got1, part1)
	}

	if _, err := wr.Write(part2); err != nil {
		t.Fatal(err)
	}
	if err := wr.Close(); err != nil {
		t.Fatal(err)
	}

	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read rest: %v", err)
	}
	if !bytes.Equal(rest, part2) {
		t.Errorf("rest = %q, want %q", rest, part2)
	}
	if got := resp.Trailer.Get(wire.HeaderStatus); got != wire.StatusComplete {
		t.Errorf("completion trailer = %q, want %q", got, wire.StatusComplete)
	}
}

// TestGetLiveHeadersBeforeBody is the regression for the live-attach contract: a GET of a clip
// that is still being written returns its status and metadata as soon as it attaches, before the
// producer has written a single byte. The handler flushes the live response's headers on attach,
// so a consumer can begin following an idle live clip rather than blocking until the first byte
// happens to arrive. The body then still follows — bytes written after the attach arrive, to a
// clean EOF on finalize.
func TestGetLiveHeadersBeforeBody(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	ctx := context.Background()

	// The clip exists and is live but empty: no bytes have been written.
	wr, err := st.Create(ctx, "live", clip.Meta{Kind: clip.KindBytes}, store.PutOpts{})
	if err != nil {
		t.Fatal(err)
	}

	// http.Get returns once the response headers are read. Run it off the test goroutine so a
	// regression — headers withheld until the first byte — is a bounded timeout, not a hang.
	type result struct {
		resp *http.Response
		err  error
	}
	got := make(chan result, 1)
	go func() {
		resp, err := http.Get(ts.URL + "/v1/clips/live")
		got <- result{resp, err}
	}()

	var resp *http.Response
	select {
	case r := <-got:
		if r.err != nil {
			t.Fatalf("GET of an empty live clip: %v", r.err)
		}
		resp = r.resp
	case <-time.After(3 * time.Second):
		t.Fatal("GET of an empty live clip blocked waiting for the first byte; headers were not flushed on attach")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if resp.ContentLength != -1 || len(resp.TransferEncoding) == 0 {
		t.Errorf("live response should be chunked with no Content-Length, got len=%d te=%v", resp.ContentLength, resp.TransferEncoding)
	}
	if got := resp.Header.Get(wire.HeaderFinalized); got != "false" {
		t.Errorf("Buff-Finalized = %q, want false (attached while live)", got)
	}
	if got := resp.Header.Get(wire.HeaderKind); got != string(clip.KindBytes) {
		t.Errorf("Buff-Kind = %q, want bytes (metadata present on attach)", got)
	}
	if got := resp.Header.Get(wire.HeaderSize); got != "" {
		t.Errorf("live response must omit Buff-Size, got %q", got)
	}

	// The follow still works: bytes produced after the attach arrive, then a clean EOF and the
	// completion trailer on finalize.
	payload := []byte("written after attach")
	if _, err := wr.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := wr.Close(); err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read live body: %v", err)
	}
	if !bytes.Equal(body, payload) {
		t.Errorf("body = %q, want %q", body, payload)
	}
	if got := resp.Trailer.Get(wire.HeaderStatus); got != wire.StatusComplete {
		t.Errorf("completion trailer = %q, want %q", got, wire.StatusComplete)
	}
}

// TestGetLiveAbort tears a live stream mid-follow. The torn stream must reach the client as a read
// error with no completion trailer — the property that stops a truncated follow from ever looking
// complete.
func TestGetLiveAbort(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	ctx := context.Background()

	part1 := []byte("partial-data-")
	wr, err := st.Create(ctx, "torn", clip.Meta{Kind: clip.KindBytes}, store.PutOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wr.Write(part1); err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(ts.URL + "/v1/clips/torn")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got1 := make([]byte, len(part1))
	readFullTimeout(t, resp.Body, got1, 3*time.Second)
	if !bytes.Equal(got1, part1) {
		t.Fatalf("first chunk = %q, want %q", got1, part1)
	}

	if err := wr.Abort(); err != nil {
		t.Fatal(err)
	}

	_, err = io.ReadAll(resp.Body)
	if err == nil {
		t.Error("expected a truncation error after abort, got a clean read")
	}
	if got := resp.Trailer.Get(wire.HeaderStatus); got == wire.StatusComplete {
		t.Error("an aborted stream must not carry the complete trailer")
	}
}

// TestConsumeOnce proves at-most-once delivery over HTTP: a finalized consume-once clip is
// delivered to the first GET and then gone — a second GET never re-delivers it.
func TestConsumeOnce(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	payload := []byte("the one secret")
	if resp := put(t, ts, "secret", payload, map[string]string{wire.HeaderConsume: "1"}); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d", resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	resp := do(t, http.MethodGet, ts.URL+"/v1/clips/secret", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first GET = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get(wire.HeaderConsume); got != "true" {
		t.Errorf("Buff-Consume = %q, want true", got)
	}
	if got := readBody(t, resp); !bytes.Equal(got, payload) {
		t.Errorf("first GET body = %q, want %q", got, payload)
	}

	// Reading and closing the first delivery ran its cleanup; a second GET cannot re-deliver. Either
	// 404 (cleaned up) or 410 (mid-cleanup) is correct — both deny a second delivery.
	resp2 := do(t, http.MethodGet, ts.URL+"/v1/clips/secret", nil, nil)
	body2 := readBody(t, resp2)
	if resp2.StatusCode != http.StatusNotFound && resp2.StatusCode != http.StatusGone {
		t.Errorf("second GET = %d, want 404 or 410", resp2.StatusCode)
	}
	if bytes.Equal(body2, payload) {
		t.Error("consume-once secret delivered twice")
	}
}

// TestHeadNeverConsumes is the load-bearing consume-once guard: a HEAD probe (and any number of
// them) must not claim the clip, so a later GET still delivers it. If HEAD were routed into the GET
// handler it would claim the secret on a metadata probe — exactly what this rejects.
func TestHeadNeverConsumes(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	payload := []byte("still here")
	if resp := put(t, ts, "probe", payload, map[string]string{wire.HeaderConsume: "1"}); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d", resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	for i := range 2 {
		resp := do(t, http.MethodHead, ts.URL+"/v1/clips/probe", nil, nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("HEAD %d = %d, want 200", i, resp.StatusCode)
		}
		if got := resp.Header.Get(wire.HeaderConsume); got != "true" {
			t.Errorf("HEAD Buff-Consume = %q, want true", got)
		}
		readBody(t, resp)
	}

	// The clip survived the probes: the GET still delivers it.
	resp := do(t, http.MethodGet, ts.URL+"/v1/clips/probe", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET after HEADs = %d, want 200 (HEAD must not consume)", resp.StatusCode)
	}
	if got := readBody(t, resp); !bytes.Equal(got, payload) {
		t.Errorf("GET body = %q, want %q", got, payload)
	}
	// And now it is consumed.
	resp2 := do(t, http.MethodGet, ts.URL+"/v1/clips/probe", nil, nil)
	readBody(t, resp2)
	if resp2.StatusCode == http.StatusOK {
		t.Error("clip still readable after its one GET")
	}
}

// TestConsumeOnceLiveInvisible checks that a consume-once clip is invisible while live — a reader
// during its upload gets a not-found, never a chance to attach to and race for the secret — and
// becomes readable once finalized.
func TestConsumeOnceLiveInvisible(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	ctx := context.Background()
	wr, err := st.Create(ctx, "livesecret", clip.Meta{Kind: clip.KindBytes}, store.PutOpts{ConsumeOnce: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wr.Write([]byte("secret in flight")); err != nil {
		t.Fatal(err)
	}

	if resp := do(t, http.MethodGet, ts.URL+"/v1/clips/livesecret", nil, nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET of live consume-once = %d, want 404 (invisible)", resp.StatusCode)
		resp.Body.Close()
	} else {
		resp.Body.Close()
	}
	if resp := do(t, http.MethodHead, ts.URL+"/v1/clips/livesecret", nil, nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("HEAD of live consume-once = %d, want 404 (invisible)", resp.StatusCode)
		resp.Body.Close()
	} else {
		resp.Body.Close()
	}

	if err := wr.Close(); err != nil {
		t.Fatal(err)
	}
	resp := do(t, http.MethodGet, ts.URL+"/v1/clips/livesecret", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET after finalize = %d, want 200", resp.StatusCode)
	}
	readBody(t, resp)
}

// TestReplacementInvisible checks that a finalized value stands while a replacement is being
// written: a reader sees the old complete value, never the in-progress one, until the new
// generation finalizes.
func TestReplacementInvisible(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	ctx := context.Background()

	if resp := put(t, ts, "slot", []byte("AAAA"), nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT A = %d", resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	// Open a live replacement directly on the store; the finalized A still stands.
	wr, err := st.Create(ctx, "slot", clip.Meta{Kind: clip.KindBytes}, store.PutOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wr.Write([]byte("BBBBBB")); err != nil {
		t.Fatal(err)
	}
	resp := do(t, http.MethodGet, ts.URL+"/v1/clips/slot", nil, nil)
	if got := readBody(t, resp); string(got) != "AAAA" {
		t.Errorf("GET during replacement = %q, want the standing value AAAA", got)
	}

	// After B finalizes it becomes the readable value.
	if err := wr.Close(); err != nil {
		t.Fatal(err)
	}
	resp = do(t, http.MethodGet, ts.URL+"/v1/clips/slot", nil, nil)
	if got := readBody(t, resp); string(got) != "BBBBBB" {
		t.Errorf("GET after replacement finalized = %q, want BBBBBB", got)
	}
}

// TestFinalizedFramingAcrossSizes pins the finalized arm's only completion proof — the exact
// Content-Length — at every size that matters, the guard that keeps the single load-bearing line
// from regressing silently. The decisive boundary is 2048→2049: a finalized body that overflows
// Go's response buffer (2048 bytes) mid-handler falls back to chunked, and the finalized arm
// declares no trailer, so without the explicit length a wholly-delivered clip larger than 2048
// bytes would arrive carrying no completion proof and read as torn. Every prior finalized-read
// test uses a sub-2048 payload, so that regression would ship green; this one would not. The larger
// sizes force the copy to span many server write cycles, confirming the declared length holds
// across them. A deterministic payload (byte i = byte(i)) makes the read assertion catch corruption
// or reordering, not merely a short count.
func TestFinalizedFramingAcrossSizes(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	for _, n := range []int{0, 1, 2048, 2049, (64 << 10) + 1, 320 << 10} {
		t.Run(fmt.Sprintf("size=%d", n), func(t *testing.T) {
			payload := make([]byte, n)
			for i := range payload {
				payload[i] = byte(i)
			}
			name := fmt.Sprintf("clip-%d", n)
			if resp := put(t, ts, name, payload, nil); resp.StatusCode != http.StatusOK {
				resp.Body.Close()
				t.Fatalf("PUT = %d, want 200", resp.StatusCode)
			} else {
				resp.Body.Close()
			}

			resp := do(t, http.MethodGet, ts.URL+wire.PathClips+"/"+name, nil, nil)
			defer resp.Body.Close()
			if resp.ContentLength != int64(n) {
				t.Errorf("Content-Length = %d, want %d", resp.ContentLength, n)
			}
			if len(resp.TransferEncoding) != 0 {
				t.Errorf("TransferEncoding = %v, want [] — a finalized read is never chunked", resp.TransferEncoding)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if !bytes.Equal(body, payload) {
				t.Errorf("body = %d bytes, want %d identical bytes", len(body), n)
			}
			if got := resp.Trailer.Get(wire.HeaderStatus); got != "" {
				t.Errorf("finalized read must carry no trailer, got Buff-Status %q", got)
			}
		})
	}
}

// TestGetLiveEmptyFinalize is the empty-clip twin of TestGetLiveFraming: a clip attached while live
// and then finalized with no bytes written. It must frame chunked (no Content-Length), deliver a
// clean zero-byte read, and still carry the Buff-Status: complete trailer — the completion proof
// a live stream carries even when it carried no data. The GET runs off the test goroutine so a
// regression that withholds the response until a first byte which never comes is a bounded timeout,
// not a hang.
func TestGetLiveEmptyFinalize(t *testing.T) {
	st := store.NewMemory(store.Config{})
	ts := newServer(t, st, api.Options{})
	ctx := context.Background()

	wr, err := st.Create(ctx, "emptylive", clip.Meta{Kind: clip.KindBytes}, store.PutOpts{})
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		resp *http.Response
		err  error
	}
	got := make(chan result, 1)
	go func() {
		resp, err := http.Get(ts.URL + wire.PathClips + "/emptylive")
		got <- result{resp, err}
	}()

	var resp *http.Response
	select {
	case r := <-got:
		if r.err != nil {
			t.Fatalf("GET of an empty live clip: %v", r.err)
		}
		resp = r.resp
	case <-time.After(3 * time.Second):
		t.Fatal("GET of an empty live clip blocked on a first byte; headers were not flushed on attach")
	}
	defer resp.Body.Close()

	if resp.ContentLength != -1 || len(resp.TransferEncoding) == 0 {
		t.Errorf("empty live response should be chunked with no Content-Length, got len=%d te=%v", resp.ContentLength, resp.TransferEncoding)
	}

	// Finalize with zero writes; the follow then reaches a clean EOF and the completion trailer is
	// promoted on the nil-error copy tail.
	if err := wr.Close(); err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read empty live body: %v", err)
	}
	if len(body) != 0 {
		t.Errorf("body = %q, want a clean zero-byte read", body)
	}
	if got := resp.Trailer.Get(wire.HeaderStatus); got != wire.StatusComplete {
		t.Errorf("completion trailer = %q, want %q", got, wire.StatusComplete)
	}
}
