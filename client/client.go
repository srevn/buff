// Package client is buff's transport-only client for the /v1 content protocol. It is
// the exact mirror of the server's HTTP edge: where the server is the only place a
// domain error becomes an HTTP status, this is the only place an HTTP response becomes
// a typed domain error, and where the server frames a stream's completion, this is the
// place that reads that framing back and refuses to mistake a truncated read for a
// finished one.
//
// It speaks only the wire — it tars nothing, extracts nothing, and decides no output:
// an archive clip round-trips through it as opaque bytes, and turning those into files
// is a layer above. It imports only the shared domain vocabulary and the protocol
// constants, never the server, so the two sides agree through the wire alone and the
// client never couples to a particular server implementation.
package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/srevn/buff/wire"
)

// Client is a handle to one buff server. It is safe for concurrent use: it holds only
// an immutable base URL and an *http.Client, which is itself concurrency-safe. Build one
// with New and call the five content methods plus the optional Health probe.
type Client struct {
	base string       // validated and canonicalised, with any trailing slash trimmed, e.g. "http://host:8080"
	http *http.Client // no whole-request Timeout — see newHTTPClient
}

// PutOpts carries the write-time choices a Put may set. It is the client's own type
// rather than the store's: the two never share a type, so the client stays a pure wire
// peer of the server. The fields map one-to-one onto the Buff-TTL, Buff-Keep, and
// Buff-Consume request headers.
type PutOpts struct {
	TTL         time.Duration // retention from finalize; zero omits the header, asking for the server default
	Keep        bool          // never expire, overriding any TTL
	ConsumeOnce bool          // deliver to at most one reader, then the server destroys it
}

// New builds a Client for a server reachable at baseURL (for example "http://host:8080").
// A nil hc installs a built-in client tuned for streaming — crucially with no
// whole-request timeout, which would otherwise kill a long upload or a live follow. A
// caller-supplied hc is used as given; do not set its Timeout, for the same reason, and
// rely on the request context for cancellation instead. The base URL must be an absolute
// http or https URL with a host; anything else is rejected here rather than failing later
// on the first request.
func New(baseURL string, hc *http.Client) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("buff: invalid base URL %q: %w", baseURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("buff: base URL must be http or https: %q", baseURL)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("buff: base URL has no host: %q", baseURL)
	}
	// A query or fragment has no place in a base URL and would splice into the middle of
	// every request URL, since the path and escaped name are appended to the raw string. The
	// base may carry a path prefix for a server mounted under one, but never these.
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("buff: base URL must not carry a query or fragment: %q", baseURL)
	}
	if hc == nil {
		hc = newHTTPClient()
	}
	// Store the re-serialised URL, not the raw input: u.String() escapes any unescaped byte in
	// a path prefix (a space, a non-ASCII rune) so the base is a clean prefix for every
	// request URL, the boundary-canonicalisation clipURL already applies to a name with
	// PathEscape. It equals the raw input for an ordinary base and is never destructive —
	// userinfo and the path are preserved, only escaping is tightened — and is safe because
	// the base never carries a name, so no segment containment rides on it.
	return &Client{base: strings.TrimRight(u.String(), "/"), http: hc}, nil
}

// newHTTPClient returns the default transport for a buff client. It clones the standard
// transport — inheriting its connection-setup dial bound, idle-connection pool, and
// proxy handling — but leaves the client's whole-request Timeout at zero. A whole-request
// timeout bounds the entire exchange including the streamed body, so it would cut off a
// legitimate long upload or a live follow that lasts minutes; the dial bound is safe
// because it only covers establishing the connection, never the transfer, and the request
// context is the cancellation path for everything after.
func newHTTPClient() *http.Client {
	return &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
}

// clipURL builds the request URL for a named clip. The name is escaped as a single path
// segment: PathEscape encodes a slash to %2F and every other unsafe byte, so a name can
// never break out of its segment or inject into the path. The client does not pre-validate
// the name — the server is the sole authority on its namespace and rejects an invalid one
// as name_invalid — which avoids a second validation path that could drift from the
// server's.
func (c *Client) clipURL(name string) string {
	return c.base + wire.PathClips + "/" + url.PathEscape(name)
}

// do builds a request, sends it, and returns the response without reading its body — each
// method owns its body, since one streams and the others read a small reply. The only
// error do itself produces is a transport failure, wrapped as ErrUnreachable so the caller
// can distinguish "never reached the server" from any status the server actually returned.
// The underlying cause rides along under the same error so a context cancellation or a dial
// error stays inspectable.
func (c *Client) do(ctx context.Context, method, url string, body io.Reader, h http.Header) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err // only a malformed method or URL reaches here — a programming error, not transport
	}
	if h != nil {
		req.Header = h
	}
	resp, err := c.http.Do(req)
	if err != nil {
		// Render req.URL, not the raw url string: NewRequestWithContext having succeeded above
		// means req.URL is the parsed form, and Redacted() strips any userinfo password before it
		// reaches a terminal or a log. A base may legitimately carry Basic-auth userinfo — the
		// Transport applies it — so the credential is a working feature to hide at render time,
		// the same refusal to echo untrusted or sensitive bytes that drains an error body rather
		// than returning it.
		return nil, fmt.Errorf("%s %s: %w: %w", method, req.URL.Redacted(), ErrUnreachable, err)
	}
	return resp, nil
}

// Delete removes a clip's finalized generation. A 204 is success; a name with only a live
// generation, or none at all, comes back as ErrNotFound through the reverse error map. It
// never disturbs a generation still being written — that is the server's contract, not a
// thing the client could affect.
func (c *Client) Delete(ctx context.Context, name string) error {
	resp, err := c.do(ctx, http.MethodDelete, c.clipURL(name), nil, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusNoContent {
		return responseError(resp)
	}
	defer drain(resp.Body)
	return nil
}
