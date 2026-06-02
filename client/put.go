package client

import (
	"context"
	"io"
	"net/http"

	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/wire"
)

// Put streams r to a clip and returns the finalized clip on success. The body is handed
// straight to the request so it streams to the server and never accumulates in memory — the
// client buffers no clip bytes. A 200 means stored and durable: the server sends it only
// after its durable commit returns, so a nil error here is a real acknowledgement, not an
// optimistic one.
//
// A non-2xx response is authoritative even if the body did not finish streaming. When the
// server enforces a cap mid-upload it replies and stops reading; net/http prefers that
// already-arrived response over the resulting body-write failure, so the status is what the
// caller honours — a real ErrTooLarge or ErrNoSpace, not a bare connection reset. Only a
// transport failure with no response at all is reported as unreachable. A caller streaming
// from a source it also drives (a pipe it fills concurrently) should prefer its own source
// error over the transport error this returns, since the client, being transport-only,
// cannot tell a source failure from a network one.
//
// The returned clip echoes what the caller set — the metadata and the consume-once choice,
// both of which a 200 confirms the server accepted — plus the generation and size the server
// assigns. It deliberately leaves ExpiresAt zero: a PUT response carries only the generation
// and the size, never the absolute expiry the server computed, so a caller that needs the
// expiry reads it with a follow-up Stat rather than trusting a fabricated one here.
func (c *Client) Put(ctx context.Context, name string, r io.Reader, m clip.Meta, o PutOpts) (clip.Clip, error) {
	resp, err := c.do(ctx, http.MethodPut, c.clipURL(name), r, encodeHeaders(m, o))
	if err != nil {
		return clip.Clip{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return clip.Clip{}, responseError(resp)
	}
	defer drain(resp.Body)
	return clip.Clip{
		Name:        name,
		Meta:        m,
		Generation:  resp.Header.Get(wire.HeaderGeneration),
		Size:        atoi64(resp.Header.Get(wire.HeaderSize)),
		ConsumeOnce: o.ConsumeOnce,
		Finalized:   true,
	}, nil
}
