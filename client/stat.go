package client

import (
	"context"
	"net/http"

	"github.com/srevn/buff/clip"
)

// Stat returns a clip's metadata without reading its bytes and without ever claiming it. It
// is a HEAD, which the server routes to its non-claiming path, so a metadata probe of a
// consume-once clip does not spend its one delivery: a finalized consume-once clip reports
// ConsumeOnce true so a caller can warn that a Get will consume it, while a live or
// already-consumed one is invisible and comes back as not-found or consumed. A live clip
// reports Finalized false and no size; a finalized one carries its size and any expiry.
func (c *Client) Stat(ctx context.Context, name string) (clip.Clip, error) {
	resp, err := c.do(ctx, http.MethodHead, c.clipURL(name), nil, nil)
	if err != nil {
		return clip.Clip{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return clip.Clip{}, responseError(resp)
	}
	defer drain(resp.Body)
	return parseClip(name, resp.Header), nil
}
