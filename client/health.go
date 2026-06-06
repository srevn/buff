package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/srevn/buff/wire"
)

// Health is the server's liveness-and-capability report from /health. Unlike the content methods
// there is no domain type behind it — it is purely operational — so it is decoded directly here.
// Features is the capability list a caller checks before relying on an optional server feature.
type Health struct {
	Status   string   `json:"status"`
	Version  string   `json:"version"`
	API      []string `json:"api"`
	Features []string `json:"features"`
}

// Health probes the server's unversioned /health endpoint. It is the basis for a version or
// capability display, and a cheap liveness check. A non-2xx reply becomes a typed error through the
// reverse map, the same as any other request.
func (c *Client) Health(ctx context.Context) (Health, error) {
	resp, err := c.do(ctx, http.MethodGet, c.base+wire.PathHealth, nil, nil)
	if err != nil {
		return Health{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return Health{}, responseError(resp)
	}
	defer drain(resp.Body)
	var h Health
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		return Health{}, fmt.Errorf("buff: decoding health: %w", err)
	}
	return h, nil
}
