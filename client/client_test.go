package client_test

import (
	"testing"

	"github.com/srevn/buff/client"
)

// TestNewRejects pins the base-URL validation New performs at construction, so a config typo
// becomes a clear error there rather than a corrupt request URL later. A scheme that is not
// http or https, a missing host, and a query or fragment — each of which would splice into
// the middle of every request URL, since the path and escaped name are appended to the raw
// base — are all rejected. A well-formed URL, including one with a trailing slash to trim, is
// accepted.
func TestNewRejects(t *testing.T) {
	bad := []struct{ name, url string }{
		{"empty", ""},
		{"no scheme", "host:8080"},
		{"wrong scheme", "ftp://host"},
		{"no host", "http://"},
		{"query", "http://host:8080?x=1"},
		{"fragment", "http://host:8080#frag"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := client.New(tc.url, nil); err == nil {
				t.Errorf("New(%q) = nil error, want a rejection", tc.url)
			}
		})
	}

	for _, ok := range []string{"http://host:8080", "http://host:8080/", "https://example.com/prefix"} {
		if _, err := client.New(ok, nil); err != nil {
			t.Errorf("New(%q) = %v, want it accepted", ok, err)
		}
	}
}
