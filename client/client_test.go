package client_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/srevn/buff/client"
	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
)

// TestNewRejects pins the base-URL validation New performs at construction, so a config typo
// becomes a clear error there rather than a corrupt request URL later. A scheme that is not http
// or https, a missing host, and a query or fragment — each of which would splice into the middle
// of every request URL, since the path and escaped name are appended to the raw base — are all
// rejected. A well-formed URL, including one with a trailing slash to trim, is accepted.
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

// TestHost pins what Host reports: the base URL's host with its port when one is named, and never
// its userinfo — Host exists for display, so a Basic-auth credential in the base must not reach a
// caller through it. A path prefix and an IPv6 literal pass through as the authority they are.
func TestHost(t *testing.T) {
	for _, tc := range []struct{ base, want string }{
		{"http://host:8080", "host:8080"},
		{"https://example.com/prefix/", "example.com"},
		{"http://user:secret@host:8080", "host:8080"},
		{"http://[::1]:9090", "[::1]:9090"},
	} {
		c, err := client.New(tc.base, nil)
		if err != nil {
			t.Fatalf("New(%q): %v", tc.base, err)
		}
		if got := c.Host(); got != tc.want {
			t.Errorf("New(%q).Host() = %q, want %q", tc.base, got, tc.want)
		}
	}
}

// TestConnectHook pins the milestone the hook exists for: a read parked at the server — a waiting
// Get on an absent clip — has already reported its connection while no response header has come
// back, which is what lets a caller tell that park from a dial still in progress. A write then
// releases the park, and Get returns the clip it was waiting for. The Get runs under a context the
// test cancels on the way out, registered after the server's own teardown so it runs first: a
// failing case would otherwise leave the read parked, and the server's Close waits on it forever.
func TestConnectHook(t *testing.T) {
	_, c := memClient(t, store.Config{})
	parent, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	connected := make(chan struct{})
	var once sync.Once
	ctx := client.WithConnectHook(parent, func() { once.Do(func() { close(connected) }) })
	got := make(chan error, 1)
	go func() {
		rc, _, err := c.Get(ctx, "later", client.GetOpts{Wait: true})
		if err == nil {
			rc.Close()
		}
		got <- err
	}()

	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("a Get parked at the server reported no connection")
	}
	select {
	case err := <-got:
		t.Fatalf("Get returned (%v) with nothing written, want it parked at the server", err)
	default:
	}
	if _, err := c.Put(context.Background(), "later", strings.NewReader("x"), clip.Meta{}, client.PutOpts{}); err != nil {
		t.Fatal(err)
	}
	if err := <-got; err != nil {
		t.Errorf("the released Get = %v, want the clip", err)
	}
}
