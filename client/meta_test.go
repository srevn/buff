package client_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/srevn/buff/client"
	"github.com/srevn/buff/clip"
	"github.com/srevn/buff/store"
	"github.com/srevn/buff/wire"
)

// TestForeignServerMetaNormalized proves the read-side decode defends against a non-buff peer that
// echoes a file-scoped field on a kind that cannot carry it — a bytes clip announcing a filename and
// an executable bit. Both read decodes clean it as the response becomes a domain clip, so the illegal
// shape never reaches a sink or a renderer. The two subtests cover the two decodes: header decode on
// a HEAD (parseClip, shared with GET) and JSON decode on a list (toClip). The forged servers send a
// response buff's own server never would, which is exactly the peer this normalize exists to guard.
func TestForeignServerMetaNormalized(t *testing.T) {
	ctx := context.Background()
	clean := clip.Meta{Kind: clip.KindBytes}

	t.Run("header decode", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := w.Header()
			h.Set(wire.HeaderKind, "bytes")
			h.Set(wire.HeaderExecutable, "true")
			h.Set(wire.HeaderFilename, "evil")
			h.Set(wire.HeaderFinalized, "true")
			h.Set(wire.HeaderSize, "1")
			w.WriteHeader(http.StatusOK)
		}))
		defer ts.Close()
		cl, err := newClient(t, ts.URL).Stat(ctx, "x")
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if cl.Meta != clean {
			t.Errorf("Stat meta from a foreign server = %+v, want %+v", cl.Meta, clean)
		}
	})

	t.Run("list decode", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, `{"clips":[{"name":"x","generation":"g","kind":"bytes","filename":"evil","executable":true,"size":1}]}`)
		}))
		defer ts.Close()
		clips, err := newClient(t, ts.URL).List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(clips) != 1 {
			t.Fatalf("List returned %d clips, want 1", len(clips))
		}
		if clips[0].Meta != clean {
			t.Errorf("List meta from a foreign server = %+v, want %+v", clips[0].Meta, clean)
		}
	})
}

// TestPutReturnsNormalizedMeta pins Put's return contract: a caller-built Meta with a file-scoped
// field on a non-file kind is normalized before both the wire and the returned clip, so the clip
// handed back echoes what the server actually stored rather than the illegal value the caller passed.
// A follow-up Stat confirms the agreement — the same normalized shape on both sides.
func TestPutReturnsNormalizedMeta(t *testing.T) {
	ctx := context.Background()
	_, c := memClient(t, store.Config{})
	clean := clip.Meta{Kind: clip.KindBytes}

	put, err := c.Put(ctx, "raw", bytes.NewReader([]byte("x")), clip.Meta{Kind: clip.KindBytes, Filename: "evil", Executable: true}, client.PutOpts{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if put.Meta != clean {
		t.Errorf("Put returned %+v, want %+v", put.Meta, clean)
	}
	st, err := c.Stat(ctx, "raw")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.Meta != clean {
		t.Errorf("server stored %+v, want %+v — returned clip disagrees with stored", st.Meta, clean)
	}
}
