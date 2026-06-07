// Package api is buff's HTTP edge: a thin, stateless skin over a store.Store that speaks the /v1
// content protocol and the unversioned /health probe. It is the only place a domain error becomes
// an HTTP status, and the only place the framing, trailer, and abort contract lives — everything
// below it is transport-agnostic.
//
// The server constructs no store; it takes the Store interface and relays opaque bytes through it,
// so a different storage medium is a wiring change elsewhere, never a change here. Three things
// make the relay correct under streaming. A read's framing is fixed once, at header time, from
// whether the target is finalized: a finalized clip is sent with an exact Content-Length, a live
// one chunked with a completion trailer — and the followable buffer returns a clean end only on
// a clean finalize, so a torn stream becomes an aborted connection and can never reach a client
// looking complete. A write is acknowledged only after its durable commit returns. And clip bytes
// only ever move through a pooled copy buffer, never onto the heap in bulk.
package api

import (
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/srevn/buff/store"
	"github.com/srevn/buff/wire"
)

// Buffer and default sizes. The copy buffer is large enough that fd-to-fd streaming makes few
// syscalls yet small enough to pool cheaply per in-flight request.
const (
	copyBufSize    = 64 << 10
	defaultVersion = "buff/dev"

	// Safety-timeout defaults. Unlike the policy knobs, a zero here must not mean "disabled": a zero
	// ReadHeaderTimeout is a slowloris invitation, and a zero UploadIdle leaves a connected-but-
	// stalled peer unbounded on every streaming path — the same threat on the body and the response —
	// so the constructor substitutes these.
	defaultReadHeaderTimeout = 10 * time.Second
	defaultIdleTimeout       = 120 * time.Second
	defaultMaxHeaderBytes    = 1 << 20
	defaultUploadIdle        = 30 * time.Second
)

// Options configures a Server. Two kinds of knob live here, with deliberately opposite zero
// behaviour. The policy knobs — UploadMax, AccessLog — are zero-means-disabled, so the zero Options
// is a frictionless server for tests and embedding and a server's environment layer supplies real
// values. The safety defaults — ReadHeaderTimeout, IdleTimeout, MaxHeaderBytes, and UploadIdle
// — are zero-means-defaulted, because a zero there would unharden the server rather than relax a
// policy: UploadIdle is the standing stall bound on every streaming path and cannot be disabled at
// all, only the absolute UploadMax is opt-out. Logger and Version default when unset.
type Options struct {
	UploadIdle time.Duration // standing idle deadline for a stalled upload read or download write; 0 (or any non-positive) means the built-in default, never disabled
	UploadMax  time.Duration // absolute cap on one upload's duration; 0 disables it — the only opt-out of the two upload bounds
	Logger     *slog.Logger  // 5xx causes and recovered panics at Error, plus access lines at Info when AccessLog; nil means slog.Default()
	Version    string        // /health version string; empty means a built-in default
	AccessLog  bool          // emit one structured access line per request at Info on Logger; zero ⇒ off

	ReadHeaderTimeout time.Duration // slowloris bound on headers; 0 means the built-in default
	IdleTimeout       time.Duration // keep-alive idle between requests; 0 means the default
	MaxHeaderBytes    int           // header-size bound; 0 means the default
}

// Server is the HTTP handler for the content API. It holds the store it relays to, the routed mux,
// the same mux wrapped in the recover backstop, and the copy-buffer pool every streaming handler
// draws from. Construct one with New and use it as an http.Handler directly, or call HTTPServer for
// a tuned *http.Server.
type Server struct {
	store   store.Store
	opt     Options
	mux     *http.ServeMux
	h       http.Handler
	bufPool sync.Pool
}

// Server is an http.Handler, checked at compile time.
var _ http.Handler = (*Server)(nil)

// New builds a Server over a store, filling unset Options with their defaults, registering the
// routes, and wrapping them in the recover backstop. The copy-buffer pool yields pointers to fixed-
// size buffers, so returning one to the pool never allocates.
func New(s store.Store, o Options) *Server {
	srv := &Server{
		store:   s,
		opt:     withDefaults(o),
		mux:     http.NewServeMux(),
		bufPool: sync.Pool{New: func() any { b := make([]byte, copyBufSize); return &b }},
	}
	srv.routes()
	srv.h = srv.recoverer(srv.mux)
	return srv
}

// withDefaults fills the unset fields of o. The policy knobs (UploadMax, AccessLog) keep their
// zero, meaning disabled; the safety timeouts, the logger, and the version take a built-in value
// when zero. UploadIdle is the one timeout this server interprets itself rather than handing to
// net/http, and it is a safety bound, so it is defaulted from any non-positive value — never left
// in a disabled state.
func withDefaults(o Options) Options {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Version == "" {
		o.Version = defaultVersion
	}
	if o.ReadHeaderTimeout == 0 {
		o.ReadHeaderTimeout = defaultReadHeaderTimeout
	}
	if o.IdleTimeout == 0 {
		o.IdleTimeout = defaultIdleTimeout
	}
	if o.MaxHeaderBytes == 0 {
		o.MaxHeaderBytes = defaultMaxHeaderBytes
	}
	// arm() reads a non-positive idle as "no deadline", so coercing every non-positive value here
	// — not only zero, as for the net/http-owned timeouts above — is what keeps this standing stall
	// bound un-disable-able: two states only, the built-in default or a positive value.
	if o.UploadIdle <= 0 {
		o.UploadIdle = defaultUploadIdle
	}
	return o
}

// routes registers the six handlers. HEAD has its own route, not a fallthrough to GET: Go maps HEAD
// to a GET handler when no HEAD route exists, and routing a metadata probe into the GET handler
// would claim a consume-once clip. The explicit HEAD route takes precedence and keeps the probe on
// the non-claiming Stat path. The list and health routes carry no name wildcard.
func (s *Server) routes() {
	s.mux.HandleFunc("PUT "+wire.PathClips+"/{name}", s.put)
	s.mux.HandleFunc("GET "+wire.PathClips+"/{name}", s.get)
	s.mux.HandleFunc("HEAD "+wire.PathClips+"/{name}", s.head)
	s.mux.HandleFunc("DELETE "+wire.PathClips+"/{name}", s.delete)
	s.mux.HandleFunc("GET "+wire.PathClips, s.list)
	s.mux.HandleFunc("GET "+wire.PathHealth, s.health)
}

// ServeHTTP routes a request through the recover backstop and the mux.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.h.ServeHTTP(w, r) }

// HTTPServer returns an *http.Server bound to addr and tuned for streaming. The whole-request
// ReadTimeout and WriteTimeout are deliberately left zero — they would kill a long upload or a slow
// live follow — so a stall is bounded by the per-request idle deadlines and the request context
// instead, while only the header read, the keep-alive idle, and the header size are capped here.
// The lifecycle — BaseContext, ListenAndServe, Shutdown — is the caller's to wire.
func (s *Server) HTTPServer(addr string) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           s,
		ReadHeaderTimeout: s.opt.ReadHeaderTimeout,
		IdleTimeout:       s.opt.IdleTimeout,
		MaxHeaderBytes:    s.opt.MaxHeaderBytes,
	}
}

// itoa renders a byte count for a header value.
func itoa(n int64) string { return strconv.FormatInt(n, 10) }
