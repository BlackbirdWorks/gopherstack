package gcprest

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const (
	readHeaderTimeout = 10 * time.Second
	pipeBuffer        = 1 << 20
)

// Server holds the REST routes derived from a gRPC server's registered services.
type Server struct {
	conn   *grpc.ClientConn
	routes []*route
}

// New indexes every annotated unary and server-streaming method registered on srv and returns a Server
// that calls them over conn.
func New(srv *grpc.Server, conn *grpc.ClientConn) *Server {
	s := &Server{conn: conn}
	s.addServices(srv)

	return s
}

// Serve serves srv's gRPC API and its REST/JSON transcoding on l, telling them apart by the HTTP/2
// connection preface. REST calls reach the handlers through an in-memory pipe, never the network.
// Stopping srv closes l and the REST server with it.
func Serve(srv *grpc.Server, l net.Listener) error {
	return ServeWithFallback(srv, l, nil)
}

// ServeWithFallback is Serve with one extra HTTP handler: requests under /v2 (the Docker Registry API) go to
// fallback instead of the REST transcoder. A nil fallback behaves like Serve.
func ServeWithFallback(srv *grpc.Server, l net.Listener, fallback http.Handler) error {
	return ServeWithExtras(srv, l, fallback)
}

// Extra routes requests whose URL path Match accepts to Handler instead of the REST transcoder.
type Extra struct {
	Match   func(path string) bool
	Handler http.Handler
}

// ServeWithExtras is ServeWithFallback with additional path-matched HTTP handlers.
func ServeWithExtras(srv *grpc.Server, l net.Listener, fallback http.Handler, extras ...Extra) error {
	pipe := bufconn.Listen(pipeBuffer)

	conn, err := grpc.NewClient(
		"passthrough:///gcprest",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return pipe.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32), grpc.MaxCallSendMsgSize(math.MaxInt32)),
	)
	if err != nil {
		return fmt.Errorf("gcprest: dial in-process: %w", err)
	}

	defer func() { _ = conn.Close() }()

	go func() { _ = srv.Serve(pipe) }()

	rest := &http.Server{
		Handler:           withExtras(withFallback(New(srv, conn).Handler(), fallback), extras),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	sp := newSplitter(l, rest)

	go sp.run()

	return srv.Serve(sp)
}

func withFallback(rest, fallback http.Handler) http.Handler {
	if fallback == nil {
		return rest
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2" || strings.HasPrefix(r.URL.Path, "/v2/") {
			fallback.ServeHTTP(w, r)

			return
		}

		rest.ServeHTTP(w, r)
	})
}

func withExtras(next http.Handler, extras []Extra) http.Handler {
	if len(extras) == 0 {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, e := range extras {
			if e.Match(r.URL.Path) {
				e.Handler.ServeHTTP(w, r)

				return
			}
		}

		next.ServeHTTP(w, r)
	})
}
