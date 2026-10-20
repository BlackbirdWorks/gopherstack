package gcprest

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/http"
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

	rest := &http.Server{Handler: New(srv, conn).Handler(), ReadHeaderTimeout: readHeaderTimeout}
	sp := newSplitter(l, rest)

	go sp.run()

	return srv.Serve(sp)
}
