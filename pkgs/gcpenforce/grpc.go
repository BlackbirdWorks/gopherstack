package gcpenforce

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Target describes the API one gRPC server or HTTP listener serves.
type Target struct {
	// ProjectFields maps a gRPC full method to field paths holding the project when the generic scan is wrong.
	ProjectFields map[string][]string
	// Host is the API host used for services without a default_host option, and for REST.
	Host string
	// QueryOnly takes the REST project from the query string only.
	QueryOnly bool
}

// ServerOptions returns the interceptors for a gRPC server serving t, or nil when enforcement is off in ctx.
func ServerOptions(ctx context.Context, t Target) []grpc.ServerOption {
	g := GateFrom(ctx)
	if g == nil {
		return nil
	}

	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unary(g, t)),
		grpc.ChainStreamInterceptor(stream(g, t)),
	}
}

// NewServer is grpc.NewServer with the enforcement interceptors added when ctx carries a Gate.
func NewServer(ctx context.Context, host string, opts ...grpc.ServerOption) *grpc.Server {
	return NewServerFor(ctx, Target{Host: host}, opts...)
}

// NewServerFor is NewServer with per-method project overrides.
func NewServerFor(ctx context.Context, t Target, opts ...grpc.ServerOption) *grpc.Server {
	return grpc.NewServer(append(opts, ServerOptions(ctx, t)...)...)
}

func check(g *Gate, t Target, fullMethod string, req any) error {
	if Exempt(fullMethod) {
		return nil
	}

	host := ServiceForMethod(fullMethod, t.Host)
	if host == "" || exemptHost(host) {
		return nil
	}

	msg, ok := req.(proto.Message)
	if !ok {
		return nil
	}

	return g.Check(FromMessage(msg, t.ProjectFields[fullMethod]...), host)
}

func unary(g *Gate, t Target) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if err := check(g, t, info.FullMethod, req); err != nil {
			return nil, err
		}

		return next(ctx, req)
	}
}

func stream(g *Gate, t Target) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		return next(srv, &checkedStream{ServerStream: ss, g: g, t: t, method: info.FullMethod})
	}
}

// checkedStream checks the first received message, which carries the resource name of every GCP streaming RPC.
type checkedStream struct {
	grpc.ServerStream

	g       *Gate
	method  string
	t       Target
	checked bool
}

func (s *checkedStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}

	if !s.checked {
		s.checked = true

		return check(s.g, s.t, s.method, m)
	}

	return nil
}
