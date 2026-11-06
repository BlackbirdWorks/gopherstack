package gcpenforce

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// RecoverOptions turns a handler panic into an Internal status for that one RPC; grpc-go does not recover.
func RecoverOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(recoverUnary),
		grpc.ChainStreamInterceptor(recoverStream),
	}
}

func recoverUnary(
	ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler,
) (resp any, err error) {
	defer logger.Recover(ctx, "grpc.unary "+info.FullMethod, func(any) {
		resp, err = nil, status.Error(codes.Internal, "internal server error")
	})

	return next(ctx, req)
}

func recoverStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) (err error) {
	defer logger.Recover(ss.Context(), "grpc.stream "+info.FullMethod, func(any) {
		err = status.Error(codes.Internal, "internal server error")
	})

	return next(srv, ss)
}
