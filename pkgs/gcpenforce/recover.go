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

func recoverUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	var (
		resp any
		err  error
	)

	func() {
		defer logger.Recover(ctx, "grpc.unary "+info.FullMethod, func(any) {
			resp, err = nil, status.Error(codes.Internal, errInternal)
		})

		resp, err = next(ctx, req)
	}()

	return resp, err
}

func recoverStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
	var err error

	func() {
		defer logger.Recover(ss.Context(), "grpc.stream "+info.FullMethod, func(any) {
			err = status.Error(codes.Internal, errInternal)
		})

		err = next(srv, ss)
	}()

	return err
}

const errInternal = "internal server error"
