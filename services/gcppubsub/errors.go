package gcppubsub

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func errNotFound(kind, name string) error {
	return status.Error(codes.NotFound, fmt.Sprintf("Resource not found (resource=%s). %s", kind, name))
}

func errExists(kind, name string) error {
	return status.Error(
		codes.AlreadyExists,
		fmt.Sprintf("Resource already exists in the project (resource=%s). %s", kind, name),
	)
}

func errInvalid(format string, a ...any) error {
	return status.Error(codes.InvalidArgument, fmt.Sprintf(format, a...))
}

func errPrecondition(format string, a ...any) error {
	return status.Error(codes.FailedPrecondition, fmt.Sprintf(format, a...))
}

func errUnsupported(msg string) error { return status.Error(codes.Unimplemented, msg) }
