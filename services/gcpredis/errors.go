package gcpredis

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func errNotFound(format string, a ...any) error {
	return status.Error(codes.NotFound, fmt.Sprintf(format, a...))
}

func errExists(format string, a ...any) error {
	return status.Error(codes.AlreadyExists, fmt.Sprintf(format, a...))
}

func errInvalid(format string, a ...any) error {
	return status.Error(codes.InvalidArgument, fmt.Sprintf(format, a...))
}

func errPrecondition(format string, a ...any) error {
	return status.Error(codes.FailedPrecondition, fmt.Sprintf(format, a...))
}

func errExhausted(format string, a ...any) error {
	return status.Error(codes.ResourceExhausted, fmt.Sprintf(format, a...))
}

func errInstanceNotFound(name string) error {
	return errNotFound("Instance %q not found.", name)
}
