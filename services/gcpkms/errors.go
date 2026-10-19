package gcpkms

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func errNotFound(kind, name string) error {
	return status.Errorf(codes.NotFound, "%s %s not found.", kind, name)
}

func errExists(kind, name string) error {
	return status.Errorf(codes.AlreadyExists, "%s %s already exists.", kind, name)
}

func errInvalid(format string, a ...any) error {
	return status.Error(codes.InvalidArgument, fmt.Sprintf(format, a...))
}

func errPrecondition(format string, a ...any) error {
	return status.Error(codes.FailedPrecondition, fmt.Sprintf(format, a...))
}

func errUnsupported(format string, a ...any) error {
	return status.Error(codes.Unimplemented, fmt.Sprintf(format, a...))
}
