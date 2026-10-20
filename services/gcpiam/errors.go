package gcpiam

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

func errAborted(msg string) error {
	return status.Error(codes.Aborted, msg)
}

func errInternal(cause error) error {
	if cause == nil {
		return status.Error(codes.Internal, "internal error")
	}

	return status.Error(codes.Internal, "internal error: "+cause.Error())
}
