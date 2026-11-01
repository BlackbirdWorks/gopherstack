package gcpdatastore

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func errInvalid(format string, a ...any) error {
	return status.Error(codes.InvalidArgument, fmt.Sprintf(format, a...))
}

func errNotFound(format string, a ...any) error {
	return status.Error(codes.NotFound, fmt.Sprintf(format, a...))
}

func errExists(format string, a ...any) error {
	return status.Error(codes.AlreadyExists, fmt.Sprintf(format, a...))
}

func errPrecondition(format string, a ...any) error {
	return status.Error(codes.FailedPrecondition, fmt.Sprintf(format, a...))
}

func errUnimplemented(msg string) error {
	return status.Error(codes.Unimplemented, msg)
}

func errContention() error {
	return status.Error(codes.Aborted, "too much contention on these datastore entities. please try again.")
}

func errBadTxn() error {
	return status.Error(codes.InvalidArgument, "The referenced transaction has expired or is no longer valid.")
}
