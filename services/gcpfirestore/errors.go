package gcpfirestore

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

func errExists(name string) error {
	return status.Errorf(codes.AlreadyExists, "Document already exists: %s", name)
}

func errAborted() error {
	return status.Error(codes.Aborted,
		"Aborted due to cross-transaction contention. This occurs when multiple transactions attempt to access the "+
			"same data, requiring Firestore to abort at least one in order to enforce serializability.")
}

func errPrecondition(format string, a ...any) error {
	return status.Error(codes.FailedPrecondition, fmt.Sprintf(format, a...))
}
