package gcpbigtable

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errBadFilter = errors.New("invalid row filter")

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

func errAborted(format string, a ...any) error {
	return status.Error(codes.Aborted, fmt.Sprintf(format, a...))
}

func errUnimplemented(format string, a ...any) error {
	return status.Error(codes.Unimplemented, fmt.Sprintf(format, a...))
}

func errInstanceNotFound(name string) error { return errNotFound("Instance %q not found.", name) }

func errClusterNotFound(name string) error { return errNotFound("Cluster %q not found.", name) }

func errTableNotFound(name string) error { return errNotFound("Table %q not found.", name) }

func errBackupNotFound(name string) error { return errNotFound("Backup %q not found.", name) }
