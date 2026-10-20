package gcpfunctions

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errNoRuntime = errors.New("container runtime is unavailable")

func errNotFound(n resName) error {
	return status.Errorf(
		codes.NotFound,
		"Resource 'projects/%s/locations/%s/functions/%s' of kind 'FUNCTION' in region '%s' in project '%s' does not exist.",
		n.project,
		n.location,
		n.id,
		n.location,
		n.project,
	)
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
