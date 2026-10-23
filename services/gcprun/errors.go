package gcprun

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errNoRuntime = errors.New("container runtime is unavailable")

func errNotFound(kind, id, location, project string) error {
	return status.Errorf(codes.NotFound,
		"Resource '%s' of kind '%s' in region '%s' in project '%s' does not exist.", id, kind, location, project)
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
