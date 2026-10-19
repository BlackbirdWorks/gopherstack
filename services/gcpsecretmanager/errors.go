package gcpsecretmanager

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func errSecretNotFound(name string) error {
	return status.Errorf(codes.NotFound, "Secret [%s] not found or has been deleted.", name)
}

func errVersionNotFound(name string) error {
	return status.Errorf(codes.NotFound, "Secret Version [%s] not found.", name)
}

func errSecretExists(name string) error {
	return status.Errorf(codes.AlreadyExists, "Secret [%s] already exists.", name)
}

func errInvalid(format string, a ...any) error {
	return status.Error(codes.InvalidArgument, fmt.Sprintf(format, a...))
}

func errPrecondition(format string, a ...any) error {
	return status.Error(codes.FailedPrecondition, fmt.Sprintf(format, a...))
}

func errEtag(kind, name string) error {
	return errPrecondition("Etag does not match the current etag of %s [%s].", kind, name)
}
