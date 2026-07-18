package transfer

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrServerNotFound is returned when a Transfer server is not found.
	ErrServerNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrUserNotFound is returned when a Transfer user is not found.
	ErrUserNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrUserAlreadyExists is returned when a Transfer user already exists.
	ErrUserAlreadyExists = awserr.New("ResourceExistsException", awserr.ErrConflict)
	// ErrInvalidProtocol is returned when an unsupported protocol is specified.
	ErrInvalidProtocol = awserr.New(
		"InvalidRequestException: unsupported protocol",
		awserr.ErrInvalidParameter,
	)
	// ErrServerStateConflict is returned when a state transition is invalid.
	ErrServerStateConflict = awserr.New(
		"ConflictException: server is already in the requested state",
		awserr.ErrConflict,
	)
	// ErrAccessNotFound is returned when a Transfer access is not found.
	ErrAccessNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAccessAlreadyExists is returned when an access with the same ExternalId already exists.
	ErrAccessAlreadyExists = awserr.New("ResourceExistsException", awserr.ErrConflict)
	// ErrAgreementNotFound is returned when a Transfer agreement is not found.
	ErrAgreementNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrConnectorNotFound is returned when a Transfer connector is not found.
	ErrConnectorNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrProfileNotFound is returned when a Transfer profile is not found.
	ErrProfileNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrWebAppNotFound is returned when a Transfer web app is not found.
	ErrWebAppNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrWorkflowNotFound is returned when a Transfer workflow is not found.
	ErrWorkflowNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrCertificateNotFound is returned when a Transfer certificate is not found.
	ErrCertificateNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrHostKeyNotFound is returned when a Transfer host key is not found.
	ErrHostKeyNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrSSHPublicKeyNotFound is returned when a Transfer SSH public key is not found.
	ErrSSHPublicKeyNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrValidation is returned when a required parameter is missing or invalid.
	ErrValidation = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
)

// ErrServerOnline is returned when an operation requires the server to be OFFLINE.
var ErrServerOnline = awserr.New(
	"ConflictException: server must be offline to be deleted",
	awserr.ErrConflict,
)

// ErrSSHPublicKeyDuplicate is returned when an SSH public key body already exists for the user.
var ErrSSHPublicKeyDuplicate = awserr.New("ResourceExistsException", awserr.ErrConflict)
