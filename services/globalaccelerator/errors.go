package globalaccelerator

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinels, one per exception in the pinned SDK's error set.
var (
	ErrAcceleratorNotDisabled       = errors.New("the accelerator must be disabled")
	ErrAcceleratorNotFound          = errors.New("accelerator not found")
	ErrAssociatedEndpointGroupFound = errors.New("the listener has endpoint groups")
	ErrAssociatedListenerFound      = errors.New("the accelerator has listeners")
	ErrAttachmentNotFound           = errors.New("attachment not found")
	ErrByoipCidrNotFound            = errors.New("byoip cidr not found")
	ErrConflict                     = errors.New("conflict")
	ErrEndpointAlreadyExists        = errors.New("endpoint already exists")
	ErrEndpointGroupAlreadyExists   = errors.New("endpoint group already exists")
	ErrEndpointGroupNotFound        = errors.New("endpoint group not found")
	ErrEndpointNotFound             = errors.New("endpoint not found")
	ErrIncorrectCidrState           = errors.New("incorrect cidr state")
	ErrInternalServiceError         = errors.New("internal service error")
	ErrInvalidArgument              = errors.New("invalid argument")
	ErrInvalidNextToken             = errors.New("invalid next token")
	ErrInvalidPortRange             = errors.New("invalid port range")
	ErrLimitExceeded                = errors.New("limit exceeded")
	ErrListenerNotFound             = errors.New("listener not found")
	errUnknownOperation             = errors.New("unknown operation")
)

func fail(sentinel error, msg string, args ...any) error {
	return awserr.Newf(msg, sentinel, args...)
}

func apiErr(code string, status int) awserr.APIError {
	return awserr.APIError{Code: code, HTTPStatus: status}
}

func errorTable() map[error]awserr.APIError {
	return map[error]awserr.APIError{
		ErrAcceleratorNotDisabled:       apiErr("AcceleratorNotDisabledException", http.StatusBadRequest),
		ErrAcceleratorNotFound:          apiErr("AcceleratorNotFoundException", http.StatusBadRequest),
		ErrAssociatedEndpointGroupFound: apiErr("AssociatedEndpointGroupFoundException", http.StatusBadRequest),
		ErrAssociatedListenerFound:      apiErr("AssociatedListenerFoundException", http.StatusBadRequest),
		ErrAttachmentNotFound:           apiErr("AttachmentNotFoundException", http.StatusBadRequest),
		ErrByoipCidrNotFound:            apiErr("ByoipCidrNotFoundException", http.StatusBadRequest),
		ErrConflict:                     apiErr("ConflictException", http.StatusBadRequest),
		ErrEndpointAlreadyExists:        apiErr("EndpointAlreadyExistsException", http.StatusBadRequest),
		ErrEndpointGroupAlreadyExists:   apiErr("EndpointGroupAlreadyExistsException", http.StatusBadRequest),
		ErrEndpointGroupNotFound:        apiErr("EndpointGroupNotFoundException", http.StatusBadRequest),
		ErrEndpointNotFound:             apiErr("EndpointNotFoundException", http.StatusBadRequest),
		ErrIncorrectCidrState:           apiErr("IncorrectCidrStateException", http.StatusBadRequest),
		ErrInternalServiceError:         apiErr("InternalServiceErrorException", http.StatusInternalServerError),
		ErrInvalidArgument:              apiErr("InvalidArgumentException", http.StatusBadRequest),
		ErrInvalidNextToken:             apiErr("InvalidNextTokenException", http.StatusBadRequest),
		ErrInvalidPortRange:             apiErr("InvalidPortRangeException", http.StatusBadRequest),
		ErrLimitExceeded:                apiErr("LimitExceededException", http.StatusBadRequest),
		ErrListenerNotFound:             apiErr("ListenerNotFoundException", http.StatusBadRequest),
		errUnknownOperation:             apiErr("InvalidArgumentException", http.StatusBadRequest),
	}
}
