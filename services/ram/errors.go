package ram

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrValidation is returned when a request contains an invalid or missing parameter.
	ErrValidation = awserr.New("MalformedQueryStringException", awserr.ErrInvalidParameter)
	// ErrNotFound is returned when a resource share does not exist.
	ErrNotFound = awserr.New("UnknownResourceException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource share already exists.
	ErrAlreadyExists = awserr.New("ResourceShareAlreadyExistsException", awserr.ErrConflict)
	// ErrPermissionNotFound is returned when a permission does not exist.
	ErrPermissionNotFound = awserr.New("InvalidParameterException", awserr.ErrNotFound)
	// ErrInvitationNotFound is returned when an invitation does not exist.
	ErrInvitationNotFound = awserr.New(
		"ResourceShareInvitationArnNotFoundException",
		awserr.ErrNotFound,
	)
	// ErrInvitationAlreadyAccepted is returned when accepting an already-accepted invitation.
	ErrInvitationAlreadyAccepted = awserr.New(
		"ResourceShareInvitationAlreadyAcceptedException",
		awserr.ErrConflict,
	)
	// ErrInvitationAlreadyRejected is returned when accepting or rejecting an already-rejected invitation.
	ErrInvitationAlreadyRejected = awserr.New(
		"ResourceShareInvitationAlreadyRejectedException",
		awserr.ErrConflict,
	)
	// ErrInvitationExpired is returned when acting on an expired invitation.
	ErrInvitationExpired = awserr.New(
		"ResourceShareInvitationExpiredException",
		awserr.ErrConflict,
	)
	// ErrPermissionVersionNotFound is returned when a permission version does not exist.
	ErrPermissionVersionNotFound = awserr.New("InvalidParameterException", awserr.ErrNotFound)
	// ErrOperationNotPermitted is returned when an operation is not permitted on an AWS-managed resource.
	ErrOperationNotPermitted = awserr.New("OperationNotPermittedException", awserr.ErrConflict)
	// ErrPermissionInUse is returned when deleting a permission that is associated with active shares.
	ErrPermissionInUse = awserr.New("PermissionInUseException", awserr.ErrConflict)
	// ErrInvalidParameter is returned when a parameter value is out of the allowed range.
	ErrInvalidParameter = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)
)
