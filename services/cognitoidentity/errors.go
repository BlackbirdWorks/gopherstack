// Package cognitoidentity provides a mock implementation of the AWS Cognito Federated Identities service.
package cognitoidentity

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinel errors for Cognito Identity Pool operations.
var (
	// ErrIdentityPoolNotFound is returned when the requested identity pool does not exist.
	ErrIdentityPoolNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)

	// ErrIdentityPoolAlreadyExists is returned when an identity pool with the given name already exists.
	ErrIdentityPoolAlreadyExists = awserr.New("ResourceConflictException", awserr.ErrAlreadyExists)

	// ErrInvalidParameter is returned when an invalid or missing parameter is supplied.
	ErrInvalidParameter = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)

	// ErrNotAuthorized is returned when the caller lacks required permissions.
	ErrNotAuthorized = awserr.New("NotAuthorizedException", awserr.ErrInvalidParameter)
)
