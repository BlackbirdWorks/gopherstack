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

	// ErrResourceConflict is returned when a request's parameters conflict with
	// existing state (for example, LookupDeveloperIdentity supplying both an
	// IdentityId and a DeveloperUserIdentifier that do not resolve to the same
	// identity). Distinct from ErrIdentityPoolAlreadyExists (also wire-typed
	// ResourceConflictException) which is specifically about pool-name collisions.
	ErrResourceConflict = awserr.New("ResourceConflictException", awserr.ErrConflict)

	// ErrDeveloperUserAlreadyRegistered is returned by GetOpenIdTokenForDeveloperIdentity
	// when the developer user identifier supplied in Logins is already linked to a
	// different identity than the one requested.
	ErrDeveloperUserAlreadyRegistered = awserr.New(
		"DeveloperUserAlreadyRegisteredException",
		awserr.ErrConflict,
	)

	// ErrInvalidIdentityPoolConfiguration is returned by GetCredentialsForIdentity when
	// the identity pool has no IAM role configured for the identity's auth state
	// (authenticated vs. unauthenticated).
	ErrInvalidIdentityPoolConfiguration = awserr.New(
		"InvalidIdentityPoolConfigurationException",
		awserr.ErrInvalidParameter,
	)
)
