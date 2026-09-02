// Package cognitoidp provides a mock implementation of the AWS Cognito User Pools service.
package cognitoidp

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinel errors for Cognito IDP operations.
var (
	// ErrUserNotFound is returned when a user does not exist in the user pool.
	ErrUserNotFound = awserr.New("UserNotFoundException", awserr.ErrNotFound)

	// ErrUserPoolNotFound is returned when the requested user pool does not exist.
	ErrUserPoolNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)

	// ErrClientNotFound is returned when the requested app client does not exist.
	ErrClientNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)

	// ErrInvalidPassword is returned when the password does not meet policy requirements.
	ErrInvalidPassword = awserr.New("InvalidPasswordException", awserr.ErrInvalidParameter)

	// ErrNotAuthorized is returned when authentication fails (wrong password, etc.).
	ErrNotAuthorized = awserr.New("NotAuthorizedException", awserr.ErrInvalidParameter)

	// ErrTokenUnauthorized is returned when RevokeToken is called with a token
	// issued for a different client. RevokeToken's own deserializer models
	// UnauthorizedException ("the request isn't authorized... invalid access
	// token"), not the generic NotAuthorizedException most other ops use.
	ErrTokenUnauthorized = awserr.New("UnauthorizedException", awserr.ErrInvalidParameter)

	// ErrCodeMismatch is returned when the provided confirmation code does not match.
	ErrCodeMismatch = awserr.New("CodeMismatchException", awserr.ErrInvalidParameter)

	// ErrExpiredCode is returned when the provided confirmation code has expired.
	ErrExpiredCode = awserr.New("ExpiredCodeException", awserr.ErrNotFound)

	// ErrUsernameExists is returned when attempting to sign up with an existing username.
	ErrUsernameExists = awserr.New("UsernameExistsException", awserr.ErrAlreadyExists)

	// ErrUserNotConfirmed is returned when a user has not yet confirmed their account.
	ErrUserNotConfirmed = awserr.New("UserNotConfirmedException", awserr.ErrInvalidParameter)

	// ErrPasswordResetRequired is returned when the user must reset their password before authenticating.
	ErrPasswordResetRequired = awserr.New("PasswordResetRequiredException", awserr.ErrInvalidParameter)

	// ErrInvalidUserPoolConfig is returned when the user pool configuration is invalid.
	ErrInvalidUserPoolConfig = awserr.New("InvalidUserPoolConfigurationException", awserr.ErrInvalidParameter)

	// ErrInvalidToken is returned when a JWT token is structurally invalid or has unexpected claims.
	ErrInvalidToken = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)

	// ErrInvalidParameter signals an InvalidParameterException for general
	// request-validation failures (e.g., out-of-range password policy fields).
	ErrInvalidParameter = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)

	// ErrGroupNotFound is returned when a user pool group does not exist.
	ErrGroupNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)

	// ErrAlreadyExists is returned when a resource with the same identifier already exists.
	ErrAlreadyExists = awserr.New("GroupExistsException", awserr.ErrAlreadyExists)

	// ErrDuplicateProvider is returned when an identity provider with the same name already exists.
	ErrDuplicateProvider = awserr.New("DuplicateProviderException", awserr.ErrAlreadyExists)

	// ErrDeviceNotFound is returned when a tracked device does not exist for a user.
	ErrDeviceNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)

	// ErrWebAuthnCredentialNotFound is returned when a WebAuthn/passkey credential does not exist.
	ErrWebAuthnCredentialNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)

	// ErrAuthEventNotFound is returned when an adaptive-authentication event does not exist.
	ErrAuthEventNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)

	// ErrUserLambdaValidation is returned when a configured Lambda trigger (PreSignUp,
	// PostConfirmation, PreTokenGeneration, CustomMessage, ...) fails during invocation --
	// mirrors AWS Cognito's UserLambdaValidationException, which wraps any error the
	// trigger function returns.
	ErrUserLambdaValidation = awserr.New("UserLambdaValidationException", awserr.ErrInvalidParameter)

	// ErrUnexpectedLambda is returned when a configured Lambda trigger responds with a
	// malformed payload (missing/invalid "response" object) rather than a genuine
	// business-logic error -- mirrors AWS Cognito's UnexpectedLambdaException.
	ErrUnexpectedLambda = awserr.New("UnexpectedLambdaException", awserr.ErrInvalidParameter)

	// ErrReplicaNotFound is returned when a user pool has no secondary replica
	// in the requested Region (UpdateUserPoolReplica/DeleteUserPoolReplica).
	ErrReplicaNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)

	// ErrServiceQuotaExceeded is returned when a requested provisioned-limit
	// value falls outside what the account is allowed to provision
	// (UpdateProvisionedLimit) -- mirrors AWS Cognito's ServiceQuotaExceededException.
	ErrServiceQuotaExceeded = awserr.New("ServiceQuotaExceededException", awserr.ErrConflict)

	// ErrTermsNotFound is returned when the requested terms documents do not exist.
	ErrTermsNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)

	// ErrTermsExists is returned when a terms document with the same TermsName
	// already exists for the given app client.
	ErrTermsExists = awserr.New("TermsExistsException", awserr.ErrAlreadyExists)

	// ErrSecretNotFound is returned when a ClientSecretId does not match any
	// secret added via AddUserPoolClientSecret for the given app client
	// (DeleteUserPoolClientSecret).
	ErrSecretNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)

	// ErrLimitExceeded is returned when AddUserPoolClientSecret would exceed
	// the real API's documented cap of 2 active secrets per app client
	// (verified against aws-sdk-go-v2/service/cognitoidentityprovider's
	// AddUserPoolClientSecret deserializer, which declares
	// LimitExceededException).
	ErrLimitExceeded = awserr.New("LimitExceededException", awserr.ErrConflict)
)

// ErrJWTKeyNotFound is returned when a JWT key ID is not found for a known issuer.
var ErrJWTKeyNotFound = errors.New("JWT key ID not found for issuer")

// ErrJWTIssuerUnknown is returned when no pool matches the given issuer URL.
var ErrJWTIssuerUnknown = errors.New("JWT issuer not managed by this emulator")
