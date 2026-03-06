// Package cognitoidp provides a mock implementation of the AWS Cognito User Pools service.
package cognitoidp

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinel errors for Cognito IDP operations.
var (
	// ErrUserNotFound is returned when a user does not exist in the user pool.
	ErrUserNotFound = awserr.New("UserNotFoundException", awserr.ErrNotFound)

	// ErrUserAlreadyExists is returned when a user already exists in the user pool.
	ErrUserAlreadyExists = awserr.New("UserAlreadyExistsException", awserr.ErrAlreadyExists)

	// ErrUserPoolNotFound is returned when the requested user pool does not exist.
	ErrUserPoolNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)

	// ErrUserPoolAlreadyExists is returned when a user pool with the given name already exists.
	ErrUserPoolAlreadyExists = awserr.New("UserPoolAlreadyExistsException", awserr.ErrAlreadyExists)

	// ErrClientNotFound is returned when the requested app client does not exist.
	ErrClientNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)

	// ErrInvalidPassword is returned when the password does not meet policy requirements.
	ErrInvalidPassword = awserr.New("InvalidPasswordException", awserr.ErrInvalidParameter)

	// ErrNotAuthorized is returned when authentication fails (wrong password, etc.).
	ErrNotAuthorized = awserr.New("NotAuthorizedException", awserr.ErrInvalidParameter)

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
)
