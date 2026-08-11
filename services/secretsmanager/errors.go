package secretsmanager

import "errors"

var (
	// ErrSecretNotFound is returned when the specified secret does not exist.
	ErrSecretNotFound = errors.New(errResourceNotFoundException)

	// ErrMalformedPolicyDocument is returned when the resource policy is malformed.
	ErrMalformedPolicyDocument = errors.New("MalformedPolicyDocumentException")

	// ErrPublicPolicyException is returned when the policy is overly broad.
	ErrPublicPolicyException = errors.New("PublicPolicyException")

	// ErrSecretAlreadyExists is returned when a secret with the given name already exists.
	ErrSecretAlreadyExists = errors.New("ResourceExistsException")
	// ErrSecretDeleted is returned when an operation is attempted on a deleted secret.
	ErrSecretDeleted = errors.New("InvalidRequestException")
	// ErrVersionNotFound is returned when the specified version does not exist.
	ErrVersionNotFound = errors.New(errResourceNotFoundException)
	// ErrInvalidPasswordParameters is returned when password generation parameters are invalid
	// (e.g. PasswordLength out of range, or empty charset after exclusions, or too few positions
	// to satisfy RequireEachIncludedType).
	ErrInvalidPasswordParameters = errors.New("InvalidParameterException")
	// ErrCryptoRandInvalidRange is returned when cryptoRandInt is called with a non-positive bound.
	ErrCryptoRandInvalidRange = errors.New("random integer bound must be positive")
	// ErrSecretValueTooLarge is returned when a secret value exceeds the 64 KB AWS limit.
	ErrSecretValueTooLarge = errors.New("InvalidParameterException")
	// ErrInvalidParameter is returned when an invalid parameter value is provided.
	ErrInvalidParameter = errors.New("InvalidParameterException")
	// ErrInvalidSecretName is returned when a secret name does not match the allowed pattern.
	ErrInvalidSecretName = errors.New("InvalidParameterException")
	// ErrRotationStrategyRequired is returned when RotateSecret is called on a secret with
	// no RotationLambdaARN configured (neither already stored nor supplied in the request).
	// Matches aws-sdk-go-v2/service/secretsmanager@v1.44.4 types/errors.go's
	// InvalidRequestException doc comment: you tried to enable rotation on a secret that
	// doesn't already have a Lambda function ARN configured, and didn't include one as a
	// parameter in this call.
	ErrRotationStrategyRequired = errors.New("InvalidRequestException")
)
