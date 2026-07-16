package lambda

import "errors"

// ErrFunctionNotFound is returned when the specified Lambda function does not exist.
var ErrFunctionNotFound = errors.New("ResourceNotFoundException")

// ErrFunctionAlreadyExists is returned when creating a function that already exists.
var ErrFunctionAlreadyExists = errors.New("ResourceConflictException")

// ErrLambdaUnavailable is returned when Lambda cannot invoke (no Docker or no port range).
var ErrLambdaUnavailable = errors.New("ServiceException")

// ErrInvalidParameterValue is returned when a request parameter has an invalid value.
var ErrInvalidParameterValue = errors.New("InvalidParameterValueException")

// ErrESMNotFound is returned when an event source mapping UUID is not found.
var ErrESMNotFound = errors.New("ResourceNotFoundException")

// ErrFunctionURLNotFound is returned when no function URL config exists for the function.
var ErrFunctionURLNotFound = errors.New("ResourceNotFoundException")

// ErrVersionNotFound is returned when the specified function version does not exist.
var ErrVersionNotFound = errors.New("ResourceNotFoundException")

// ErrAliasNotFound is returned when the specified alias does not exist.
var ErrAliasNotFound = errors.New("ResourceNotFoundException")

// ErrAliasAlreadyExists is returned when creating an alias that already exists.
var ErrAliasAlreadyExists = errors.New("ResourceConflictException")

// ErrLayerNotFound is returned when the specified layer does not exist.
var ErrLayerNotFound = errors.New("ResourceNotFoundException")

// ErrLayerVersionNotFound is returned when the specified layer version does not exist.
var ErrLayerVersionNotFound = errors.New("ResourceNotFoundException")

// ErrZipSlip is returned when a ZIP archive entry has a path that would escape the target directory.
var ErrZipSlip = errors.New("zip entry escapes target directory")

// ErrEventInvokeConfigNotFound is returned when no event invoke config exists for the function.
var ErrEventInvokeConfigNotFound = errors.New("ResourceNotFoundException")

// ErrTooManyRequests is returned when a function's reserved concurrency limit is exhausted.
var ErrTooManyRequests = errors.New("TooManyRequestsException")

// ErrFunctionConcurrencyNotFound is returned when a function has no reserved concurrency configured.
var ErrFunctionConcurrencyNotFound = errors.New("ResourceNotFoundException")

// ErrProvisionedConcurrencyConfigNotFound is returned when no provisioned concurrency config exists for the qualifier.
var ErrProvisionedConcurrencyConfigNotFound = errors.New("ResourceNotFoundException")

// ErrCodeSigningConfigNotFound is returned when a function has no code signing config associated.
var ErrCodeSigningConfigNotFound = errors.New("CodeSigningConfigNotFoundException")

// ErrNoPolicyFound is returned when a function has no resource-based policy (no permissions).
var ErrNoPolicyFound = errors.New("ResourceNotFoundException")

// ErrFunctionURLForbidden is returned when an AWS_IAM function URL request is
// missing or has an invalid SigV4 signature.
var ErrFunctionURLForbidden = errors.New("Forbidden")
