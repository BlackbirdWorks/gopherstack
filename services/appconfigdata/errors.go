package appconfigdata

import "errors"

// AWS exception type names as returned in error response bodies and X-Amzn-ErrorType header.
// These are the complete set of exceptions modeled for AppConfigData -- verified against the
// real service-2.json: BadRequestException, InternalServerException, ResourceNotFoundException,
// and ThrottlingException. There is no PayloadTooLargeException in the real API for this
// service (SetConfiguration is an internal seeding helper reached only via the dashboard, not
// a routed AWS operation, so its size-limit error never needs an AWS exception-type mapping).
const (
	exceptionBadRequest       = "BadRequestException"
	exceptionResourceNotFound = "ResourceNotFoundException"
	exceptionInternalServer   = "InternalServerException"
	exceptionThrottling       = "ThrottlingException"
)

// AWS BadRequestReason values.
const (
	badRequestReasonInvalidParameters = "InvalidParameters"
)

// AWS InvalidParameterProblem values — identify why a specific parameter was rejected.
const (
	invalidParamProblemCorrupted                = "Corrupted"
	invalidParamProblemExpired                  = "Expired"
	invalidParamProblemPollIntervalNotSatisfied = "PollIntervalNotSatisfied"
)

// AWS ResourceType values for ResourceNotFoundException.
const (
	resourceTypeApplication          = "Application"
	resourceTypeEnvironment          = "Environment"
	resourceTypeConfigurationProfile = "ConfigurationProfile"
	resourceTypeDeployment           = "Deployment"
	resourceTypeConfiguration        = "Configuration"
)

var (
	// ErrSessionNotFound is returned when the requested session token does not exist in the map.
	ErrSessionNotFound = errors.New("bad request: invalid configuration token")
	// ErrTokenCorrupted is returned when the token format or HMAC is invalid.
	ErrTokenCorrupted = errors.New("bad request: configuration token is corrupted")
	// ErrTokenExpired is returned when the session token has passed its expiry time.
	// AWS returns BadRequestException (400) for expired tokens, not 401.
	ErrTokenExpired = errors.New("bad request: configuration token has expired")
	// ErrProfileNotFound is returned when no configuration has been stored for a profile.
	ErrProfileNotFound = errors.New("resource not found: configuration profile not found")
	// ErrResourceRemoved is returned when a session's app/env/profile was deleted after the session started.
	ErrResourceRemoved = errors.New("resource not found: application, environment, or profile no longer exists")
	// ErrContentTooLarge is returned when configuration content exceeds the size limit.
	ErrContentTooLarge = errors.New("bad request: content exceeds maximum size of 1 MiB")
	// ErrInvalidPollInterval is returned when RequiredMinimumPollIntervalInSeconds is out of range.
	ErrInvalidPollInterval = errors.New(
		"bad request: RequiredMinimumPollIntervalInSeconds must be 0 or between 15 and 86400",
	)
	// ErrPollTooFrequent is returned when a client polls faster than its declared minimum interval.
	ErrPollTooFrequent = errors.New(
		"bad request: polling too frequently — wait for the interval in Next-Poll-Interval-In-Seconds",
	)
	// ErrContentTypeMismatch is returned when content is declared as JSON but is not valid JSON.
	ErrContentTypeMismatch = errors.New("bad request: content is not valid for the declared content type")
	// ErrNoActiveDeployment is returned when StartConfigurationSession is called for a profile
	// that has no active deployment (no configuration has been published yet).
	ErrNoActiveDeployment = errors.New(
		"resource not found: no active deployment found for the given application, environment, and configuration profile",
	)
	// ErrIdentifierTooLong is returned when an identifier exceeds the maximum allowed length.
	ErrIdentifierTooLong = errors.New("bad request: identifier exceeds maximum length of 128 characters")
)
