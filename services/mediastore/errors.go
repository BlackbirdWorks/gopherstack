package mediastore

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrContainerNotFound is returned when a container does not exist.
	ErrContainerNotFound = awserr.New(
		"container not found",
		awserr.ErrNotFound,
	)
	// ErrContainerAlreadyExists is returned when a container already exists.
	ErrContainerAlreadyExists = awserr.New(
		"container already exists",
		awserr.ErrAlreadyExists,
	)
	// ErrPolicyNotFound is returned when no container policy has been set.
	ErrPolicyNotFound = awserr.New(
		"no policy found for container",
		awserr.ErrNotFound,
	)
	// ErrCorsPolicyNotFound is returned when no CORS policy has been set.
	ErrCorsPolicyNotFound = awserr.New(
		"no CORS policy found for container",
		awserr.ErrNotFound,
	)
	// ErrLifecyclePolicyNotFound is returned when no lifecycle policy has been set.
	ErrLifecyclePolicyNotFound = awserr.New(
		"no lifecycle policy found for container",
		awserr.ErrNotFound,
	)
	// ErrMetricPolicyNotFound is returned when no metric policy has been set.
	ErrMetricPolicyNotFound = awserr.New(
		"no metric policy found for container",
		awserr.ErrNotFound,
	)
	// ErrMissingContainerName is returned when the container name is missing.
	ErrMissingContainerName = errors.New("ContainerName is required")
	// ErrInvalidContainerName is returned when the container name is invalid.
	//
	// The message intentionally does NOT repeat the "ValidationException:" type
	// prefix: writeBackendError already carries that in the response envelope's
	// __type field (see JSONErrorResponse), so baking it into the message text
	// too would double it up on the wire (e.g. "ValidationException:
	// ValidationException: ...", as every AWS SDK formats client-visible errors
	// as "api error <Type>: <message>").
	ErrInvalidContainerName = errors.New(
		"container name must be 1-255 characters" +
			" and contain only letters, numbers, hyphens, and underscores",
	)
	// ErrInvalidPolicy is returned when a policy string is not valid JSON.
	ErrInvalidPolicy = errors.New("policy must be valid JSON")
	// ErrCorsRuleInvalid is returned when a CORS rule is missing required fields.
	ErrCorsRuleInvalid = errors.New(
		"each CORS rule must have at least one AllowedOrigin and one AllowedHeader",
	)
	// ErrInvalidMetricPolicy is returned when ContainerLevelMetrics has an invalid value.
	ErrInvalidMetricPolicy = errors.New(
		"ContainerLevelMetrics must be ENABLED or DISABLED",
	)
	// ErrTooManyMetricRules is returned when more than 5 metric policy rules are provided.
	ErrTooManyMetricRules = errors.New(
		"metric policy may have at most 5 rules",
	)
	// ErrEmptyTagKey is returned when a tag with an empty key is provided.
	ErrEmptyTagKey = errors.New("tag key must not be empty")
)
