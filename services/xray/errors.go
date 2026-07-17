package xray

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrGroupNotFound is returned when an X-Ray group is not found.
	ErrGroupNotFound = awserr.New("InvalidRequestException", awserr.ErrNotFound)
	// ErrGroupAlreadyExists is returned when an X-Ray group already exists.
	ErrGroupAlreadyExists = awserr.New("GroupAlreadyExistsException", awserr.ErrConflict)
	// ErrSamplingRuleNotFound is returned when a sampling rule is not found.
	ErrSamplingRuleNotFound = awserr.New("InvalidRequestException", awserr.ErrNotFound)
	// ErrSamplingRuleAlreadyExists is returned when a sampling rule already exists.
	ErrSamplingRuleAlreadyExists = awserr.New("RuleAlreadyExistsException", awserr.ErrConflict)
	// ErrInsightNotFound is returned when an X-Ray insight is not found.
	ErrInsightNotFound = awserr.New("InvalidRequestException", awserr.ErrNotFound)
	// ErrResourcePolicyNotFound is returned when a resource policy is not found.
	ErrResourcePolicyNotFound = awserr.New("InvalidRequestException", awserr.ErrNotFound)
	// ErrIndexingRuleNotFound is returned when an indexing rule is not found.
	ErrIndexingRuleNotFound = awserr.New("InvalidRequestException", awserr.ErrNotFound)
	// ErrValidation is returned when a request fails field-level validation.
	ErrValidation = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
	// ErrInvalidSamplingRule is returned when sampling rule fields fail validation.
	ErrInvalidSamplingRule = awserr.New("InvalidSamplingRuleException", awserr.ErrInvalidParameter)
	// ErrInvalidPolicyRevisionID is returned when a policy revision ID does not match.
	ErrInvalidPolicyRevisionID = awserr.New("InvalidPolicyRevisionIdException", awserr.ErrConflict)
	// ErrMalformedPolicyDocument is returned when a policy document is not valid JSON.
	ErrMalformedPolicyDocument = awserr.New("MalformedPolicyDocumentException", awserr.ErrInvalidParameter)
	// ErrTooManyPolicies is returned when the max policy count is exceeded.
	ErrTooManyPolicies = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
	// ErrBatchGetTracesLimit is returned when more than 5 trace IDs are requested.
	ErrBatchGetTracesLimit = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
	// ErrDefaultRuleUndeletable is returned when the built-in Default sampling rule is deleted.
	ErrDefaultRuleUndeletable = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
)
