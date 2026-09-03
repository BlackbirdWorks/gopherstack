package xray

import (
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrGroupNotFound is returned when an X-Ray group is not found.
	ErrGroupNotFound = awserr.New("InvalidRequestException", awserr.ErrNotFound)
	// ErrGroupAlreadyExists is returned when an X-Ray group already exists.
	// CreateGroup's own error model (xray@v1.39.4 deserializers.go
	// awsRestjson1_deserializeOpErrorCreateGroup) defines no AlreadyExists-shaped
	// exception at all -- it models only InvalidRequestException and
	// ThrottledException -- so InvalidRequestException is the correct code
	// (gopherstack-101r; was the fabricated GroupAlreadyExistsException, which
	// names no type anywhere in this SDK).
	ErrGroupAlreadyExists = awserr.New("InvalidRequestException", awserr.ErrConflict)
	// ErrSamplingRuleNotFound is returned when a sampling rule is not found.
	ErrSamplingRuleNotFound = awserr.New("InvalidRequestException", awserr.ErrNotFound)
	// ErrSamplingRuleAlreadyExists is returned when a sampling rule already exists.
	// CreateSamplingRule's own error model defines InvalidRequestException,
	// RuleLimitExceededException, and ThrottledException -- no AlreadyExists-shaped
	// exception -- so InvalidRequestException is the correct code (gopherstack-101r;
	// was the fabricated RuleAlreadyExistsException, absent from this SDK entirely).
	ErrSamplingRuleAlreadyExists = awserr.New("InvalidRequestException", awserr.ErrConflict)
	// ErrInsightNotFound is returned when an X-Ray insight is not found.
	ErrInsightNotFound = awserr.New("InvalidRequestException", awserr.ErrNotFound)
	// ErrResourcePolicyNotFound is returned when a resource policy is not found.
	ErrResourcePolicyNotFound = awserr.New("InvalidRequestException", awserr.ErrNotFound)
	// ErrIndexingRuleNotFound is returned when an indexing rule is not found.
	// UpdateIndexingRule's modeled error set uses ResourceNotFoundException here
	// (unlike GetGroup/DeleteGroup/etc., which only ever return InvalidRequestException).
	ErrIndexingRuleNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrValidation is returned when a request fails field-level validation.
	ErrValidation = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
	// ErrInvalidSamplingRule is returned when sampling rule fields fail
	// validation (CreateSamplingRule's own ValidateSamplingRule caller).
	// CreateSamplingRule's own error model defines no InvalidSamplingRuleException
	// type -- InvalidRequestException is the correct code (gopherstack-101r; was
	// the fabricated InvalidSamplingRuleException, absent from this SDK entirely).
	ErrInvalidSamplingRule = awserr.New("InvalidRequestException", awserr.ErrInvalidParameter)
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
	// ErrPolicySizeLimitExceeded is returned when a resource policy document exceeds the maximum size.
	ErrPolicySizeLimitExceeded = awserr.New("PolicySizeLimitExceededException", awserr.ErrInvalidParameter)
	// ErrRuleLimitExceeded is returned when the maximum number of sampling rules is exceeded.
	ErrRuleLimitExceeded = awserr.New("RuleLimitExceededException", awserr.ErrInvalidParameter)
	// ErrTooManyTags is returned when a resource would exceed the maximum number of tags.
	ErrTooManyTags = awserr.New("TooManyTagsException", awserr.ErrInvalidParameter)
	// ErrResourceNotFound is returned for operations whose modeled error is
	// ResourceNotFoundException rather than InvalidRequestException (TagResource,
	// UntagResource, ListTagsForResource, UpdateIndexingRule, and the trace-retrieval
	// token operations StartTraceRetrieval/CancelTraceRetrieval/ListRetrievedTraces/
	// GetRetrievedTracesGraph -- confirmed against each operation's declared error set
	// in aws-sdk-go-v2/service/xray's deserializers.go).
	ErrResourceNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrTraceRetrievalNotFound is returned when a RetrievalToken passed to
	// CancelTraceRetrieval, ListRetrievedTraces, or GetRetrievedTracesGraph does not
	// correspond to a retrieval started by StartTraceRetrieval. All three declare
	// ResourceNotFoundException in their modeled error set.
	ErrTraceRetrievalNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
)
