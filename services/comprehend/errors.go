package comprehend

import "errors"

// Sentinel errors map 1:1 to Comprehend's modeled exception shapes
// (aws-sdk-go-v2/service/comprehend/types/errors.go). handleError in
// handler.go matches each with errors.Is and emits the exact "__type" wire
// code with HTTP 400 (every exception here has smithy.FaultClient; only
// InternalServerException is FaultServer, so it stays the unmapped 500
// default rather than getting its own sentinel).
//
// Not every exception the real SDK models for these operations has a
// sentinel here: ResourceLimitExceededException, ResourceUnavailableException,
// TooManyRequestsException, and ConcurrentModificationException are real
// modeled errors for several ops in this service, but none has a
// non-fabricated deterministic trigger in a synchronous, single-lock,
// unbounded in-memory emulator (no rate limiting, no enforced per-account
// resource quotas, no real concurrent-write races -- see PARITY.md gaps).
// Generic throttling/5xx injection for any operation is available instead
// through the chaos fault-injection system (ChaosOperations/ChaosServiceName).
var (
	// ErrNotFound is returned when a requested Comprehend resource is absent.
	ErrNotFound = errors.New("ResourceNotFoundException")
	// ErrConflict is returned when a named Comprehend resource already exists.
	ErrConflict = errors.New("ResourceInUseException")
	// ErrValidation is returned for invalid request values.
	ErrValidation = errors.New("InvalidRequestException")
	// ErrJobNotFound is returned when an async job ID does not exist. Real AWS
	// uses this distinct code (not ResourceNotFoundException) for every
	// Describe*Job/Stop*Job operation -- confirmed against every
	// awsAwsjson11_deserializeOpErrorDescribe*Job/Stop*Job case in the SDK's
	// generated deserializers.go.
	ErrJobNotFound = errors.New("JobNotFoundException")
	// ErrTooManyTags is returned when a resource would carry more than the
	// 50-tag-per-resource limit (both existing and newly requested tags count).
	ErrTooManyTags = errors.New("TooManyTagsException")
	// ErrBatchSizeLimitExceeded is returned when a Batch* request's TextList
	// carries more than 25 documents.
	ErrBatchSizeLimitExceeded = errors.New("BatchSizeLimitExceededException")
	// ErrTextSizeLimitExceeded is returned when a single document's text
	// exceeds the per-operation byte limit.
	ErrTextSizeLimitExceeded = errors.New("TextSizeLimitExceededException")
	// ErrUnsupportedLanguage is returned when a request's LanguageCode is not
	// one of the languages the target operation supports.
	ErrUnsupportedLanguage = errors.New("UnsupportedLanguageException")
	// ErrKmsKeyValidation is returned when a supplied KMS key ID/ARN does not
	// match a valid KMS key ID or ARN shape.
	ErrKmsKeyValidation = errors.New("KmsKeyValidationException")
)
