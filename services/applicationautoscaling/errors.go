package applicationautoscaling

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when a requested scalable target, scaling
	// policy, or scheduled action does not exist. Wire type ObjectNotFoundException.
	ErrNotFound = awserr.New("ObjectNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists. No backend
	// method currently returns this: every Put*/Register* op is upsert-only
	// by design, matching real AWS semantics (there is no create-only path
	// that could conflict). Kept for completeness of the error-handling
	// switch, not dead in the sense of being unreachable-and-harmful.
	ErrAlreadyExists = awserr.New("ValidationException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when a request parameter fails validation.
	// Wire type ValidationException.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrResourceNotFound is returned by the tagging operations
	// (ListTagsForResource/TagResource/UntagResource) when the resource ARN
	// does not correspond to a registered scalable target. Real AWS models
	// these three ops with ResourceNotFoundException, NOT
	// ObjectNotFoundException -- confirmed against the modeled error sets in
	// aws-sdk-go-v2/service/applicationautoscaling's deserializers.go
	// (awsAwsjson11_deserializeOpErrorTagResource et al. switch only on
	// ResourceNotFoundException/TooManyTagsException/ValidationException,
	// never ObjectNotFoundException).
	ErrResourceNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrTooManyTags is returned by TagResource when applying the requested
	// tags would exceed the per-resource tag limit. Wire type
	// TooManyTagsException (only modeled on TagResource, not on
	// RegisterScalableTarget -- see ErrLimitExceeded).
	ErrTooManyTags = awserr.New("TooManyTagsException", awserr.ErrInvalidParameter)
	// ErrLimitExceeded is returned when a per-account/per-target quota is
	// exceeded: tags on RegisterScalableTarget (that op's modeled error set
	// has LimitExceededException, not TooManyTagsException), scaling
	// policies per scalable target (50, not adjustable), step adjustments
	// per step scaling policy (20, adjustable), and scheduled actions per
	// scalable target (200, not adjustable) -- quotas confirmed against the
	// "Quotas for Application Auto Scaling" AWS documentation page.
	ErrLimitExceeded = awserr.New("LimitExceededException", awserr.ErrInvalidParameter)
	// ErrInvalidNextToken is returned by Describe* operations when the
	// caller supplies a NextToken that fails to decode.
	ErrInvalidNextToken = awserr.New("InvalidNextTokenException", awserr.ErrInvalidParameter)
	// ErrConcurrentUpdate corresponds to real AWS's ConcurrentUpdateException
	// (thrown when a resource already has a pending update). gopherstack's
	// backend serializes every operation behind one coarse write lock, so
	// there is no window in which two updates to the same resource can race
	// -- this sentinel exists so the error-handling switch and HTTP-status
	// mapping are complete, but no backend method currently returns it.
	ErrConcurrentUpdate = awserr.New("ConcurrentUpdateException", awserr.ErrConflict)
	// ErrFailedResourceAccess corresponds to real AWS's
	// FailedResourceAccessException (thrown when Application Auto Scaling
	// cannot retrieve the CloudWatch alarms behind a scaling policy, e.g. a
	// bad RoleARN). gopherstack does not perform real CloudWatch
	// cross-service calls (see the deferred CloudWatch alarm integration
	// note in PARITY.md), so no backend method currently returns it; the
	// sentinel exists for a complete error-handling switch.
	ErrFailedResourceAccess = awserr.New("FailedResourceAccessException", awserr.ErrInvalidParameter)
	// ErrInternalService is the generic InternalServiceException fallback
	// used for the handler's default error case, matching real AWS's wire
	// shape for unexpected server-side failures instead of a bespoke,
	// non-AWS-shaped JSON body. No existing awserr category (NotFound/
	// AlreadyExists/InvalidParameter/Conflict) represents a server fault, so
	// this is a plain local sentinel rather than an awserr.New wrapper.
	ErrInternalService = errors.New("InternalServiceException")
)

// resourceNameError decorates an error with the AWS "ResourceName" field that
// TooManyTagsException and ResourceNotFoundException carry on the wire
// (confirmed against their deserializeDocument* functions in the SDK, both of
// which read a "ResourceName" key alongside "message"). Use [withResourceName]
// to attach one and errors.As to retrieve it in the handler's error path.
type resourceNameError struct {
	err          error
	resourceName string
}

func (e *resourceNameError) Error() string        { return e.err.Error() }
func (e *resourceNameError) Unwrap() error        { return e.err }
func (e *resourceNameError) ResourceName() string { return e.resourceName }

// withResourceName wraps err so the handler layer can recover resourceName
// for inclusion in the AWS error envelope's ResourceName field.
func withResourceName(err error, resourceName string) error {
	return &resourceNameError{err: err, resourceName: resourceName}
}
