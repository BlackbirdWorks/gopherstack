package mgn

import (
	"errors"
	"fmt"
)

// ErrNilAppContext is returned by Provider.Init when appCtx is nil.
var ErrNilAppContext = errors.New("AppContext is required")

// sentinel errors, matched via errors.Is by handler.go's handleError to pick
// the wire error type/HTTP status. All 8 modeled exception shapes
// (AccessDeniedException, ConflictException, InternalServerException,
// ResourceNotFoundException, ServiceQuotaExceededException,
// ThrottlingException, UninitializedAccountException, ValidationException)
// were confirmed by reading types/errors.go directly, and per-op membership
// was confirmed by extracting every op's own
// awsRestjson1_deserializeOpError<Op> switch body in deserializers.go (all
// 95, not sampled) -- see PARITY.md.
//
// errUninitializedAccount and errThrottling never coexist on the same op
// (PARITY.md's "two generations" split: 69 legacy ops draw
// UninitializedAccountException and never ThrottlingException; the tagging
// trio plus all 25 /network-migration/ ops draw ThrottlingException and
// never UninitializedAccountException). Each backend method in this package
// only returns the sentinel(s) valid for the real op(s) that call it -- this
// file does not enforce that structurally, so getting it right per-call-site
// matters (see each family file's own doc comments).
var (
	errAccessDenied         = errors.New("access denied")
	errConflictSentinel     = errors.New("conflict")
	errInternalServer       = errors.New("internal server error")
	errNotFoundSentinel     = errors.New("resource not found")
	errQuotaExceeded        = errors.New("service quota exceeded")
	errThrottling           = errors.New("throttling")
	errUninitializedAccount = errors.New("uninitialized account")
	errValidationSentinel   = errors.New("validation error")
)

// apiError carries every field any of this service's 8 wire exception
// shapes might need. Only the fields relevant to the concrete shape being
// rendered are populated by any one constructor below; handler.go's
// handleError reads only the fields that shape's JSON envelope defines.
type apiError struct {
	cause        error
	message      string
	resourceID   string
	resourceType string
	reason       string
	serviceCode  string
	quotaCode    string
	quotaValue   int32
	hasQuota     bool
}

func (e *apiError) Error() string { return e.message }
func (e *apiError) Unwrap() error { return e.cause }

// conflictErrorWithResource builds a ConflictException-shaped error that
// also carries the wire ResourceId/ResourceType fields.
func conflictErrorWithResource(resourceType, resourceID, msg string) error {
	return &apiError{cause: errConflictSentinel, message: msg, resourceType: resourceType, resourceID: resourceID}
}

// internalServerError builds an InternalServerException-shaped error --
// only the tagging trio's own error set includes this shape (PARITY.md).
// Used by handler.go's marshalResponse: a JSON-marshal failure on this
// package's own well-typed wire structs should never happen, but if it
// somehow did, it is a genuine internal-server-fault condition, not a
// caller-input problem -- so it is classified accordingly rather than
// folded into a generic error.
func internalServerError(msg string) error {
	return &apiError{cause: errInternalServer, message: msg}
}

// notFoundError builds a ResourceNotFoundException-shaped error.
func notFoundError(resourceType, resourceID string) error {
	return &apiError{
		cause:        errNotFoundSentinel,
		message:      fmt.Sprintf("%s %s not found", resourceType, resourceID),
		resourceType: resourceType,
		resourceID:   resourceID,
	}
}

// errAccessDenied/errQuotaExceeded/errThrottling (declared above) back
// classifyMGNError's wire-shape classification for
// AccessDeniedException/ServiceQuotaExceededException/ThrottlingException,
// even though no call site in this package currently constructs one: this
// backend has no permission model, no account-level resource-count quota
// model (no AWS-published default quota numbers to enforce without
// fabricating one, matching services/outposts/resiliencehub/grafana's
// identical treatment), and no rate limiter to simulate. Documented here as
// a real, deliberate gap -- not silently missing -- rather than forcing a
// fake trigger just to exercise an unused constructor.

// uninitializedAccountError builds an UninitializedAccountException-shaped
// error -- returned by every legacy (non-tagging, non-/network-migration/)
// op when InitializeService has never been called for the caller's account.
// See serviceinit.go.
func uninitializedAccountError(msg string) error {
	return &apiError{cause: errUninitializedAccount, message: msg}
}

// validationError builds a ValidationException-shaped error with Reason
// defaulted to "fieldValidationFailed" (types.ValidationExceptionReason's
// lower-camelCase wire values -- PARITY.md wire-trap #6).
func validationError(msg string) error {
	return &apiError{cause: errValidationSentinel, message: msg, reason: ValidationReasonFieldValidationFailed}
}
