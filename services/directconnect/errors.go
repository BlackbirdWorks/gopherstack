package directconnect

import (
	"errors"
	"fmt"
)

// ErrNilAppContext is returned by Provider.Init when appCtx is nil.
var ErrNilAppContext = errors.New("AppContext is required")

// sentinel errors, matched via errors.Is by handler.go's handleError to pick
// the wire error type/HTTP status. Exactly 5 modeled exception shapes exist
// service-wide (types/errors.go, confirmed for all 63 ops individually --
// see PARITY.md): DirectConnectClientException, DirectConnectServerException,
// DuplicateTagKeysException, LimitExceededException, TooManyTagsException.
// Every one of the five carries ONLY a bare Message field -- there is no
// ResourceId/ResourceType wire field anywhere in this service (unlike
// outposts' NotFoundException or resiliencehub's ConflictException), so
// apiError below is far thinner than its siblings in those two packages.
var (
	errClientSentinel   = errors.New("direct connect client error")
	errServerSentinel   = errors.New("direct connect server error")
	errDuplicateTagKeys = errors.New("duplicate tag keys")
	errLimitExceeded    = errors.New("limit exceeded")
	errTooManyTags      = errors.New("too many tags")
)

// apiError carries the message for one of Direct Connect's five wire
// exception shapes. There is no resourceId/resourceType to carry -- see the
// package doc comment above.
type apiError struct {
	cause   error
	message string
}

func (e *apiError) Error() string { return e.message }
func (e *apiError) Unwrap() error { return e.cause }

// clientError builds a DirectConnectClientException-shaped error -- the
// generic catch-all for essentially every bad-input, not-found, and
// conflict condition in this API (there is no dedicated
// ResourceNotFoundException or ValidationException in this service; see
// PARITY.md's wire-trap #2).
func clientError(msg string) error {
	return &apiError{cause: errClientSentinel, message: msg}
}

// notFoundError builds a DirectConnectClientException-shaped "not found"
// error for the named resource kind/id -- folded into the same generic
// client exception every other bad-input condition uses, per this
// service's unusually thin error model.
func notFoundError(resourceType, id string) error {
	return clientError(fmt.Sprintf("%s %s not found", resourceType, id))
}

// serverError builds a DirectConnectServerException-shaped error.
func serverError(msg string) error {
	return &apiError{cause: errServerSentinel, message: msg}
}

// duplicateTagKeysError builds a DuplicateTagKeysException-shaped error.
func duplicateTagKeysError(msg string) error {
	return &apiError{cause: errDuplicateTagKeys, message: msg}
}

// limitExceededError builds a LimitExceededException-shaped error.
func limitExceededError(msg string) error {
	return &apiError{cause: errLimitExceeded, message: msg}
}

// tooManyTagsError builds a TooManyTagsException-shaped error.
func tooManyTagsError(msg string) error {
	return &apiError{cause: errTooManyTags, message: msg}
}

// maxTagsPerResource bounds TagResource/Create*'s Tags input. AWS does not
// publish this service's exact tag-count quota in the SDK; 50 matches the
// account/resource tagging default used broadly across AWS services and is
// a defensible, documented stand-in, NOT a verified Direct Connect number.
const maxTagsPerResource = 50

// validateNewTags checks a caller-supplied tag list for duplicate keys and
// the tag-count limit, returning a DuplicateTagKeysException or
// TooManyTagsException-shaped error as appropriate. Only called from the
// ops PARITY.md's per-op exception table confirms accept these two
// exceptions (see consts.go/PARITY.md's table); ops with no Tags field, or
// with Tags but no tag-exceptions in their own deserializeOpError switch
// (e.g. CreateDirectConnectGateway), must not call this.
func validateNewTags(keys []string) error {
	if len(keys) > maxTagsPerResource {
		return tooManyTagsError(fmt.Sprintf("cannot exceed %d tags per resource", maxTagsPerResource))
	}

	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		if seen[k] {
			return duplicateTagKeysError("duplicate tag key: " + k)
		}

		seen[k] = true
	}

	return nil
}
