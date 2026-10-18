package servicequotas

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinels, one per exception the pinned SDK models that this service raises.
var (
	ErrAccessDenied                     = errors.New("access denied")
	ErrAWSServiceAccessNotEnabled       = errors.New("service access is not enabled")
	ErrIllegalArgument                  = errors.New("illegal argument")
	ErrInvalidPaginationToken           = errors.New("invalid pagination token")
	ErrInvalidResourceState             = errors.New("invalid resource state")
	ErrNoAvailableOrganization          = errors.New("no available organization")
	ErrNoSuchResource                   = errors.New("no such resource")
	ErrOrganizationNotInAllFeaturesMode = errors.New("organization is not in all features mode")
	ErrQuotaExceeded                    = errors.New("quota exceeded")
	ErrResourceAlreadyExists            = errors.New("resource already exists")
	ErrServiceQuotaTemplateNotInUse     = errors.New("quota request template is not in use")
	ErrTooManyTags                      = errors.New("too many tags")
	errUnknownOperation                 = errors.New("unknown operation")
)

func fail(sentinel error, msg string, args ...any) error {
	return awserr.Newf(msg, sentinel, args...)
}

func apiErr(code string, status int) awserr.APIError {
	return awserr.APIError{Code: code, HTTPStatus: status}
}

func errorTable() map[error]awserr.APIError {
	return map[error]awserr.APIError{
		ErrAccessDenied:                     apiErr("AccessDeniedException", http.StatusBadRequest),
		ErrAWSServiceAccessNotEnabled:       apiErr("AWSServiceAccessNotEnabledException", http.StatusBadRequest),
		ErrIllegalArgument:                  apiErr("IllegalArgumentException", http.StatusBadRequest),
		ErrInvalidPaginationToken:           apiErr("InvalidPaginationTokenException", http.StatusBadRequest),
		ErrInvalidResourceState:             apiErr("InvalidResourceStateException", http.StatusBadRequest),
		ErrNoAvailableOrganization:          apiErr("NoAvailableOrganizationException", http.StatusBadRequest),
		ErrNoSuchResource:                   apiErr("NoSuchResourceException", http.StatusBadRequest),
		ErrOrganizationNotInAllFeaturesMode: apiErr("OrganizationNotInAllFeaturesModeException", http.StatusBadRequest),
		ErrQuotaExceeded:                    apiErr("QuotaExceededException", http.StatusBadRequest),
		ErrResourceAlreadyExists:            apiErr("ResourceAlreadyExistsException", http.StatusBadRequest),
		ErrServiceQuotaTemplateNotInUse:     apiErr("ServiceQuotaTemplateNotInUseException", http.StatusBadRequest),
		ErrTooManyTags:                      apiErr("TooManyTagsException", http.StatusBadRequest),
		errUnknownOperation:                 apiErr("IllegalArgumentException", http.StatusBadRequest),
	}
}
