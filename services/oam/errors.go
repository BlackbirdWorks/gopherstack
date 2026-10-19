package oam

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Sentinels, one per exception in the pinned SDK's error set.
var (
	ErrConflict             = errors.New("conflict")
	ErrInvalidParameter     = errors.New("invalid parameter")
	ErrMissingParameter     = errors.New("missing required parameter")
	ErrResourceNotFound     = errors.New("resource not found")
	ErrServiceQuotaExceeded = errors.New("service quota exceeded")
	ErrTooManyTags          = errors.New("too many tags")
)

func fail(sentinel error, msg string, args ...any) error {
	return awserr.Newf(msg, sentinel, args...)
}

type errMapping struct {
	sentinel error
	code     string
	status   int
}

func errorMappings() []errMapping {
	return []errMapping{
		{ErrConflict, "ConflictException", http.StatusConflict},
		{ErrInvalidParameter, "InvalidParameterException", http.StatusBadRequest},
		{ErrMissingParameter, "MissingRequiredParameterException", http.StatusBadRequest},
		{ErrResourceNotFound, "ResourceNotFoundException", http.StatusNotFound},
		{ErrServiceQuotaExceeded, "ServiceQuotaExceededException", http.StatusPaymentRequired},
		{ErrTooManyTags, "TooManyTagsException", http.StatusBadRequest},
	}
}

// toAPIError maps a backend error to its REST-JSON exception.
func toAPIError(err error) error {
	if _, ok := errors.AsType[*restroute.Error](err); ok {
		return err
	}

	for _, m := range errorMappings() {
		if errors.Is(err, m.sentinel) {
			return restroute.NewError(m.status, m.code, "%s", err.Error())
		}
	}

	return restroute.NewError(http.StatusInternalServerError, "InternalServiceFault", "internal service error")
}
