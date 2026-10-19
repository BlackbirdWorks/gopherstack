package connect

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func apiErr(status int, code, format string, args ...any) *restroute.Error {
	return restroute.NewError(status, code, format, args...)
}

func notFoundErr(kind, id string) error {
	return apiErr(http.StatusNotFound, "ResourceNotFoundException", "%s %q was not found", kind, id)
}

func duplicateErr(kind, id string) error {
	return apiErr(http.StatusConflict, "DuplicateResourceException", "%s %q already exists", kind, id)
}

func inUseErr(format string, args ...any) error {
	return apiErr(http.StatusConflict, "ResourceInUseException", format, args...)
}

func invalidParamErr(format string, args ...any) error {
	return apiErr(http.StatusBadRequest, "InvalidParameterException", format, args...)
}

func invalidRequestErr(format string, args ...any) error {
	return apiErr(http.StatusBadRequest, "InvalidRequestException", format, args...)
}

func limitErr(format string, args ...any) error {
	return apiErr(http.StatusTooManyRequests, "LimitExceededException", format, args...)
}

func contactNotFoundErr(id string) error {
	return apiErr(http.StatusGone, "ContactNotFoundException", "contact %q was not found", id)
}

func idempotencyErr(err error) error {
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return apiErr(http.StatusConflict, "IdempotencyException", "%v", err)
	}

	return err
}

func invalidFlowErr(code string, problems ...string) error {
	list := make([]map[string]string, len(problems))
	for i, p := range problems {
		list[i] = map[string]string{"message": p}
	}

	return apiErr(http.StatusBadRequest, code, "%s", problems[0]).With("problems", list)
}
