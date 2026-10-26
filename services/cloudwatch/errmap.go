package cloudwatch

import (
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

type errClass struct {
	query  string
	cbor   string
	status int
}

func badRequest(query, cbor string) errClass {
	return errClass{query: query, cbor: cbor, status: http.StatusBadRequest}
}

func classifyError(err error) errClass {
	switch {
	case errors.Is(err, ErrResourceMetricsConfigNotFound), errors.Is(err, ErrOTelEnrichmentNotRunning):
		return errClass{query: "ResourceNotFound", cbor: errResourceNotFoundException, status: http.StatusNotFound}
	case errors.Is(err, ErrResourceMetricsConfigExists):
		return errClass{query: errCodeConflict, cbor: errCodeConflict, status: http.StatusConflict}
	case errors.Is(err, ErrInvalidNextToken):
		return badRequest("InvalidNextToken", "InvalidNextToken")
	case errors.Is(err, ErrParamCombination), errors.Is(err, ErrValueAndStatisticSet):
		return badRequest("InvalidParameterCombination", "InvalidParameterCombinationException")
	case errors.Is(err, ErrValidationFault):
		return badRequest("ValidationError", "ValidationError")
	case errors.Is(err, ErrMissingParameter):
		return badRequest("MissingParameter", "MissingRequiredParameterException")
	case errors.Is(err, ErrMetricSeriesLimitExceeded):
		return badRequest("LimitExceeded", "LimitExceededFault")
	case errors.Is(err, ErrValidation),
		errors.Is(err, ErrValuesCountsLengthMismatch),
		errors.Is(err, ErrTooManyValues),
		errors.Is(err, ErrInvalidMetricValue),
		errors.Is(err, ErrMetricTimestampOutOfRange):
		return badRequest(codeInvalidParam, "InvalidParameterValueException")
	default:
		return errClass{
			query:  errCodeInternalFailure,
			cbor:   errCodeInternalFailure,
			status: http.StatusInternalServerError,
		}
	}
}

func (h *Handler) xmlBackendError(c *echo.Context, err error) error {
	cl := classifyError(err)

	return h.xmlError(c, cl.status, cl.query, err.Error())
}

func (h *Handler) cborBackendError(c *echo.Context, err error) error {
	cl := classifyError(err)

	return h.cborError(c, cl.status, cl.cbor, err.Error())
}

// stripCodePrefix drops a leading "<ErrorCode>: " that sentinel wrapping puts in
// messages, so clients do not see the code twice (once as code, once in text).
func stripCodePrefix(msg string) string {
	code, rest, ok := strings.Cut(msg, ": ")
	if !ok {
		return msg
	}

	switch code {
	case codeInvalidParam, "InvalidParameterCombination", "LimitExceeded", "MissingParameter",
		"ValidationError", "InvalidNextToken", "ResourceNotFound", "ResourceNotFoundException", errCodeConflict:
		return rest
	default:
		return msg
	}
}

func (h *Handler) setAlarmStateError(c *echo.Context, alarmName string, err error, cborMode bool) error {
	if !errors.Is(err, ErrAlarmNotFound) {
		if cborMode {
			return h.cborBackendError(c, err)
		}

		return h.xmlBackendError(c, err)
	}

	msg := "Alarm " + alarmName + " does not exist."
	if cborMode {
		return h.cborError(c, http.StatusNotFound, "ResourceNotFound", msg)
	}

	return h.xmlError(c, http.StatusNotFound, "ResourceNotFound", msg)
}
