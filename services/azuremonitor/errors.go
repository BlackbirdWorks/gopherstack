package azuremonitor

import (
	"errors"
	"fmt"
	"net/http"
)

// Sentinel errors returned by the backend.
var (
	ErrNilAppContext = errors.New("azuremonitor: nil app context")
	ErrNotFound      = errors.New("azuremonitor: resource not found")
	ErrConflict      = errors.New("azuremonitor: resource conflict")
	ErrInvalid       = errors.New("azuremonitor: invalid request")
	ErrUnsupported   = errors.New("azuremonitor: unsupported resource type")
)

func invalid(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, a...))
}

func notFound(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrNotFound, fmt.Sprintf(format, a...))
}

// apiError is a data-plane failure rendered in the Azure Monitor error envelope.
type apiError struct {
	inner   *innerError
	code    string
	message string
	status  int
}

type innerError struct {
	Inner   *innerError `json:"innererror,omitempty"`
	Code    string      `json:"code"`
	Message string      `json:"message"`
}

func (e *apiError) Error() string { return e.code + ": " + e.message }

func newErr(status int, code, format string, a ...any) *apiError {
	return &apiError{status: status, code: code, message: fmt.Sprintf(format, a...)}
}

type errorBody struct {
	Inner         *innerError `json:"innererror,omitempty"`
	Message       string      `json:"message"`
	Code          string      `json:"code"`
	CorrelationID string      `json:"correlationId,omitempty"`
}

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

func (e *apiError) envelope(correlationID string) errorEnvelope {
	return errorEnvelope{Error: errorBody{
		Code: e.code, Message: e.message, Inner: e.inner, CorrelationID: correlationID,
	}}
}

func errUnauthorized(msg string) *apiError {
	return newErr(http.StatusUnauthorized, "AuthorizationRequiredError", "%s", msg)
}
