package gcpidentitytoolkit

import (
	"errors"
	"fmt"
	"net/http"
)

// APIError is a Google JSON REST error; Message carries the Identity Toolkit error code.
type APIError struct {
	Message string
	Status  string
	HTTP    int
}

// Error implements error.
func (e *APIError) Error() string { return fmt.Sprintf("gcpidentitytoolkit: %d %s", e.HTTP, e.Message) }

// ErrSnapshotNull is returned when a snapshot holds a null record.
var ErrSnapshotNull = errors.New("gcpidentitytoolkit: snapshot contains a null record")

// idtk builds a 400 error whose message is "CODE" or "CODE : detail", the form the Admin SDK parses.
func idtk(code string, detail ...string) *APIError {
	msg := code
	if len(detail) > 0 && detail[0] != "" {
		msg = code + " : " + detail[0]
	}

	return &APIError{HTTP: http.StatusBadRequest, Message: msg}
}

func errNotFound(code string) *APIError {
	return &APIError{HTTP: http.StatusNotFound, Message: code, Status: "NOT_FOUND"}
}

func errUnauth() *APIError {
	return &APIError{
		HTTP: http.StatusUnauthorized, Status: "UNAUTHENTICATED",
		Message: "Request is missing required authentication credential. Expected OAuth 2 access token, " +
			"login cookie or other valid authentication credential.",
	}
}

func errNoKey() *APIError {
	return &APIError{
		HTTP: http.StatusForbidden, Status: "PERMISSION_DENIED",
		Message: "The request is missing a valid API key.",
	}
}

func errInvalidArg(format string, a ...any) *APIError {
	return &APIError{HTTP: http.StatusBadRequest, Status: "INVALID_ARGUMENT", Message: fmt.Sprintf(format, a...)}
}

type errorItem struct {
	Message string `json:"message"`
	Domain  string `json:"domain"`
	Reason  string `json:"reason"`
}

type errorDetail struct {
	Message string      `json:"message"`
	Status  string      `json:"status,omitempty"`
	Errors  []errorItem `json:"errors,omitempty"`
	Code    int         `json:"code"`
}

func (e *APIError) body() map[string]errorDetail {
	d := errorDetail{Code: e.HTTP, Message: e.Message, Status: e.Status}
	if e.Status == "" {
		d.Errors = []errorItem{{Message: e.Message, Domain: "global", Reason: "invalid"}}
	}

	return map[string]errorDetail{"error": d}
}

var (
	errBadBase64    = errors.New("gcpidentitytoolkit: invalid base64")
	errBadAlgorithm = errors.New("gcpidentitytoolkit: unsupported hash configuration")
)
