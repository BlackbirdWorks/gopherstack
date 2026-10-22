package bcmdataexports

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinels, one per exception in the pinned SDK's error set.
var (
	ErrAccessDenied         = errors.New("access denied")
	ErrInternalServer       = errors.New("internal server error")
	ErrResourceNotFound     = errors.New("resource not found")
	ErrServiceQuotaExceeded = errors.New("service quota exceeded")
	ErrThrottling           = errors.New("throttled")
	ErrValidation           = errors.New("validation failed")
	errUnknownOperation     = errors.New("unknown operation")
)

func fail(sentinel error, msg string, args ...any) error {
	return awserr.Newf(msg, sentinel, args...)
}

func apiErr(code string, status int) awserr.APIError {
	return awserr.APIError{Code: code, HTTPStatus: status}
}

func errorTable() map[error]awserr.APIError {
	return map[error]awserr.APIError{
		ErrAccessDenied:         apiErr("AccessDeniedException", http.StatusForbidden),
		ErrInternalServer:       apiErr("InternalServerException", http.StatusInternalServerError),
		ErrResourceNotFound:     apiErr("ResourceNotFoundException", http.StatusNotFound),
		ErrServiceQuotaExceeded: apiErr("ServiceQuotaExceededException", http.StatusBadRequest),
		ErrThrottling:           apiErr("ThrottlingException", http.StatusBadRequest),
		ErrValidation:           apiErr("ValidationException", http.StatusBadRequest),
		errUnknownOperation:     apiErr("ValidationException", http.StatusBadRequest),
	}
}

// ValidationException reasons from the SDK's ValidationExceptionReason enum.
const (
	reasonUnknownOperation = "unknownOperation"
	reasonCannotParse      = "cannotParse"
	reasonFieldFailed      = "fieldValidationFailed"
	reasonOther            = "other"
)

type fieldError struct {
	Name    string `json:"Name"`
	Message string `json:"Message"`
}

// detail carries the modeled members beyond Message: ResourceNotFound ids, ValidationException reason and fields.
type detail struct {
	resourceID, resourceType, reason string
	fields                           []fieldError
}

type detailedError struct {
	error
	d detail
}

func (e *detailedError) Unwrap() error { return e.error }

func notFound(resourceType, id, msg string, args ...any) error {
	return &detailedError{
		error: fail(ErrResourceNotFound, msg, args...),
		d:     detail{resourceID: id, resourceType: resourceType},
	}
}

func failReason(reason, msg string, args ...any) error {
	return &detailedError{error: fail(ErrValidation, msg, args...), d: detail{reason: reason}}
}

func failField(field, msg string, args ...any) error {
	text := fmt.Sprintf(msg, args...)

	return &detailedError{
		error: fail(ErrValidation, "%s", text),
		d:     detail{reason: reasonFieldFailed, fields: []fieldError{{Name: field, Message: text}}},
	}
}

func errorDetail(err error) detail {
	if de, ok := errors.AsType[*detailedError](err); ok {
		return de.d
	}

	if errors.Is(err, errUnknownOperation) {
		return detail{reason: reasonUnknownOperation}
	}

	if errors.Is(err, ErrValidation) {
		return detail{reason: reasonOther}
	}

	return detail{}
}

type errorBody struct {
	Type         string       `json:"__type"`
	Message      string       `json:"Message"`
	ResourceID   string       `json:"ResourceId,omitempty"`
	ResourceType string       `json:"ResourceType,omitempty"`
	Reason       string       `json:"Reason,omitempty"`
	Fields       []fieldError `json:"Fields,omitempty"`
}

// writeErrorBody writes the JSON 1.1 error envelope with the modeled members the SDK deserializes.
func writeErrorBody(c *echo.Context, e awserr.APIError, d detail) error {
	body, _ := json.Marshal(errorBody{
		Type: e.Code, Message: e.Message, ResourceID: d.resourceID,
		ResourceType: d.resourceType, Reason: d.reason, Fields: d.fields,
	})

	c.Response().Header().Set("X-Amz-Crc32", strconv.FormatUint(uint64(crc32.ChecksumIEEE(body)), 10))
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	return c.JSONBlob(e.HTTPStatus, body)
}
