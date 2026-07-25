package iot

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

func readBody(c *echo.Context, dst any) error {
	if err := json.NewDecoder(c.Request().Body).Decode(dst); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	return nil
}

type awsErrBody struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

func respondNotFound(c *echo.Context, msg string) error {
	return c.JSON(http.StatusNotFound, awsErrBody{"ResourceNotFoundException", msg})
}

func respondConflict(c *echo.Context, msg string) error {
	return c.JSON(http.StatusConflict, awsErrBody{"ResourceAlreadyExistsException", msg})
}

// writeIoTError maps a backend sentinel error to the AWS IoT restjson1 error
// shape ({"__type", "message"}) and HTTP status. It is the single source of
// truth for error-code mapping shared by respondErr (used by the
// batch2/batch3 "extended" op handlers) and Handler.handleError (used by the
// core op handlers), so every handler gets the exact same
// ResourceNotFoundException/InvalidRequestException/
// ResourceAlreadyExistsException/VersionConflictException/
// DeleteConflictException/VersionsLimitExceededException/
// InvalidStateTransitionException mapping regardless
// of which helper it calls. Previously respondErr only recognized
// ErrResourceNotFound and ErrAlreadyExists, so domain-specific not-found
// sentinels (ErrCertificateNotFound, ErrThingNotFound, etc.) and
// ErrVersionConflict/ErrDeleteConflict/ErrVersionsLimitExceeded fell through
// to a wrong status code (400 InvalidRequestException, or the 500 default)
// in every handler that used respondErr instead of handleError.
func writeIoTError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrThingNotFound),
		errors.Is(err, ErrRuleNotFound),
		errors.Is(err, ErrPolicyNotFound),
		errors.Is(err, ErrThingTypeNotFound),
		errors.Is(err, ErrThingGroupNotFound),
		errors.Is(err, ErrCertificateNotFound),
		errors.Is(err, ErrCertificateProviderNotFound),
		errors.Is(err, ErrTopicRuleDestinationNotFound),
		errors.Is(err, ErrPolicyVersionNotFound),
		errors.Is(err, ErrRegistrationTaskNotFound),
		errors.Is(err, ErrManagedJobTemplateNotFound),
		errors.Is(err, ErrIndexNotFound),
		errors.Is(err, ErrShadowNotFound),
		errors.Is(err, ErrResourceNotFound):

		return respondNotFound(c, err.Error())
	case errors.Is(err, ErrValidation):

		return c.JSON(http.StatusBadRequest, awsErrBody{"InvalidRequestException", err.Error()})
	case errors.Is(err, ErrAlreadyExists):

		return respondConflict(c, err.Error())
	case errors.Is(err, ErrVersionConflict):

		return c.JSON(http.StatusConflict, awsErrBody{"VersionConflictException", err.Error()})
	case errors.Is(err, ErrDeleteConflict):

		return c.JSON(http.StatusConflict, awsErrBody{"DeleteConflictException", err.Error()})
	case errors.Is(err, ErrVersionsLimitExceeded):

		return c.JSON(http.StatusConflict, awsErrBody{"VersionsLimitExceededException", err.Error()})
	case errors.Is(err, ErrInvalidStateTransition):

		return c.JSON(http.StatusConflict, awsErrBody{"InvalidStateTransitionException", err.Error()})
	default:

		return c.JSON(http.StatusInternalServerError, awsErrBody{"InternalFailureException", err.Error()})
	}
}

func respondErr(c *echo.Context, err error) error {
	return writeIoTError(c, err)
}

func parseInt32(s string, out *int32) error {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	if err != nil {
		return err
	}
	*out = int32(n) //nolint:gosec // safe: versionId values are small positive integers

	return nil
}

// parseInt32QueryParam reads an int32 query parameter, defaulting to 0 when
// absent or invalid.
func parseInt32QueryParam(c *echo.Context, name string) int32 {
	v := c.QueryParam(name)
	if v == "" {
		return 0
	}

	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return 0
	}

	return int32(n)
}
