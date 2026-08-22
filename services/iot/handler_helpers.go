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

// errTypeInvalidRequest is the AWS IoT restjson1 __type for a malformed or
// otherwise invalid request body -- shared across every call site that
// rejects a request before it reaches backend validation.
const errTypeInvalidRequest = "InvalidRequestException"

func readBody(c *echo.Context, dst any) error {
	if err := json.NewDecoder(c.Request().Body).Decode(dst); err != nil && !errors.Is(err, io.EOF) {
		if jsonErr := c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()}); jsonErr != nil {
			return jsonErr
		}

		return err
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
// shape ({"__type", "message"}) and HTTP status. Single source of truth
// shared by respondErr and Handler.handleError, so every handler gets the
// same ResourceNotFoundException/InvalidRequestException/
// ResourceAlreadyExistsException/VersionConflictException/
// DeleteConflictException/VersionsLimitExceededException/
// InvalidStateTransitionException mapping.
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

		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()})
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
