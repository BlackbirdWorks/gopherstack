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

func respondErr(c *echo.Context, err error) error {
	if errors.Is(err, ErrResourceNotFound) {
		return respondNotFound(c, err.Error())
	}
	if errors.Is(err, ErrAlreadyExists) {
		return respondConflict(c, err.Error())
	}

	return c.JSON(http.StatusBadRequest, awsErrBody{"InvalidRequestException", err.Error()})
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
