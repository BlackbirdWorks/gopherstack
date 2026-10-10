package iot

import (
	"errors"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

// publicMessage removes the sentinel text error wrapping adds around a detail,
// so clients do not see "resource already exists: ... already exists".
func publicMessage(err error) string {
	msg := err.Error()

	for _, s := range []error{ErrValidation, ErrAlreadyExists, ErrMalformedPolicy} {
		if errors.Is(err, s) {
			msg = strings.TrimPrefix(msg, s.Error()+": ")
			msg = strings.TrimSuffix(msg, ": "+s.Error())
		}
	}

	if errors.Is(err, ErrResourceNotFound) {
		code := ErrResourceNotFound.Error()
		if rest, ok := strings.CutPrefix(msg, code+": "); ok {
			return rest + " not found"
		}

		msg = strings.TrimSuffix(msg, ": "+code)
	}

	return msg
}

// invalidPaginationQuery reports a malformed list-page token or size query member.
func invalidPaginationQuery(c *echo.Context) string {
	q := c.Request().URL.Query()

	for _, key := range []string{"nextToken", "marker"} {
		if v := q.Get(key); v != "" {
			if n, err := strconv.Atoi(v); err != nil || n < 0 {
				return "Invalid " + key
			}
		}
	}

	for _, key := range []string{"maxResults", "pageSize"} {
		if v := q.Get(key); v != "" {
			if n, err := strconv.Atoi(v); err != nil || n < 1 {
				return "Invalid " + key + ": must be a positive integer"
			}
		}
	}

	return ""
}
