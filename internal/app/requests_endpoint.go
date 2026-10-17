package app

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	pkgstrs "github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	defaultRequestsLimit = 50
	statusClassDivisor   = 100
	statusClassSuffix    = "xx"
	statusFailed         = "failed"
)

var errBadRequestsParam = errors.New("invalid query parameter")

type requestsFilter struct {
	match   func(int) bool
	service string
	n       int
}

func parseRequestsFilter(c *echo.Context, buf *logger.RequestRingBuffer) (requestsFilter, error) {
	f := requestsFilter{n: min(defaultRequestsLimit, buf.MaxSize()), service: c.QueryParam("service")}

	if raw := c.QueryParam("n"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > buf.MaxSize() {
			return f, fmt.Errorf("%w: n must be an integer between 1 and %d", errBadRequestsParam, buf.MaxSize())
		}

		f.n = n
	}

	if raw := c.QueryParam("status"); raw != "" {
		match, err := parseStatusMatcher(raw)
		if err != nil {
			return f, err
		}

		f.match = match
	}

	return f, nil
}

func parseStatusMatcher(raw string) (func(int) bool, error) {
	lower := strings.ToLower(raw)

	if lower == statusFailed {
		return func(s int) bool { return s >= http.StatusBadRequest }, nil
	}

	if class, ok := strings.CutSuffix(
		lower,
		statusClassSuffix,
	); ok && len(class) == 1 && class[0] >= '1' &&
		class[0] <= '5' {
		lo := int(class[0]-'0') * statusClassDivisor

		return func(s int) bool { return s >= lo && s < lo+statusClassDivisor }, nil
	}

	code, err := strconv.Atoi(raw)
	if err != nil || code < 100 || code > 599 {
		return nil, fmt.Errorf("%w: status must be a code like 500, a class like 4xx/5xx, or %q",
			errBadRequestsParam, statusFailed)
	}

	return func(s int) bool { return s == code }, nil
}

// buildRequestsHandler returns the GET /_gopherstack/requests handler: recent
// captured requests, newest first, filtered by n, status and service.
func buildRequestsHandler(buf *logger.RequestRingBuffer) echo.HandlerFunc {
	return func(c *echo.Context) error {
		f, err := parseRequestsFilter(c, buf)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessageField: err.Error()})
		}

		reqs := buf.Recent(f.n, func(r *logger.CapturedRequest) bool {
			if f.match != nil && !f.match(r.Status) {
				return false
			}

			return f.service == "" || pkgstrs.Equal(r.Service, f.service)
		})

		return c.JSON(http.StatusOK, reqs)
	}
}
