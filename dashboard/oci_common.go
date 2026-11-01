package dashboard

import (
	"errors"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/services/ocifunctions"
	"github.com/blackbirdworks/gopherstack/services/ociidentity"
	"github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
	"github.com/blackbirdworks/gopherstack/services/ocistreaming"
	"github.com/blackbirdworks/gopherstack/services/ocivault"
)

const (
	ociKeyGroups  = "groups"
	ociKeyObjects = "objects"
	ociKeyRules   = "rules"
	ociMimeOctet  = "application/octet-stream"

	ociProxyPrefix     = "/dashboard/api/v1/oci"
	ociCompartmentsKey = "compartments"
	ociListLimit       = 1000
	ociMaxListPages    = 100
)

// ociAPIError extracts the HTTP status, code and message of any OCI service error.
func ociAPIError(err error) (int, string, string, bool) {
	var (
		oe *ociobjectstorage.APIError
		ve *ocivault.APIError
		se *ocistreaming.APIError
		ie *ociidentity.APIError
		fe *ocifunctions.APIError
	)

	switch {
	case errors.As(err, &oe):
		return oe.Status, oe.Code, oe.Message, true
	case errors.As(err, &ve):
		return ve.Status, ve.Code, ve.Message, true
	case errors.As(err, &se):
		return se.Status, se.Code, se.Message, true
	case errors.As(err, &ie):
		return ie.Status, ie.Code, ie.Message, true
	case errors.As(err, &fe):
		return fe.Status, fe.Code, fe.Message, true
	}

	return 0, "", "", false
}

// ociFail renders a backend error in the envelope the SPA's restCall client parses.
func ociFail(c *echo.Context, err error) error {
	if status, code, msg, ok := ociAPIError(err); ok {
		return cloudJSONFail(c, status, code, msg)
	}

	return cloudJSONFail(c, http.StatusInternalServerError, "InternalServerError", err.Error())
}

// ociGuard answers 503 when the service behind a proxy is not served.
func ociGuard(served bool, name string, next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if !served {
			return cloudUnavailable(c, name)
		}

		return next(c)
	}
}

// ociCollect drains every page of a paginated backend listing.
func ociCollect[T any](fetch func(token string) (page.Page[T], error)) ([]T, error) {
	var out []T

	token := ""

	for range ociMaxListPages {
		pg, err := fetch(token)
		if err != nil {
			return nil, err
		}

		out = append(out, pg.Data...)

		if pg.Next == "" {
			break
		}

		token = pg.Next
	}

	if out == nil {
		out = []T{}
	}

	return out, nil
}

// ociAcross gathers a listing across every compartment.
func ociAcross[T any](compartments []string, list func(compartment string) ([]T, error)) ([]T, error) {
	out := []T{}

	for _, comp := range compartments {
		items, err := list(comp)
		if err != nil {
			return nil, err
		}

		out = append(out, items...)
	}

	return out, nil
}

// anyPage erases a page's element type so listings of unexported wire types can share one collector.
func anyPage[T any](data []T, next string) page.Page[any] {
	out := make([]any, 0, len(data))
	for _, d := range data {
		out = append(out, d)
	}

	return page.Page[any]{Data: out, Next: next}
}

// unescapedPath decodes a percent-encoded URL path, keeping the input when it is malformed.
func unescapedPath(escaped string) string {
	if p, err := url.PathUnescape(escaped); err == nil {
		return p
	}

	return escaped
}
