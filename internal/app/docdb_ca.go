package app

import (
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/docdb"
)

const (
	docdbCAPath     = "/_gopherstack/docdb/ca.pem"
	docdbBundlePath = "/_gopherstack/docdb/global-bundle.pem"
)

// wireDocDBCA serves the docker-mode DocumentDB CA, the stand-in for AWS's global-bundle.pem trust store.
func wireDocDBCA(e *echo.Echo, services []service.Registerable) {
	var h *docdb.Handler

	for _, svc := range services {
		if dh, ok := svc.(*docdb.Handler); ok {
			h = dh
		}
	}

	if h == nil {
		return
	}

	serve := func(c *echo.Context) error {
		pem := h.Backend.TLSCAPEM()
		if pem == nil {
			return c.String(http.StatusNotFound, "DocumentDB docker engine TLS is not enabled")
		}

		return c.Blob(http.StatusOK, "application/x-pem-file", pem)
	}

	e.GET(docdbCAPath, serve)
	e.GET(docdbBundlePath, serve)
}
