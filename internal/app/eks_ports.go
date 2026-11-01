package app

import (
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
)

// registerEKSEndpoints serves GET /_gopherstack/eks/clusters/<name>/ports.
func registerEKSEndpoints(e *echo.Echo, services []service.Registerable) {
	for _, svc := range services {
		if h, ok := svc.(*eksbackend.Handler); ok {
			e.Add(http.MethodGet, "/_gopherstack/eks/clusters/:name/ports", h.ServePublishedPorts)
		}
	}
}
