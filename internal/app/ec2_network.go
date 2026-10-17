package app

import (
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

// registerEC2NetworkEndpoints serves GET /_gopherstack/ec2/instances/<id>/ports.
func registerEC2NetworkEndpoints(e *echo.Echo, services []service.Registerable) {
	for _, svc := range services {
		if h, ok := svc.(*ec2backend.Handler); ok {
			e.Add(http.MethodGet, "/_gopherstack/ec2/instances/:instance/ports", h.ServePublishedPorts)
		}
	}
}
