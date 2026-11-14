package app

import (
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	transcribebackend "github.com/blackbirdworks/gopherstack/services/transcribe"
)

// registerTranscribeEndpoints serves the fetch URLs of service-managed Transcribe output.
func registerTranscribeEndpoints(e *echo.Echo, services []service.Registerable) {
	for _, svc := range services {
		if h, ok := svc.(*transcribebackend.Handler); ok {
			e.Add(http.MethodGet, "/_gopherstack/transcribe/output/*", h.ServeManagedOutput)
		}
	}
}
