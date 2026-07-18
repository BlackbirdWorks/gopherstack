package mediaconvert

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Endpoints handler ---

type endpointsOutput struct {
	Endpoints []endpointEntry `json:"endpoints"`
}

type endpointEntry struct {
	URL string `json:"url"`
}

func (h *Handler) handleDescribeEndpoints(c *echo.Context) error {
	r := c.Request()
	scheme := "http"

	if r.TLS != nil {
		scheme = "https"
	}

	url := scheme + "://" + r.Host
	out := endpointsOutput{
		Endpoints: []endpointEntry{{URL: url}},
	}

	return c.JSON(http.StatusOK, out)
}
