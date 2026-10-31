package eks

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

type publishedPortsResponse struct {
	ClusterName string          `json:"clusterName"`
	Ports       []PublishedPort `json:"ports"`
}

// ServePublishedPorts serves GET /_gopherstack/eks/clusters/:name/ports?region=: the host port behind each
// load-balancer port of a docker-engine cluster.
func (h *Handler) ServePublishedPorts(c *echo.Context) error {
	name := c.Param("name")

	region := c.QueryParam("region")
	if region == "" {
		region = h.Backend.region
	}

	ports, err := h.BackendFor(region).PublishedLBPorts(name)
	if err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, ErrNotFound) {
			code = http.StatusNotFound
		}

		return c.JSON(code, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, publishedPortsResponse{ClusterName: name, Ports: ports})
}
