package ec2

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

type publishedPortsResponse struct {
	InstanceID string          `json:"instanceId"`
	Ports      []PublishedPort `json:"ports"`
	SSHPort    int             `json:"sshPort,omitempty"`
}

// ServePublishedPorts serves GET /_gopherstack/ec2/instances/:instance/ports: the host port behind
// each security-group port of a Compute-backed instance.
func (h *Handler) ServePublishedPorts(c *echo.Context) error {
	id := c.Param("instance")

	for _, bk := range h.imdsBackends() {
		inst, ok := bk.lookupInstance(id)
		if !ok {
			continue
		}

		if inst.ProviderID == "" {
			return c.JSON(http.StatusConflict, messageBody("instance has no container"))
		}

		ports := []PublishedPort{}

		if rep, isRep := bk.Compute().(publishedPortReporter); isRep {
			got, err := rep.PublishedPorts(c.Request().Context(), inst.ProviderID)
			if err != nil {
				return c.JSON(http.StatusBadGateway, messageBody(err.Error()))
			}

			ports = append(ports, got...)
		}

		return c.JSON(http.StatusOK, publishedPortsResponse{InstanceID: id, Ports: ports, SSHPort: inst.SSHPort})
	}

	return c.JSON(http.StatusNotFound, messageBody("instance not found"))
}

func (b *InMemoryBackend) lookupInstance(id string) (Instance, bool) {
	b.mu.RLock("lookupInstance")
	defer b.mu.RUnlock()

	inst, ok := b.instances.Get(id)
	if !ok {
		return Instance{}, false
	}

	return *inst, true
}

func messageBody(msg string) map[string]string { return map[string]string{"message": msg} }
