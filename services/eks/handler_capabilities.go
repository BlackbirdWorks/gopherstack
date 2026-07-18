package eks

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// dispatchCapabilityOps handles capability CRUD operations.
func (h *Handler) dispatchCapabilityOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opCreateCapability:
		return true, h.handleCreateCapability(c, route.clusterName, body)
	case opDeleteCapability:
		return true, h.handleDeleteCapability(c, route.clusterName, route.nodegroupName)
	case opDescribeCapability:
		return true, h.handleDescribeCapability(c, route.clusterName, route.nodegroupName)
	case opListCapabilities:
		return true, h.handleListCapabilities(c, route.clusterName)
	case opUpdateCapability:
		return true, h.handleUpdateCapability(c, route.clusterName, route.nodegroupName, body)
	}

	return false, nil
}

// parseCapabilityRoute returns the route for
// /clusters/{name}/capabilities[/{capabilityName}]. UpdateCapability is POST
// to the same leaf path as Describe/Delete -- verified against the SDK
// serializer.
func parseCapabilityRoute(method, clusterName string, parts []string) eksRoute {
	const capabilityParts = 2

	if len(parts) == capabilityParts {
		switch method {
		case http.MethodPost:
			return eksRoute{operation: opCreateCapability, clusterName: clusterName}
		case http.MethodGet:
			return eksRoute{operation: opListCapabilities, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	capabilityName := parts[2]

	switch method {
	case http.MethodGet:
		return eksRoute{operation: opDescribeCapability, clusterName: clusterName, nodegroupName: capabilityName}
	case http.MethodDelete:
		return eksRoute{operation: opDeleteCapability, clusterName: clusterName, nodegroupName: capabilityName}
	case http.MethodPost:
		return eksRoute{operation: opUpdateCapability, clusterName: clusterName, nodegroupName: capabilityName}
	}

	return eksRoute{operation: opUnknown}
}

func capabilityToJSON(capa *Capability) map[string]any {
	m := map[string]any{
		keyClusterName:   capa.ClusterName,
		"capabilityName": capa.CapabilityName,
		keyArn:           capa.ARN,
		keyStatusField:   capa.Status,
		keyCreatedAt:     capa.CreatedAt.Unix(),
	}

	if capa.Type != "" {
		m[keyType] = capa.Type
	}

	if capa.RoleARN != "" {
		m["roleArn"] = capa.RoleARN
	}

	if capa.DeletePropagationPolicy != "" {
		m["deletePropagationPolicy"] = capa.DeletePropagationPolicy
	}

	if capa.Tags != nil {
		m[keyTags] = capa.Tags.Clone()
	} else {
		m[keyTags] = map[string]string{}
	}

	return m
}

type createCapabilityBody struct {
	Tags                    map[string]string `json:"tags"`
	CapabilityName          string            `json:"capabilityName"`
	Type                    string            `json:"type"`
	RoleArn                 string            `json:"roleArn"`
	DeletePropagationPolicy string            `json:"deletePropagationPolicy"`
}

func (h *Handler) handleCreateCapability(c *echo.Context, clusterName string, body []byte) error {
	var in createCapabilityBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.CapabilityName == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "capabilityName is required"))
	}

	if in.Type == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "type is required"))
	}

	if in.RoleArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "roleArn is required"))
	}

	if in.DeletePropagationPolicy == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterException", "deletePropagationPolicy is required"),
		)
	}

	capa, err := h.Backend.CreateCapability(
		clusterName, in.CapabilityName, in.Type, in.RoleArn, in.DeletePropagationPolicy, in.Tags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCapability: capabilityToJSON(capa),
	})
}

func (h *Handler) handleDeleteCapability(c *echo.Context, clusterName, capabilityName string) error {
	capa, err := h.Backend.DeleteCapability(clusterName, capabilityName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCapability: capabilityToJSON(capa),
	})
}

func (h *Handler) handleDescribeCapability(c *echo.Context, clusterName, capabilityName string) error {
	capa, err := h.Backend.DescribeCapability(clusterName, capabilityName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCapability: capabilityToJSON(capa),
	})
}

func (h *Handler) handleListCapabilities(c *echo.Context, clusterName string) error {
	names := h.Backend.ListCapabilities(clusterName)

	return c.JSON(http.StatusOK, map[string]any{
		"capabilities": names,
	})
}

type updateCapabilityBody struct {
	RoleArn                 string `json:"roleArn"`
	DeletePropagationPolicy string `json:"deletePropagationPolicy"`
}

func (h *Handler) handleUpdateCapability(c *echo.Context, clusterName, capabilityName string, body []byte) error {
	var in updateCapabilityBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}
	}

	capa, err := h.Backend.UpdateCapability(clusterName, capabilityName, in.RoleArn, in.DeletePropagationPolicy)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCapability: capabilityToJSON(capa),
	})
}
