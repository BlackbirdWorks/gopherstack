package bedrock

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// extractResourcePolicyOperation matches core bedrock's PutResourcePolicy
// (POST /resource-policy) and GetResourcePolicy/DeleteResourcePolicy
// (GET/DELETE /resource-policy/{resourceArn}). See resource_policy.go's
// package doc comment for how this differs from the bedrock-agent flavor.
func extractResourcePolicyOperation(path, method string) (string, bool) {
	switch {
	case path == resourcePolicyPath && method == http.MethodPost:
		return opPutResourcePolicy, true
	case strings.HasPrefix(path, resourcePolicyPath+"/") && method == http.MethodGet:
		return opGetResourcePolicy, true
	case strings.HasPrefix(path, resourcePolicyPath+"/") && method == http.MethodDelete:
		return opDeleteResourcePolicy, true
	default:
		return "", false
	}
}

func (h *Handler) routeResourcePolicy(c *echo.Context, path, method string, body []byte) (bool, error) {
	switch {
	case path == resourcePolicyPath && method == http.MethodPost:
		return true, h.handlePutResourcePolicy(c, body)
	case strings.HasPrefix(path, resourcePolicyPath+"/") && method == http.MethodGet:
		resourceArn := decodePath(strings.TrimPrefix(path, resourcePolicyPath+"/"))

		return true, h.handleGetResourcePolicy(c, resourceArn)
	case strings.HasPrefix(path, resourcePolicyPath+"/") && method == http.MethodDelete:
		resourceArn := decodePath(strings.TrimPrefix(path, resourcePolicyPath+"/"))

		return true, h.handleDeleteResourcePolicy(c, resourceArn)
	default:
		return false, nil
	}
}

type putResourcePolicyInput struct {
	ResourceArn    string `json:"resourceArn"`
	ResourcePolicy string `json:"resourcePolicy"`
}

type putResourcePolicyOutput struct {
	ResourceArn string `json:"resourceArn"`
}

func (h *Handler) handlePutResourcePolicy(c *echo.Context, body []byte) error {
	in, err := parseBody[putResourcePolicyInput](body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	rp, opErr := h.Backend.PutResourcePolicy(in.ResourceArn, in.ResourcePolicy)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	// Real PutResourcePolicy returns HTTP 201, unlike this service's other
	// Put ops (verified via AWS API Reference: "the service sends back an
	// HTTP 201 response").
	return c.JSON(http.StatusCreated, putResourcePolicyOutput{ResourceArn: rp.ResourceArn})
}

type getResourcePolicyOutput struct {
	ResourcePolicy string `json:"resourcePolicy"`
}

func (h *Handler) handleGetResourcePolicy(c *echo.Context, resourceArn string) error {
	rp, err := h.Backend.GetResourcePolicy(resourceArn)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, getResourcePolicyOutput{ResourcePolicy: rp.PolicyDocument})
}

func (h *Handler) handleDeleteResourcePolicy(c *echo.Context, resourceArn string) error {
	if err := h.Backend.DeleteResourcePolicy(resourceArn); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
