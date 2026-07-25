package bedrockagent

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// resourcePolicyBase is bedrock-agent's real PutResourcePolicy/
// GetResourcePolicy/DeleteResourcePolicy path prefix: PUT/GET/DELETE
// /resourcepolicy/{resourceArn} -- no hyphen, singular, distinct from core
// bedrock's "/resource-policy" (see resource_policy.go's package doc
// comment). Verified against aws-sdk-go-v2/service/bedrockagent's
// serializers.go (httpbinding.SplitURI("/resourcepolicy/{resourceArn}") in
// all three of awsRestjson1_serializeOp{Put,Get,Delete}ResourcePolicy) and
// AWS's published API reference.
const resourcePolicyBase = "/resourcepolicy/"

// dispatchResourcePolicy handles /resourcepolicy/{resourceArn}. The entire
// remainder of the path after the prefix is taken as resourceArn verbatim
// (no path-segment splitting), matching how dispatchTags already treats
// ARN-in-path segments elsewhere in this package (ARNs routinely contain
// their own "/" and ":" characters, e.g.
// "arn:aws:bedrock:us-east-1:123456789012:knowledge-base/kb-00000001").
func (h *Handler) dispatchResourcePolicy(
	ctx context.Context, c *echo.Context, path, method string, body []byte,
) error {
	resourceArn, ok := strings.CutPrefix(path, resourcePolicyBase)
	if !ok || resourceArn == "" {
		return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown resource policy op"))
	}

	switch method {
	case http.MethodPut:
		return h.handlePutResourcePolicy(ctx, c, resourceArn, body)
	case http.MethodGet:
		return h.handleGetResourcePolicy(ctx, c, resourceArn)
	case http.MethodDelete:
		return h.handleDeleteResourcePolicy(ctx, c, resourceArn)
	}

	return c.JSON(http.StatusMethodNotAllowed, errResp("MethodNotAllowedException", method))
}

// classifyResourcePolicyPath is classifySubPath's resource-policy case (see
// handler_helpers.go's classifyPath/classifySubPath).
func classifyResourcePolicyPath(method, path string) string {
	rest, ok := strings.CutPrefix(path, resourcePolicyBase)
	if !ok || rest == "" {
		return opUnknown
	}

	switch method {
	case http.MethodPut:
		return opPutResourcePolicy
	case http.MethodGet:
		return opGetResourcePolicy
	case http.MethodDelete:
		return opDeleteResourcePolicy
	}

	return opUnknown
}

// putResourcePolicyRequest is PutResourcePolicyInput's real wire shape:
// field names "policy"/"expectedRevisionId" (verified against
// awsRestjson1_serializeOpDocumentPutResourcePolicyInput in serializers.go).
type putResourcePolicyRequest struct {
	Policy             string `json:"policy"`
	ExpectedRevisionID string `json:"expectedRevisionId,omitempty"`
}

// resourcePolicyMutationResponse is the shared Put/DeleteResourcePolicyOutput
// wire shape: both carry only "resourceArn"/"revisionId" (Delete's output
// omits "policy" -- only Get and Put echo the policy document back).
type resourcePolicyMutationResponse struct {
	ResourceArn string `json:"resourceArn"`
	RevisionID  string `json:"revisionId"`
}

func (h *Handler) handlePutResourcePolicy(
	ctx context.Context, c *echo.Context, resourceArn string, body []byte,
) error {
	var req putResourcePolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	rp, err := h.Backend.PutResourcePolicy(ctx, resourceArn, req.Policy, req.ExpectedRevisionID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, resourcePolicyMutationResponse{
		ResourceArn: rp.ResourceArn,
		RevisionID:  rp.RevisionID,
	})
}

// getResourcePolicyResponse is GetResourcePolicyOutput's real wire shape:
// "policy"/"resourceArn"/"revisionId", all required per the SDK deserializer.
type getResourcePolicyResponse struct {
	Policy      string `json:"policy"`
	ResourceArn string `json:"resourceArn"`
	RevisionID  string `json:"revisionId"`
}

func (h *Handler) handleGetResourcePolicy(ctx context.Context, c *echo.Context, resourceArn string) error {
	rp, err := h.Backend.GetResourcePolicy(ctx, resourceArn)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, getResourcePolicyResponse{
		Policy:      rp.Policy,
		ResourceArn: rp.ResourceArn,
		RevisionID:  rp.RevisionID,
	})
}

func (h *Handler) handleDeleteResourcePolicy(ctx context.Context, c *echo.Context, resourceArn string) error {
	// expectedRevisionId is a query parameter on Delete (not a body field --
	// Delete has no request body at all), unlike Put where it is a JSON body
	// field. Verified against
	// awsRestjson1_serializeOpHttpBindingsDeleteResourcePolicyInput in
	// serializers.go.
	expectedRevisionID := c.QueryParam("expectedRevisionId")

	revisionID, err := h.Backend.DeleteResourcePolicy(ctx, resourceArn, expectedRevisionID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, resourcePolicyMutationResponse{
		ResourceArn: resourceArn,
		RevisionID:  revisionID,
	})
}
