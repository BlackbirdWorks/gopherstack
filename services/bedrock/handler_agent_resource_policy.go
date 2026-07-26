package bedrock

import (
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// agentResourcePolicyPathPrefix is the real bedrock-agent PutResourcePolicy/
// GetResourcePolicy/DeleteResourcePolicy path prefix: PUT/GET/DELETE
// /resourcepolicy/{resourceArn} -- no hyphen, singular, distinct from core
// bedrock's "/resource-policy" (see resource_policy.go's package doc
// comment).
const agentResourcePolicyPathPrefix = "/resourcepolicy/"

// extractAgentResourcePolicyOperation matches bedrock-agent's
// PutResourcePolicy/GetResourcePolicy/DeleteResourcePolicy.
func extractAgentResourcePolicyOperation(path, method string) string {
	resourceArn, ok := strings.CutPrefix(path, agentResourcePolicyPathPrefix)
	if !ok || resourceArn == "" {
		return ""
	}

	switch method {
	case http.MethodPut:
		return opPutResourcePolicy
	case http.MethodGet:
		return opGetResourcePolicy
	case http.MethodDelete:
		return opDeleteResourcePolicy
	default:
		return ""
	}
}

// dispatchResourcePolicyRoutes handles /resourcepolicy/{resourceArn}.
// Returns (true, err) when the path was matched; (false, nil) otherwise.
func (h *AgentsHandler) dispatchResourcePolicyRoutes(
	c *echo.Context, path, method string, body []byte,
) (bool, error) {
	rest, ok := strings.CutPrefix(path, agentResourcePolicyPathPrefix)
	if !ok || rest == "" {
		return false, nil
	}

	resourceArn := decodePath(rest)

	switch method {
	case http.MethodPut:
		return true, h.handlePutKnowledgeBaseResourcePolicy(c, resourceArn, body)
	case http.MethodGet:
		return true, h.handleGetKnowledgeBaseResourcePolicy(c, resourceArn)
	case http.MethodDelete:
		return true, h.handleDeleteKnowledgeBaseResourcePolicy(c, resourceArn)
	}

	return true, c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown resource policy operation"),
	)
}

// resourcePolicyErrorResponse maps a ResourcePolicy backend error to its
// bedrock-agent HTTP status/code, mirroring the inline error-mapping style
// this package's other AgentsHandler handlers already use (e.g.
// handler_knowledge_bases.go's handleCreateKnowledgeBase).
func resourcePolicyErrorResponse(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, agentErrResp("ConflictException", err.Error()))
	default:
		return c.JSON(http.StatusBadRequest, agentErrResp("ValidationException", err.Error()))
	}
}

type putKnowledgeBaseResourcePolicyInput struct {
	Policy             string `json:"policy"`
	ExpectedRevisionID string `json:"expectedRevisionId,omitempty"`
}

type knowledgeBaseResourcePolicyOutput struct {
	ResourceArn string `json:"resourceArn"`
	RevisionID  string `json:"revisionId"`
}

func (h *AgentsHandler) handlePutKnowledgeBaseResourcePolicy(
	c *echo.Context, resourceArn string, body []byte,
) error {
	in, err := parseBody[putKnowledgeBaseResourcePolicyInput](body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, agentErrResp("ValidationException", "invalid request body"))
	}

	rp, opErr := h.Backend.PutKnowledgeBaseResourcePolicy(resourceArn, in.Policy, in.ExpectedRevisionID)
	if opErr != nil {
		return resourcePolicyErrorResponse(c, opErr)
	}

	return c.JSON(http.StatusOK, knowledgeBaseResourcePolicyOutput{
		ResourceArn: rp.ResourceArn,
		RevisionID:  rp.RevisionID,
	})
}

type getKnowledgeBaseResourcePolicyOutput struct {
	Policy      string `json:"policy"`
	ResourceArn string `json:"resourceArn"`
	RevisionID  string `json:"revisionId"`
}

func (h *AgentsHandler) handleGetKnowledgeBaseResourcePolicy(c *echo.Context, resourceArn string) error {
	rp, err := h.Backend.GetKnowledgeBaseResourcePolicy(resourceArn)
	if err != nil {
		return resourcePolicyErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, getKnowledgeBaseResourcePolicyOutput{
		Policy:      rp.PolicyDocument,
		ResourceArn: rp.ResourceArn,
		RevisionID:  rp.RevisionID,
	})
}

func (h *AgentsHandler) handleDeleteKnowledgeBaseResourcePolicy(c *echo.Context, resourceArn string) error {
	expectedRevisionID := c.QueryParam("expectedRevisionId")

	revisionID, err := h.Backend.DeleteKnowledgeBaseResourcePolicy(resourceArn, expectedRevisionID)
	if err != nil {
		return resourcePolicyErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, knowledgeBaseResourcePolicyOutput{
		ResourceArn: resourceArn,
		RevisionID:  revisionID,
	})
}
