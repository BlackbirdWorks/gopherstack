package bedrock

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

// extractCustomModelDeploymentSubOp maps List/Get/Update/Delete on
// /model-customization/custom-model-deployments[/{id}] — Create (POST, exact base path)
// is handled separately above.
func extractCustomModelDeploymentSubOp(path, method string) (string, bool) {
	isSubPath := strings.HasPrefix(path, customModelDeploymentsPath+"/")

	switch {
	case path == customModelDeploymentsPath && method == http.MethodGet:
		return "ListCustomModelDeployments", true
	case isSubPath && method == http.MethodGet:
		return "GetCustomModelDeployment", true
	case isSubPath && method == http.MethodPatch:
		return "UpdateCustomModelDeployment", true
	case isSubPath && method == http.MethodDelete:
		return "DeleteCustomModelDeployment", true
	default:
		return "", false
	}
}

// routeStubDeploymentOps handles custom model deployment operations.
func (h *Handler) routeStubDeploymentOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == customModelDeploymentsPath && method == http.MethodGet:
		return true, h.handleListCustomModelDeployments(c)
	case strings.HasPrefix(path, customModelDeploymentsPath+"/") && method == http.MethodGet:
		deployARN, _ := url.PathUnescape(strings.TrimPrefix(path, customModelDeploymentsPath+"/"))

		return true, h.handleGetCustomModelDeployment(c, deployARN)
	case strings.HasPrefix(path, customModelDeploymentsPath+"/") && method == http.MethodPatch:
		deployARN, _ := url.PathUnescape(strings.TrimPrefix(path, customModelDeploymentsPath+"/"))

		return true, h.handleUpdateCustomModelDeployment(c, deployARN)
	case strings.HasPrefix(path, customModelDeploymentsPath+"/") && method == http.MethodDelete:
		deployARN, _ := url.PathUnescape(strings.TrimPrefix(path, customModelDeploymentsPath+"/"))

		return true, h.handleDeleteCustomModelDeployment(c, deployARN)
	}

	return false, nil
}

type createCustomModelDeploymentInput struct {
	ModelArn            string `json:"modelArn"`
	ModelDeploymentName string `json:"modelDeploymentName"`
	Tags                []Tag  `json:"tags,omitempty"`
}

type createCustomModelDeploymentOutput struct {
	CustomModelDeploymentArn string `json:"customModelDeploymentArn"`
}

func (h *Handler) handleCreateCustomModelDeployment(c *echo.Context, body []byte) error {
	in, err := parseBody[createCustomModelDeploymentInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	deployment, opErr := h.Backend.CreateCustomModelDeployment(
		in.ModelArn,
		in.ModelDeploymentName,
		in.Tags,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, createCustomModelDeploymentOutput{
		CustomModelDeploymentArn: deployment.CustomModelDeploymentArn,
	})
}

func (h *Handler) handleGetCustomModelDeployment(c *echo.Context, deployARN string) error {
	d, err := h.Backend.GetCustomModelDeployment(deployARN)
	if err != nil {
		return h.writeError(c, err)
	}

	// GetCustomModelDeploymentOutput uses createdAt/lastUpdatedAt, not the
	// keyCreationTime/keyLastModifiedTime ("creationTime"/"lastModifiedTime")
	// constants correct for this package's job-summary ops (bedrock@v1.66.4
	// deserializers.go, awsRestjson1_deserializeOpDocumentGetCustomModelDeploymentOutput).
	return c.JSON(http.StatusOK, map[string]any{
		keyCustomModelDeploymentArn: d.CustomModelDeploymentArn,
		"modelDeploymentName":       d.ModelDeploymentName,
		keyModelArn:                 d.ModelArn,
		keyStatus:                   d.Status,
		keyCreatedAt:                d.CreationTime.Format(time.RFC3339),
		"lastUpdatedAt":             d.LastModifiedTime.Format(time.RFC3339),
	})
}

// parseListCustomModelDeploymentsQuery is structurally similar to
// parseListProvisionedModelThroughputsQuery (same query-parsing shape) but
// targets a distinct Input type and query key set.
//
//nolint:dupl // see doc comment above.
func parseListCustomModelDeploymentsQuery(c *echo.Context) *ListCustomModelDeploymentsInput {
	q := c.Request().URL.Query()

	maxResults, _ := strconv.ParseInt(q.Get("maxResults"), 10, 32)

	in := &ListCustomModelDeploymentsInput{
		StatusEquals:   q.Get("statusEquals"),
		ModelArnEquals: q.Get("modelArnEquals"),
		NameContains:   q.Get("nameContains"),
		SortBy:         q.Get("sortBy"),
		SortOrder:      q.Get("sortOrder"),
		NextToken:      q.Get("nextToken"),
		MaxResults:     int32(maxResults),
	}

	if v := q.Get("createdAfter"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			in.CreatedAfter = &t
		}
	}

	if v := q.Get("createdBefore"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			in.CreatedBefore = &t
		}
	}

	return in
}

func (h *Handler) handleListCustomModelDeployments(c *echo.Context) error {
	deployments, nextToken := h.Backend.ListCustomModelDeployments(parseListCustomModelDeploymentsQuery(c))
	summaries := make([]map[string]any, 0, len(deployments))

	for _, d := range deployments {
		// CustomModelDeploymentSummary uses customModelDeploymentName (not
		// modelDeploymentName, which only the singular Get shape uses) and
		// createdAt/lastUpdatedAt (bedrock@v1.66.4 deserializers.go,
		// awsRestjson1_deserializeDocumentCustomModelDeploymentSummary).
		summaries = append(summaries, map[string]any{
			keyCustomModelDeploymentArn: d.CustomModelDeploymentArn,
			"customModelDeploymentName": d.ModelDeploymentName,
			keyModelArn:                 d.ModelArn,
			keyStatus:                   d.Status,
			keyCreatedAt:                d.CreationTime.Format(time.RFC3339),
			"lastUpdatedAt":             d.LastModifiedTime.Format(time.RFC3339),
		})
	}

	// Real key is modelDeploymentSummaries (bedrock@v1.66.4 deserializers.go,
	// awsRestjson1_deserializeOpDocumentListCustomModelDeploymentsOutput).
	resp := map[string]any{"modelDeploymentSummaries": summaries}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateCustomModelDeployment(c *echo.Context, deployARN string) error {
	d, err := h.Backend.UpdateCustomModelDeployment(deployARN)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyCustomModelDeploymentArn: d.CustomModelDeploymentArn})
}

func (h *Handler) handleDeleteCustomModelDeployment(c *echo.Context, deployARN string) error {
	if err := h.Backend.DeleteCustomModelDeployment(deployARN); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
