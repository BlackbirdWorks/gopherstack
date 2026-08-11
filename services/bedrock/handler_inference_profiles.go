package bedrock

import (
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

func extractInferenceProfileOperation(path, method string) (string, bool) {
	switch {
	case path == inferenceProfilesPrefix && method == http.MethodPost:
		return "CreateInferenceProfile", true
	case path == inferenceProfilesPrefix && method == http.MethodGet:
		return "ListInferenceProfiles", true
	case strings.HasPrefix(path, inferenceProfilesPrefix+"/") && method == http.MethodGet:
		return "GetInferenceProfile", true
	case strings.HasPrefix(path, inferenceProfilesPrefix+"/") && method == http.MethodDelete:
		return "DeleteInferenceProfile", true
	default:
		return "", false
	}
}

func (h *Handler) routeInferenceProfile(
	c *echo.Context,
	path, method string,
	body []byte,
) (bool, error) {
	switch {
	case path == inferenceProfilesPrefix && method == http.MethodPost:
		return true, h.handleCreateInferenceProfile(c, body)
	case path == inferenceProfilesPrefix && method == http.MethodGet:
		return true, h.handleListInferenceProfiles(c)
	case strings.HasPrefix(path, inferenceProfilesPrefix+"/") && method == http.MethodGet:
		id := decodePath(strings.TrimPrefix(path, inferenceProfilesPrefix+"/"))

		return true, h.handleGetInferenceProfile(c, id)
	case strings.HasPrefix(path, inferenceProfilesPrefix+"/") && method == http.MethodDelete:
		id := decodePath(strings.TrimPrefix(path, inferenceProfilesPrefix+"/"))

		return true, h.handleDeleteInferenceProfile(c, id)
	default:
		return false, nil
	}
}

type createInferenceProfileInput struct {
	InferenceProfileName string `json:"inferenceProfileName"`
	Description          string `json:"description,omitempty"`
	Tags                 []Tag  `json:"tags,omitempty"`
}

type createInferenceProfileOutput struct {
	InferenceProfileArn string `json:"inferenceProfileArn"`
	Status              string `json:"status"`
}

func (h *Handler) handleCreateInferenceProfile(c *echo.Context, body []byte) error {
	in, err := parseBody[createInferenceProfileInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	profile, opErr := h.Backend.CreateInferenceProfile(
		in.InferenceProfileName,
		in.Description,
		in.Tags,
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusCreated, createInferenceProfileOutput{
		InferenceProfileArn: profile.InferenceProfileArn,
		Status:              profile.Status,
	})
}

type inferenceProfileOutput struct {
	CreatedAt            string `json:"createdAt"`
	UpdatedAt            string `json:"updatedAt"`
	InferenceProfileArn  string `json:"inferenceProfileArn"`
	InferenceProfileID   string `json:"inferenceProfileId"`
	InferenceProfileName string `json:"inferenceProfileName"`
	Status               string `json:"status"`
	Type                 string `json:"type"`
	Description          string `json:"description,omitempty"`
}

func inferenceProfileToOutput(p *InferenceProfile) inferenceProfileOutput {
	return inferenceProfileOutput{
		InferenceProfileArn:  p.InferenceProfileArn,
		InferenceProfileID:   p.InferenceProfileID,
		InferenceProfileName: p.InferenceProfileName,
		Status:               p.Status,
		Type:                 p.Type,
		Description:          p.Description,
		CreatedAt:            p.CreatedAt.Format(time.RFC3339),
		UpdatedAt:            p.UpdatedAt.Format(time.RFC3339),
	}
}

func (h *Handler) handleGetInferenceProfile(c *echo.Context, id string) error {
	profile, err := h.Backend.GetInferenceProfile(id)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, inferenceProfileToOutput(profile))
}

type listInferenceProfilesOutput struct {
	NextToken                 string                   `json:"nextToken,omitempty"`
	InferenceProfileSummaries []inferenceProfileOutput `json:"inferenceProfileSummaries"`
}

func (h *Handler) handleListInferenceProfiles(c *echo.Context) error {
	q := c.Request().URL.Query()
	profiles, outToken := h.Backend.ListInferenceProfiles(q.Get("nextToken"), q.Get("type"))
	summaries := make([]inferenceProfileOutput, 0, len(profiles))

	for _, p := range profiles {
		summaries = append(summaries, inferenceProfileToOutput(p))
	}

	return c.JSON(http.StatusOK, listInferenceProfilesOutput{
		InferenceProfileSummaries: summaries,
		NextToken:                 outToken,
	})
}

func (h *Handler) handleDeleteInferenceProfile(c *echo.Context, id string) error {
	if err := h.Backend.DeleteInferenceProfile(id); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
