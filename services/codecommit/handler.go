package codecommit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const codecommitTargetPrefix = "CodeCommit_20150413."

var errUnknownAction = errors.New("unknown action")

// Handler is the Echo HTTP handler for AWS CodeCommit operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new CodeCommit handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodeCommit" }

// GetSupportedOperations returns the list of supported CodeCommit operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateRepository",
		"GetRepository",
		"DeleteRepository",
		"ListRepositories",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "codecommit" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this CodeCommit instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches AWS CodeCommit requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), codecommitTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the CodeCommit operation from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, codecommitTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts the repository name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var input struct {
		RepositoryName string `json:"repositoryName"`
	}
	if err := json.Unmarshal(body, &input); err != nil {
		return ""
	}

	return input.RepositoryName
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CodeCommit", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

// dispatch routes the operation to the appropriate handler and marshals the response.
func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	resp, err := h.dispatchJSON(action, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(resp)
}

//nolint:cyclop // dispatch table for CodeCommit operations
func (h *Handler) dispatchJSON(action string, body []byte) (any, error) {
	switch action {
	case "CreateRepository":
		return h.handleCreateRepository(body)
	case "GetRepository":
		return h.handleGetRepository(body)
	case "DeleteRepository":
		return h.handleDeleteRepository(body)
	case "ListRepositories":
		return h.handleListRepositories(body)
	case "TagResource":
		return h.handleTagResource(body)
	case "UntagResource":
		return h.handleUntagResource(body)
	case "ListTagsForResource":
		return h.handleListTagsForResource(body)
	}

	return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
}

// handleError maps backend errors to HTTP error responses.
func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	code := http.StatusBadRequest
	errType := "ValidationException"

	switch {
	case errors.Is(err, ErrNotFound):
		code = http.StatusNotFound
		errType = "RepositoryDoesNotExistException"
	case errors.Is(err, ErrAlreadyExists):
		code = http.StatusBadRequest
		errType = "RepositoryNameExistsException"
	}

	return c.JSON(code, map[string]string{
		"__type":  errType,
		"message": err.Error(),
	})
}

// --- Request body types ---

type createRepositoryInput struct {
	RepositoryName        string            `json:"repositoryName"`
	RepositoryDescription string            `json:"repositoryDescription"`
	Tags                  map[string]string `json:"tags"`
}

type getRepositoryInput struct {
	RepositoryName string `json:"repositoryName"`
}

type deleteRepositoryInput struct {
	RepositoryName string `json:"repositoryName"`
}

type listRepositoriesInput struct {
	SortBy string `json:"sortBy"`
	Order  string `json:"order"`
}

type tagResourceInput struct {
	ResourceARN string            `json:"resourceArn"`
	Tags        map[string]string `json:"tags"`
}

type untagResourceInput struct {
	ResourceARN string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type listTagsForResourceInput struct {
	ResourceARN string `json:"resourceArn"`
}

// --- Response helpers ---

func repoMetadata(r *Repository) map[string]any {
	m := map[string]any{
		"repositoryId":   r.RepositoryID,
		"repositoryName": r.RepositoryName,
		"Arn":            r.ARN,
		"accountId":      r.AccountID,
		"cloneUrlHttp":   r.CloneURLHTTP,
		"cloneUrlSsh":    r.CloneURLSSH,
	}
	if r.Description != "" {
		m["repositoryDescription"] = r.Description
	}

	return m
}

// --- Operation handlers ---

func (h *Handler) handleCreateRepository(body []byte) (any, error) {
	var in createRepositoryInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errUnknownAction)
	}

	r, err := h.Backend.CreateRepository(in.RepositoryName, in.RepositoryDescription, in.Tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"repositoryMetadata": repoMetadata(r),
	}, nil
}

func (h *Handler) handleGetRepository(body []byte) (any, error) {
	var in getRepositoryInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	r, err := h.Backend.GetRepository(in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"repositoryMetadata": repoMetadata(r),
	}, nil
}

func (h *Handler) handleDeleteRepository(body []byte) (any, error) {
	var in deleteRepositoryInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	r, err := h.Backend.DeleteRepository(in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"repositoryId": r.RepositoryID,
	}, nil
}

func (h *Handler) handleListRepositories(_ []byte) (any, error) {
	repos := h.Backend.ListRepositories()
	items := make([]map[string]any, 0, len(repos))

	for _, r := range repos {
		items = append(items, map[string]any{
			"repositoryId":   r.RepositoryID,
			"repositoryName": r.RepositoryName,
		})
	}

	return map[string]any{
		"repositories": items,
	}, nil
}

func (h *Handler) handleTagResource(body []byte) (any, error) {
	var in tagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errUnknownAction)
	}

	if err := h.Backend.TagResource(in.ResourceARN, in.Tags); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) handleUntagResource(body []byte) (any, error) {
	var in untagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errUnknownAction)
	}

	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) handleListTagsForResource(body []byte) (any, error) {
	var in listTagsForResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errUnknownAction)
	}

	kv, err := h.Backend.ListTagsForResource(in.ResourceARN)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"tags": kv,
	}, nil
}
