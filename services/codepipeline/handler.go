package codepipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	codepipelineTargetPrefix = "CodePipeline_20150709."
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for CodePipeline operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new CodePipeline handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodePipeline" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreatePipeline",
		"GetPipeline",
		"UpdatePipeline",
		"DeletePipeline",
		"ListPipelines",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "codepipeline" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches CodePipeline requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), codepipelineTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the CodePipeline action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, codepipelineTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request (not used for CodePipeline).
func (h *Handler) ExtractResource(_ *echo.Context) string {
	return ""
}

// Handler returns the Echo handler function for CodePipeline requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CodePipeline", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreatePipeline":      service.WrapOp(h.handleCreatePipeline),
		"GetPipeline":         service.WrapOp(h.handleGetPipeline),
		"UpdatePipeline":      service.WrapOp(h.handleUpdatePipeline),
		"DeletePipeline":      service.WrapOp(h.handleDeletePipeline),
		"ListPipelines":       service.WrapOp(h.handleListPipelines),
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
		"TagResource":         service.WrapOp(h.handleTagResource),
		"UntagResource":       service.WrapOp(h.handleUntagResource),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "PipelineNotFoundException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrAlreadyExists):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "InvalidStructureException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errUnknownAction):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "InvalidActionException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ValidationException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	default:
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "InternalFailure",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusInternalServerError, payload)
	}
}

// --- Pipeline operations ---

type createPipelineInput struct {
	Pipeline *PipelineDeclaration `json:"pipeline"`
	Tags     []Tag                `json:"tags"`
}

type createPipelineOutput struct {
	Pipeline *PipelineDeclaration `json:"pipeline"`
	Tags     []Tag                `json:"tags"`
}

func (h *Handler) handleCreatePipeline(
	_ context.Context,
	in *createPipelineInput,
) (*createPipelineOutput, error) {
	if in.Pipeline == nil {
		return nil, fmt.Errorf("%w: pipeline is required", errInvalidRequest)
	}

	if in.Pipeline.Name == "" {
		return nil, fmt.Errorf("%w: pipeline name is required", errInvalidRequest)
	}

	tagMap := tagsToMap(in.Tags)

	p, err := h.Backend.CreatePipeline(*in.Pipeline, tagMap)
	if err != nil {
		return nil, err
	}

	return &createPipelineOutput{
		Pipeline: &p.Declaration,
		Tags:     in.Tags,
	}, nil
}

type getPipelineInput struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
}

type getPipelineOutput struct {
	Pipeline *PipelineDeclaration `json:"pipeline"`
	Metadata *PipelineMetadata    `json:"metadata"`
}

func (h *Handler) handleGetPipeline(
	_ context.Context,
	in *getPipelineInput,
) (*getPipelineOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	p, err := h.Backend.GetPipeline(in.Name)
	if err != nil {
		return nil, err
	}

	if in.Version != 0 && in.Version != p.Declaration.Version {
		return nil, fmt.Errorf("%w: pipeline %q version %d not found (current: %d)",
			ErrNotFound, in.Name, in.Version, p.Declaration.Version)
	}

	return &getPipelineOutput{
		Pipeline: &p.Declaration,
		Metadata: &p.Metadata,
	}, nil
}

type updatePipelineInput struct {
	Pipeline *PipelineDeclaration `json:"pipeline"`
}

type updatePipelineOutput struct {
	Pipeline *PipelineDeclaration `json:"pipeline"`
}

func (h *Handler) handleUpdatePipeline(
	_ context.Context,
	in *updatePipelineInput,
) (*updatePipelineOutput, error) {
	if in.Pipeline == nil {
		return nil, fmt.Errorf("%w: pipeline is required", errInvalidRequest)
	}

	if in.Pipeline.Name == "" {
		return nil, fmt.Errorf("%w: pipeline name is required", errInvalidRequest)
	}

	p, err := h.Backend.UpdatePipeline(*in.Pipeline)
	if err != nil {
		return nil, err
	}

	return &updatePipelineOutput{Pipeline: &p.Declaration}, nil
}

type deletePipelineInput struct {
	Name string `json:"name"`
}

type deletePipelineOutput struct{}

func (h *Handler) handleDeletePipeline(
	_ context.Context,
	in *deletePipelineInput,
) (*deletePipelineOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	if err := h.Backend.DeletePipeline(in.Name); err != nil {
		return nil, err
	}

	return &deletePipelineOutput{}, nil
}

type listPipelinesInput struct {
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int    `json:"maxResults,omitempty"`
}

type listPipelinesOutput struct {
	NextToken string            `json:"nextToken,omitempty"`
	Pipelines []PipelineSummary `json:"pipelines"`
}

func (h *Handler) handleListPipelines(
	_ context.Context,
	_ *listPipelinesInput,
) (*listPipelinesOutput, error) {
	summaries := h.Backend.ListPipelines()

	return &listPipelinesOutput{Pipelines: summaries}, nil
}

// --- Tagging operations ---

type listTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []Tag `json:"tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}

type tagResourceInput struct {
	ResourceArn string `json:"resourceArn"`
	Tags        []Tag  `json:"tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(in.ResourceArn, in.Tags); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

func tagsToMap(tags []Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}
