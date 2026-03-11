package codebuild

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
	codebuildTargetPrefix = "CodeBuild_20161006."
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for CodeBuild operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new CodeBuild handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodeBuild" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateProject",
		"BatchGetProjects",
		"UpdateProject",
		"DeleteProject",
		"ListProjects",
		"StartBuild",
		"BatchGetBuilds",
		"StopBuild",
		"ListBuildsForProject",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "codebuild" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches CodeBuild requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), codebuildTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the CodeBuild action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, codebuildTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request (not used for CodeBuild).
func (h *Handler) ExtractResource(_ *echo.Context) string {
	return ""
}

// Handler returns the Echo handler function for CodeBuild requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CodeBuild", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateProject":        service.WrapOp(h.handleCreateProject),
		"BatchGetProjects":     service.WrapOp(h.handleBatchGetProjects),
		"UpdateProject":        service.WrapOp(h.handleUpdateProject),
		"DeleteProject":        service.WrapOp(h.handleDeleteProject),
		"ListProjects":         service.WrapOp(h.handleListProjects),
		"StartBuild":           service.WrapOp(h.handleStartBuild),
		"BatchGetBuilds":       service.WrapOp(h.handleBatchGetBuilds),
		"StopBuild":            service.WrapOp(h.handleStopBuild),
		"ListBuildsForProject": service.WrapOp(h.handleListBuildsForProject),
		"ListTagsForResource":  service.WrapOp(h.handleListTagsForResource),
		"TagResource":          service.WrapOp(h.handleTagResource),
		"UntagResource":        service.WrapOp(h.handleUntagResource),
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
			Type:    "ResourceNotFoundException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrAlreadyExists):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "InvalidInputException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

// --- Project operations ---

type createProjectInput struct {
	Tags        map[string]string  `json:"tags"`
	Name        string             `json:"name"`
	Description string             `json:"description"`
	ServiceRole string             `json:"serviceRole"`
	Source      ProjectSource      `json:"source"`
	Artifacts   ProjectArtifacts   `json:"artifacts"`
	Environment ProjectEnvironment `json:"environment"`
}

type createProjectOutput struct {
	Project *Project `json:"project"`
}

func (h *Handler) handleCreateProject(
	_ context.Context,
	in *createProjectInput,
) (*createProjectOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	p, err := h.Backend.CreateProject(
		in.Name, in.Description,
		in.Source, in.Artifacts, in.Environment,
		in.ServiceRole, in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &createProjectOutput{Project: p}, nil
}

type batchGetProjectsInput struct {
	Names []string `json:"names"`
}

type batchGetProjectsOutput struct {
	Projects         []*Project `json:"projects"`
	ProjectsNotFound []string   `json:"projectsNotFound"`
}

func (h *Handler) handleBatchGetProjects(
	_ context.Context,
	in *batchGetProjectsInput,
) (*batchGetProjectsOutput, error) {
	found, notFound := h.Backend.BatchGetProjects(in.Names)

	return &batchGetProjectsOutput{
		Projects:         found,
		ProjectsNotFound: notFound,
	}, nil
}

type updateProjectInput struct {
	Source      *ProjectSource      `json:"source,omitempty"`
	Artifacts   *ProjectArtifacts   `json:"artifacts,omitempty"`
	Environment *ProjectEnvironment `json:"environment,omitempty"`
	Name        string              `json:"name"`
	Description string              `json:"description"`
	ServiceRole string              `json:"serviceRole"`
}

type updateProjectOutput struct {
	Project *Project `json:"project"`
}

func (h *Handler) handleUpdateProject(
	_ context.Context,
	in *updateProjectInput,
) (*updateProjectOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	p, err := h.Backend.UpdateProject(
		in.Name, in.Description,
		in.Source, in.Artifacts, in.Environment,
		in.ServiceRole,
	)
	if err != nil {
		return nil, err
	}

	return &updateProjectOutput{Project: p}, nil
}

type deleteProjectInput struct {
	Name string `json:"name"`
}

type deleteProjectOutput struct{}

func (h *Handler) handleDeleteProject(
	_ context.Context,
	in *deleteProjectInput,
) (*deleteProjectOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteProject(in.Name); err != nil {
		return nil, err
	}

	return &deleteProjectOutput{}, nil
}

type listProjectsInput struct{}

type listProjectsOutput struct {
	Projects []string `json:"projects"`
}

func (h *Handler) handleListProjects(
	_ context.Context,
	_ *listProjectsInput,
) (*listProjectsOutput, error) {
	names := h.Backend.ListProjects()

	return &listProjectsOutput{Projects: names}, nil
}

// --- Build operations ---

type startBuildInput struct {
	ProjectName string `json:"projectName"`
}

type startBuildOutput struct {
	Build *Build `json:"build"`
}

func (h *Handler) handleStartBuild(
	_ context.Context,
	in *startBuildInput,
) (*startBuildOutput, error) {
	if in.ProjectName == "" {
		return nil, fmt.Errorf("%w: projectName is required", errInvalidRequest)
	}

	build, err := h.Backend.StartBuild(in.ProjectName)
	if err != nil {
		return nil, err
	}

	return &startBuildOutput{Build: build}, nil
}

type batchGetBuildsInput struct {
	IDs []string `json:"ids"`
}

type batchGetBuildsOutput struct {
	Builds         []*Build `json:"builds"`
	BuildsNotFound []string `json:"buildsNotFound"`
}

func (h *Handler) handleBatchGetBuilds(
	_ context.Context,
	in *batchGetBuildsInput,
) (*batchGetBuildsOutput, error) {
	found, notFound := h.Backend.BatchGetBuilds(in.IDs)

	return &batchGetBuildsOutput{
		Builds:         found,
		BuildsNotFound: notFound,
	}, nil
}

type stopBuildInput struct {
	ID string `json:"id"`
}

type stopBuildOutput struct {
	Build *Build `json:"build"`
}

func (h *Handler) handleStopBuild(
	_ context.Context,
	in *stopBuildInput,
) (*stopBuildOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: id is required", errInvalidRequest)
	}

	build, err := h.Backend.StopBuild(in.ID)
	if err != nil {
		return nil, err
	}

	return &stopBuildOutput{Build: build}, nil
}

type listBuildsForProjectInput struct {
	ProjectName string `json:"projectName"`
}

type listBuildsForProjectOutput struct {
	IDs []string `json:"ids"`
}

func (h *Handler) handleListBuildsForProject(
	_ context.Context,
	in *listBuildsForProjectInput,
) (*listBuildsForProjectOutput, error) {
	if in.ProjectName == "" {
		return nil, fmt.Errorf("%w: projectName is required", errInvalidRequest)
	}

	ids, err := h.Backend.ListBuildsForProject(in.ProjectName)
	if err != nil {
		return nil, err
	}

	return &listBuildsForProjectOutput{IDs: ids}, nil
}

// --- Tagging operations ---

type listTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type listTagsForResourceOutput struct {
	Tags map[string]string `json:"tags"`
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
	Tags        map[string]string `json:"tags"`
	ResourceArn string            `json:"resourceArn"`
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
