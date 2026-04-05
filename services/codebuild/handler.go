package codebuild

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

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
	janitor *Janitor
}

// NewHandler creates a new CodeBuild handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// WithJanitor attaches a background janitor to the handler.
func (h *Handler) WithJanitor(interval, buildTTL time.Duration, taskTimeout ...time.Duration) *Handler {
	j := NewJanitor(h.Backend, interval, buildTTL)
	if len(taskTimeout) > 0 {
		j.TaskTimeout = taskTimeout[0]
	}
	h.janitor = j

	return h
}

// StartWorker starts the background janitor if configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodeBuild" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"BatchDeleteBuilds",
		"BatchGetBuildBatches",
		"BatchGetBuilds",
		"BatchGetCommandExecutions",
		"BatchGetFleets",
		"BatchGetProjects",
		"BatchGetReportGroups",
		"BatchGetReports",
		"BatchGetSandboxes",
		"CreateFleet",
		"CreateProject",
		"CreateReportGroup",
		"CreateWebhook",
		"DeleteProject",
		"ListBuilds",
		"ListBuildsForProject",
		"ListProjects",
		"ListTagsForResource",
		"RetryBuild",
		"StartBuild",
		"StopBuild",
		"TagResource",
		"UntagResource",
		"UpdateProject",
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
		"BatchDeleteBuilds":         service.WrapOp(h.handleBatchDeleteBuilds),
		"BatchGetBuildBatches":      service.WrapOp(h.handleBatchGetBuildBatches),
		"BatchGetBuilds":            service.WrapOp(h.handleBatchGetBuilds),
		"BatchGetCommandExecutions": service.WrapOp(h.handleBatchGetCommandExecutions),
		"BatchGetFleets":            service.WrapOp(h.handleBatchGetFleets),
		"BatchGetProjects":          service.WrapOp(h.handleBatchGetProjects),
		"BatchGetReportGroups":      service.WrapOp(h.handleBatchGetReportGroups),
		"BatchGetReports":           service.WrapOp(h.handleBatchGetReports),
		"BatchGetSandboxes":         service.WrapOp(h.handleBatchGetSandboxes),
		"CreateFleet":               service.WrapOp(h.handleCreateFleet),
		"CreateProject":             service.WrapOp(h.handleCreateProject),
		"CreateReportGroup":         service.WrapOp(h.handleCreateReportGroup),
		"CreateWebhook":             service.WrapOp(h.handleCreateWebhook),
		"DeleteProject":             service.WrapOp(h.handleDeleteProject),
		"ListBuilds":                service.WrapOp(h.handleListBuilds),
		"ListBuildsForProject":      service.WrapOp(h.handleListBuildsForProject),
		"ListProjects":              service.WrapOp(h.handleListProjects),
		"ListTagsForResource":       service.WrapOp(h.handleListTagsForResource),
		"RetryBuild":                service.WrapOp(h.handleRetryBuild),
		"StartBuild":                service.WrapOp(h.handleStartBuild),
		"StopBuild":                 service.WrapOp(h.handleStopBuild),
		"TagResource":               service.WrapOp(h.handleTagResource),
		"UntagResource":             service.WrapOp(h.handleUntagResource),
		"UpdateProject":             service.WrapOp(h.handleUpdateProject),
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

// --- Build lifecycle operations ---

type listBuildsInput struct{}

type listBuildsOutput struct {
	IDs []string `json:"ids"`
}

func (h *Handler) handleListBuilds(_ context.Context, _ *listBuildsInput) (*listBuildsOutput, error) {
	return &listBuildsOutput{IDs: h.Backend.ListBuilds()}, nil
}

type batchDeleteBuildsInput struct {
	IDs []string `json:"ids"`
}

type batchDeleteBuildsOutput struct {
	BuildsDeleted []string `json:"buildsDeleted"`
}

func (h *Handler) handleBatchDeleteBuilds(
	_ context.Context,
	in *batchDeleteBuildsInput,
) (*batchDeleteBuildsOutput, error) {
	if len(in.IDs) == 0 {
		return &batchDeleteBuildsOutput{BuildsDeleted: []string{}}, nil
	}

	deleted := h.Backend.BatchDeleteBuilds(in.IDs)

	return &batchDeleteBuildsOutput{BuildsDeleted: deleted}, nil
}

type retryBuildInput struct {
	ID string `json:"id"`
}

type retryBuildOutput struct {
	Build *Build `json:"build"`
}

func (h *Handler) handleRetryBuild(
	_ context.Context,
	in *retryBuildInput,
) (*retryBuildOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: id is required", errInvalidRequest)
	}

	build, err := h.Backend.RetryBuild(in.ID)
	if err != nil {
		return nil, err
	}

	return &retryBuildOutput{Build: build}, nil
}

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

// --- Fleet operations ---

type createFleetInput struct {
	Tags            map[string]string `json:"tags"`
	Name            string            `json:"name"`
	ComputeType     string            `json:"computeType"`
	EnvironmentType string            `json:"environmentType"`
	BaseCapacity    int32             `json:"baseCapacity"`
}

type createFleetOutput struct {
	Fleet *Fleet `json:"fleet"`
}

func (h *Handler) handleCreateFleet(
	_ context.Context,
	in *createFleetInput,
) (*createFleetOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	f, err := h.Backend.CreateFleet(in.Name, in.BaseCapacity, in.ComputeType, in.EnvironmentType, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createFleetOutput{Fleet: f}, nil
}

type batchGetFleetsInput struct {
	Names []string `json:"names"`
}

type batchGetFleetsOutput struct {
	Fleets         []*Fleet `json:"fleets"`
	FleetsNotFound []string `json:"fleetsNotFound"`
}

func (h *Handler) handleBatchGetFleets(
	_ context.Context,
	in *batchGetFleetsInput,
) (*batchGetFleetsOutput, error) {
	found, notFound := h.Backend.BatchGetFleets(in.Names)

	return &batchGetFleetsOutput{
		Fleets:         found,
		FleetsNotFound: notFound,
	}, nil
}

// --- ReportGroup operations ---

type createReportGroupInput struct {
	Tags         map[string]string  `json:"tags"`
	ExportConfig ReportExportConfig `json:"exportConfig"`
	Name         string             `json:"name"`
	Type         string             `json:"type"`
}

type createReportGroupOutput struct {
	ReportGroup *ReportGroup `json:"reportGroup"`
}

func (h *Handler) handleCreateReportGroup(
	_ context.Context,
	in *createReportGroupInput,
) (*createReportGroupOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	if in.Type == "" {
		return nil, fmt.Errorf("%w: type is required", errInvalidRequest)
	}

	rg, err := h.Backend.CreateReportGroup(in.Name, in.Type, in.ExportConfig, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createReportGroupOutput{ReportGroup: rg}, nil
}

type batchGetReportGroupsInput struct {
	ReportGroupArns []string `json:"reportGroupArns"`
}

type batchGetReportGroupsOutput struct {
	ReportGroups         []*ReportGroup `json:"reportGroups"`
	ReportGroupsNotFound []string       `json:"reportGroupsNotFound"`
}

func (h *Handler) handleBatchGetReportGroups(
	_ context.Context,
	in *batchGetReportGroupsInput,
) (*batchGetReportGroupsOutput, error) {
	found, notFound := h.Backend.BatchGetReportGroups(in.ReportGroupArns)

	return &batchGetReportGroupsOutput{
		ReportGroups:         found,
		ReportGroupsNotFound: notFound,
	}, nil
}

// --- Report operations ---

type batchGetReportsInput struct {
	ReportArns []string `json:"reportArns"`
}

type batchGetReportsOutput struct {
	Reports         []*Report `json:"reports"`
	ReportsNotFound []string  `json:"reportsNotFound"`
}

func (h *Handler) handleBatchGetReports(
	_ context.Context,
	in *batchGetReportsInput,
) (*batchGetReportsOutput, error) {
	found, notFound := h.Backend.BatchGetReports(in.ReportArns)

	return &batchGetReportsOutput{
		Reports:         found,
		ReportsNotFound: notFound,
	}, nil
}

// --- BuildBatch operations ---

type batchGetBuildBatchesInput struct {
	IDs []string `json:"ids"`
}

type batchGetBuildBatchesOutput struct {
	BuildBatches         []*BuildBatch `json:"buildBatches"`
	BuildBatchesNotFound []string      `json:"buildBatchesNotFound"`
}

func (h *Handler) handleBatchGetBuildBatches(
	_ context.Context,
	in *batchGetBuildBatchesInput,
) (*batchGetBuildBatchesOutput, error) {
	found, notFound := h.Backend.BatchGetBuildBatches(in.IDs)

	return &batchGetBuildBatchesOutput{
		BuildBatches:         found,
		BuildBatchesNotFound: notFound,
	}, nil
}

// --- CommandExecution operations ---

type batchGetCommandExecutionsInput struct {
	SandboxID           string   `json:"sandboxId"`
	CommandExecutionIDs []string `json:"commandExecutionIds"`
}

type batchGetCommandExecutionsOutput struct {
	CommandExecutions         []*CommandExecution `json:"commandExecutions"`
	CommandExecutionsNotFound []string            `json:"commandExecutionsNotFound"`
}

func (h *Handler) handleBatchGetCommandExecutions(
	_ context.Context,
	in *batchGetCommandExecutionsInput,
) (*batchGetCommandExecutionsOutput, error) {
	if in.SandboxID == "" {
		return nil, fmt.Errorf("%w: sandboxId is required", errInvalidRequest)
	}

	found, notFound := h.Backend.BatchGetCommandExecutions(in.SandboxID, in.CommandExecutionIDs)

	return &batchGetCommandExecutionsOutput{
		CommandExecutions:         found,
		CommandExecutionsNotFound: notFound,
	}, nil
}

// --- Sandbox operations ---

type batchGetSandboxesInput struct {
	IDs []string `json:"ids"`
}

type batchGetSandboxesOutput struct {
	Sandboxes         []*Sandbox `json:"sandboxes"`
	SandboxesNotFound []string   `json:"sandboxesNotFound"`
}

func (h *Handler) handleBatchGetSandboxes(
	_ context.Context,
	in *batchGetSandboxesInput,
) (*batchGetSandboxesOutput, error) {
	found, notFound := h.Backend.BatchGetSandboxes(in.IDs)

	return &batchGetSandboxesOutput{
		Sandboxes:         found,
		SandboxesNotFound: notFound,
	}, nil
}

// --- Webhook operations ---

type createWebhookInput struct {
	ProjectName  string `json:"projectName"`
	BranchFilter string `json:"branchFilter"`
	BuildType    string `json:"buildType"`
}

type createWebhookOutput struct {
	Webhook *Webhook `json:"webhook"`
}

func (h *Handler) handleCreateWebhook(
	_ context.Context,
	in *createWebhookInput,
) (*createWebhookOutput, error) {
	if in.ProjectName == "" {
		return nil, fmt.Errorf("%w: projectName is required", errInvalidRequest)
	}

	w, err := h.Backend.CreateWebhook(in.ProjectName, in.BranchFilter, in.BuildType)
	if err != nil {
		return nil, err
	}

	return &createWebhookOutput{Webhook: w}, nil
}
