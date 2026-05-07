package databrew

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

const (
	databrewPathPrefix = "/databrew/v1/"

	segDatasets    = "datasets"
	segRecipes     = "recipes"
	segProjects    = "projects"
	segProfileJobs = "profileJobs"
	segRecipeJobs  = "recipeJobs"
	segJobs        = "jobs"

	opCreateDataset    = "CreateDataset"
	opDescribeDataset  = "DescribeDataset"
	opListDatasets     = "ListDatasets"
	opUpdateDataset    = "UpdateDataset"
	opDeleteDataset    = "DeleteDataset"
	opCreateRecipe     = "CreateRecipe"
	opDescribeRecipe   = "DescribeRecipe"
	opListRecipes      = "ListRecipes"
	opPublishRecipe    = "PublishRecipe"
	opUpdateRecipe     = "UpdateRecipe"
	opDeleteRecipe     = "DeleteRecipe"
	opCreateProject    = "CreateProject"
	opDescribeProject  = "DescribeProject"
	opListProjects     = "ListProjects"
	opUpdateProject    = "UpdateProject"
	opDeleteProject    = "DeleteProject"
	opCreateProfileJob = "CreateProfileJob"
	opCreateRecipeJob  = "CreateRecipeJob"
	opDescribeJob      = "DescribeJob"
	opListJobs         = "ListJobs"
	opUpdateProfileJob = "UpdateProfileJob"
	opUpdateRecipeJob  = "UpdateRecipeJob"
	opDeleteJob        = "DeleteJob"
	opStartJobRun      = "StartJobRun"
	opListJobRuns      = "ListJobRuns"
	opUnknown          = "Unknown"

	minPathSegments = 2

	keyName    = "Name"
	keyMessage = "Message"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request body")
)

// Handler is the HTTP handler for AWS Glue DataBrew operations.
type Handler struct {
	Backend   StorageBackend
	AccountID string
	Region    string
}

// NewHandler creates a new DataBrew handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.AccountID(),
		Region:    backend.Region(),
	}
}

func (h *Handler) Name() string                        { return "DataBrew" }
func (h *Handler) Reset()                              { h.Backend.Reset() }
func (h *Handler) StartWorker(_ context.Context) error { return nil }

func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateDataset, opDescribeDataset, opListDatasets, opUpdateDataset, opDeleteDataset,
		opCreateRecipe, opDescribeRecipe, opListRecipes, opPublishRecipe, opUpdateRecipe, opDeleteRecipe,
		opCreateProject, opDescribeProject, opListProjects, opUpdateProject, opDeleteProject,
		opCreateProfileJob, opCreateRecipeJob, opDescribeJob, opListJobs,
		opUpdateProfileJob, opUpdateRecipeJob, opDeleteJob, opStartJobRun, opListJobRuns,
	}
}

func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, databrewPathPrefix) || path == "/databrew/v1"
	}
}

func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseDataBrewRESTPath(c.Request().Method, c.Request().URL.Path)

	return op
}

func (h *Handler) ExtractResource(c *echo.Context) string {
	_, name := parseDataBrewRESTPath(c.Request().Method, c.Request().URL.Path)

	return name
}

func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		action, name := parseDataBrewRESTPath(c.Request().Method, c.Request().URL.Path)
		if action == opUnknown {
			return c.String(http.StatusNotFound, "not found")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "databrew: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		body = enrichDataBrewBody(action, name, body)

		result, dispErr := h.dispatch(ctx, action, body)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.JSON(http.StatusOK, map[string]any{})
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, map[string]string{keyMessage: err.Error()})
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, map[string]string{keyMessage: err.Error()})
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessage: err.Error()})
	case errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusNotFound, map[string]string{keyMessage: err.Error()})
	case errors.Is(err, errInvalidRequest):
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusInternalServerError, map[string]string{keyMessage: err.Error()})
}

func parseDataBrewRESTPath(method, path string) (string, string) {
	after, ok := strings.CutPrefix(path, databrewPathPrefix)
	if !ok {
		return opUnknown, ""
	}

	segments := strings.SplitN(after, "/", minPathSegments+1)
	if len(segments) == 0 || segments[0] == "" {
		return opUnknown, ""
	}

	resource := segments[0]
	name := ""
	if len(segments) >= minPathSegments {
		name = segments[1]
	}

	var subOp string
	if len(segments) >= 3 { //nolint:mnd // 3 segments: resource/name/subOp
		subOp = segments[2]
	}

	switch resource {
	case segDatasets:
		return parseDatasetOp(method, name), name
	case segRecipes:
		return parseRecipeOp(method, name, subOp), name
	case segProjects:
		return parseProjectOp(method, name), name
	case segProfileJobs:
		return parseProfileJobOp(method, name), name
	case segRecipeJobs:
		return parseRecipeJobOp(method, name), name
	case segJobs:
		return parseJobOp(method, name, subOp), name
	}

	return opUnknown, ""
}

func parseDatasetOp(method, name string) string {
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateDataset
		}
	case http.MethodGet:
		if name == "" {
			return opListDatasets
		}

		return opDescribeDataset
	case http.MethodPut:
		if name != "" {
			return opUpdateDataset
		}
	case http.MethodDelete:
		if name != "" {
			return opDeleteDataset
		}
	}

	return opUnknown
}

func parseRecipeOp(method, name, subOp string) string {
	if subOp == "publishRecipe" {
		return opPublishRecipe
	}
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateRecipe
		}
	case http.MethodGet:
		if name == "" {
			return opListRecipes
		}

		return opDescribeRecipe
	case http.MethodPut:
		if name != "" {
			return opUpdateRecipe
		}
	case http.MethodDelete:
		if name != "" {
			return opDeleteRecipe
		}
	}

	return opUnknown
}

func parseProjectOp(method, name string) string {
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateProject
		}
	case http.MethodGet:
		if name == "" {
			return opListProjects
		}

		return opDescribeProject
	case http.MethodPut:
		if name != "" {
			return opUpdateProject
		}
	case http.MethodDelete:
		if name != "" {
			return opDeleteProject
		}
	}

	return opUnknown
}

func parseProfileJobOp(method, name string) string {
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateProfileJob
		}
	case http.MethodPut:
		if name != "" {
			return opUpdateProfileJob
		}
	}

	return opUnknown
}

func parseRecipeJobOp(method, name string) string {
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateRecipeJob
		}
	case http.MethodPut:
		if name != "" {
			return opUpdateRecipeJob
		}
	}

	return opUnknown
}

func parseJobOp(method, name, subOp string) string {
	switch {
	case subOp == "startJobRun" && method == http.MethodPost:
		return opStartJobRun
	case subOp == "jobRuns" && method == http.MethodGet:
		return opListJobRuns
	case method == http.MethodGet && name == "":
		return opListJobs
	case method == http.MethodGet && name != "":
		return opDescribeJob
	case method == http.MethodDelete && name != "":
		return opDeleteJob
	}

	return opUnknown
}

func enrichDataBrewBody(_, name string, body []byte) []byte {
	if name == "" {
		return body
	}
	m := make(map[string]json.RawMessage)
	_ = json.Unmarshal(body, &m)
	nameJSON, _ := json.Marshal(name)
	m["Name"] = nameJSON
	result, _ := json.Marshal(m)

	return result
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	if result, ok, err := h.dispatchDataset(ctx, action, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchRecipe(ctx, action, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchProject(ctx, action, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchJob(ctx, action, body); ok {
		return result, err
	}

	return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
}

func (h *Handler) dispatchDataset(
	ctx context.Context,
	action string,
	body []byte,
) ([]byte, bool, error) {
	switch action {
	case opCreateDataset:
		r, e := h.handleCreateDataset(ctx, body)

		return r, true, e
	case opDescribeDataset:
		r, e := h.handleDescribeDataset(ctx, body)

		return r, true, e
	case opListDatasets:
		r, e := h.handleListDatasets(ctx, body)

		return r, true, e
	case opUpdateDataset:
		r, e := h.handleUpdateDataset(ctx, body)

		return r, true, e
	case opDeleteDataset:
		r, e := h.handleDeleteDataset(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) dispatchRecipe(
	ctx context.Context,
	action string,
	body []byte,
) ([]byte, bool, error) {
	switch action {
	case opCreateRecipe:
		r, e := h.handleCreateRecipe(ctx, body)

		return r, true, e
	case opDescribeRecipe:
		r, e := h.handleDescribeRecipe(ctx, body)

		return r, true, e
	case opListRecipes:
		r, e := h.handleListRecipes(ctx, body)

		return r, true, e
	case opPublishRecipe:
		r, e := h.handlePublishRecipe(ctx, body)

		return r, true, e
	case opUpdateRecipe:
		r, e := h.handleUpdateRecipe(ctx, body)

		return r, true, e
	case opDeleteRecipe:
		r, e := h.handleDeleteRecipe(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) dispatchProject(
	ctx context.Context,
	action string,
	body []byte,
) ([]byte, bool, error) {
	switch action {
	case opCreateProject:
		r, e := h.handleCreateProject(ctx, body)

		return r, true, e
	case opDescribeProject:
		r, e := h.handleDescribeProject(ctx, body)

		return r, true, e
	case opListProjects:
		r, e := h.handleListProjects(ctx, body)

		return r, true, e
	case opUpdateProject:
		r, e := h.handleUpdateProject(ctx, body)

		return r, true, e
	case opDeleteProject:
		r, e := h.handleDeleteProject(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) dispatchJob(
	ctx context.Context,
	action string,
	body []byte,
) ([]byte, bool, error) {
	switch action {
	case opCreateProfileJob:
		r, e := h.handleCreateProfileJob(ctx, body)

		return r, true, e
	case opCreateRecipeJob:
		r, e := h.handleCreateRecipeJob(ctx, body)

		return r, true, e
	case opDescribeJob:
		r, e := h.handleDescribeJob(ctx, body)

		return r, true, e
	case opListJobs:
		r, e := h.handleListJobs(ctx, body)

		return r, true, e
	case opUpdateProfileJob, opUpdateRecipeJob:
		r, e := h.handleUpdateJob(ctx, body)

		return r, true, e
	case opDeleteJob:
		r, e := h.handleDeleteJob(ctx, body)

		return r, true, e
	case opStartJobRun:
		r, e := h.handleStartJobRun(ctx, body)

		return r, true, e
	case opListJobRuns:
		r, e := h.handleListJobRuns(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) handleCreateDataset(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		FormatOptions DatasetFormatOptions `json:"FormatOptions"`
		Input         DatasetInput         `json:"Input"`
		Tags          map[string]string    `json:"Tags"`
		Name          string               `json:"Name"`
		Format        string               `json:"Format"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	ds, err := h.Backend.CreateDataset(req.Name, req.Format, req.Input, req.FormatOptions, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: ds.Name})
}

func (h *Handler) handleDescribeDataset(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	ds, err := h.Backend.DescribeDataset(req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(ds)
}

func (h *Handler) handleListDatasets(_ context.Context, _ []byte) ([]byte, error) {
	return json.Marshal(map[string]any{"Datasets": h.Backend.ListDatasets()})
}

func (h *Handler) handleUpdateDataset(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		FormatOptions DatasetFormatOptions `json:"FormatOptions"`
		Input         DatasetInput         `json:"Input"`
		Name          string               `json:"Name"`
		Format        string               `json:"Format"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateDataset(req.Name, req.Format, req.Input, req.FormatOptions); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleDeleteDataset(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.DeleteDataset(req.Name); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleCreateRecipe(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		Name        string            `json:"Name"`
		Description string            `json:"Description"`
		Steps       []RecipeStep      `json:"Steps"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	r, err := h.Backend.CreateRecipe(req.Name, req.Description, req.Steps, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: r.Name})
}

func (h *Handler) handleDescribeRecipe(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	r, err := h.Backend.DescribeRecipe(req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(r)
}

func (h *Handler) handleListRecipes(_ context.Context, _ []byte) ([]byte, error) {
	return json.Marshal(map[string]any{"Recipes": h.Backend.ListRecipes()})
}

func (h *Handler) handlePublishRecipe(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.PublishRecipe(req.Name, req.Description); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleUpdateRecipe(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name        string       `json:"Name"`
		Description string       `json:"Description"`
		Steps       []RecipeStep `json:"Steps"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateRecipe(req.Name, req.Description, req.Steps); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleDeleteRecipe(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.DeleteRecipe(req.Name); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleCreateProject(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		Name        string            `json:"Name"`
		DatasetName string            `json:"DatasetName"`
		RecipeName  string            `json:"RecipeName"`
		RoleArn     string            `json:"RoleArn"`
		Sample      Sample            `json:"Sample"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	p, err := h.Backend.CreateProject(
		req.Name,
		req.DatasetName,
		req.RecipeName,
		req.RoleArn,
		req.Sample,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: p.Name})
}

func (h *Handler) handleDescribeProject(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	p, err := h.Backend.DescribeProject(req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(p)
}

func (h *Handler) handleListProjects(_ context.Context, _ []byte) ([]byte, error) {
	return json.Marshal(map[string]any{"Projects": h.Backend.ListProjects()})
}

func (h *Handler) handleUpdateProject(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name        string `json:"Name"`
		DatasetName string `json:"DatasetName"`
		RoleArn     string `json:"RoleArn"`
		Sample      Sample `json:"Sample"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateProject(req.Name, req.DatasetName, req.RoleArn, req.Sample); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleDeleteProject(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.DeleteProject(req.Name); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleCreateProfileJob(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		Name        string            `json:"Name"`
		DatasetName string            `json:"DatasetName"`
		RoleArn     string            `json:"RoleArn"`
		Outputs     []Output          `json:"Outputs"`
		MaxCapacity int               `json:"MaxCapacity"`
		MaxRetries  int               `json:"MaxRetries"`
		Timeout     int               `json:"Timeout"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	j, err := h.Backend.CreateJob(
		req.Name,
		"PROFILE",
		req.DatasetName,
		"",
		"",
		req.RoleArn,
		req.Outputs,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: j.Name})
}

func (h *Handler) handleCreateRecipeJob(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		Name        string            `json:"Name"`
		DatasetName string            `json:"DatasetName"`
		ProjectName string            `json:"ProjectName"`
		RecipeName  string            `json:"RecipeName"`
		RoleArn     string            `json:"RoleArn"`
		Outputs     []Output          `json:"Outputs"`
		MaxCapacity int               `json:"MaxCapacity"`
		MaxRetries  int               `json:"MaxRetries"`
		Timeout     int               `json:"Timeout"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	j, err := h.Backend.CreateJob(
		req.Name,
		"RECIPE",
		req.DatasetName,
		req.ProjectName,
		req.RecipeName,
		req.RoleArn,
		req.Outputs,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: j.Name})
}

func (h *Handler) handleDescribeJob(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	j, err := h.Backend.DescribeJob(req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(j)
}

func (h *Handler) handleListJobs(_ context.Context, _ []byte) ([]byte, error) {
	return json.Marshal(map[string]any{"Jobs": h.Backend.ListJobs()})
}

func (h *Handler) handleUpdateJob(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name        string   `json:"Name"`
		RoleArn     string   `json:"RoleArn"`
		Outputs     []Output `json:"Outputs"`
		MaxCapacity int      `json:"MaxCapacity"`
		MaxRetries  int      `json:"MaxRetries"`
		Timeout     int      `json:"Timeout"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateJob(
		req.Name, req.RoleArn, req.Outputs, req.MaxCapacity, req.MaxRetries, req.Timeout,
	); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleDeleteJob(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.DeleteJob(req.Name); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleStartJobRun(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	run, err := h.Backend.StartJobRun(req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"RunID": run.RunID})
}

func (h *Handler) handleListJobRuns(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	runs, err := h.Backend.ListJobRuns(req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"JobRuns": runs})
}
