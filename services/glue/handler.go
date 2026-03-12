package glue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	glueTargetPrefix = "AWSGlue."
	unknownAction    = "Unknown"
)

var errUnknownAction = errors.New("UnknownOperationException")

// Handler is the Echo HTTP handler for AWS Glue operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Glue handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Glue" }

// GetSupportedOperations returns the list of supported Glue operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateDatabase",
		"GetDatabase",
		"GetDatabases",
		"UpdateDatabase",
		"DeleteDatabase",
		"CreateTable",
		"GetTable",
		"GetTables",
		"UpdateTable",
		"DeleteTable",
		"CreateCrawler",
		"GetCrawler",
		"GetCrawlers",
		"UpdateCrawler",
		"DeleteCrawler",
		"CreateJob",
		"GetJob",
		"GetJobs",
		"UpdateJob",
		"DeleteJob",
		"TagResource",
		"UntagResource",
		"GetTags",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "glue" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Glue requests via X-Amz-Target.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, glueTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation returns the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, glueTargetPrefix)

	if action == "" || action == target {
		return unknownAction
	}

	return action
}

// ExtractResource extracts a resource identifier from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		Name         string `json:"Name"`
		DatabaseName string `json:"DatabaseName"`
		ResourceArn  string `json:"ResourceArn"`
	}

	_ = json.Unmarshal(body, &req)

	switch {
	case req.ResourceArn != "":
		return req.ResourceArn
	case req.Name != "":
		return req.Name
	case req.DatabaseName != "":
		return req.DatabaseName
	}

	return ""
}

// Handler returns the Echo handler function for Glue requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Glue", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateDatabase": service.WrapOp(h.handleCreateDatabase),
		"GetDatabase":    service.WrapOp(h.handleGetDatabase),
		"GetDatabases":   service.WrapOp(h.handleGetDatabases),
		"UpdateDatabase": service.WrapOp(h.handleUpdateDatabase),
		"DeleteDatabase": service.WrapOp(h.handleDeleteDatabase),
		"CreateTable":    service.WrapOp(h.handleCreateTable),
		"GetTable":       service.WrapOp(h.handleGetTable),
		"GetTables":      service.WrapOp(h.handleGetTables),
		"UpdateTable":    service.WrapOp(h.handleUpdateTable),
		"DeleteTable":    service.WrapOp(h.handleDeleteTable),
		"CreateCrawler":  service.WrapOp(h.handleCreateCrawler),
		"GetCrawler":     service.WrapOp(h.handleGetCrawler),
		"GetCrawlers":    service.WrapOp(h.handleGetCrawlers),
		"UpdateCrawler":  service.WrapOp(h.handleUpdateCrawler),
		"DeleteCrawler":  service.WrapOp(h.handleDeleteCrawler),
		"CreateJob":      service.WrapOp(h.handleCreateJob),
		"GetJob":         service.WrapOp(h.handleGetJob),
		"GetJobs":        service.WrapOp(h.handleGetJobs),
		"UpdateJob":      service.WrapOp(h.handleUpdateJob),
		"DeleteJob":      service.WrapOp(h.handleDeleteJob),
		"TagResource":    service.WrapOp(h.handleTagResource),
		"UntagResource":  service.WrapOp(h.handleUntagResource),
		"GetTags":        service.WrapOp(h.handleGetTags),
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
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, errorResponse("EntityNotFoundException", err.Error()))
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest, errorResponse("AlreadyExistsException", err.Error()))
	case errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusBadRequest, errorResponse("UnknownOperationException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}

// --- Database handlers ---

type createDatabaseInput struct {
	Tags          map[string]string `json:"Tags,omitempty"`
	DatabaseInput DatabaseInput     `json:"DatabaseInput"`
}

type emptyOutput struct{}

func (h *Handler) handleCreateDatabase(_ context.Context, in *createDatabaseInput) (*emptyOutput, error) {
	if _, err := h.Backend.CreateDatabase(in.DatabaseInput, in.Tags); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type getDatabaseInput struct {
	Name string `json:"Name"`
}

type getDatabaseOutput struct {
	Database *Database `json:"Database"`
}

func (h *Handler) handleGetDatabase(_ context.Context, in *getDatabaseInput) (*getDatabaseOutput, error) {
	db, err := h.Backend.GetDatabase(in.Name)
	if err != nil {
		return nil, err
	}

	return &getDatabaseOutput{Database: db}, nil
}

type getDatabasesInput struct{}

type getDatabasesOutput struct {
	DatabaseList []*Database `json:"DatabaseList"`
}

func (h *Handler) handleGetDatabases(_ context.Context, _ *getDatabasesInput) (*getDatabasesOutput, error) {
	dbs := h.Backend.GetDatabases()

	return &getDatabasesOutput{DatabaseList: dbs}, nil
}

type updateDatabaseInput struct {
	Name          string        `json:"Name"`
	DatabaseInput DatabaseInput `json:"DatabaseInput"`
}

func (h *Handler) handleUpdateDatabase(_ context.Context, in *updateDatabaseInput) (*emptyOutput, error) {
	if err := h.Backend.UpdateDatabase(in.Name, in.DatabaseInput); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type deleteDatabaseInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteDatabase(_ context.Context, in *deleteDatabaseInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteDatabase(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- Table handlers ---

type createTableInput struct {
	DatabaseName string     `json:"DatabaseName"`
	TableInput   TableInput `json:"TableInput"`
}

func (h *Handler) handleCreateTable(_ context.Context, in *createTableInput) (*emptyOutput, error) {
	if _, err := h.Backend.CreateTable(in.DatabaseName, in.TableInput); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type getTableInput struct {
	DatabaseName string `json:"DatabaseName"`
	Name         string `json:"Name"`
}

type getTableOutput struct {
	Table *Table `json:"Table"`
}

func (h *Handler) handleGetTable(_ context.Context, in *getTableInput) (*getTableOutput, error) {
	t, err := h.Backend.GetTable(in.DatabaseName, in.Name)
	if err != nil {
		return nil, err
	}

	return &getTableOutput{Table: t}, nil
}

type getTablesInput struct {
	DatabaseName string `json:"DatabaseName"`
}

type getTablesOutput struct {
	TableList []*Table `json:"TableList"`
}

func (h *Handler) handleGetTables(_ context.Context, in *getTablesInput) (*getTablesOutput, error) {
	tables, err := h.Backend.GetTables(in.DatabaseName)
	if err != nil {
		return nil, err
	}

	return &getTablesOutput{TableList: tables}, nil
}

type updateTableInput struct {
	DatabaseName string     `json:"DatabaseName"`
	TableInput   TableInput `json:"TableInput"`
}

func (h *Handler) handleUpdateTable(_ context.Context, in *updateTableInput) (*emptyOutput, error) {
	if err := h.Backend.UpdateTable(in.DatabaseName, in.TableInput); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type deleteTableInput struct {
	DatabaseName string `json:"DatabaseName"`
	Name         string `json:"Name"`
}

func (h *Handler) handleDeleteTable(_ context.Context, in *deleteTableInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteTable(in.DatabaseName, in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- Crawler handlers ---

type createCrawlerInput struct {
	Tags         map[string]string `json:"Tags,omitempty"`
	Name         string            `json:"Name"`
	Role         string            `json:"Role"`
	DatabaseName string            `json:"DatabaseName"`
	Targets      CrawlerTarget     `json:"Targets,omitzero"`
}

func (h *Handler) handleCreateCrawler(_ context.Context, in *createCrawlerInput) (*emptyOutput, error) {
	if _, err := h.Backend.CreateCrawler(in.Name, in.Role, in.DatabaseName, in.Targets, in.Tags); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type getCrawlerInput struct {
	Name string `json:"Name"`
}

type getCrawlerOutput struct {
	Crawler *Crawler `json:"Crawler"`
}

func (h *Handler) handleGetCrawler(_ context.Context, in *getCrawlerInput) (*getCrawlerOutput, error) {
	c, err := h.Backend.GetCrawler(in.Name)
	if err != nil {
		return nil, err
	}

	return &getCrawlerOutput{Crawler: c}, nil
}

type getCrawlersInput struct{}

type getCrawlersOutput struct {
	Crawlers []*Crawler `json:"Crawlers"`
}

func (h *Handler) handleGetCrawlers(_ context.Context, _ *getCrawlersInput) (*getCrawlersOutput, error) {
	crawlers := h.Backend.GetCrawlers()

	return &getCrawlersOutput{Crawlers: crawlers}, nil
}

type updateCrawlerInput struct {
	Name         string        `json:"Name"`
	Role         string        `json:"Role"`
	DatabaseName string        `json:"DatabaseName"`
	Targets      CrawlerTarget `json:"Targets,omitzero"`
}

func (h *Handler) handleUpdateCrawler(_ context.Context, in *updateCrawlerInput) (*emptyOutput, error) {
	if err := h.Backend.UpdateCrawler(in.Name, in.Role, in.DatabaseName, in.Targets); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type deleteCrawlerInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteCrawler(_ context.Context, in *deleteCrawlerInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteCrawler(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- Job handlers ---

type createJobInput struct {
	Tags              map[string]string `json:"Tags,omitempty"`
	DefaultArguments  map[string]string `json:"DefaultArguments,omitempty"`
	Command           JobCommand        `json:"Command,omitzero"`
	WorkerType        string            `json:"WorkerType,omitempty"`
	Role              string            `json:"Role,omitempty"`
	GlueVersion       string            `json:"GlueVersion,omitempty"`
	Name              string            `json:"Name"`
	Description       string            `json:"Description,omitempty"`
	Connections       ConnectionsList   `json:"Connections,omitzero"`
	NumberOfWorkers   int               `json:"NumberOfWorkers,omitempty"`
	MaxRetries        int               `json:"MaxRetries,omitempty"`
	Timeout           int               `json:"Timeout,omitempty"`
	ExecutionProperty ExecutionProperty `json:"ExecutionProperty,omitzero"`
}

type createJobOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateJob(_ context.Context, in *createJobInput) (*createJobOutput, error) {
	j, err := h.Backend.CreateJob(Job{
		Name:              in.Name,
		Description:       in.Description,
		Role:              in.Role,
		Command:           in.Command,
		DefaultArguments:  in.DefaultArguments,
		GlueVersion:       in.GlueVersion,
		WorkerType:        in.WorkerType,
		NumberOfWorkers:   in.NumberOfWorkers,
		MaxRetries:        in.MaxRetries,
		Timeout:           in.Timeout,
		Tags:              in.Tags,
		ExecutionProperty: in.ExecutionProperty,
		Connections:       in.Connections,
	})
	if err != nil {
		return nil, err
	}

	return &createJobOutput{Name: j.Name}, nil
}

type getJobInput struct {
	JobName string `json:"JobName"`
}

type getJobOutput struct {
	Job *Job `json:"Job"`
}

func (h *Handler) handleGetJob(_ context.Context, in *getJobInput) (*getJobOutput, error) {
	j, err := h.Backend.GetJob(in.JobName)
	if err != nil {
		return nil, err
	}

	return &getJobOutput{Job: j}, nil
}

type getJobsInput struct{}

type getJobsOutput struct {
	Jobs []*Job `json:"Jobs"`
}

func (h *Handler) handleGetJobs(_ context.Context, _ *getJobsInput) (*getJobsOutput, error) {
	jobs := h.Backend.GetJobs()

	return &getJobsOutput{Jobs: jobs}, nil
}

// jobUpdatePayload models the allowed fields for Glue's JobUpdate shape.
// It intentionally omits create-only fields such as Name and Tags.
type jobUpdatePayload struct {
	DefaultArguments  map[string]string `json:"DefaultArguments,omitempty"`
	Command           JobCommand        `json:"Command,omitzero"`
	WorkerType        string            `json:"WorkerType,omitempty"`
	Role              string            `json:"Role,omitempty"`
	GlueVersion       string            `json:"GlueVersion,omitempty"`
	Description       string            `json:"Description,omitempty"`
	Connections       ConnectionsList   `json:"Connections,omitzero"`
	NumberOfWorkers   int               `json:"NumberOfWorkers,omitempty"`
	MaxRetries        int               `json:"MaxRetries,omitempty"`
	Timeout           int               `json:"Timeout,omitempty"`
	ExecutionProperty ExecutionProperty `json:"ExecutionProperty,omitzero"`
}

type updateJobInput struct {
	JobName   string           `json:"JobName"`
	JobUpdate jobUpdatePayload `json:"JobUpdate"`
}

type updateJobOutput struct {
	JobName string `json:"JobName"`
}

func (h *Handler) handleUpdateJob(_ context.Context, in *updateJobInput) (*updateJobOutput, error) {
	if err := h.Backend.UpdateJob(in.JobName, Job{
		Description:       in.JobUpdate.Description,
		Role:              in.JobUpdate.Role,
		Command:           in.JobUpdate.Command,
		DefaultArguments:  in.JobUpdate.DefaultArguments,
		GlueVersion:       in.JobUpdate.GlueVersion,
		WorkerType:        in.JobUpdate.WorkerType,
		NumberOfWorkers:   in.JobUpdate.NumberOfWorkers,
		MaxRetries:        in.JobUpdate.MaxRetries,
		Timeout:           in.JobUpdate.Timeout,
		ExecutionProperty: in.JobUpdate.ExecutionProperty,
		Connections:       in.JobUpdate.Connections,
	}); err != nil {
		return nil, err
	}

	return &updateJobOutput{JobName: in.JobName}, nil
}

type deleteJobInput struct {
	JobName string `json:"JobName"`
}

type deleteJobOutput struct {
	JobName string `json:"JobName"`
}

func (h *Handler) handleDeleteJob(_ context.Context, in *deleteJobInput) (*deleteJobOutput, error) {
	if err := h.Backend.DeleteJob(in.JobName); err != nil {
		return nil, err
	}

	return &deleteJobOutput{JobName: in.JobName}, nil
}

// --- Tag handlers ---

type tagResourceInput struct {
	TagsToAdd   map[string]string `json:"TagsToAdd"`
	ResourceArn string            `json:"ResourceArn"`
}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*emptyOutput, error) {
	if err := h.Backend.TagResource(in.ResourceArn, in.TagsToAdd); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn  string   `json:"ResourceArn"`
	TagsToRemove []string `json:"TagsToRemove"`
}

func (h *Handler) handleUntagResource(_ context.Context, in *untagResourceInput) (*emptyOutput, error) {
	if err := h.Backend.UntagResource(in.ResourceArn, in.TagsToRemove); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type getTagsInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type getTagsOutput struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleGetTags(_ context.Context, in *getTagsInput) (*getTagsOutput, error) {
	tags, err := h.Backend.GetTags(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &getTagsOutput{Tags: tags}, nil
}
