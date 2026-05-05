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
	Backend StorageBackend
	// ops is the pre-built dispatch table mapping operation names to handler
	// functions, initialized in NewHandler.
	ops map[string]service.JSONOpFunc
}

// NewHandler creates a new Glue handler backed by backend.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears all backend state. Used for test isolation.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns the service name.
func (h *Handler) Name() string { return "Glue" }

// GetSupportedOperations returns the list of supported Glue operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"BatchCreatePartition",
		"BatchDeleteConnection",
		"BatchDeletePartition",
		"BatchDeleteTable",
		"BatchDeleteTableVersion",
		"BatchGetBlueprints",
		"BatchGetCrawlers",
		"BatchGetCustomEntityTypes",
		"BatchGetDataQualityResult",
		"BatchGetDevEndpoints",
		"BatchStopJobRun",
		"CancelDataQualityRulesetEvaluationRun",
		"CreateConnection",
		"CreateCrawler",
		"CreateDatabase",
		"CreateDataQualityRuleset",
		"CreateJob",
		"CreateTable",
		"DeleteConnection",
		"DeleteCrawler",
		"DeleteDatabase",
		"DeleteDataQualityRuleset",
		"DeleteJob",
		"DeleteTable",
		"GetConnection",
		"GetConnections",
		"GetCrawler",
		"GetCrawlers",
		"GetDatabase",
		"GetDatabases",
		"GetDataQualityRuleset",
		"GetDataQualityRulesetEvaluationRun",
		"GetJob",
		"GetJobBookmark",
		"GetJobRun",
		"GetJobRuns",
		"GetJobs",
		"GetTable",
		"GetTables",
		"GetTags",
		"ListCrawlers",
		"ListDataQualityRulesets",
		"ResetJobBookmark",
		"StartCrawler",
		"StartCrawlerSchedule",
		"StartDataQualityRulesetEvaluationRun",
		"StartJobRun",
		"StopCrawler",
		"StopCrawlerSchedule",
		"TagResource",
		"UntagResource",
		"UpdateCrawler",
		"UpdateCrawlerSchedule",
		"UpdateDatabase",
		"UpdateDataQualityRuleset",
		"UpdateJob",
		"UpdateTable",
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
		Name           string `json:"Name"`
		DatabaseName   string `json:"DatabaseName"`
		ResourceArn    string `json:"ResourceArn"`
		CrawlerName    string `json:"CrawlerName"`
		JobName        string `json:"JobName"`
		ConnectionName string `json:"ConnectionName"`
	}

	_ = json.Unmarshal(body, &req)

	switch {
	case req.ResourceArn != "":
		return req.ResourceArn
	case req.Name != "":
		return req.Name
	case req.CrawlerName != "":
		return req.CrawlerName
	case req.JobName != "":
		return req.JobName
	case req.ConnectionName != "":
		return req.ConnectionName
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

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"BatchCreatePartition":                  service.WrapOp(h.handleBatchCreatePartition),
		"BatchDeleteConnection":                 service.WrapOp(h.handleBatchDeleteConnection),
		"BatchDeletePartition":                  service.WrapOp(h.handleBatchDeletePartition),
		"BatchDeleteTable":                      service.WrapOp(h.handleBatchDeleteTable),
		"BatchDeleteTableVersion":               service.WrapOp(h.handleBatchDeleteTableVersion),
		"BatchGetBlueprints":                    service.WrapOp(h.handleBatchGetBlueprints),
		"BatchGetCrawlers":                      service.WrapOp(h.handleBatchGetCrawlers),
		"BatchGetCustomEntityTypes":             service.WrapOp(h.handleBatchGetCustomEntityTypes),
		"BatchGetDataQualityResult":             service.WrapOp(h.handleBatchGetDataQualityResult),
		"BatchGetDevEndpoints":                  service.WrapOp(h.handleBatchGetDevEndpoints),
		"BatchStopJobRun":                       service.WrapOp(h.handleBatchStopJobRun),
		"CancelDataQualityRulesetEvaluationRun": service.WrapOp(h.handleCancelDataQualityRulesetEvaluationRun),
		"CreateConnection":                      service.WrapOp(h.handleCreateConnection),
		"CreateCrawler":                         service.WrapOp(h.handleCreateCrawler),
		"CreateDatabase":                        service.WrapOp(h.handleCreateDatabase),
		"CreateDataQualityRuleset":              service.WrapOp(h.handleCreateDataQualityRuleset),
		"CreateJob":                             service.WrapOp(h.handleCreateJob),
		"CreateTable":                           service.WrapOp(h.handleCreateTable),
		"DeleteConnection":                      service.WrapOp(h.handleDeleteConnection),
		"DeleteCrawler":                         service.WrapOp(h.handleDeleteCrawler),
		"DeleteDatabase":                        service.WrapOp(h.handleDeleteDatabase),
		"DeleteDataQualityRuleset":              service.WrapOp(h.handleDeleteDataQualityRuleset),
		"DeleteJob":                             service.WrapOp(h.handleDeleteJob),
		"DeleteTable":                           service.WrapOp(h.handleDeleteTable),
		"GetConnection":                         service.WrapOp(h.handleGetConnection),
		"GetConnections":                        service.WrapOp(h.handleGetConnections),
		"GetCrawler":                            service.WrapOp(h.handleGetCrawler),
		"GetCrawlers":                           service.WrapOp(h.handleGetCrawlers),
		"GetDatabase":                           service.WrapOp(h.handleGetDatabase),
		"GetDatabases":                          service.WrapOp(h.handleGetDatabases),
		"GetDataQualityRuleset":                 service.WrapOp(h.handleGetDataQualityRuleset),
		"GetDataQualityRulesetEvaluationRun":    service.WrapOp(h.handleGetDataQualityRulesetEvaluationRun),
		"GetJob":                                service.WrapOp(h.handleGetJob),
		"GetJobBookmark":                        service.WrapOp(h.handleGetJobBookmark),
		"GetJobRun":                             service.WrapOp(h.handleGetJobRun),
		"GetJobRuns":                            service.WrapOp(h.handleGetJobRuns),
		"GetJobs":                               service.WrapOp(h.handleGetJobs),
		"GetTable":                              service.WrapOp(h.handleGetTable),
		"GetTables":                             service.WrapOp(h.handleGetTables),
		"GetTags":                               service.WrapOp(h.handleGetTags),
		"ListCrawlers":                          service.WrapOp(h.handleListCrawlers),
		"ListDataQualityRulesets":               service.WrapOp(h.handleListDataQualityRulesets),
		"ResetJobBookmark":                      service.WrapOp(h.handleResetJobBookmark),
		"StartCrawler":                          service.WrapOp(h.handleStartCrawler),
		"StartCrawlerSchedule":                  service.WrapOp(h.handleStartCrawlerSchedule),
		"StartDataQualityRulesetEvaluationRun":  service.WrapOp(h.handleStartDataQualityRulesetEvaluationRun),
		"StartJobRun":                           service.WrapOp(h.handleStartJobRun),
		"StopCrawler":                           service.WrapOp(h.handleStopCrawler),
		"StopCrawlerSchedule":                   service.WrapOp(h.handleStopCrawlerSchedule),
		"TagResource":                           service.WrapOp(h.handleTagResource),
		"UntagResource":                         service.WrapOp(h.handleUntagResource),
		"UpdateCrawler":                         service.WrapOp(h.handleUpdateCrawler),
		"UpdateCrawlerSchedule":                 service.WrapOp(h.handleUpdateCrawlerSchedule),
		"UpdateDatabase":                        service.WrapOp(h.handleUpdateDatabase),
		"UpdateDataQualityRuleset":              service.WrapOp(h.handleUpdateDataQualityRuleset),
		"UpdateJob":                             service.WrapOp(h.handleUpdateJob),
		"UpdateTable":                           service.WrapOp(h.handleUpdateTable),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
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
	case errors.Is(err, ErrCrawlerRunning):
		return c.JSON(http.StatusBadRequest, errorResponse("CrawlerRunningException", err.Error()))
	case errors.Is(err, ErrCrawlerNotRunning):
		return c.JSON(http.StatusBadRequest, errorResponse("CrawlerNotRunningException", err.Error()))
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, errorResponse("EntityNotFoundException", err.Error()))
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest, errorResponse("AlreadyExistsException", err.Error()))
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", err.Error()))
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

// --- Batch partition handlers ---

type batchCreatePartitionInput struct {
	DatabaseName       string           `json:"DatabaseName"`
	TableName          string           `json:"TableName"`
	PartitionInputList []PartitionInput `json:"PartitionInputList"`
}

type batchCreatePartitionOutput struct {
	Errors     []PartitionError `json:"Errors"`
	Partitions []*Partition     `json:"Partitions"`
}

func (h *Handler) handleBatchCreatePartition(
	_ context.Context,
	in *batchCreatePartitionInput,
) (*batchCreatePartitionOutput, error) {
	created, errs := h.Backend.BatchCreatePartition(in.DatabaseName, in.TableName, in.PartitionInputList)

	return &batchCreatePartitionOutput{Partitions: created, Errors: errs}, nil
}

type batchDeletePartitionInput struct {
	DatabaseName       string               `json:"DatabaseName"`
	TableName          string               `json:"TableName"`
	PartitionsToDelete []PartitionValueList `json:"PartitionsToDelete"`
}

type batchDeletePartitionOutput struct {
	Errors []PartitionError `json:"Errors"`
}

func (h *Handler) handleBatchDeletePartition(
	_ context.Context,
	in *batchDeletePartitionInput,
) (*batchDeletePartitionOutput, error) {
	errs := h.Backend.BatchDeletePartition(in.DatabaseName, in.TableName, in.PartitionsToDelete)

	return &batchDeletePartitionOutput{Errors: errs}, nil
}

type batchDeleteTableInput struct {
	DatabaseName   string   `json:"DatabaseName"`
	TablesToDelete []string `json:"TablesToDelete"`
}

type batchDeleteTableOutput struct {
	Errors []TableError `json:"Errors"`
}

func (h *Handler) handleBatchDeleteTable(
	_ context.Context,
	in *batchDeleteTableInput,
) (*batchDeleteTableOutput, error) {
	errs := h.Backend.BatchDeleteTable(in.DatabaseName, in.TablesToDelete)

	return &batchDeleteTableOutput{Errors: errs}, nil
}

type batchDeleteTableVersionInput struct {
	DatabaseName string   `json:"DatabaseName"`
	TableName    string   `json:"TableName"`
	VersionIDs   []string `json:"VersionIds"`
}

type batchDeleteTableVersionOutput struct {
	Errors []TableVersionError `json:"Errors"`
}

func (h *Handler) handleBatchDeleteTableVersion(
	_ context.Context,
	in *batchDeleteTableVersionInput,
) (*batchDeleteTableVersionOutput, error) {
	errs := h.Backend.BatchDeleteTableVersion(in.DatabaseName, in.TableName, in.VersionIDs)

	return &batchDeleteTableVersionOutput{Errors: errs}, nil
}

// --- Batch connection handlers ---

type batchDeleteConnectionInput struct {
	ConnectionNameList []string `json:"ConnectionNameList"`
}

type batchDeleteConnectionOutput struct {
	Errors    []ErrorDetail `json:"Errors"`
	Succeeded []string      `json:"Succeeded"`
}

func (h *Handler) handleBatchDeleteConnection(
	_ context.Context,
	in *batchDeleteConnectionInput,
) (*batchDeleteConnectionOutput, error) {
	succeeded, errs := h.Backend.BatchDeleteConnection(in.ConnectionNameList)

	return &batchDeleteConnectionOutput{Succeeded: succeeded, Errors: errs}, nil
}

// --- Single connection handlers ---

type createConnectionInput struct {
	Tags            map[string]string `json:"Tags,omitempty"`
	ConnectionInput connectionInput   `json:"ConnectionInput"`
}

type connectionInput struct {
	ConnectionProperties map[string]string `json:"ConnectionProperties,omitempty"`
	Name                 string            `json:"Name"`
	ConnectionType       string            `json:"ConnectionType,omitempty"`
}

type createConnectionOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateConnection(
	_ context.Context,
	in *createConnectionInput,
) (*createConnectionOutput, error) {
	c, err := h.Backend.CreateConnection(
		in.ConnectionInput.Name,
		in.ConnectionInput.ConnectionType,
		in.ConnectionInput.ConnectionProperties,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &createConnectionOutput{Name: c.Name}, nil
}

type getConnectionInput struct {
	Name string `json:"Name"`
}

type getConnectionOutput struct {
	Connection *Connection `json:"Connection"`
}

func (h *Handler) handleGetConnection(
	_ context.Context,
	in *getConnectionInput,
) (*getConnectionOutput, error) {
	c, err := h.Backend.GetConnection(in.Name)
	if err != nil {
		return nil, err
	}

	return &getConnectionOutput{Connection: c}, nil
}

type getConnectionsInput struct{}

type getConnectionsOutput struct {
	ConnectionList []*Connection `json:"ConnectionList"`
}

func (h *Handler) handleGetConnections(
	_ context.Context,
	_ *getConnectionsInput,
) (*getConnectionsOutput, error) {
	conns := h.Backend.GetConnections()

	return &getConnectionsOutput{ConnectionList: conns}, nil
}

type deleteConnectionInput struct {
	ConnectionName string `json:"ConnectionName"`
}

func (h *Handler) handleDeleteConnection(
	_ context.Context,
	in *deleteConnectionInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteConnection(in.ConnectionName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- Batch blueprint handlers ---

type batchGetBlueprintsInput struct {
	Names []string `json:"Names"`
}

type batchGetBlueprintsOutput struct {
	Blueprints        []*Blueprint `json:"Blueprints"`
	MissingBlueprints []string     `json:"MissingBlueprints"`
}

func (h *Handler) handleBatchGetBlueprints(
	_ context.Context,
	in *batchGetBlueprintsInput,
) (*batchGetBlueprintsOutput, error) {
	found, missing := h.Backend.BatchGetBlueprints(in.Names)

	return &batchGetBlueprintsOutput{Blueprints: found, MissingBlueprints: missing}, nil
}

// --- Batch crawler handlers ---

type batchGetCrawlersInput struct {
	CrawlerNames []string `json:"CrawlerNames"`
}

type batchGetCrawlersOutput struct {
	Crawlers         []*Crawler `json:"Crawlers"`
	CrawlersNotFound []string   `json:"CrawlersNotFound"`
}

func (h *Handler) handleBatchGetCrawlers(
	_ context.Context,
	in *batchGetCrawlersInput,
) (*batchGetCrawlersOutput, error) {
	found, missing := h.Backend.BatchGetCrawlers(in.CrawlerNames)

	return &batchGetCrawlersOutput{Crawlers: found, CrawlersNotFound: missing}, nil
}

// --- ListCrawlers handler ---

type listCrawlersInput struct{}

type listCrawlersOutput struct {
	CrawlerNames []string `json:"CrawlerNames"`
}

func (h *Handler) handleListCrawlers(
	_ context.Context,
	_ *listCrawlersInput,
) (*listCrawlersOutput, error) {
	names := h.Backend.ListCrawlers()

	return &listCrawlersOutput{CrawlerNames: names}, nil
}

// --- Batch custom entity type handlers ---

type batchGetCustomEntityTypesInput struct {
	Names []string `json:"Names"`
}

type batchGetCustomEntityTypesOutput struct {
	CustomEntityTypes         []*CustomEntityType `json:"CustomEntityTypes"`
	CustomEntityTypesNotFound []string            `json:"CustomEntityTypesNotFound"`
}

func (h *Handler) handleBatchGetCustomEntityTypes(
	_ context.Context,
	in *batchGetCustomEntityTypesInput,
) (*batchGetCustomEntityTypesOutput, error) {
	found, missing := h.Backend.BatchGetCustomEntityTypes(in.Names)

	return &batchGetCustomEntityTypesOutput{CustomEntityTypes: found, CustomEntityTypesNotFound: missing}, nil
}

// --- Batch data quality handlers ---

type batchGetDataQualityResultInput struct {
	ResultIDs []string `json:"ResultIds"`
}

type batchGetDataQualityResultOutput struct {
	Results         []DataQualityResult `json:"Results"`
	ResultsNotFound []ErrorDetail       `json:"ResultsNotFound"`
}

func (h *Handler) handleBatchGetDataQualityResult(
	_ context.Context,
	in *batchGetDataQualityResultInput,
) (*batchGetDataQualityResultOutput, error) {
	found, errs := h.Backend.BatchGetDataQualityResult(in.ResultIDs)
	results := make([]DataQualityResult, 0, len(found))

	for _, r := range found {
		results = append(results, *r)
	}

	return &batchGetDataQualityResultOutput{Results: results, ResultsNotFound: errs}, nil
}

// --- Batch dev endpoint handlers ---

type batchGetDevEndpointsInput struct {
	DevEndpointNames []string `json:"DevEndpointNames"`
}

type batchGetDevEndpointsOutput struct {
	DevEndpoints         []*DevEndpoint `json:"DevEndpoints"`
	DevEndpointsNotFound []string       `json:"DevEndpointsNotFound"`
}

func (h *Handler) handleBatchGetDevEndpoints(
	_ context.Context,
	in *batchGetDevEndpointsInput,
) (*batchGetDevEndpointsOutput, error) {
	found, missing := h.Backend.BatchGetDevEndpoints(in.DevEndpointNames)

	return &batchGetDevEndpointsOutput{DevEndpoints: found, DevEndpointsNotFound: missing}, nil
}

// --- Job run handlers ---

type startJobRunInput struct {
	Arguments map[string]string `json:"Arguments,omitempty"`
	JobName   string            `json:"JobName"`
}

type startJobRunOutput struct {
	JobRunID string `json:"JobRunId"`
}

func (h *Handler) handleStartJobRun(_ context.Context, in *startJobRunInput) (*startJobRunOutput, error) {
	run, err := h.Backend.StartJobRun(in.JobName, in.Arguments)
	if err != nil {
		return nil, err
	}

	return &startJobRunOutput{JobRunID: run.ID}, nil
}

type getJobRunInput struct {
	JobName string `json:"JobName"`
	RunID   string `json:"RunId"`
}

type getJobRunOutput struct {
	JobRun *JobRun `json:"JobRun"`
}

func (h *Handler) handleGetJobRun(_ context.Context, in *getJobRunInput) (*getJobRunOutput, error) {
	run, err := h.Backend.GetJobRun(in.JobName, in.RunID)
	if err != nil {
		return nil, err
	}

	return &getJobRunOutput{JobRun: run}, nil
}

type getJobRunsInput struct {
	JobName string `json:"JobName"`
}

type getJobRunsOutput struct {
	JobRuns []*JobRun `json:"JobRuns"`
}

func (h *Handler) handleGetJobRuns(_ context.Context, in *getJobRunsInput) (*getJobRunsOutput, error) {
	runs, err := h.Backend.GetJobRuns(in.JobName)
	if err != nil {
		return nil, err
	}

	return &getJobRunsOutput{JobRuns: runs}, nil
}

type batchStopJobRunInput struct {
	JobName   string   `json:"JobName"`
	JobRunIDs []string `json:"JobRunIds"`
}

type batchStopJobRunOutput struct {
	Errors []BatchStopJobRunError `json:"Errors"`
}

func (h *Handler) handleBatchStopJobRun(_ context.Context, in *batchStopJobRunInput) (*batchStopJobRunOutput, error) {
	errs := h.Backend.BatchStopJobRun(in.JobName, in.JobRunIDs)

	return &batchStopJobRunOutput{Errors: errs}, nil
}

type getJobBookmarkInput struct {
	JobName string `json:"JobName"`
}

type getJobBookmarkOutput struct {
	JobBookmarkEntry *JobBookmark `json:"JobBookmarkEntry"`
}

func (h *Handler) handleGetJobBookmark(_ context.Context, in *getJobBookmarkInput) (*getJobBookmarkOutput, error) {
	bm, err := h.Backend.GetJobBookmark(in.JobName)
	if err != nil {
		return nil, err
	}

	return &getJobBookmarkOutput{JobBookmarkEntry: bm}, nil
}

type resetJobBookmarkInput struct {
	JobName string `json:"JobName"`
}

type resetJobBookmarkOutput struct {
	JobBookmarkEntry *JobBookmark `json:"JobBookmarkEntry"`
}

func (h *Handler) handleResetJobBookmark(
	_ context.Context,
	in *resetJobBookmarkInput,
) (*resetJobBookmarkOutput, error) {
	bm, err := h.Backend.ResetJobBookmarkWithResult(in.JobName)
	if err != nil {
		return nil, err
	}

	return &resetJobBookmarkOutput{JobBookmarkEntry: bm}, nil
}

// --- Crawler scheduling handlers ---

type startCrawlerInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleStartCrawler(_ context.Context, in *startCrawlerInput) (*emptyOutput, error) {
	if err := h.Backend.StartCrawler(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type stopCrawlerInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleStopCrawler(_ context.Context, in *stopCrawlerInput) (*emptyOutput, error) {
	if err := h.Backend.StopCrawler(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type updateCrawlerScheduleInput struct {
	CrawlerName string `json:"CrawlerName"`
	Schedule    string `json:"Schedule"`
}

func (h *Handler) handleUpdateCrawlerSchedule(_ context.Context, in *updateCrawlerScheduleInput) (*emptyOutput, error) {
	if err := h.Backend.UpdateCrawlerSchedule(in.CrawlerName, in.Schedule); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type startCrawlerScheduleInput struct {
	CrawlerName string `json:"CrawlerName"`
}

func (h *Handler) handleStartCrawlerSchedule(_ context.Context, in *startCrawlerScheduleInput) (*emptyOutput, error) {
	if err := h.Backend.StartCrawlerSchedule(in.CrawlerName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type stopCrawlerScheduleInput struct {
	CrawlerName string `json:"CrawlerName"`
}

func (h *Handler) handleStopCrawlerSchedule(_ context.Context, in *stopCrawlerScheduleInput) (*emptyOutput, error) {
	if err := h.Backend.StopCrawlerSchedule(in.CrawlerName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- Data quality ruleset handlers ---

type createDataQualityRulesetInput struct {
	Tags    map[string]string `json:"Tags,omitempty"`
	Name    string            `json:"Name"`
	Ruleset string            `json:"Ruleset,omitempty"`
}

type createDataQualityRulesetOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateDataQualityRuleset(
	_ context.Context,
	in *createDataQualityRulesetInput,
) (*createDataQualityRulesetOutput, error) {
	r, err := h.Backend.CreateDataQualityRuleset(in.Name, in.Ruleset, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createDataQualityRulesetOutput{Name: r.Name}, nil
}

type getDataQualityRulesetInput struct {
	Name string `json:"Name"`
}

type getDataQualityRulesetOutput struct {
	Name           string  `json:"Name"`
	Ruleset        string  `json:"Ruleset,omitempty"`
	Description    string  `json:"Description,omitempty"`
	ARN            string  `json:"Arn,omitempty"`
	CreatedOn      float64 `json:"CreatedOn,omitempty"`
	LastModifiedOn float64 `json:"LastModifiedOn,omitempty"`
}

func (h *Handler) handleGetDataQualityRuleset(
	_ context.Context,
	in *getDataQualityRulesetInput,
) (*getDataQualityRulesetOutput, error) {
	r, err := h.Backend.GetDataQualityRuleset(in.Name)
	if err != nil {
		return nil, err
	}

	return &getDataQualityRulesetOutput{
		Name:           r.Name,
		Ruleset:        r.Ruleset,
		Description:    r.Description,
		ARN:            r.ARN,
		CreatedOn:      r.CreatedOn,
		LastModifiedOn: r.LastModifiedOn,
	}, nil
}

type deleteDataQualityRulesetInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteDataQualityRuleset(
	_ context.Context,
	in *deleteDataQualityRulesetInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteDataQualityRuleset(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type updateDataQualityRulesetInput struct {
	Name    string `json:"Name"`
	Ruleset string `json:"Ruleset,omitempty"`
}

func (h *Handler) handleUpdateDataQualityRuleset(
	_ context.Context,
	in *updateDataQualityRulesetInput,
) (*emptyOutput, error) {
	if err := h.Backend.UpdateDataQualityRuleset(in.Name, in.Ruleset); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type listDataQualityRulesetsInput struct{}

type listDataQualityRulesetsOutput struct {
	Rulesets []*DataQualityRuleset `json:"Rulesets"`
}

func (h *Handler) handleListDataQualityRulesets(
	_ context.Context,
	_ *listDataQualityRulesetsInput,
) (*listDataQualityRulesetsOutput, error) {
	rulesets := h.Backend.ListDataQualityRulesets()

	return &listDataQualityRulesetsOutput{Rulesets: rulesets}, nil
}

type startDataQualityRulesetEvaluationRunInput struct {
	RulesetNames []string `json:"RulesetNames"`
}

type startDataQualityRulesetEvaluationRunOutput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStartDataQualityRulesetEvaluationRun(
	_ context.Context,
	in *startDataQualityRulesetEvaluationRunInput,
) (*startDataQualityRulesetEvaluationRunOutput, error) {
	run, err := h.Backend.StartDataQualityRulesetEvaluationRun(in.RulesetNames)
	if err != nil {
		return nil, err
	}

	return &startDataQualityRulesetEvaluationRunOutput{RunID: run.RunID}, nil
}

type getDataQualityRulesetEvaluationRunInput struct {
	RunID string `json:"RunId"`
}

type getDataQualityRulesetEvaluationRunOutput struct {
	DataQualityEvaluationRun *DataQualityEvaluationRun `json:"DataQualityEvaluationRun"`
}

func (h *Handler) handleGetDataQualityRulesetEvaluationRun(
	_ context.Context,
	in *getDataQualityRulesetEvaluationRunInput,
) (*getDataQualityRulesetEvaluationRunOutput, error) {
	run, err := h.Backend.GetDataQualityRulesetEvaluationRun(in.RunID)
	if err != nil {
		return nil, err
	}

	return &getDataQualityRulesetEvaluationRunOutput{DataQualityEvaluationRun: run}, nil
}

type cancelDataQualityRulesetEvaluationRunInput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleCancelDataQualityRulesetEvaluationRun(
	_ context.Context,
	in *cancelDataQualityRulesetEvaluationRunInput,
) (*emptyOutput, error) {
	if err := h.Backend.CancelDataQualityRulesetEvaluationRun(in.RunID); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
