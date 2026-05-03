package timestreamwrite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	targetPrefix    = "Timestream_20181101."
	keyTypeField    = "__type"
	keyMessageField = "message"
)

const endpointCachePeriodMinutes = 60

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for Amazon Timestream Write operations.
type Handler struct {
	Backend      *InMemoryBackend
	ops          map[string]service.JSONOpFunc
	supportedOps map[string]bool
}

// NewHandler creates a new Timestream Write handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()
	supported := h.GetSupportedOperations()
	h.supportedOps = make(map[string]bool, len(supported))

	for _, op := range supported {
		h.supportedOps[op] = true
	}

	return h
}

// Reset clears the backend state and rebuilds the dispatch table.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// buildOps constructs the static dispatch table for JSON operations.
func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateBatchLoadTask":   service.WrapOp(h.handleCreateBatchLoadTask),
		"CreateDatabase":        service.WrapOp(h.handleCreateDatabase),
		"CreateTable":           service.WrapOp(h.handleCreateTable),
		"DeleteDatabase":        service.WrapOp(h.handleDeleteDatabase),
		"DeleteTable":           service.WrapOp(h.handleDeleteTable),
		"DescribeBatchLoadTask": service.WrapOp(h.handleDescribeBatchLoadTask),
		"DescribeDatabase":      service.WrapOp(h.handleDescribeDatabase),
		"DescribeEndpoints":     service.WrapOp(h.handleDescribeEndpoints),
		"DescribeTable":         service.WrapOp(h.handleDescribeTable),
		"ListBatchLoadTasks":    service.WrapOp(h.handleListBatchLoadTasks),
		"ListDatabases":         service.WrapOp(h.handleListDatabases),
		"ListTables":            service.WrapOp(h.handleListTables),
		"ListTagsForResource":   service.WrapOp(h.handleListTagsForResource),
		"ResumeBatchLoadTask":   service.WrapOp(h.handleResumeBatchLoadTask),
		"TagResource":           service.WrapOp(h.handleTagResource),
		"UntagResource":         service.WrapOp(h.handleUntagResource),
		"UpdateDatabase":        service.WrapOp(h.handleUpdateDatabase),
		"UpdateTable":           service.WrapOp(h.handleUpdateTable),
		"WriteRecords":          service.WrapOp(h.handleWriteRecords),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "TimestreamWrite" }

// StartWorker starts the background janitor for Timestream record retention.
func (h *Handler) StartWorker(ctx context.Context) error {
	janitor := NewJanitor(h.Backend)
	go janitor.Run(ctx)

	return nil
}

// GetSupportedOperations returns the list of supported Timestream Write operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateBatchLoadTask",
		"CreateDatabase",
		"CreateTable",
		"DeleteDatabase",
		"DeleteTable",
		"DescribeBatchLoadTask",
		"DescribeDatabase",
		"DescribeEndpoints",
		"DescribeTable",
		"ListBatchLoadTasks",
		"ListDatabases",
		"ListTables",
		"ListTagsForResource",
		"ResumeBatchLoadTask",
		"TagResource",
		"UntagResource",
		"UpdateDatabase",
		"UpdateTable",
		"WriteRecords",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "timestreamwrite" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler covers.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches Timestream Write requests.
// It only matches operations explicitly supported by this handler to avoid
// intercepting operations belonging to other Timestream services (e.g. TimestreamQuery)
// that share the same X-Amz-Target prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")
		if !strings.HasPrefix(target, targetPrefix) {
			return false
		}

		operation := strings.TrimPrefix(target, targetPrefix)

		return h.supportedOps[operation]
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Timestream Write action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, targetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource returns an empty string (no meaningful resource in request body for routing).
func (h *Handler) ExtractResource(_ *echo.Context) string { return "" }

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"TimestreamWrite", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
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
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ResourceNotFoundException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrConflict):
		return c.JSON(http.StatusConflict, map[string]string{
			keyTypeField:    "ConflictException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrInvalidParameter) ||
		errors.Is(err, errInvalidRequest) ||
		errors.Is(err, errUnknownAction) ||
		errors.As(err, &syntaxErr) ||
		errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ValidationException",
			keyMessageField: err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			keyTypeField:    "InternalServerError",
			keyMessageField: err.Error(),
		})
	}
}

// --- input/output types ---

type createDatabaseInput struct {
	DatabaseName string     `json:"DatabaseName"`
	Tags         []tagInput `json:"Tags"`
}

type databaseOutput struct {
	Database databaseView `json:"Database"`
}

type databaseView struct {
	Arn             string  `json:"Arn"`
	DatabaseName    string  `json:"DatabaseName"`
	KmsKeyID        string  `json:"KmsKeyId,omitempty"`
	CreationTime    float64 `json:"CreationTime"`
	LastUpdatedTime float64 `json:"LastUpdatedTime"`
	TableCount      int     `json:"TableCount"`
}

type listDatabasesInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listDatabasesOutput struct {
	Databases []databaseView `json:"Databases"`
}

type databaseNameInput struct {
	DatabaseName string `json:"DatabaseName"`
}

type updateDatabaseInput struct {
	DatabaseName string `json:"DatabaseName"`
	KmsKeyID     string `json:"KmsKeyId"`
}

type createTableInput struct {
	RetentionProperties          *retentionPropertiesInput     `json:"RetentionProperties,omitempty"`
	MagneticStoreWriteProperties *magneticStoreWritePropsInput `json:"MagneticStoreWriteProperties,omitempty"`
	DatabaseName                 string                        `json:"DatabaseName"`
	TableName                    string                        `json:"TableName"`
	Tags                         []tagInput                    `json:"Tags"`
}

type retentionPropertiesInput struct {
	MemoryStoreRetentionPeriodInHours  int64 `json:"MemoryStoreRetentionPeriodInHours"`
	MagneticStoreRetentionPeriodInDays int64 `json:"MagneticStoreRetentionPeriodInDays"`
}

type magneticStoreWritePropsInput struct {
	EnableMagneticStoreWrites bool `json:"EnableMagneticStoreWrites"`
}

type tableInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

type updateTableInput struct {
	RetentionProperties          *retentionPropertiesInput     `json:"RetentionProperties,omitempty"`
	MagneticStoreWriteProperties *magneticStoreWritePropsInput `json:"MagneticStoreWriteProperties,omitempty"`
	DatabaseName                 string                        `json:"DatabaseName"`
	TableName                    string                        `json:"TableName"`
}

type tableOutput struct {
	Table tableView `json:"Table"`
}

type tableView struct {
	RetentionProperties          *retentionPropertiesInput     `json:"RetentionProperties,omitempty"`
	MagneticStoreWriteProperties *magneticStoreWritePropsInput `json:"MagneticStoreWriteProperties,omitempty"`
	Arn                          string                        `json:"Arn"`
	DatabaseName                 string                        `json:"DatabaseName"`
	TableName                    string                        `json:"TableName"`
	TableStatus                  string                        `json:"TableStatus"`
	CreationTime                 float64                       `json:"CreationTime"`
	LastUpdatedTime              float64                       `json:"LastUpdatedTime"`
}

type listTablesInput struct {
	DatabaseName string `json:"DatabaseName"`
	NextToken    string `json:"NextToken"`
	MaxResults   int    `json:"MaxResults"`
}

type listTablesOutput struct {
	Tables []tableView `json:"Tables"`
}

type writeRecordsInput struct {
	DatabaseName string        `json:"DatabaseName"`
	TableName    string        `json:"TableName"`
	Records      []recordInput `json:"Records"`
}

type recordInput struct {
	MeasureName      string           `json:"MeasureName"`
	MeasureValue     string           `json:"MeasureValue"`
	MeasureValueType string           `json:"MeasureValueType"`
	Time             string           `json:"Time"`
	TimeUnit         string           `json:"TimeUnit"`
	Dimensions       []dimensionInput `json:"Dimensions"`
	Version          int64            `json:"Version"`
}

type dimensionInput struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

type writeRecordsOutput struct {
	RecordsIngested struct {
		Total         int32 `json:"Total"`
		MemoryStore   int32 `json:"MemoryStore"`
		MagneticStore int32 `json:"MagneticStore"`
	} `json:"RecordsIngested"`
}

type tagResourceInput struct {
	ResourceARN string     `json:"ResourceARN"`
	Tags        []tagInput `json:"Tags"`
}

type tagInput struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

type listTagsInput struct {
	ResourceARN string `json:"ResourceARN"`
}

type listTagsOutput struct {
	Tags []tagInput `json:"Tags"`
}

type describeEndpointsInput struct{}

type endpointOutput struct {
	Endpoints []endpointEntry `json:"Endpoints"`
}

type endpointEntry struct {
	Address              string `json:"Address"`
	CachePeriodInMinutes int64  `json:"CachePeriodInMinutes"`
}

type emptyOutput struct{}

// --- handlers ---

func toDatabaseView(db *Database) databaseView {
	return databaseView{
		Arn:             db.ARN,
		CreationTime:    float64(db.CreationTime.Unix()),
		DatabaseName:    db.DatabaseName,
		KmsKeyID:        db.KmsKeyID,
		LastUpdatedTime: float64(db.LastUpdatedTime.Unix()),
		TableCount:      db.TableCount,
	}
}

func toTableView(tbl *Table) tableView {
	v := tableView{
		Arn:             tbl.ARN,
		CreationTime:    float64(tbl.CreationTime.Unix()),
		DatabaseName:    tbl.DatabaseName,
		LastUpdatedTime: float64(tbl.LastUpdatedTime.Unix()),
		TableName:       tbl.TableName,
		TableStatus:     tbl.TableStatus,
	}

	if tbl.RetentionProperties != nil {
		v.RetentionProperties = &retentionPropertiesInput{
			MemoryStoreRetentionPeriodInHours:  tbl.RetentionProperties.MemoryStoreRetentionPeriodInHours,
			MagneticStoreRetentionPeriodInDays: tbl.RetentionProperties.MagneticStoreRetentionPeriodInDays,
		}
	}

	if tbl.MagneticStoreWriteProperties != nil {
		v.MagneticStoreWriteProperties = &magneticStoreWritePropsInput{
			EnableMagneticStoreWrites: tbl.MagneticStoreWriteProperties.EnableMagneticStoreWrites,
		}
	}

	return v
}

func (h *Handler) handleCreateDatabase(
	_ context.Context,
	in *createDatabaseInput,
) (*databaseOutput, error) {
	if in.DatabaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	db, err := h.Backend.CreateDatabase(in.DatabaseName, tags)
	if err != nil {
		return nil, err
	}

	return &databaseOutput{Database: toDatabaseView(db)}, nil
}

func (h *Handler) handleDescribeDatabase(
	_ context.Context,
	in *databaseNameInput,
) (*databaseOutput, error) {
	if in.DatabaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", errInvalidRequest)
	}

	db, err := h.Backend.DescribeDatabase(in.DatabaseName)
	if err != nil {
		return nil, err
	}

	return &databaseOutput{Database: toDatabaseView(db)}, nil
}

func (h *Handler) handleListDatabases(
	_ context.Context,
	_ *listDatabasesInput,
) (*listDatabasesOutput, error) {
	dbs := h.Backend.ListDatabases()
	views := make([]databaseView, 0, len(dbs))

	for i := range dbs {
		views = append(views, toDatabaseView(&dbs[i]))
	}

	return &listDatabasesOutput{Databases: views}, nil
}

func (h *Handler) handleDeleteDatabase(
	_ context.Context,
	in *databaseNameInput,
) (*emptyOutput, error) {
	if in.DatabaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteDatabase(in.DatabaseName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

func (h *Handler) handleUpdateDatabase(
	_ context.Context,
	in *updateDatabaseInput,
) (*databaseOutput, error) {
	if in.DatabaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", errInvalidRequest)
	}

	db, err := h.Backend.UpdateDatabase(in.DatabaseName, in.KmsKeyID)
	if err != nil {
		return nil, err
	}

	return &databaseOutput{Database: toDatabaseView(db)}, nil
}

func (h *Handler) handleCreateTable(
	_ context.Context,
	in *createTableInput,
) (*tableOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	var inp *CreateTableInput

	if in.RetentionProperties != nil || in.MagneticStoreWriteProperties != nil {
		inp = &CreateTableInput{}

		if in.RetentionProperties != nil {
			inp.RetentionProperties = &RetentionProperties{
				MemoryStoreRetentionPeriodInHours:  in.RetentionProperties.MemoryStoreRetentionPeriodInHours,
				MagneticStoreRetentionPeriodInDays: in.RetentionProperties.MagneticStoreRetentionPeriodInDays,
			}
		}

		if in.MagneticStoreWriteProperties != nil {
			inp.MagneticStoreWriteProperties = &MagneticStoreWriteProperties{
				EnableMagneticStoreWrites: in.MagneticStoreWriteProperties.EnableMagneticStoreWrites,
			}
		}
	}

	tbl, err := h.Backend.CreateTable(in.DatabaseName, in.TableName, tags, inp)
	if err != nil {
		return nil, err
	}

	return &tableOutput{Table: toTableView(tbl)}, nil
}

func (h *Handler) handleDescribeTable(
	_ context.Context,
	in *tableInput,
) (*tableOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", errInvalidRequest)
	}

	tbl, err := h.Backend.DescribeTable(in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}

	return &tableOutput{Table: toTableView(tbl)}, nil
}

func (h *Handler) handleListTables(
	_ context.Context,
	in *listTablesInput,
) (*listTablesOutput, error) {
	if in.DatabaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", errInvalidRequest)
	}

	tbls, err := h.Backend.ListTables(in.DatabaseName)
	if err != nil {
		return nil, err
	}

	views := make([]tableView, 0, len(tbls))
	for i := range tbls {
		views = append(views, toTableView(&tbls[i]))
	}

	return &listTablesOutput{Tables: views}, nil
}

func (h *Handler) handleDeleteTable(
	_ context.Context,
	in *tableInput,
) (*emptyOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", errInvalidRequest)
	}

	if err := h.Backend.DeleteTable(in.DatabaseName, in.TableName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

func (h *Handler) handleUpdateTable(
	_ context.Context,
	in *updateTableInput,
) (*tableOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", errInvalidRequest)
	}

	var inp *UpdateTableInput

	if in.RetentionProperties != nil || in.MagneticStoreWriteProperties != nil {
		inp = &UpdateTableInput{}

		if in.RetentionProperties != nil {
			inp.RetentionProperties = &RetentionProperties{
				MemoryStoreRetentionPeriodInHours:  in.RetentionProperties.MemoryStoreRetentionPeriodInHours,
				MagneticStoreRetentionPeriodInDays: in.RetentionProperties.MagneticStoreRetentionPeriodInDays,
			}
		}

		if in.MagneticStoreWriteProperties != nil {
			inp.MagneticStoreWriteProperties = &MagneticStoreWriteProperties{
				EnableMagneticStoreWrites: in.MagneticStoreWriteProperties.EnableMagneticStoreWrites,
			}
		}
	}

	tbl, err := h.Backend.UpdateTable(in.DatabaseName, in.TableName, inp)
	if err != nil {
		return nil, err
	}

	return &tableOutput{Table: toTableView(tbl)}, nil
}

func (h *Handler) handleWriteRecords(
	_ context.Context,
	in *writeRecordsInput,
) (*writeRecordsOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", errInvalidRequest)
	}

	records := make([]Record, 0, len(in.Records))

	for _, r := range in.Records {
		dims := make([]Dimension, 0, len(r.Dimensions))
		for _, d := range r.Dimensions {
			dims = append(dims, Dimension(d))
		}

		records = append(records, Record{
			Dimensions:       dims,
			MeasureName:      r.MeasureName,
			MeasureValue:     r.MeasureValue,
			MeasureValueType: r.MeasureValueType,
			Time:             r.Time,
			TimeUnit:         r.TimeUnit,
			Version:          r.Version,
		})
	}

	result, err := h.Backend.WriteRecords(in.DatabaseName, in.TableName, records)
	if err != nil {
		return nil, err
	}

	out := &writeRecordsOutput{}
	out.RecordsIngested.Total = result.Total
	out.RecordsIngested.MemoryStore = result.MemoryStore

	return out, nil
}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*emptyOutput, error) {
	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tags := make(map[string]string, len(in.Tags))
	for _, t := range in.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(in.ResourceARN, tags); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*emptyOutput, error) {
	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsInput,
) (*listTagsOutput, error) {
	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tagsMap := h.Backend.ListTagsForResource(in.ResourceARN)
	tags := make([]tagInput, 0, len(tagsMap))

	for k, v := range tagsMap {
		tags = append(tags, tagInput{Key: k, Value: v})
	}

	sort.Slice(tags, func(i, j int) bool { return tags[i].Key < tags[j].Key })

	return &listTagsOutput{Tags: tags}, nil
}

func (h *Handler) handleDescribeEndpoints(
	_ context.Context,
	_ *describeEndpointsInput,
) (*endpointOutput, error) {
	return &endpointOutput{
		Endpoints: []endpointEntry{
			{Address: "localhost", CachePeriodInMinutes: endpointCachePeriodMinutes},
		},
	}, nil
}

// --- batch load task types ---

type dataSourceS3ConfigInput struct {
	BucketName      string `json:"BucketName"`
	ObjectKeyPrefix string `json:"ObjectKeyPrefix,omitempty"`
	DataFormat      string `json:"DataFormat,omitempty"`
}

type dataSourceConfigInput struct {
	DataSourceS3Configuration *dataSourceS3ConfigInput `json:"DataSourceS3Configuration,omitempty"`
	DataFormat                string                   `json:"DataFormat,omitempty"`
}

type reportConfigInput struct {
	ReportS3Configuration *dataSourceS3ConfigInput `json:"ReportS3Configuration,omitempty"`
}

type createBatchLoadTaskInput struct {
	DataSourceConfiguration *dataSourceConfigInput `json:"DataSourceConfiguration,omitempty"`
	ReportConfiguration     *reportConfigInput     `json:"ReportConfiguration,omitempty"`
	TargetDatabaseName      string                 `json:"TargetDatabaseName"`
	TargetTableName         string                 `json:"TargetTableName"`
	ClientToken             string                 `json:"ClientToken"`
}

type createBatchLoadTaskOutput struct {
	TaskID string `json:"TaskId"`
}

type batchLoadTaskIDInput struct {
	TaskID string `json:"TaskId"`
}

type batchLoadTaskDescriptionView struct {
	ResumableUntil          *float64               `json:"ResumableUntil,omitempty"`
	DataSourceConfiguration *dataSourceConfigInput `json:"DataSourceConfiguration,omitempty"`
	ReportConfiguration     *reportConfigInput     `json:"ReportConfiguration,omitempty"`
	TaskID                  string                 `json:"TaskId"`
	TargetDatabaseName      string                 `json:"TargetDatabaseName"`
	TargetTableName         string                 `json:"TargetTableName"`
	TaskStatus              string                 `json:"TaskStatus"`
	ErrorMessage            string                 `json:"ErrorMessage,omitempty"`
	CreationTime            float64                `json:"CreationTime"`
	LastUpdatedTime         float64                `json:"LastUpdatedTime"`
	RecordVersion           int64                  `json:"RecordVersion,omitempty"`
}

type describeBatchLoadTaskOutput struct {
	BatchLoadTaskDescription batchLoadTaskDescriptionView `json:"BatchLoadTaskDescription"`
}

type listBatchLoadTasksInput struct {
	NextToken  string `json:"NextToken"`
	TaskStatus string `json:"TaskStatus"`
	MaxResults int    `json:"MaxResults"`
}

type batchLoadTaskSummaryView struct {
	ResumableUntil  *float64 `json:"ResumableUntil,omitempty"`
	TaskID          string   `json:"TaskId"`
	DatabaseName    string   `json:"DatabaseName"`
	TableName       string   `json:"TableName"`
	TaskStatus      string   `json:"TaskStatus"`
	CreationTime    float64  `json:"CreationTime"`
	LastUpdatedTime float64  `json:"LastUpdatedTime"`
}

type listBatchLoadTasksOutput struct {
	BatchLoadTasks []batchLoadTaskSummaryView `json:"BatchLoadTasks"`
}

func toBatchLoadTaskDescriptionView(task *BatchLoadTask) batchLoadTaskDescriptionView {
	v := batchLoadTaskDescriptionView{
		TaskID:             task.TaskID,
		TargetDatabaseName: task.TargetDatabaseName,
		TargetTableName:    task.TargetTableName,
		TaskStatus:         task.TaskStatus,
		CreationTime:       float64(task.CreationTime.Unix()),
		LastUpdatedTime:    float64(task.LastUpdatedTime.Unix()),
		ErrorMessage:       task.ErrorMessage,
		RecordVersion:      task.RecordVersion,
	}

	if task.ResumableUntil != nil {
		ts := float64(task.ResumableUntil.Unix())
		v.ResumableUntil = &ts
	}

	if task.DataSourceConfiguration != nil {
		cfg := &dataSourceConfigInput{
			DataFormat: task.DataSourceConfiguration.DataFormat,
		}

		if task.DataSourceConfiguration.DataSourceS3Configuration != nil {
			s3 := task.DataSourceConfiguration.DataSourceS3Configuration
			cfg.DataSourceS3Configuration = &dataSourceS3ConfigInput{
				BucketName:      s3.BucketName,
				ObjectKeyPrefix: s3.ObjectKeyPrefix,
				DataFormat:      s3.DataFormat,
			}
		}

		v.DataSourceConfiguration = cfg
	}

	if task.ReportConfiguration != nil {
		rpt := &reportConfigInput{}

		if task.ReportConfiguration.ReportS3Configuration != nil {
			s3 := task.ReportConfiguration.ReportS3Configuration
			rpt.ReportS3Configuration = &dataSourceS3ConfigInput{
				BucketName:      s3.BucketName,
				ObjectKeyPrefix: s3.ObjectKeyPrefix,
			}
		}

		v.ReportConfiguration = rpt
	}

	return v
}

func toBatchLoadTaskSummaryView(task *BatchLoadTask) batchLoadTaskSummaryView {
	v := batchLoadTaskSummaryView{
		TaskID:          task.TaskID,
		DatabaseName:    task.TargetDatabaseName,
		TableName:       task.TargetTableName,
		TaskStatus:      task.TaskStatus,
		CreationTime:    float64(task.CreationTime.Unix()),
		LastUpdatedTime: float64(task.LastUpdatedTime.Unix()),
	}

	if task.ResumableUntil != nil {
		ts := float64(task.ResumableUntil.Unix())
		v.ResumableUntil = &ts
	}

	return v
}

// tagsFromInput converts a slice of tagInput to a map[string]string.
func tagsFromInput(tags []tagInput) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	result := make(map[string]string, len(tags))

	for _, t := range tags {
		result[t.Key] = t.Value
	}

	return result
}

func (h *Handler) handleCreateBatchLoadTask(
	_ context.Context,
	in *createBatchLoadTaskInput,
) (*createBatchLoadTaskOutput, error) {
	if in.TargetDatabaseName == "" || in.TargetTableName == "" {
		return nil, fmt.Errorf("%w: TargetDatabaseName and TargetTableName are required", errInvalidRequest)
	}

	var dataSourceCfg *DataSourceConfiguration

	if in.DataSourceConfiguration != nil {
		dataSourceCfg = &DataSourceConfiguration{
			DataFormat: in.DataSourceConfiguration.DataFormat,
		}

		if in.DataSourceConfiguration.DataSourceS3Configuration != nil {
			s3 := in.DataSourceConfiguration.DataSourceS3Configuration
			dataSourceCfg.DataSourceS3Configuration = &DataSourceS3Configuration{
				BucketName:      s3.BucketName,
				ObjectKeyPrefix: s3.ObjectKeyPrefix,
				DataFormat:      s3.DataFormat,
			}
		}
	}

	var reportCfg *ReportConfiguration

	if in.ReportConfiguration != nil {
		reportCfg = &ReportConfiguration{}

		if in.ReportConfiguration.ReportS3Configuration != nil {
			s3 := in.ReportConfiguration.ReportS3Configuration
			reportCfg.ReportS3Configuration = &DataSourceS3Configuration{
				BucketName:      s3.BucketName,
				ObjectKeyPrefix: s3.ObjectKeyPrefix,
			}
		}
	}

	task, err := h.Backend.CreateBatchLoadTask(in.TargetDatabaseName, in.TargetTableName, dataSourceCfg, reportCfg)
	if err != nil {
		return nil, err
	}

	return &createBatchLoadTaskOutput{TaskID: task.TaskID}, nil
}

func (h *Handler) handleDescribeBatchLoadTask(
	_ context.Context,
	in *batchLoadTaskIDInput,
) (*describeBatchLoadTaskOutput, error) {
	if in.TaskID == "" {
		return nil, fmt.Errorf("%w: TaskId is required", errInvalidRequest)
	}

	task, err := h.Backend.DescribeBatchLoadTask(in.TaskID)
	if err != nil {
		return nil, err
	}

	return &describeBatchLoadTaskOutput{
		BatchLoadTaskDescription: toBatchLoadTaskDescriptionView(task),
	}, nil
}

func (h *Handler) handleListBatchLoadTasks(
	_ context.Context,
	in *listBatchLoadTasksInput,
) (*listBatchLoadTasksOutput, error) {
	tasks := h.Backend.ListBatchLoadTasks(in.TaskStatus)
	views := make([]batchLoadTaskSummaryView, 0, len(tasks))

	for i := range tasks {
		views = append(views, toBatchLoadTaskSummaryView(&tasks[i]))
	}

	return &listBatchLoadTasksOutput{BatchLoadTasks: views}, nil
}

func (h *Handler) handleResumeBatchLoadTask(
	_ context.Context,
	in *batchLoadTaskIDInput,
) (*emptyOutput, error) {
	if in.TaskID == "" {
		return nil, fmt.Errorf("%w: TaskId is required", errInvalidRequest)
	}

	if err := h.Backend.ResumeBatchLoadTask(in.TaskID); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
