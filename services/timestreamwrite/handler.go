package timestreamwrite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const targetPrefix = "Timestream_20181101."

const endpointCachePeriodMinutes = 60

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for Amazon Timestream Write operations.
type Handler struct {
	Backend      *InMemoryBackend
	supportedOps map[string]bool
}

// NewHandler creates a new Timestream Write handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	ops := h.GetSupportedOperations()
	h.supportedOps = make(map[string]bool, len(ops))
	for _, op := range ops {
		h.supportedOps[op] = true
	}

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "TimestreamWrite" }

// GetSupportedOperations returns the list of supported Timestream Write operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateDatabase",
		"DescribeDatabase",
		"ListDatabases",
		"DeleteDatabase",
		"UpdateDatabase",
		"CreateTable",
		"DescribeTable",
		"ListTables",
		"DeleteTable",
		"UpdateTable",
		"WriteRecords",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"DescribeEndpoints",
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

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateDatabase":      service.WrapOp(h.handleCreateDatabase),
		"DescribeDatabase":    service.WrapOp(h.handleDescribeDatabase),
		"ListDatabases":       service.WrapOp(h.handleListDatabases),
		"DeleteDatabase":      service.WrapOp(h.handleDeleteDatabase),
		"UpdateDatabase":      service.WrapOp(h.handleUpdateDatabase),
		"CreateTable":         service.WrapOp(h.handleCreateTable),
		"DescribeTable":       service.WrapOp(h.handleDescribeTable),
		"ListTables":          service.WrapOp(h.handleListTables),
		"DeleteTable":         service.WrapOp(h.handleDeleteTable),
		"UpdateTable":         service.WrapOp(h.handleUpdateTable),
		"WriteRecords":        service.WrapOp(h.handleWriteRecords),
		"TagResource":         service.WrapOp(h.handleTagResource),
		"UntagResource":       service.WrapOp(h.handleUntagResource),
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
		"DescribeEndpoints":   service.WrapOp(h.handleDescribeEndpoints),
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
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, map[string]string{
			"__type":  "ResourceNotFoundException",
			"message": err.Error(),
		})
	case errors.Is(err, awserr.ErrConflict):
		return c.JSON(http.StatusConflict, map[string]string{
			"__type":  "ConflictException",
			"message": err.Error(),
		})
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			"__type":  "ValidationException",
			"message": err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"__type":  "InternalServerError",
			"message": err.Error(),
		})
	}
}

// --- input/output types ---

type databaseNameInput struct {
	DatabaseName string `json:"DatabaseName"`
}

type databaseOutput struct {
	Database databaseView `json:"Database"`
}

type databaseView struct {
	Arn             string `json:"Arn"`
	CreationTime    string `json:"CreationTime"`
	DatabaseName    string `json:"DatabaseName"`
	KmsKeyID        string `json:"KmsKeyId,omitempty"`
	LastUpdatedTime string `json:"LastUpdatedTime"`
	TableCount      int    `json:"TableCount"`
}

type listDatabasesInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listDatabasesOutput struct {
	Databases []databaseView `json:"Databases"`
}

type updateDatabaseInput struct {
	DatabaseName string `json:"DatabaseName"`
	KmsKeyID     string `json:"KmsKeyId"`
}

type tableInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

type tableOutput struct {
	Table tableView `json:"Table"`
}

type tableView struct {
	Arn             string `json:"Arn"`
	CreationTime    string `json:"CreationTime"`
	DatabaseName    string `json:"DatabaseName"`
	LastUpdatedTime string `json:"LastUpdatedTime"`
	TableName       string `json:"TableName"`
	TableStatus     string `json:"TableStatus"`
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
		Total    int `json:"Total"`
		MemStore int `json:"MemStore"`
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
		CreationTime:    db.CreationTime.Format("2006-01-02T15:04:05Z"),
		DatabaseName:    db.DatabaseName,
		KmsKeyID:        db.KmsKeyID,
		LastUpdatedTime: db.LastUpdatedTime.Format("2006-01-02T15:04:05Z"),
		TableCount:      db.TableCount,
	}
}

func toTableView(tbl *Table) tableView {
	return tableView{
		Arn:             tbl.ARN,
		CreationTime:    tbl.CreationTime.Format("2006-01-02T15:04:05Z"),
		DatabaseName:    tbl.DatabaseName,
		LastUpdatedTime: tbl.LastUpdatedTime.Format("2006-01-02T15:04:05Z"),
		TableName:       tbl.TableName,
		TableStatus:     tbl.TableStatus,
	}
}

func (h *Handler) handleCreateDatabase(
	_ context.Context,
	in *databaseNameInput,
) (*databaseOutput, error) {
	if in.DatabaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", errInvalidRequest)
	}

	db, err := h.Backend.CreateDatabase(in.DatabaseName)
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
	in *tableInput,
) (*tableOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", errInvalidRequest)
	}

	tbl, err := h.Backend.CreateTable(in.DatabaseName, in.TableName)
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
	in *tableInput,
) (*tableOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", errInvalidRequest)
	}

	tbl, err := h.Backend.UpdateTable(in.DatabaseName, in.TableName)
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

	if err := h.Backend.WriteRecords(in.DatabaseName, in.TableName, records); err != nil {
		return nil, err
	}

	out := &writeRecordsOutput{}
	out.RecordsIngested.Total = len(records)
	out.RecordsIngested.MemStore = len(records)

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
