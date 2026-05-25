package redshiftdata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	schemaTypeRaw = "raw"
)

const (
	schemaPublic = "public"
)

const (
	keyName     = "name"
	typeVarchar = "varchar"
	keyType     = "type"
	schemaInfo  = "information_schema"
)

const (
	keyCreatedAt           = "CreatedAt"
	valColumn1             = "column1"
	keyTypeName            = "typeName"
	keyColumnSize          = "columnSize"
	keyNullable            = "nullable"
	keyNextToken           = "NextToken"
	keyStatusField         = "Status"
	keySchema              = "schema"
	valTABLE               = "TABLE"
	keyTypeField           = "__type"
	keyMessageField        = "message"
	errValidationException = "ValidationException"
	keyQueryString         = "QueryString"
	keyHasResultSet        = "HasResultSet"
	keyUpdatedAt           = "UpdatedAt"
	keyDuration            = "Duration"
	keyResultFormat        = "ResultFormat"
	valCurated             = "curated"
	keySchemaName          = "schemaName"
)

const (
	redshiftDataService      = "redshift-data"
	redshiftDataTargetPrefix = "RedshiftData."
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
	errMissingID      = errors.New("missing statement ID")
)

// Handler is the HTTP handler for the AWS Redshift Data API.
type Handler struct {
	Backend   StorageBackend
	janitor   *Janitor
	AccountID string
	Region    string
}

// NewHandler creates a new Redshift Data handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.AccountID(),
		Region:    backend.Region(),
	}
}

// WithJanitor attaches a background janitor to the handler.
// If the backend is not an *InMemoryBackend, this is a no-op.
func (h *Handler) WithJanitor(interval, statementTTL time.Duration, taskTimeout ...time.Duration) *Handler {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		j := NewJanitor(mem, interval, statementTTL)
		if len(taskTimeout) > 0 {
			j.TaskTimeout = taskTimeout[0]
		}

		h.janitor = j
	}

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
func (h *Handler) Name() string { return "RedshiftData" }

// Reset clears all backend state. Useful for test isolation.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// GetSupportedOperations returns the list of supported Redshift Data operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"BatchExecuteStatement",
		"CancelStatement",
		"DescribeStatement",
		"DescribeTable",
		"ExecuteStatement",
		"GetStatementResult",
		"GetStatementResultV2",
		"ListDatabases",
		"ListSchemas",
		"ListStatements",
		"ListTables",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return redshiftDataService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches Redshift Data API requests.
// Requests are identified by the X-Amz-Target header prefix "RedshiftData.".
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), redshiftDataTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), redshiftDataTargetPrefix)
}

// ExtractResource extracts the statement ID from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any

	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	if v, ok := data["Id"]; ok {
		if s, isStr := v.(string); isStr {
			return s
		}
	}

	return ""
}

// Handler returns the Echo handler function for Redshift Data requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "redshiftdata: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op := h.ExtractOperation(c)
		result, dispErr := h.dispatch(ctx, op, body)

		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.JSON(http.StatusOK, map[string]any{})
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(ctx context.Context, op string, body []byte) ([]byte, error) {
	switch op {
	case "ExecuteStatement":
		return h.handleExecuteStatement(ctx, body)
	case "BatchExecuteStatement":
		return h.handleBatchExecuteStatement(ctx, body)
	case "DescribeStatement":
		return h.handleDescribeStatement(ctx, body)
	case "GetStatementResult":
		return h.handleGetStatementResult(ctx, body)
	case "GetStatementResultV2":
		return h.handleGetStatementResultV2(ctx, body)
	case "ListStatements":
		return h.handleListStatements(ctx, body)
	case "CancelStatement":
		return h.handleCancelStatement(ctx, body)
	case "ListDatabases":
		return h.handleListDatabases(ctx, body)
	case "ListSchemas":
		return h.handleListSchemas(ctx, body)
	case "ListTables":
		return h.handleListTables(ctx, body)
	case "DescribeTable":
		return h.handleDescribeTable(ctx, body)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
	}
}

func (h *Handler) handleExecuteStatement(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		SQL               string `json:"Sql"`
		ClusterIdentifier string `json:"ClusterIdentifier"`
		WorkgroupName     string `json:"WorkgroupName"`
		Database          string `json:"Database"`
		DBUser            string `json:"DbUser"`
		SecretArn         string `json:"SecretArn"`
		StatementName     string `json:"StatementName"`
		ResultFormat      string `json:"ResultFormat"`
		WithEvent         bool   `json:"WithEvent"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	stmt, err := h.Backend.ExecuteStatement(
		req.SQL, req.ClusterIdentifier, req.WorkgroupName,
		req.Database, req.DBUser, req.SecretArn, req.StatementName,
		req.WithEvent, req.ResultFormat,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Id":                stmt.ID,
		"ClusterIdentifier": stmt.ClusterIdentifier,
		"WorkgroupName":     stmt.WorkgroupName,
		"Database":          stmt.Database,
		"DbUser":            stmt.DBUser,
		"SecretArn":         stmt.SecretARN,
		keyCreatedAt:        epochSeconds(stmt.CreatedAt),
	})
}

func (h *Handler) handleBatchExecuteStatement(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterIdentifier string   `json:"ClusterIdentifier"`
		WorkgroupName     string   `json:"WorkgroupName"`
		Database          string   `json:"Database"`
		DBUser            string   `json:"DbUser"`
		SecretArn         string   `json:"SecretArn"`
		StatementName     string   `json:"StatementName"`
		ResultFormat      string   `json:"ResultFormat"`
		Sqls              []string `json:"Sqls"`
		WithEvent         bool     `json:"WithEvent"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	stmt, err := h.Backend.BatchExecuteStatement(
		req.Sqls, req.ClusterIdentifier, req.WorkgroupName,
		req.Database, req.DBUser, req.SecretArn, req.StatementName,
		req.WithEvent, req.ResultFormat,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Id":                stmt.ID,
		"ClusterIdentifier": stmt.ClusterIdentifier,
		"WorkgroupName":     stmt.WorkgroupName,
		"Database":          stmt.Database,
		"DbUser":            stmt.DBUser,
		"SecretArn":         stmt.SecretARN,
		keyCreatedAt:        epochSeconds(stmt.CreatedAt),
	})
}

func (h *Handler) handleDescribeStatement(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ID string `json:"Id"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errMissingID)
	}

	stmt, err := h.Backend.DescribeStatement(req.ID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(statementToDescribeResponse(stmt))
}

func (h *Handler) handleGetStatementResult(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ID string `json:"Id"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errMissingID)
	}

	stmt, err := h.Backend.DescribeStatement(req.ID)
	if err != nil {
		return nil, err
	}

	if !stmt.HasResultSet {
		return nil, fmt.Errorf(
			"%w: statement %s does not have a result set",
			ErrNoResultSet,
			req.ID,
		)
	}

	if stmt.ResultFormat != resultFormatJSON && stmt.ResultFormat != "" {
		return nil, fmt.Errorf("%w: statement %s result format is not JSON", ErrValidation, req.ID)
	}

	// Return a single demo row so the UI can render a non-empty result table.
	// NextToken is empty because the demo result set fits on one page.
	return json.Marshal(map[string]any{
		"Records": [][]map[string]any{
			{{"stringValue": "mock_value"}},
		},
		"ColumnMetadata": []map[string]any{
			{
				keyName:       valColumn1,
				"label":       valColumn1,
				keyTypeName:   typeVarchar,
				keyColumnSize: mockColumnSize,
				keyNullable:   mockColumnNullable,
			},
		},
		"TotalNumRows": int64(1),
		keyNextToken:   "",
	})
}

func (h *Handler) handleGetStatementResultV2(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ID        string `json:"Id"`
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errMissingID)
	}

	stmt, err := h.Backend.DescribeStatement(req.ID)
	if err != nil {
		return nil, err
	}

	if !stmt.HasResultSet {
		return nil, fmt.Errorf(
			"%w: statement %s does not have a result set",
			ErrNoResultSet,
			req.ID,
		)
	}

	if stmt.ResultFormat != resultFormatCSV {
		return nil, fmt.Errorf("%w: statement %s result format is not CSV", ErrValidation, req.ID)
	}

	// Return a single demo CSV record matching the V2 format.
	// NextToken is empty because the demo result set fits on one page.
	return json.Marshal(map[string]any{
		"Records": []string{"mock_value"},
		"ColumnMetadata": []map[string]any{
			{
				keyName:       valColumn1,
				"label":       valColumn1,
				keyTypeName:   typeVarchar,
				keyColumnSize: mockColumnSize,
				keyNullable:   mockColumnNullable,
			},
		},
		"TotalNumRows":  int64(1),
		keyResultFormat: resultFormatCSV,
		keyNextToken:    "",
	})
}

func (h *Handler) handleListStatements(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterIdentifier string `json:"ClusterIdentifier"`
		WorkgroupName     string `json:"WorkgroupName"`
		Database          string `json:"Database"`
		StatementName     string `json:"StatementName"`
		Status            string `json:"Status"`
		RoleLevel         *bool  `json:"RoleLevel"`
		NextToken         string `json:"NextToken"`
		MaxResults        int    `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MaxResults > maxListStatementsResults {
		return nil, fmt.Errorf(
			"%w: MaxResults must be ≤ %d",
			ErrValidation,
			maxListStatementsResults,
		)
	}

	stmts, nextToken, err := h.Backend.ListStatements(ListStatementsFilter{
		ClusterIdentifier: req.ClusterIdentifier,
		WorkgroupName:     req.WorkgroupName,
		Database:          req.Database,
		StatementName:     req.StatementName,
		Status:            req.Status,
		NextToken:         req.NextToken,
		MaxResults:        req.MaxResults,
	})
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(stmts))

	for _, stmt := range stmts {
		items = append(items, statementToListItem(stmt))
	}

	resp := map[string]any{
		"Statements": items,
	}

	if nextToken != "" {
		resp[keyNextToken] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleCancelStatement(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ID string `json:"Id"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errMissingID)
	}

	if err := h.Backend.CancelStatement(req.ID); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyStatusField: true,
	})
}

func (h *Handler) handleListDatabases(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Database          string `json:"Database"`
		WorkgroupName     string `json:"WorkgroupName"`
		ClusterIdentifier string `json:"ClusterIdentifier"`
		SecretArn         string `json:"SecretArn"`
		DBUser            string `json:"DbUser"`
		NextToken         string `json:"NextToken"`
		MaxResults        int    `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return json.Marshal(map[string]any{
		"Databases":  buildDemoDatabases(),
		keyNextToken: "",
	})
}

func (h *Handler) handleListSchemas(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Database          string `json:"Database"`
		WorkgroupName     string `json:"WorkgroupName"`
		ClusterIdentifier string `json:"ClusterIdentifier"`
		SecretArn         string `json:"SecretArn"`
		DBUser            string `json:"DbUser"`
		SchemaPattern     string `json:"SchemaPattern"`
		NextToken         string `json:"NextToken"`
		MaxResults        int    `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return json.Marshal(map[string]any{
		"Schemas":    buildDemoSchemas(),
		keyNextToken: "",
	})
}

func (h *Handler) handleListTables(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Database          string `json:"Database"`
		WorkgroupName     string `json:"WorkgroupName"`
		ClusterIdentifier string `json:"ClusterIdentifier"`
		SecretArn         string `json:"SecretArn"`
		DBUser            string `json:"DbUser"`
		SchemaPattern     string `json:"SchemaPattern"`
		TablePattern      string `json:"TablePattern"`
		TableType         string `json:"TableType"`
		NextToken         string `json:"NextToken"`
		MaxResults        int    `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return json.Marshal(map[string]any{
		"Tables":     buildDemoTables(),
		keyNextToken: "",
	})
}

func (h *Handler) handleDescribeTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Database          string `json:"Database"`
		WorkgroupName     string `json:"WorkgroupName"`
		ClusterIdentifier string `json:"ClusterIdentifier"`
		SecretArn         string `json:"SecretArn"`
		DBUser            string `json:"DbUser"`
		Schema            string `json:"Schema"`
		Table             string `json:"Table"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return json.Marshal(map[string]any{
		"ColumnList": buildDemoColumns(),
		"TableName": map[string]any{
			keySchema: req.Schema,
			keyName:   req.Table,
			keyType:   valTABLE,
		},
	})
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "ResourceNotFoundException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrTerminalState),
		errors.Is(err, ErrValidation),
		errors.Is(err, ErrNoResultSet):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    errValidationException,
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    errValidationException,
			keyMessageField: err.Error(),
		})
	case errors.Is(err, errInvalidRequest), errors.Is(err, errMissingID),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    errValidationException,
			keyMessageField: err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			keyTypeField:    "InternalServerException",
			keyMessageField: err.Error(),
		})
	}
}

// epochSeconds converts a [time.Time] to Unix epoch seconds as float64,
// as required by the AWS JSON 1.1 protocol for timestamp fields.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix()) + float64(t.Nanosecond())/1e9
}

// statementToListItem converts a statement to the summary map used in ListStatements.
func statementToListItem(stmt *Statement) map[string]any {
	item := map[string]any{
		"Id":               stmt.ID,
		keyStatusField:     stmt.Status,
		keyQueryString:     stmt.QueryString,
		"IsBatchStatement": stmt.IsBatchStatement,
		keyHasResultSet:    stmt.HasResultSet,
		keyCreatedAt:       epochSeconds(stmt.CreatedAt),
		keyUpdatedAt:       epochSeconds(stmt.UpdatedAt),
		keyDuration:        stmt.DurationMs,
		keyResultFormat:    statementResultFormat(stmt),
	}

	if stmt.StatementName != "" {
		item["StatementName"] = stmt.StatementName
	}

	if stmt.SecretARN != "" {
		item["SecretArn"] = stmt.SecretARN
	}

	if stmt.Database != "" {
		item["Database"] = stmt.Database
	}

	if stmt.ClusterIdentifier != "" {
		item["ClusterIdentifier"] = stmt.ClusterIdentifier
	}

	if stmt.WorkgroupName != "" {
		item["WorkgroupName"] = stmt.WorkgroupName
	}

	if stmt.DBUser != "" {
		item["DbUser"] = stmt.DBUser
	}

	return item
}

// statementToDescribeResponse converts a statement to a DescribeStatement response map.
func statementToDescribeResponse(stmt *Statement) map[string]any {
	resp := map[string]any{
		"Id":               stmt.ID,
		keyStatusField:     stmt.Status,
		keyQueryString:     stmt.QueryString,
		keyHasResultSet:    stmt.HasResultSet,
		"IsBatchStatement": stmt.IsBatchStatement,
		keyCreatedAt:       epochSeconds(stmt.CreatedAt),
		keyUpdatedAt:       epochSeconds(stmt.UpdatedAt),
		keyDuration:        stmt.DurationMs,
		"ResultRows":       stmt.ResultRows,
		"ResultSize":       stmt.ResultSize,
		"WithEvent":        stmt.WithEvent,
		keyResultFormat:    statementResultFormat(stmt),
		// RedshiftQueryId is a synthetic numeric query identifier. AWS
		// includes this in DescribeStatement for provisioned clusters;
		// we return 0 since we have no real cluster backing.
		"RedshiftQueryId": int64(0),
	}

	if stmt.ClusterIdentifier != "" {
		resp["ClusterIdentifier"] = stmt.ClusterIdentifier
	}

	if stmt.WorkgroupName != "" {
		resp["WorkgroupName"] = stmt.WorkgroupName
	}

	if stmt.Database != "" {
		resp["Database"] = stmt.Database
	}

	if stmt.DBUser != "" {
		resp["DbUser"] = stmt.DBUser
	}

	if stmt.SecretARN != "" {
		resp["SecretArn"] = stmt.SecretARN
	}

	if stmt.StatementName != "" {
		resp["StatementName"] = stmt.StatementName
	}

	if stmt.Error != "" {
		resp["Error"] = stmt.Error
	}

	if len(stmt.QueryStrings) > 0 {
		resp["QueryStrings"] = stmt.QueryStrings
	}

	if len(stmt.SubStatements) > 0 {
		subs := make([]map[string]any, len(stmt.SubStatements))
		for i, sub := range stmt.SubStatements {
			subs[i] = map[string]any{
				"Id":            sub.ID,
				keyCreatedAt:    epochSeconds(sub.CreatedAt),
				keyUpdatedAt:    epochSeconds(sub.UpdatedAt),
				keyQueryString:  sub.QueryString,
				keyStatusField:  sub.Status,
				keyHasResultSet: sub.HasResultSet,
				keyDuration:     sub.DurationMs,
				"ResultRows":    sub.ResultRows,
				"ResultSize":    sub.ResultSize,
			}

			if sub.Error != "" {
				subs[i]["Error"] = sub.Error
			}
		}

		resp["SubStatements"] = subs
	}

	return resp
}

// listDemoData holds the static demo payloads for metadata list operations.
// These are package-level functions to avoid gochecknoglobals violations.

// buildDemoDatabases returns a realistic demo list of database names.
func buildDemoDatabases() []string {
	return []string{"dev", "prod", "staging", "analytics"}
}

// buildDemoSchemas returns a realistic demo list of schema names.
func buildDemoSchemas() []string {
	return []string{schemaPublic, schemaInfo, "pg_catalog", schemaTypeRaw, valCurated}
}

// buildDemoTables returns a realistic demo list of table descriptors.
func buildDemoTables() []map[string]any {
	return []map[string]any{
		{keyName: "users", keySchema: schemaPublic, keyType: valTABLE},
		{keyName: "orders", keySchema: schemaPublic, keyType: valTABLE},
		{keyName: "events", keySchema: schemaTypeRaw, keyType: valTABLE},
		{keyName: "sessions", keySchema: schemaTypeRaw, keyType: valTABLE},
		{keyName: "daily_summary", keySchema: valCurated, keyType: "VIEW"},
		{keyName: "user_metrics", keySchema: valCurated, keyType: "VIEW"},
		{keyName: "columns", keySchema: schemaInfo, keyType: valTABLE},
		{keyName: "tables", keySchema: schemaInfo, keyType: valTABLE},
	}
}

// buildDemoColumns returns a realistic demo list of column descriptors for DescribeTable.
func buildDemoColumns() []map[string]any {
	const (
		sizeInt       = int64(10)
		sizeVarchar   = int64(255)
		sizeTimestamp = int64(29)
		notNull       = int64(0)
		nullable      = int64(1)
	)

	return []map[string]any{
		{
			keyName:       "id",
			keyTypeName:   "int4",
			keyColumnSize: sizeInt,
			keyNullable:   notNull,
			keySchemaName: schemaPublic,
		},
		{
			keyName:       keyName,
			keyTypeName:   typeVarchar,
			keyColumnSize: sizeVarchar,
			keyNullable:   nullable,
			keySchemaName: schemaPublic,
		},
		{
			keyName:       "email",
			keyTypeName:   typeVarchar,
			keyColumnSize: sizeVarchar,
			keyNullable:   nullable,
			keySchemaName: schemaPublic,
		},
		{
			keyName:       "created_at",
			keyTypeName:   "timestamp",
			keyColumnSize: sizeTimestamp,
			keyNullable:   nullable,
			keySchemaName: schemaPublic,
		},
		{
			keyName:       "updated_at",
			keyTypeName:   "timestamp",
			keyColumnSize: sizeTimestamp,
			keyNullable:   nullable,
			keySchemaName: schemaPublic,
		},
	}
}
