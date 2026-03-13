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
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new Redshift Data handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "RedshiftData" }

// GetSupportedOperations returns the list of supported Redshift Data operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"ExecuteStatement",
		"BatchExecuteStatement",
		"DescribeStatement",
		"GetStatementResult",
		"ListStatements",
		"CancelStatement",
		"ListDatabases",
		"ListSchemas",
		"ListTables",
		"DescribeTable",
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
	case "ListStatements":
		return h.handleListStatements(ctx, body)
	case "CancelStatement":
		return h.handleCancelStatement(ctx, body)
	case "ListDatabases":
		return h.handleListDatabases()
	case "ListSchemas":
		return h.handleListSchemas()
	case "ListTables":
		return h.handleListTables()
	case "DescribeTable":
		return h.handleDescribeTable()
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
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.SQL == "" {
		return nil, fmt.Errorf("%w: Sql is required", errInvalidRequest)
	}

	stmt, err := h.Backend.ExecuteStatement(
		req.SQL, req.ClusterIdentifier, req.WorkgroupName,
		req.Database, req.DBUser, req.SecretArn, req.StatementName,
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
		"CreatedAt":         epochSeconds(stmt.CreatedAt),
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
		Sqls              []string `json:"Sqls"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if len(req.Sqls) == 0 {
		return nil, fmt.Errorf("%w: Sqls is required", errInvalidRequest)
	}

	stmt, err := h.Backend.BatchExecuteStatement(
		req.Sqls, req.ClusterIdentifier, req.WorkgroupName,
		req.Database, req.DBUser, req.SecretArn, req.StatementName,
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
		"CreatedAt":         epochSeconds(stmt.CreatedAt),
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

	if _, err := h.Backend.DescribeStatement(req.ID); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Records":        [][]any{},
		"ColumnMetadata": []any{},
		"TotalNumRows":   0,
	})
}

func (h *Handler) handleListStatements(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterIdentifier string `json:"ClusterIdentifier"`
		WorkgroupName     string `json:"WorkgroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	stmts := h.Backend.ListStatements(req.ClusterIdentifier, req.WorkgroupName)
	items := make([]map[string]any, 0, len(stmts))

	for _, stmt := range stmts {
		item := map[string]any{
			"Id":               stmt.ID,
			"Status":           stmt.Status,
			"QueryString":      stmt.QueryString,
			"IsBatchStatement": stmt.IsBatchStatement,
			"CreatedAt":        epochSeconds(stmt.CreatedAt),
			"UpdatedAt":        epochSeconds(stmt.UpdatedAt),
		}

		if stmt.StatementName != "" {
			item["StatementName"] = stmt.StatementName
		}

		if stmt.SecretARN != "" {
			item["SecretArn"] = stmt.SecretARN
		}

		items = append(items, item)
	}

	return json.Marshal(map[string]any{
		"Statements": items,
	})
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
		"Status": true,
	})
}

func (h *Handler) handleListDatabases() ([]byte, error) {
	return json.Marshal(map[string]any{
		"Databases": []string{},
	})
}

func (h *Handler) handleListSchemas() ([]byte, error) {
	return json.Marshal(map[string]any{
		"Schemas": []string{},
	})
}

func (h *Handler) handleListTables() ([]byte, error) {
	return json.Marshal(map[string]any{
		"Tables": []any{},
	})
}

func (h *Handler) handleDescribeTable() ([]byte, error) {
	return json.Marshal(map[string]any{
		"ColumnList": []any{},
	})
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ResourceNotFoundException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrAlreadyAborted):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ValidationException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.Is(err, errMissingID),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

// epochSeconds converts a [time.Time] to Unix epoch seconds as float64,
// as required by the AWS JSON 1.1 protocol for timestamp fields.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix()) + float64(t.Nanosecond())/1e9
}

// statementToDescribeResponse converts a statement to a DescribeStatement response map.
func statementToDescribeResponse(stmt *Statement) map[string]any {
	resp := map[string]any{
		"Id":               stmt.ID,
		"Status":           stmt.Status,
		"QueryString":      stmt.QueryString,
		"HasResultSet":     stmt.HasResultSet,
		"IsBatchStatement": stmt.IsBatchStatement,
		"CreatedAt":        epochSeconds(stmt.CreatedAt),
		"UpdatedAt":        epochSeconds(stmt.UpdatedAt),
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

	return resp
}
