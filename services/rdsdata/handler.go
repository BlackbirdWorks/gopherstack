package rdsdata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyMessageField     = "message"
	keyResourceARNField = "resourceArn"
	keySecretARNField   = "secretArn"
)

const (
	opBatchExecuteStatement = "BatchExecuteStatement"
	opBeginTransaction      = "BeginTransaction"
	opCommitTransaction     = "CommitTransaction"
	opExecuteSQL            = "ExecuteSql"
	opExecuteStatement      = "ExecuteStatement"
	opRollbackTransaction   = "RollbackTransaction"
)

const (
	rdsdataService       = "rds-data"
	rdsdataMatchPriority = 87

	pathExecute             = "/Execute"
	pathBatchExecute        = "/BatchExecute"
	pathBeginTransaction    = "/BeginTransaction"
	pathCommitTransaction   = "/CommitTransaction"
	pathRollbackTransaction = "/RollbackTransaction"
	pathExecuteSQL          = "/ExecuteSql"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the HTTP handler for the RDS Data REST API.
type Handler struct {
	Backend   StorageBackend
	AccountID string
	Region    string
}

// NewHandler creates a new RDS Data handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.AccountID(),
		Region:    backend.Region(),
	}
}

// Reset clears all handler and backend state. Useful for test isolation.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "RDSData" }

// GetSupportedOperations returns the list of supported RDS Data operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opBatchExecuteStatement,
		opBeginTransaction,
		opCommitTransaction,
		opExecuteSQL,
		opExecuteStatement,
		opRollbackTransaction,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return rdsdataService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches RDS Data API requests.
// All path-based matches are gated on the SigV4 service name to prevent
// routing conflicts with other services that share similar REST paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if httputils.ExtractServiceFromRequest(c.Request()) != rdsdataService {
			return false
		}

		path := c.Request().URL.Path

		switch path {
		case pathExecute, pathBatchExecute, pathBeginTransaction,
			pathCommitTransaction, pathRollbackTransaction, pathExecuteSQL:
			return true
		}

		return false
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return rdsdataMatchPriority }

// ExtractOperation extracts the operation name from the request path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	switch c.Request().URL.Path {
	case pathExecute:
		return opExecuteStatement
	case pathBatchExecute:
		return opBatchExecuteStatement
	case pathBeginTransaction:
		return opBeginTransaction
	case pathCommitTransaction:
		return opCommitTransaction
	case pathRollbackTransaction:
		return opRollbackTransaction
	case pathExecuteSQL:
		return opExecuteSQL
	default:
		return "Unknown"
	}
}

// ExtractResource always returns an empty string for the RDS Data API.
// The resource is identified by a resourceArn in the request body, but
// parsing the body here would require double-buffering; metrics and logging
// can rely on ExtractOperation instead.
func (h *Handler) ExtractResource(_ *echo.Context) string {
	return ""
}

// Handler returns the Echo handler function for RDS Data requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		// Resolve the per-request region (from SigV4 / X-Amz-Region) and attach
		// it to the context so backend operations are region-scoped.
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
		ctx = context.WithValue(ctx, regionContextKey{}, region)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "rdsdata: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op := h.ExtractOperation(c)

		result, dispErr := h.dispatch(ctx, op, body)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.NoContent(http.StatusOK)
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(ctx context.Context, op string, body []byte) ([]byte, error) {
	switch op {
	case opExecuteStatement:
		return h.handleExecuteStatement(ctx, body)
	case opBatchExecuteStatement:
		return h.handleBatchExecuteStatement(ctx, body)
	case opBeginTransaction:
		return h.handleBeginTransaction(ctx, body)
	case opCommitTransaction:
		return h.handleCommitTransaction(ctx, body)
	case opRollbackTransaction:
		return h.handleRollbackTransaction(ctx, body)
	case opExecuteSQL:
		return h.handleExecuteSQL(ctx, body)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrTransactionNotFound):
		payload, _ := json.Marshal(map[string]string{
			"__type":        "TransactionNotFoundException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errIsValidation(err):
		payload, _ := json.Marshal(map[string]string{
			"__type":        "BadRequestException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{keyMessageField: err.Error()})
	}
}

type executeStatementRequest struct {
	ResourceArn           string         `json:"resourceArn"`
	SecretArn             string         `json:"secretArn"`
	SQL                   string         `json:"sql"`
	Database              string         `json:"database"`
	Schema                string         `json:"schema"`
	TransactionID         string         `json:"transactionId"`
	Parameters            []SQLParameter `json:"parameters"`
	IncludeResultMetadata bool           `json:"includeResultMetadata"`
}

type requiredField struct {
	name  string
	value string
}

func validateRequiredFields(fields ...requiredField) error {
	for _, field := range fields {
		if field.value == "" {
			return fmt.Errorf("%w: missing %s", errInvalidRequest, field.name)
		}
	}

	return nil
}

func (h *Handler) handleExecuteStatement(ctx context.Context, body []byte) ([]byte, error) {
	var req executeStatementRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := validateRequiredFields(
		requiredField{name: keyResourceARNField, value: req.ResourceArn},
		requiredField{name: keySecretARNField, value: req.SecretArn},
		requiredField{name: "sql", value: req.SQL},
	); err != nil {
		return nil, err
	}

	records, columns, updated, err := h.Backend.ExecuteStatement(
		ctx, req.ResourceArn, req.SQL, req.TransactionID, req.Parameters...)
	if err != nil {
		return nil, err
	}

	// Use a map so columnMetadata can be conditionally included.
	// Real AWS only adds columnMetadata to the response when includeResultMetadata=true.
	resp := map[string]any{
		"generatedFields":        []Field{},
		"records":                records,
		"numberOfRecordsUpdated": updated,
	}

	if req.IncludeResultMetadata {
		resp["columnMetadata"] = columns
	}

	return json.Marshal(resp)
}

type batchExecuteStatementRequest struct {
	ResourceArn   string           `json:"resourceArn"`
	SecretArn     string           `json:"secretArn"`
	SQL           string           `json:"sql"`
	Database      string           `json:"database"`
	Schema        string           `json:"schema"`
	TransactionID string           `json:"transactionId"`
	ParameterSets [][]SQLParameter `json:"parameterSets"`
}

type batchExecuteStatementResponse struct {
	UpdateResults []UpdateResult `json:"updateResults"`
}

func (h *Handler) handleBatchExecuteStatement(ctx context.Context, body []byte) ([]byte, error) {
	var req batchExecuteStatementRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := validateRequiredFields(
		requiredField{name: keyResourceARNField, value: req.ResourceArn},
		requiredField{name: keySecretARNField, value: req.SecretArn},
		requiredField{name: "sql", value: req.SQL},
	); err != nil {
		return nil, err
	}

	results, err := h.Backend.BatchExecuteStatement(ctx, req.ResourceArn, req.SQL, req.TransactionID, req.ParameterSets)
	if err != nil {
		return nil, err
	}

	return json.Marshal(batchExecuteStatementResponse{UpdateResults: results})
}

type beginTransactionRequest struct {
	ResourceArn string `json:"resourceArn"`
	SecretArn   string `json:"secretArn"`
	Database    string `json:"database"`
	Schema      string `json:"schema"`
}

type beginTransactionResponse struct {
	TransactionID string `json:"transactionId"`
}

func (h *Handler) handleBeginTransaction(ctx context.Context, body []byte) ([]byte, error) {
	var req beginTransactionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := validateRequiredFields(
		requiredField{name: keyResourceARNField, value: req.ResourceArn},
		requiredField{name: keySecretARNField, value: req.SecretArn},
	); err != nil {
		return nil, err
	}

	txID, err := h.Backend.BeginTransaction(ctx, req.ResourceArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(beginTransactionResponse{TransactionID: txID})
}

type commitTransactionRequest struct {
	ResourceArn   string `json:"resourceArn"`
	SecretArn     string `json:"secretArn"`
	TransactionID string `json:"transactionId"`
}

type commitTransactionResponse struct {
	TransactionStatus string `json:"transactionStatus"`
}

func (h *Handler) handleCommitTransaction(ctx context.Context, body []byte) ([]byte, error) {
	var req commitTransactionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := validateRequiredFields(
		requiredField{name: keyResourceARNField, value: req.ResourceArn},
		requiredField{name: keySecretARNField, value: req.SecretArn},
		requiredField{name: "transactionId", value: req.TransactionID},
	); err != nil {
		return nil, err
	}

	status, err := h.Backend.CommitTransaction(ctx, req.TransactionID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(commitTransactionResponse{TransactionStatus: status})
}

type rollbackTransactionRequest struct {
	ResourceArn   string `json:"resourceArn"`
	SecretArn     string `json:"secretArn"`
	TransactionID string `json:"transactionId"`
}

type rollbackTransactionResponse struct {
	TransactionStatus string `json:"transactionStatus"`
}

func (h *Handler) handleRollbackTransaction(ctx context.Context, body []byte) ([]byte, error) {
	var req rollbackTransactionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := validateRequiredFields(
		requiredField{name: keyResourceARNField, value: req.ResourceArn},
		requiredField{name: keySecretARNField, value: req.SecretArn},
		requiredField{name: "transactionId", value: req.TransactionID},
	); err != nil {
		return nil, err
	}

	status, err := h.Backend.RollbackTransaction(ctx, req.TransactionID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(rollbackTransactionResponse{TransactionStatus: status})
}

type executeSQLRequest struct {
	AwsSecretStoreArn      string `json:"awsSecretStoreArn"`
	DBClusterOrInstanceArn string `json:"dbClusterOrInstanceArn"`
	SQLStatements          string `json:"sqlStatements"`
	Database               string `json:"database"`
	Schema                 string `json:"schema"`
}

type executeSQLResponse struct {
	SQLStatementResults []SQLStatementResult `json:"sqlStatementResults"`
}

func (h *Handler) handleExecuteSQL(ctx context.Context, body []byte) ([]byte, error) {
	var req executeSQLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := validateRequiredFields(
		requiredField{name: "dbClusterOrInstanceArn", value: req.DBClusterOrInstanceArn},
		requiredField{name: "awsSecretStoreArn", value: req.AwsSecretStoreArn},
		requiredField{name: "sqlStatements", value: req.SQLStatements},
	); err != nil {
		return nil, err
	}

	results, err := h.Backend.ExecuteSQL(ctx, req.DBClusterOrInstanceArn, req.SQLStatements)
	if err != nil {
		return nil, err
	}

	return json.Marshal(executeSQLResponse{SQLStatementResults: results})
}
