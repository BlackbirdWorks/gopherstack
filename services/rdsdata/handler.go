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
	rdsdataService       = "rds-data"
	rdsdataMatchPriority = 87

	pathExecute             = "/Execute"
	pathBatchExecute        = "/BatchExecute"
	pathBeginTransaction    = "/BeginTransaction"
	pathCommitTransaction   = "/CommitTransaction"
	pathRollbackTransaction = "/RollbackTransaction"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the HTTP handler for the RDS Data REST API.
type Handler struct {
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new RDS Data handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "RDSData" }

// GetSupportedOperations returns the list of supported RDS Data operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"ExecuteStatement",
		"BatchExecuteStatement",
		"BeginTransaction",
		"CommitTransaction",
		"RollbackTransaction",
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
			pathCommitTransaction, pathRollbackTransaction:
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
		return "ExecuteStatement"
	case pathBatchExecute:
		return "BatchExecuteStatement"
	case pathBeginTransaction:
		return "BeginTransaction"
	case pathCommitTransaction:
		return "CommitTransaction"
	case pathRollbackTransaction:
		return "RollbackTransaction"
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
	case "ExecuteStatement":
		return h.handleExecuteStatement(ctx, body)
	case "BatchExecuteStatement":
		return h.handleBatchExecuteStatement(ctx, body)
	case "BeginTransaction":
		return h.handleBeginTransaction(ctx, body)
	case "CommitTransaction":
		return h.handleCommitTransaction(ctx, body)
	case "RollbackTransaction":
		return h.handleRollbackTransaction(ctx, body)
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
			"__type":  "TransactionNotFoundException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

type executeStatementRequest struct {
	ResourceArn   string         `json:"resourceArn"`
	SecretArn     string         `json:"secretArn"`
	SQL           string         `json:"sql"`
	Database      string         `json:"database"`
	Schema        string         `json:"schema"`
	TransactionID string         `json:"transactionId"`
	Parameters    []SQLParameter `json:"parameters"`
}

type executeStatementResponse struct {
	ColumnMetadata         []ColumnMetadata `json:"columnMetadata"`
	GeneratedFields        []Field          `json:"generatedFields"`
	Records                [][]Field        `json:"records"`
	NumberOfRecordsUpdated int64            `json:"numberOfRecordsUpdated"`
}

func (h *Handler) handleExecuteStatement(_ context.Context, body []byte) ([]byte, error) {
	var req executeStatementRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: missing resourceArn", errInvalidRequest)
	}

	if req.SQL == "" {
		return nil, fmt.Errorf("%w: missing sql", errInvalidRequest)
	}

	records, columns, updated, err := h.Backend.ExecuteStatement(req.ResourceArn, req.SQL, req.TransactionID)
	if err != nil {
		return nil, err
	}

	resp := executeStatementResponse{
		ColumnMetadata:         columns,
		GeneratedFields:        []Field{},
		Records:                records,
		NumberOfRecordsUpdated: updated,
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

func (h *Handler) handleBatchExecuteStatement(_ context.Context, body []byte) ([]byte, error) {
	var req batchExecuteStatementRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: missing resourceArn", errInvalidRequest)
	}

	if req.SQL == "" {
		return nil, fmt.Errorf("%w: missing sql", errInvalidRequest)
	}

	results, err := h.Backend.BatchExecuteStatement(req.ResourceArn, req.SQL, req.TransactionID, req.ParameterSets)
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

func (h *Handler) handleBeginTransaction(_ context.Context, body []byte) ([]byte, error) {
	var req beginTransactionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: missing resourceArn", errInvalidRequest)
	}

	txID, err := h.Backend.BeginTransaction(req.ResourceArn)
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

func (h *Handler) handleCommitTransaction(_ context.Context, body []byte) ([]byte, error) {
	var req commitTransactionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TransactionID == "" {
		return nil, fmt.Errorf("%w: missing transactionId", errInvalidRequest)
	}

	status, err := h.Backend.CommitTransaction(req.TransactionID)
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

func (h *Handler) handleRollbackTransaction(_ context.Context, body []byte) ([]byte, error) {
	var req rollbackTransactionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TransactionID == "" {
		return nil, fmt.Errorf("%w: missing transactionId", errInvalidRequest)
	}

	status, err := h.Backend.RollbackTransaction(req.TransactionID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(rollbackTransactionResponse{TransactionStatus: status})
}
