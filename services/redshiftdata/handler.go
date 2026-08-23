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
	"github.com/blackbirdworks/gopherstack/pkgs/safemap"
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
	keyLength              = "length"
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
	keyCSVRecords          = "CSVRecords"
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
	Backend     StorageBackend
	janitor     *Janitor
	idempotency *safemap.Map[string, idempotentStatement]
	AccountID   string
	Region      string
}

// regionFromRequest resolves the AWS region for a request from its SigV4
// credential scope, falling back to the backend's default region.
func (h *Handler) regionFromRequest(c *echo.Context) string {
	return httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
}

// NewHandler creates a new Redshift Data handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend:     backend,
		AccountID:   backend.AccountID(),
		Region:      backend.Region(),
		idempotency: safemap.New[string, idempotentStatement]("redshiftdata.idempotency"),
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
	h.idempotency.Clear()
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
		"ListSessions",
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
		// Attach the SigV4-derived region so backend ops route to the correct region store.
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, h.regionFromRequest(c))
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "redshiftdata: failed to read request body", "error", err)

			return writeInternalServerError(c)
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
	case "ListSessions":
		return h.handleListSessions(ctx, body)
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

// writeInternalServerError renders a ReadBody-failure (body too large, read
// error) as one of redshiftdata's own awsjson1.1 error envelopes. The
// deserializer's awsAwsjson11_deserializeOpError path JSON-decodes the body
// for __type/message, so the bare text/plain this used to send deserialized
// client-side as smithy.GenericAPIError{Code:"UnknownError"}
// (gopherstack-o7gx). InternalServerException matches this handler's own
// default fallback (modeled at redshiftdata@v1.43.4 types/errors.go).
func writeInternalServerError(c *echo.Context) error {
	payload, err := json.Marshal(map[string]string{
		keyTypeField:    "InternalServerException",
		keyMessageField: "internal server error",
	})
	if err != nil {
		return err
	}

	return c.JSONBlob(http.StatusInternalServerError, payload)
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

// matchSQLLike implements basic SQL LIKE pattern matching where % matches any
// sequence of characters and _ matches any single character.
func matchSQLLike(s, pattern string) bool {
	if pattern == "" {
		return s == ""
	}

	if pattern == "%" {
		return true
	}

	switch pattern[0] {
	case '%':
		for i := range len(s) + 1 {
			if matchSQLLike(s[i:], pattern[1:]) {
				return true
			}
		}

		return false
	case '_':
		return len(s) > 0 && matchSQLLike(s[1:], pattern[1:])
	default:
		return len(s) > 0 && s[0] == pattern[0] && matchSQLLike(s[1:], pattern[1:])
	}
}
