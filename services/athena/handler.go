package athena

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

var ErrUnknownOperation = errors.New("InvalidRequestException")

// Handler is the Echo HTTP service handler for Athena operations.
type Handler struct {
	Backend StorageBackend
	janitor *Janitor
	// dispatch is the pre-built immutable dispatch table, set once in NewHandler.
	dispatch map[string]athenaActionFn
}

// NewHandler creates a new Athena handler with the given storage backend.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.dispatch = h.buildDispatchTable()

	return h
}

// WithJanitor attaches a background janitor to the handler.
// If the backend is not an *InMemoryBackend, this is a no-op.
func (h *Handler) WithJanitor(interval, executionTTL time.Duration, taskTimeout ...time.Duration) *Handler {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		j := NewJanitor(mem, interval, executionTTL)
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
func (h *Handler) Name() string { return "Athena" }

// GetSupportedOperations returns the list of mocked Athena operations.
func (h *Handler) GetSupportedOperations() []string {
	return append(h.baseSupportedOperations(), h.extendedSupportedOperations()...)
}

func (h *Handler) baseSupportedOperations() []string {
	return []string{
		"CreateWorkGroup",
		"GetWorkGroup",
		"ListWorkGroups",
		"UpdateWorkGroup",
		"DeleteWorkGroup",
		"CreateNamedQuery",
		"BatchGetNamedQuery",
		"GetNamedQuery",
		"ListNamedQueries",
		"DeleteNamedQuery",
		"CreateDataCatalog",
		"GetDataCatalog",
		"ListDataCatalogs",
		"UpdateDataCatalog",
		"DeleteDataCatalog",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"StartQueryExecution",
		"StopQueryExecution",
		"GetQueryExecution",
		"ListQueryExecutions",
		"BatchGetQueryExecution",
		"GetQueryResults",
		"BatchGetPreparedStatement",
		"CreatePreparedStatement",
		"DeletePreparedStatement",
		"GetPreparedStatement",
		"ListPreparedStatements",
		"CancelCapacityReservation",
		"CreateCapacityReservation",
		"DeleteCapacityReservation",
		"CreateNotebook",
		"CreatePresignedNotebookUrl",
		"DeleteNotebook",
		"ExportNotebook",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "athena" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Athena instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches incoming requests for Athena.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, "AmazonAthena")
	}
}

// MatchPriority returns the routing priority for the Athena handler.
func (h *Handler) MatchPriority() int {
	return service.PriorityHeaderExact
}

// ExtractOperation extracts the specific Athena operation from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	parts := strings.Split(target, ".")

	const targetParts = 2
	if len(parts) == targetParts {
		return parts[1]
	}

	return "Unknown"
}

// ExtractResource extracts the primary resource name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	if name, exists := data["Name"]; exists {
		if nameStr, ok := name.(string); ok {
			return nameStr
		}
	}

	return ""
}

// Handler returns the Echo HTTP handler for Athena operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Athena", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.doDispatch,
			h.handleError,
		)
	}
}

// --- Input types ---

type createWorkGroupInput struct {
	Name          string                 `json:"Name"`
	Description   string                 `json:"Description"`
	State         string                 `json:"State"`
	Tags          []Tag                  `json:"Tags"`
	Configuration WorkGroupConfiguration `json:"Configuration"`
}

type updateWorkGroupInput struct {
	ConfigurationUpdates *WorkGroupConfiguration `json:"ConfigurationUpdates"`
	WorkGroup            string                  `json:"WorkGroup"`
	Description          string                  `json:"Description"`
	State                string                  `json:"State"`
}

type deleteWorkGroupInput struct {
	WorkGroup string `json:"WorkGroup"`
}

type getWorkGroupInput struct {
	WorkGroup string `json:"WorkGroup"`
}

type createNamedQueryInput struct {
	Name        string `json:"Name"`
	Description string `json:"Description"`
	Database    string `json:"Database"`
	QueryString string `json:"QueryString"`
	WorkGroup   string `json:"WorkGroup"`
}

type getNamedQueryInput struct {
	NamedQueryID string `json:"NamedQueryId"`
}

type listNamedQueriesInput struct {
	WorkGroup string `json:"WorkGroup"`
}

type batchGetNamedQueryInput struct {
	NamedQueryIDs []string `json:"NamedQueryIds"`
}

type deleteNamedQueryInput struct {
	NamedQueryID string `json:"NamedQueryId"`
}

type createDataCatalogInput struct {
	Name           string            `json:"Name"`
	Type           string            `json:"Type"`
	Description    string            `json:"Description"`
	Parameters     map[string]string `json:"Parameters"`
	Tags           []Tag             `json:"Tags"`
	ConnectionType string            `json:"ConnectionType"`
}

type getDataCatalogInput struct {
	Name string `json:"Name"`
}

type updateDataCatalogInput struct {
	Parameters     map[string]string `json:"Parameters"`
	Name           string            `json:"Name"`
	Type           string            `json:"Type"`
	Description    string            `json:"Description"`
	ConnectionType string            `json:"ConnectionType"`
}

type deleteDataCatalogInput struct {
	Name string `json:"Name"`
}

type tagResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
	Tags        []Tag  `json:"Tags"`
}

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

type listTagsForResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
}

type startQueryExecutionInput struct {
	QueryString           string                `json:"QueryString"`
	WorkGroup             string                `json:"WorkGroup"`
	QueryExecutionContext QueryExecutionContext `json:"QueryExecutionContext"`
	ResultConfiguration   ResultConfiguration   `json:"ResultConfiguration"`
	ExecutionParameters   []string              `json:"ExecutionParameters"`
}

type stopQueryExecutionInput struct {
	QueryExecutionID string `json:"QueryExecutionId"`
}

type getQueryExecutionInput struct {
	QueryExecutionID string `json:"QueryExecutionId"`
}

type listQueryExecutionsInput struct {
	WorkGroup string `json:"WorkGroup"`
}

type batchGetQueryExecutionInput struct {
	QueryExecutionIDs []string `json:"QueryExecutionIds"`
}

type batchGetPreparedStatementInput struct {
	WorkGroup      string   `json:"WorkGroup"`
	StatementNames []string `json:"StatementNames"`
}

type createPreparedStatementInput struct {
	StatementName  string `json:"StatementName"`
	Description    string `json:"Description"`
	WorkGroup      string `json:"WorkGroup"`
	QueryStatement string `json:"QueryStatement"`
}

type deletePreparedStatementInput struct {
	StatementName string `json:"StatementName"`
	WorkGroup     string `json:"WorkGroup"`
}

type getPreparedStatementInput struct {
	StatementName string `json:"StatementName"`
	WorkGroup     string `json:"WorkGroup"`
}

type listPreparedStatementsInput struct {
	WorkGroup string `json:"WorkGroup"`
}

type cancelCapacityReservationInput struct {
	Name string `json:"Name"`
}

type createCapacityReservationInput struct {
	Name       string `json:"Name"`
	Tags       []Tag  `json:"Tags"`
	TargetDpus int32  `json:"TargetDpus"`
}

type deleteCapacityReservationInput struct {
	Name string `json:"Name"`
}

type createNotebookInput struct {
	WorkGroup string `json:"WorkGroup"`
	Name      string `json:"Name"`
	Tags      []Tag  `json:"Tags"`
}

type createPresignedNotebookURLInput struct {
	SessionID string `json:"SessionId"`
}

type deleteNotebookInput struct {
	NotebookID string `json:"NotebookId"`
}

type exportNotebookInput struct {
	NotebookID string `json:"NotebookId"`
}

// --- Dispatch ---

type athenaActionFn func([]byte) (any, error)

const errTypeInvalidRequest = "InvalidRequestException"

func (h *Handler) buildDispatchTable() map[string]athenaActionFn {
	ops := h.workGroupOps()
	maps.Copy(ops, h.namedQueryOps())
	maps.Copy(ops, h.dataCatalogOps())
	maps.Copy(ops, h.queryExecutionOps())
	maps.Copy(ops, h.tagOps())
	maps.Copy(ops, h.preparedStatementOps())
	maps.Copy(ops, h.capacityReservationOps())
	maps.Copy(ops, h.notebookOps())
	maps.Copy(ops, h.extendedDispatchTable())

	return ops
}

func (h *Handler) workGroupOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"CreateWorkGroup": func(b []byte) (any, error) {
			var input createWorkGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.CreateWorkGroup(
				input.Name, input.Description, input.State, input.Configuration, tagsFromSlice(input.Tags),
			)
		},
		"GetWorkGroup": func(b []byte) (any, error) {
			var input getWorkGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			wg, err := h.Backend.GetWorkGroup(input.WorkGroup)
			if err != nil {
				return nil, err
			}

			return map[string]any{"WorkGroup": wg}, nil
		},
		"ListWorkGroups": func(_ []byte) (any, error) {
			list, err := h.Backend.ListWorkGroups()
			if err != nil {
				return nil, err
			}

			return map[string]any{"WorkGroups": list}, nil
		},
		"UpdateWorkGroup": func(b []byte) (any, error) {
			var input updateWorkGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateWorkGroup(
				input.WorkGroup, input.Description, input.State, input.ConfigurationUpdates,
			)
		},
		"DeleteWorkGroup": func(b []byte) (any, error) {
			var input deleteWorkGroupInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeleteWorkGroup(input.WorkGroup)
		},
	}
}

func (h *Handler) namedQueryOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"CreateNamedQuery": func(b []byte) (any, error) {
			var input createNamedQueryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			id, err := h.Backend.CreateNamedQuery(
				input.Name, input.Description, input.Database, input.QueryString, input.WorkGroup,
			)
			if err != nil {
				return nil, err
			}

			return map[string]any{"NamedQueryId": id}, nil
		},
		"GetNamedQuery": func(b []byte) (any, error) {
			var input getNamedQueryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			q, err := h.Backend.GetNamedQuery(input.NamedQueryID)
			if err != nil {
				return nil, err
			}

			return map[string]any{"NamedQuery": q}, nil
		},
		"ListNamedQueries": func(b []byte) (any, error) {
			var input listNamedQueriesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			ids, err := h.Backend.ListNamedQueries(input.WorkGroup)
			if err != nil {
				return nil, err
			}

			return map[string]any{"NamedQueryIds": ids}, nil
		},
		"BatchGetNamedQuery": func(b []byte) (any, error) {
			var input batchGetNamedQueryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			found, unprocessed := h.Backend.BatchGetNamedQuery(input.NamedQueryIDs)

			return map[string]any{
				"NamedQueries":             found,
				"UnprocessedNamedQueryIds": unprocessed,
			}, nil
		},
		"DeleteNamedQuery": func(b []byte) (any, error) {
			var input deleteNamedQueryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeleteNamedQuery(input.NamedQueryID)
		},
	}
}

func (h *Handler) dataCatalogOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"CreateDataCatalog": func(b []byte) (any, error) {
			var input createDataCatalogInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.CreateDataCatalog(
				input.Name, input.Type, input.Description, input.ConnectionType, input.Parameters, tagsFromSlice(input.Tags),
			)
		},
		"GetDataCatalog": func(b []byte) (any, error) {
			var input getDataCatalogInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			dc, err := h.Backend.GetDataCatalog(input.Name)
			if err != nil {
				return nil, err
			}

			return map[string]any{"DataCatalog": dc}, nil
		},
		"ListDataCatalogs": func(_ []byte) (any, error) {
			list, err := h.Backend.ListDataCatalogs()
			if err != nil {
				return nil, err
			}

			return map[string]any{"DataCatalogsSummary": list, "NextToken": ""}, nil
		},
		"UpdateDataCatalog": func(b []byte) (any, error) {
			var input updateDataCatalogInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateDataCatalog(
				input.Name, input.Type, input.Description, input.ConnectionType, input.Parameters,
			)
		},
		"DeleteDataCatalog": func(b []byte) (any, error) {
			var input deleteDataCatalogInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeleteDataCatalog(input.Name)
		},
	}
}

func (h *Handler) queryExecutionOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"StartQueryExecution": func(b []byte) (any, error) {
			var input startQueryExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			id, err := h.Backend.StartQueryExecution(
				input.QueryString, input.WorkGroup, input.QueryExecutionContext, input.ResultConfiguration,
				input.ExecutionParameters,
			)
			if err != nil {
				return nil, err
			}

			return map[string]any{"QueryExecutionId": id}, nil
		},
		"StopQueryExecution": func(b []byte) (any, error) {
			var input stopQueryExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.StopQueryExecution(input.QueryExecutionID)
		},
		"GetQueryExecution": func(b []byte) (any, error) {
			var input getQueryExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			qe, err := h.Backend.GetQueryExecution(input.QueryExecutionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{"QueryExecution": qe}, nil
		},
		"ListQueryExecutions": func(b []byte) (any, error) {
			var input listQueryExecutionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			ids, err := h.Backend.ListQueryExecutions(input.WorkGroup)
			if err != nil {
				return nil, err
			}

			return map[string]any{"QueryExecutionIds": ids, "NextToken": ""}, nil
		},
		"BatchGetQueryExecution": func(b []byte) (any, error) {
			var input batchGetQueryExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			found, unprocessed := h.Backend.BatchGetQueryExecution(input.QueryExecutionIDs)

			return map[string]any{
				"QueryExecutions":              found,
				"UnprocessedQueryExecutionIds": unprocessed,
			}, nil
		},
		"GetQueryResults": h.handleGetQueryResults,
	}
}

// athenaMaxQueryResultsPageSize matches the AWS-documented maximum page size
// for Athena GetQueryResults. The minimum is 1.
const athenaMaxQueryResultsPageSize = 1000

type getQueryResultsInput struct {
	QueryExecutionID string `json:"QueryExecutionId"`
	NextToken        string `json:"NextToken,omitempty"`
	MaxResults       int    `json:"MaxResults,omitempty"`
}

// handleGetQueryResults validates the query execution exists and the requested
// MaxResults page size is in range [1, 1000]. The mock backend stores no rows
// so the response is always an empty page; pagination tokens are accepted and
// returned as nil to model end-of-stream.
func (h *Handler) handleGetQueryResults(b []byte) (any, error) {
	var input getQueryResultsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrValidation, err)
	}

	if input.QueryExecutionID == "" {
		return nil, fmt.Errorf("%w: QueryExecutionId is required", ErrValidation)
	}

	if input.MaxResults < 0 || input.MaxResults > athenaMaxQueryResultsPageSize {
		return nil, fmt.Errorf(
			"%w: MaxResults must be between 1 and %d",
			ErrValidation, athenaMaxQueryResultsPageSize,
		)
	}

	if _, err := h.Backend.GetQueryExecution(input.QueryExecutionID); err != nil {
		return nil, err
	}

	return map[string]any{
		"ResultSet": map[string]any{
			"Rows": []any{},
			"ResultSetMetadata": map[string]any{
				"ColumnInfo": []any{},
			},
		},
		"UpdateCount": 0,
	}, nil
}

func (h *Handler) tagOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"TagResource": func(b []byte) (any, error) {
			var input tagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.TagResource(input.ResourceARN, tagsFromSlice(input.Tags))
		},
		"UntagResource": func(b []byte) (any, error) {
			var input untagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UntagResource(input.ResourceARN, input.TagKeys)
		},
		"ListTagsForResource": func(b []byte) (any, error) {
			var input listTagsForResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			tags, err := h.Backend.ListTagsForResource(input.ResourceARN)
			if err != nil {
				return nil, err
			}

			return map[string]any{"Tags": tags}, nil
		},
	}
}

func (h *Handler) preparedStatementOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"CreatePreparedStatement": func(b []byte) (any, error) {
			var input createPreparedStatementInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.CreatePreparedStatement(
				input.StatementName, input.Description, input.WorkGroup, input.QueryStatement,
			)
		},
		"BatchGetPreparedStatement": func(b []byte) (any, error) {
			var input batchGetPreparedStatementInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			found, unprocessed := h.Backend.BatchGetPreparedStatement(input.WorkGroup, input.StatementNames)

			return map[string]any{
				"PreparedStatements":        found,
				"UnprocessedStatementNames": unprocessed,
			}, nil
		},
		"DeletePreparedStatement": func(b []byte) (any, error) {
			var input deletePreparedStatementInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeletePreparedStatement(input.StatementName, input.WorkGroup)
		},
		"GetPreparedStatement": func(b []byte) (any, error) {
			var input getPreparedStatementInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			ps, err := h.Backend.GetPreparedStatement(input.StatementName, input.WorkGroup)
			if err != nil {
				return nil, err
			}

			return map[string]any{"PreparedStatement": ps}, nil
		},
		"ListPreparedStatements": func(b []byte) (any, error) {
			var input listPreparedStatementsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			stmts, err := h.Backend.ListPreparedStatements(input.WorkGroup)
			if err != nil {
				return nil, err
			}

			return map[string]any{"PreparedStatements": stmts}, nil
		},
	}
}

func (h *Handler) capacityReservationOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"CreateCapacityReservation": func(b []byte) (any, error) {
			var input createCapacityReservationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.CreateCapacityReservation(
				input.Name, input.TargetDpus, tagsFromSlice(input.Tags),
			)
		},
		"CancelCapacityReservation": func(b []byte) (any, error) {
			var input cancelCapacityReservationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.CancelCapacityReservation(input.Name)
		},
		"DeleteCapacityReservation": func(b []byte) (any, error) {
			var input deleteCapacityReservationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeleteCapacityReservation(input.Name)
		},
	}
}

func (h *Handler) notebookOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"CreateNotebook": func(b []byte) (any, error) {
			var input createNotebookInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			id, err := h.Backend.CreateNotebook(input.WorkGroup, input.Name, tagsFromSlice(input.Tags))
			if err != nil {
				return nil, err
			}

			return map[string]any{"NotebookId": id}, nil
		},
		"CreatePresignedNotebookUrl": func(b []byte) (any, error) {
			var input createPresignedNotebookURLInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			url, err := h.Backend.CreatePresignedNotebookURL(input.SessionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{"NotebookSessionUrl": url}, nil
		},
		"DeleteNotebook": func(b []byte) (any, error) {
			var input deleteNotebookInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeleteNotebook(input.NotebookID)
		},
		"ExportNotebook": func(b []byte) (any, error) {
			var input exportNotebookInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			metadata, content, err := h.Backend.ExportNotebook(input.NotebookID)
			if err != nil {
				return nil, err
			}

			return map[string]any{
				"NotebookMetadata": metadata,
				"Payload":          content,
			}, nil
		},
	}
}

// doDispatch routes the operation to the appropriate handler.
func (h *Handler) doDispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatch[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownOperation, action)
	}

	response, err := fn(body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(response)
}

// handleError writes a standardized error response back to the client.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, reqErr error) error {
	log := logger.Load(ctx)
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	statusCode := http.StatusBadRequest

	var errorType string

	switch {
	case errors.Is(reqErr, ErrNotFound):
		errorType = errTypeInvalidRequest
	case errors.Is(reqErr, ErrAlreadyExists):
		errorType = errTypeInvalidRequest
	case errors.Is(reqErr, ErrProtected):
		errorType = errTypeInvalidRequest
	case errors.Is(reqErr, ErrValidation):
		errorType = errTypeInvalidRequest
	case errors.Is(reqErr, ErrUnknownOperation):
		errorType = errTypeInvalidRequest
	default:
		errorType = "InternalServerError"
		statusCode = http.StatusInternalServerError
	}

	if statusCode == http.StatusInternalServerError {
		log.ErrorContext(ctx, "Athena internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "Athena request error", "error", reqErr, "action", action)
	}

	errResp := service.JSONErrorResponse{
		Type:    errorType,
		Message: reqErr.Error(),
	}

	payload, _ := json.Marshal(errResp)

	return c.JSONBlob(statusCode, payload)
}

// tagsFromSlice converts a slice of Tag to a map[string]string.
func tagsFromSlice(tags []Tag) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}
