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
	// tokens signs and verifies opaque ListQueryExecutions pagination tokens.
	tokens *pageTokenCodec
	// dispatch is the pre-built immutable dispatch table, set once in NewHandler.
	dispatch map[string]athenaActionFn
}

// NewHandler creates a new Athena handler with the given storage backend.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend, tokens: newPageTokenCodec()}
	h.dispatch = h.buildDispatchTable()

	return h
}

// WithJanitor attaches a background janitor to the handler.
// If the backend is not an *InMemoryBackend, this is a no-op.
func (h *Handler) WithJanitor(
	interval, executionTTL time.Duration,
	taskTimeout ...time.Duration,
) *Handler {
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

// extendedSupportedOperations are the operations added after the initial
// AWS Athena operation set above.
func (h *Handler) extendedSupportedOperations() []string {
	return []string{
		"StartSession",
		"GetSession",
		"GetSessionStatus",
		"GetSessionEndpoint",
		"TerminateSession",
		"ListSessions",
		"ListNotebookSessions",
		"StartCalculationExecution",
		"GetCalculationExecution",
		"GetCalculationExecutionStatus",
		"GetCalculationExecutionCode",
		"StopCalculationExecution",
		"ListCalculationExecutions",
		"GetCapacityReservation",
		"ListCapacityReservations",
		"UpdateCapacityReservation",
		"PutCapacityAssignmentConfiguration",
		"GetCapacityAssignmentConfiguration",
		"GetDatabase",
		"ListDatabases",
		"GetTableMetadata",
		"ListTableMetadata",
		"GetNotebookMetadata",
		"ListNotebookMetadata",
		"ImportNotebook",
		"UpdateNotebook",
		"UpdateNotebookMetadata",
		"UpdateNamedQuery",
		"UpdatePreparedStatement",
		"ListExecutors",
		"ListEngineVersions",
		"ListApplicationDPUSizes",
		"GetQueryRuntimeStatistics",
		"GetResourceDashboard",
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

// resourceNameEnvelope is the minimal projection ExtractResource decodes. Only
// the "Name" field is needed for the telemetry resource label, so we decode into
// this one-field struct instead of unmarshalling the entire request body into a
// map[string]any on every request.
type resourceNameEnvelope struct {
	Name string `json:"Name"`
}

// ExtractResource extracts the primary resource name from the request body.
// It decodes only the "Name" field rather than the whole body: httputils.ReadBody
// caches and rewinds the body, so this does not conflict with the dispatcher's
// own read, and the typed decode avoids allocating a map for every field.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil || len(body) == 0 {
		return ""
	}

	var env resourceNameEnvelope
	if uerr := json.Unmarshal(body, &env); uerr != nil {
		return ""
	}

	return env.Name
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

// --- Dispatch ---

type athenaActionFn func([]byte) (any, error)

func (h *Handler) buildDispatchTable() map[string]athenaActionFn {
	ops := h.workGroupOps()
	maps.Copy(ops, h.namedQueryOps())
	maps.Copy(ops, h.namedQueryUpdateOps())
	maps.Copy(ops, h.dataCatalogOps())
	maps.Copy(ops, h.queryExecutionOps())
	maps.Copy(ops, h.tagOps())
	maps.Copy(ops, h.preparedStatementOps())
	maps.Copy(ops, h.capacityReservationOps())
	maps.Copy(ops, h.notebookOps())
	maps.Copy(ops, h.extendedDispatchTable())

	return ops
}

// extendedDispatchTable returns the dispatch entries for the operations added
// after the initial AWS Athena operation set.
func (h *Handler) extendedDispatchTable() map[string]athenaActionFn {
	ops := h.sessionCoreOps()
	maps.Copy(ops, h.sessionListOps())
	maps.Copy(ops, h.sessionInfoOps())
	maps.Copy(ops, h.calcCoreOps())
	maps.Copy(ops, h.calcControlOps())
	maps.Copy(ops, h.capacityExtraOps())
	maps.Copy(ops, h.databaseOps())
	maps.Copy(ops, h.notebookExtraOps())

	return ops
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
func (h *Handler) handleError(
	ctx context.Context,
	c *echo.Context,
	action string,
	reqErr error,
) error {
	log := logger.Load(ctx)
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	// AWS Athena returns HTTP 400 for every modeled client-side exception
	// (InvalidRequestException, MetadataException, ResourceNotFoundException,
	// SessionAlreadyExistsException, TooManyRequestsException); only the __type
	// distinguishes them. The order of these cases matters: the more specific
	// sentinels are checked before the InvalidRequestException-backed ones.
	statusCode := http.StatusBadRequest

	var errorType string

	switch {
	case errors.Is(reqErr, ErrResourceNotFound):
		errorType = errTypeResourceNotFoundExc
	case errors.Is(reqErr, ErrMetadata):
		errorType = errTypeMetadataExc
	case errors.Is(reqErr, ErrSessionExists):
		errorType = errTypeSessionExistsExc
	case errors.Is(reqErr, ErrNotFound),
		errors.Is(reqErr, ErrAlreadyExists),
		errors.Is(reqErr, ErrProtected),
		errors.Is(reqErr, ErrValidation),
		errors.Is(reqErr, ErrUnknownOperation):
		errorType = errTypeInvalidRequestExc
	default:
		errorType = errTypeInternalServer
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
