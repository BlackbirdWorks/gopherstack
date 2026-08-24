package datasync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	datasyncTargetPrefix = "FmrsService."
	matchPriority        = service.PriorityHeaderExact
	contentType          = "application/x-amz-json-1.1"

	keyType    = "__type"
	keyMessage = "message"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler handles DataSync HTTP requests.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	h := &Handler{Backend: b}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "DataSync" }

// Reset resets the backend and rebuilds the dispatch table.
func (h *Handler) Reset() {
	h.Backend.Reset()
	h.ops = h.buildOps()
}

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateAgent",
		"DescribeAgent",
		"UpdateAgent",
		"DeleteAgent",
		"ListAgents",
		"CreateLocationS3",
		"DescribeLocationS3",
		"DeleteLocation",
		"ListLocations",
		"CreateTask",
		"DescribeTask",
		"UpdateTask",
		"DeleteTask",
		"ListTasks",
		"StartTaskExecution",
		"CancelTaskExecution",
		"DescribeTaskExecution",
		"ListTaskExecutions",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"UpdateLocationS3",
		"UpdateTaskExecution",
		"CreateLocationAzureBlob",
		"DescribeLocationAzureBlob",
		"UpdateLocationAzureBlob",
		"CreateLocationEfs",
		"DescribeLocationEfs",
		"UpdateLocationEfs",
		"CreateLocationFsxLustre",
		"DescribeLocationFsxLustre",
		"UpdateLocationFsxLustre",
		"CreateLocationFsxOntap",
		"DescribeLocationFsxOntap",
		"UpdateLocationFsxOntap",
		"CreateLocationFsxOpenZfs",
		"DescribeLocationFsxOpenZfs",
		"UpdateLocationFsxOpenZfs",
		"CreateLocationFsxWindows",
		"DescribeLocationFsxWindows",
		"UpdateLocationFsxWindows",
		"CreateLocationHdfs",
		"DescribeLocationHdfs",
		"UpdateLocationHdfs",
		"CreateLocationNfs",
		"DescribeLocationNfs",
		"UpdateLocationNfs",
		"CreateLocationObjectStorage",
		"DescribeLocationObjectStorage",
		"UpdateLocationObjectStorage",
		"CreateLocationSmb",
		"DescribeLocationSmb",
		"UpdateLocationSmb",
	}
}

// RouteMatcher returns a function that matches DataSync API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), datasyncTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, datasyncTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request body.
func (h *Handler) ExtractResource(_ *echo.Context) string { return "" }

// Handler returns the Echo handler function for DataSync requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"DataSync", contentType,
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateAgent":           service.WrapOp(h.handleCreateAgent),
		"DescribeAgent":         service.WrapOp(h.handleDescribeAgent),
		"UpdateAgent":           service.WrapOp(h.handleUpdateAgent),
		"DeleteAgent":           service.WrapOp(h.handleDeleteAgent),
		"ListAgents":            service.WrapOp(h.handleListAgents),
		"CreateLocationS3":      service.WrapOp(h.handleCreateLocationS3),
		"DescribeLocationS3":    service.WrapOp(h.handleDescribeLocationS3),
		"DeleteLocation":        service.WrapOp(h.handleDeleteLocation),
		"ListLocations":         service.WrapOp(h.handleListLocations),
		"CreateTask":            service.WrapOp(h.handleCreateTask),
		"DescribeTask":          service.WrapOp(h.handleDescribeTask),
		"UpdateTask":            service.WrapOp(h.handleUpdateTask),
		"DeleteTask":            service.WrapOp(h.handleDeleteTask),
		"ListTasks":             service.WrapOp(h.handleListTasks),
		"StartTaskExecution":    service.WrapOp(h.handleStartTaskExecution),
		"CancelTaskExecution":   service.WrapOp(h.handleCancelTaskExecution),
		"DescribeTaskExecution": service.WrapOp(h.handleDescribeTaskExecution),
		"ListTaskExecutions":    service.WrapOp(h.handleListTaskExecutions),
		"TagResource":           service.WrapOp(h.handleTagResource),
		"UntagResource":         service.WrapOp(h.handleUntagResource),
		"ListTagsForResource":   service.WrapOp(h.handleListTagsForResource),
		// Extended location operations
		"UpdateLocationS3":              service.WrapOp(h.handleUpdateLocationS3),
		"UpdateTaskExecution":           service.WrapOp(h.handleUpdateTaskExecution),
		"CreateLocationAzureBlob":       service.WrapOp(h.handleCreateLocationAzureBlob),
		"DescribeLocationAzureBlob":     service.WrapOp(h.handleDescribeLocationAzureBlob),
		"UpdateLocationAzureBlob":       service.WrapOp(h.handleUpdateLocationAzureBlob),
		"CreateLocationEfs":             service.WrapOp(h.handleCreateLocationEfs),
		"DescribeLocationEfs":           service.WrapOp(h.handleDescribeLocationEfs),
		"UpdateLocationEfs":             service.WrapOp(h.handleUpdateLocationEfs),
		"CreateLocationFsxLustre":       service.WrapOp(h.handleCreateLocationFsxLustre),
		"DescribeLocationFsxLustre":     service.WrapOp(h.handleDescribeLocationFsxLustre),
		"UpdateLocationFsxLustre":       service.WrapOp(h.handleUpdateLocationFsxLustre),
		"CreateLocationFsxOntap":        service.WrapOp(h.handleCreateLocationFsxOntap),
		"DescribeLocationFsxOntap":      service.WrapOp(h.handleDescribeLocationFsxOntap),
		"UpdateLocationFsxOntap":        service.WrapOp(h.handleUpdateLocationFsxOntap),
		"CreateLocationFsxOpenZfs":      service.WrapOp(h.handleCreateLocationFsxOpenZfs),
		"DescribeLocationFsxOpenZfs":    service.WrapOp(h.handleDescribeLocationFsxOpenZfs),
		"UpdateLocationFsxOpenZfs":      service.WrapOp(h.handleUpdateLocationFsxOpenZfs),
		"CreateLocationFsxWindows":      service.WrapOp(h.handleCreateLocationFsxWindows),
		"DescribeLocationFsxWindows":    service.WrapOp(h.handleDescribeLocationFsxWindows),
		"UpdateLocationFsxWindows":      service.WrapOp(h.handleUpdateLocationFsxWindows),
		"CreateLocationHdfs":            service.WrapOp(h.handleCreateLocationHdfs),
		"DescribeLocationHdfs":          service.WrapOp(h.handleDescribeLocationHdfs),
		"UpdateLocationHdfs":            service.WrapOp(h.handleUpdateLocationHdfs),
		"CreateLocationNfs":             service.WrapOp(h.handleCreateLocationNfs),
		"DescribeLocationNfs":           service.WrapOp(h.handleDescribeLocationNfs),
		"UpdateLocationNfs":             service.WrapOp(h.handleUpdateLocationNfs),
		"CreateLocationObjectStorage":   service.WrapOp(h.handleCreateLocationObjectStorage),
		"DescribeLocationObjectStorage": service.WrapOp(h.handleDescribeLocationObjectStorage),
		"UpdateLocationObjectStorage":   service.WrapOp(h.handleUpdateLocationObjectStorage),
		"CreateLocationSmb":             service.WrapOp(h.handleCreateLocationSmb),
		"DescribeLocationSmb":           service.WrapOp(h.handleDescribeLocationSmb),
		"UpdateLocationSmb":             service.WrapOp(h.handleUpdateLocationSmb),
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
	// ErrNotFound/ErrAlreadyExists deliberately map to the SAME wire code as
	// ErrInvalidParameter below: every one of datasync's 53
	// awsAwsjson11_deserializeOpError<Op> switches (aws-sdk-go-v2/service/
	// datasync@v1.61.4 deserializers.go) types only InternalException and
	// InvalidRequestException -- confirmed against types/errors.go, which
	// defines exactly those two exception structs and no
	// ResourceNotFoundException/ResourceExistsException type at all, in the
	// whole service. Emitting either fabricated code here decodes client-side
	// as an untyped smithy.GenericAPIError for every not-found/already-exists
	// condition in datasync, not just one operation.
	case errors.Is(err, awserr.ErrNotFound),
		errors.Is(err, awserr.ErrAlreadyExists),
		errors.Is(err, awserr.ErrInvalidParameter),
		errors.Is(err, errInvalidRequest),
		errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr),
		errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyType:    "InvalidRequestException",
			keyMessage: err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			keyType:    internalExceptionType,
			keyMessage: err.Error(),
		})
	}
}
