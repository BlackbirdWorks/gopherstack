package emr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	emrTargetPrefix = "ElasticMapReduce."
	unknownAction   = "Unknown"
)

var errUnknownAction = errors.New("UnknownOperationException")

// Handler is the Echo HTTP handler for AWS EMR operations.
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]service.JSONOpFunc
	janitor *Janitor
}

// NewHandler creates a new EMR handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// WithJanitor attaches a background janitor to the handler.
func (h *Handler) WithJanitor(interval, terminatedTTL time.Duration, taskTimeout ...time.Duration) *Handler {
	j := NewJanitor(h.Backend, interval, terminatedTTL)
	if len(taskTimeout) > 0 {
		j.TaskTimeout = taskTimeout[0]
	}

	h.janitor = j

	return h
}

// StartWorker starts the background janitor if configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// Reset clears all in-memory state from the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "EMR" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"RunJobFlow",
		"DescribeCluster",
		"ListClusters",
		"TerminateJobFlows",
		"AddTags",
		"RemoveTags",
		"ListTagsForResource",
		"ListSteps",
		"AddJobFlowSteps",
		"ListInstanceGroups",
		"ListInstanceFleets",
		"ListBootstrapActions",
		"GetAutoTerminationPolicy",
		"GetManagedScalingPolicy",
		"AddInstanceFleet",
		"AddInstanceGroups",
		"CancelSteps",
		"CreatePersistentAppUI",
		"CreateSecurityConfiguration",
		"CreateStudio",
		"CreateStudioSessionMapping",
		"DeleteSecurityConfiguration",
		"DeleteStudio",
		"DeleteStudioSessionMapping",
		"DescribeSecurityConfiguration",
		"DescribeJobFlows",
		"DescribeNotebookExecution",
		"DescribePersistentAppUI",
		"DescribeReleaseLabel",
		"DescribeStep",
		"DescribeStudio",
		"GetBlockPublicAccessConfiguration",
		"GetClusterSessionCredentials",
		"GetOnClusterAppUIPresignedURL",
		"GetPersistentAppUIPresignedURL",
		"GetStudioSessionMapping",
		"ListInstances",
		"ListNotebookExecutions",
		"ListReleaseLabels",
		"ListSecurityConfigurations",
		"ListStudioSessionMappings",
		"ListStudios",
		"ListSupportedInstanceTypes",
		"ModifyCluster",
		"ModifyInstanceFleet",
		"ModifyInstanceGroups",
		"PutAutoScalingPolicy",
		"PutAutoTerminationPolicy",
		"PutBlockPublicAccessConfiguration",
		"PutManagedScalingPolicy",
		"RemoveAutoScalingPolicy",
		"RemoveAutoTerminationPolicy",
		"RemoveManagedScalingPolicy",
		"SetKeepJobFlowAliveWhenNoSteps",
		"SetTerminationProtection",
		"SetUnhealthyNodeReplacement",
		"SetVisibleToAllUsers",
		"StartNotebookExecution",
		"StopNotebookExecution",
		"UpdateStudio",
		"UpdateStudioSessionMapping",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "emr" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches EMR requests via X-Amz-Target.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, emrTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation returns the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, emrTargetPrefix)

	if action == "" || action == target {
		return unknownAction
	}

	return action
}

// ExtractResource extracts a resource identifier from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		ClusterID         string `json:"ClusterId"`
		JobFlowID         string `json:"JobFlowId"`
		ResourceID        string `json:"ResourceId"`
		StudioID          string `json:"StudioId"`
		TargetResourceArn string `json:"TargetResourceArn"`
	}

	_ = json.Unmarshal(body, &req)

	switch {
	case req.ClusterID != "":
		return req.ClusterID
	case req.JobFlowID != "":
		return req.JobFlowID
	case req.ResourceID != "":
		return req.ResourceID
	case req.StudioID != "":
		return req.StudioID
	case req.TargetResourceArn != "":
		return req.TargetResourceArn
	}

	return ""
}

// Handler returns the Echo handler function for EMR requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// Resolve the per-request region (from SigV4 / X-Amz-Region) and attach
		// it to the context so backend operations are region-scoped.
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"EMR", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(context.WithValue(ctx, regionContextKey{}, region), action, body)
			},
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"RunJobFlow":                        service.WrapOp(h.handleRunJobFlow),
		"DescribeCluster":                   service.WrapOp(h.handleDescribeCluster),
		"ListClusters":                      service.WrapOp(h.handleListClusters),
		"TerminateJobFlows":                 service.WrapOp(h.handleTerminateJobFlows),
		"AddTags":                           service.WrapOp(h.handleAddTags),
		"RemoveTags":                        service.WrapOp(h.handleRemoveTags),
		"ListTagsForResource":               service.WrapOp(h.handleListTagsForResource),
		"ListSteps":                         service.WrapOp(h.handleListSteps),
		"AddJobFlowSteps":                   service.WrapOp(h.handleAddJobFlowSteps),
		"ListInstanceGroups":                service.WrapOp(h.handleListInstanceGroups),
		"ListInstanceFleets":                service.WrapOp(h.handleListInstanceFleets),
		"ListBootstrapActions":              service.WrapOp(h.handleListBootstrapActions),
		"GetAutoTerminationPolicy":          service.WrapOp(h.handleGetAutoTerminationPolicy),
		"GetManagedScalingPolicy":           service.WrapOp(h.handleGetManagedScalingPolicy),
		"AddInstanceFleet":                  service.WrapOp(h.handleAddInstanceFleet),
		"AddInstanceGroups":                 service.WrapOp(h.handleAddInstanceGroups),
		"CancelSteps":                       service.WrapOp(h.handleCancelSteps),
		"CreatePersistentAppUI":             service.WrapOp(h.handleCreatePersistentAppUI),
		"CreateSecurityConfiguration":       service.WrapOp(h.handleCreateSecurityConfiguration),
		"CreateStudio":                      service.WrapOp(h.handleCreateStudio),
		"CreateStudioSessionMapping":        service.WrapOp(h.handleCreateStudioSessionMapping),
		"DeleteSecurityConfiguration":       service.WrapOp(h.handleDeleteSecurityConfiguration),
		"DeleteStudio":                      service.WrapOp(h.handleDeleteStudio),
		"DeleteStudioSessionMapping":        service.WrapOp(h.handleDeleteStudioSessionMapping),
		"DescribeSecurityConfiguration":     service.WrapOp(h.handleDescribeSecurityConfiguration),
		"DescribeJobFlows":                  service.WrapOp(h.handleDescribeJobFlows),
		"DescribeNotebookExecution":         service.WrapOp(h.handleDescribeNotebookExecution),
		"DescribePersistentAppUI":           service.WrapOp(h.handleDescribePersistentAppUI),
		"DescribeReleaseLabel":              service.WrapOp(h.handleDescribeReleaseLabel),
		"DescribeStep":                      service.WrapOp(h.handleDescribeStep),
		"DescribeStudio":                    service.WrapOp(h.handleDescribeStudio),
		"GetBlockPublicAccessConfiguration": service.WrapOp(h.handleGetBlockPublicAccessConfiguration),
		"GetClusterSessionCredentials":      service.WrapOp(h.handleGetClusterSessionCredentials),
		"GetOnClusterAppUIPresignedURL":     service.WrapOp(h.handleGetOnClusterAppUIPresignedURL),
		"GetPersistentAppUIPresignedURL":    service.WrapOp(h.handleGetPersistentAppUIPresignedURL),
		"GetStudioSessionMapping":           service.WrapOp(h.handleGetStudioSessionMapping),
		"ListInstances":                     service.WrapOp(h.handleListInstances),
		"ListNotebookExecutions":            service.WrapOp(h.handleListNotebookExecutions),
		"ListReleaseLabels":                 service.WrapOp(h.handleListReleaseLabels),
		"ListSecurityConfigurations":        service.WrapOp(h.handleListSecurityConfigurations),
		"ListStudioSessionMappings":         service.WrapOp(h.handleListStudioSessionMappings),
		"ListStudios":                       service.WrapOp(h.handleListStudios),
		"ListSupportedInstanceTypes":        service.WrapOp(h.handleListSupportedInstanceTypes),
		"ModifyCluster":                     service.WrapOp(h.handleModifyCluster),
		"ModifyInstanceFleet":               service.WrapOp(h.handleModifyInstanceFleet),
		"ModifyInstanceGroups":              service.WrapOp(h.handleModifyInstanceGroups),
		"PutAutoScalingPolicy":              service.WrapOp(h.handlePutAutoScalingPolicy),
		"PutAutoTerminationPolicy":          service.WrapOp(h.handlePutAutoTerminationPolicy),
		"PutBlockPublicAccessConfiguration": service.WrapOp(h.handlePutBlockPublicAccessConfiguration),
		"PutManagedScalingPolicy":           service.WrapOp(h.handlePutManagedScalingPolicy),
		"RemoveAutoScalingPolicy":           service.WrapOp(h.handleRemoveAutoScalingPolicy),
		"RemoveAutoTerminationPolicy":       service.WrapOp(h.handleRemoveAutoTerminationPolicy),
		"RemoveManagedScalingPolicy":        service.WrapOp(h.handleRemoveManagedScalingPolicy),
		"SetKeepJobFlowAliveWhenNoSteps":    service.WrapOp(h.handleSetKeepJobFlowAliveWhenNoSteps),
		"SetTerminationProtection":          service.WrapOp(h.handleSetTerminationProtection),
		"SetUnhealthyNodeReplacement":       service.WrapOp(h.handleSetUnhealthyNodeReplacement),
		"SetVisibleToAllUsers":              service.WrapOp(h.handleSetVisibleToAllUsers),
		"StartNotebookExecution":            service.WrapOp(h.handleStartNotebookExecution),
		"StopNotebookExecution":             service.WrapOp(h.handleStopNotebookExecution),
		"UpdateStudio":                      service.WrapOp(h.handleUpdateStudio),
		"UpdateStudioSessionMapping":        service.WrapOp(h.handleUpdateStudioSessionMapping),
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

// handleError maps backend errors to EMR's wire error shape. EMR's modeled
// error surface (aws-sdk-go-v2/service/emr/types/errors.go) has exactly two
// exception types -- InvalidRequestException (client fault, 400) and
// InternalServerException (server fault, 500); the SDK's deserializeError
// dispatch matches the "__type" value against those two strings verbatim
// (case-insensitively) and falls back to a generic, untyped error for
// anything else, so "ValidationException"/"InternalFailure" (neither of
// which EMR defines) would silently fail errors.As(*types.InvalidRequestException)
// / errors.As(*types.InternalServerException) checks in a real client.
func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", err.Error()))
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", err.Error()))
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", err.Error()))
	case errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusBadRequest, errorResponse("UnknownOperationException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerException", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}

// epochSecondsToTime converts a wire-format epoch-seconds JSON number (the
// awsjson1.1 Timestamp format the real EMR SDK serializer uses for
// CreatedAfter/CreatedBefore -- see smithytime.FormatEpochSeconds) into a
// time.Time, preserving sub-second precision carried in the fractional part.
func epochSecondsToTime(sec float64) time.Time {
	whole := int64(sec)
	frac := sec - float64(whole)

	return time.Unix(whole, int64(frac*float64(time.Second))).UTC()
}

type emptyOutput struct{}
