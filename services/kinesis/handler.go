package kinesis

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Handler is the Echo HTTP handler for Kinesis operations.
type Handler struct {
	Backend       StorageBackend
	janitor       *Janitor
	ops           map[string]kinesisDispatchFn
	DefaultRegion string
	AccountID     string
}

// NewHandler creates a new Kinesis Handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{
		Backend: backend,
	}
	h.ops = h.buildOps()

	return h
}

// WithJanitor attaches a background janitor to the handler.
// If the backend is not an *InMemoryBackend, this is a no-op.
func (h *Handler) WithJanitor(interval time.Duration, taskTimeout ...time.Duration) *Handler {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		j := NewJanitor(mem, interval)
		if len(taskTimeout) > 0 {
			j.TaskTimeout = taskTimeout[0]
		}
		h.janitor = j
	}

	return h
}

// StartWorker starts the background janitor if one is configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// StopWorker stops the background worker if one is configured.
func (h *Handler) StopWorker() {
	if h.janitor != nil {
		h.janitor.Stop()
	}
}

// defaultRegion returns the region the handler should fall back to when a
// request carries no SigV4 region. It prefers the explicitly configured
// DefaultRegion and otherwise mirrors the backend's region so that the
// handler-level tag store and the backend's stream store agree on the region.
func (h *Handler) defaultRegion() string {
	if h.DefaultRegion != "" {
		return h.DefaultRegion
	}

	if br, ok := h.Backend.(interface{ Region() string }); ok {
		return br.Region()
	}

	return h.DefaultRegion
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "Kinesis"
}

// GetSupportedOperations returns the list of supported Kinesis operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateStream",
		"DeleteStream",
		"DescribeStream",
		"DescribeStreamSummary",
		"ListStreams",
		"PutRecord",
		"PutRecords",
		"GetShardIterator",
		"GetRecords",
		"ListShards",
		"AddTagsToStream",
		"RemoveTagsFromStream",
		"ListTagsForStream",
		"TagResource",
		"UntagResource",
		"IncreaseStreamRetentionPeriod",
		"DecreaseStreamRetentionPeriod",
		"RegisterStreamConsumer",
		"DescribeStreamConsumer",
		"ListStreamConsumers",
		"DeregisterStreamConsumer",
		"SubscribeToShard",
		"UpdateShardCount",
		"EnableEnhancedMonitoring",
		"DisableEnhancedMonitoring",
		"DescribeLimits",
		"DescribeAccountSettings",
		"UpdateAccountSettings",
		"UpdateMaxRecordSize",
		"UpdateStreamWarmThroughput",
		"MergeShards",
		"SplitShard",
		"StartStreamEncryption",
		"StopStreamEncryption",
		"DeleteResourcePolicy",
		"GetResourcePolicy",
		"PutResourcePolicy",
		"ListTagsForResource",
		"UpdateStreamMode",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "kinesis" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Kinesis instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.defaultRegion()} }

// kinesisTargetPrefix is the X-Amz-Target prefix used by the AWS Kinesis SDK.
const kinesisTargetPrefix = "Kinesis_20131202."

// RouteMatcher returns a function that matches incoming Kinesis requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), kinesisTargetPrefix)
	}
}

// MatchPriority returns the routing priority for the Kinesis handler.
func (h *Handler) MatchPriority() int {
	return service.PriorityTargetPrefixed
}

// ExtractOperation extracts the Kinesis action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, kinesisTargetPrefix)

	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

type extractStreamNameInput struct {
	StreamName string `json:"StreamName"`
}

// ExtractResource extracts the stream name from the JSON request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req extractStreamNameInput

	if err = json.Unmarshal(body, &req); err != nil {
		return ""
	}

	return req.StreamName
}

// Handler returns the Echo handler function for Kinesis operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// SubscribeToShard uses the AWS event-stream binary protocol and must be
		// dispatched before the normal JSON target handler.
		if c.Request().Header.Get("X-Amz-Target") == kinesisTargetPrefix+"SubscribeToShard" {
			return h.handleSubscribeToShardHTTP(c)
		}

		if service.IsCBORRequest(c.Request()) {
			ctx := c.Request().Context()
			log := logger.Load(ctx)
			target := c.Request().Header.Get("X-Amz-Target")
			action := strings.TrimPrefix(target, kinesisTargetPrefix)

			return h.handleCBORRequest(ctx, c, log, action)
		}

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Kinesis", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.kinesisRoute(ctx, c.Request(), action, body)
			},
			h.handleError,
		)
	}
}

type kinesisDispatchFn func(ctx context.Context, r *http.Request, body []byte) (any, error)

func (h *Handler) buildOps() map[string]kinesisDispatchFn {
	return map[string]kinesisDispatchFn{
		"CreateStream":                  h.handleCreateStream,
		"DeleteStream":                  h.handleDeleteStream,
		"DescribeStream":                h.handleDescribeStream,
		"DescribeStreamSummary":         h.handleDescribeStreamSummary,
		"ListStreams":                   h.handleListStreams,
		"PutRecord":                     h.handlePutRecord,
		"PutRecords":                    h.handlePutRecords,
		"GetShardIterator":              h.handleGetShardIterator,
		"GetRecords":                    h.handleGetRecords,
		"ListShards":                    h.handleListShards,
		"AddTagsToStream":               h.handleAddTagsToStream,
		"RemoveTagsFromStream":          h.handleRemoveTagsFromStream,
		"ListTagsForStream":             h.handleListTagsForStream,
		"TagResource":                   h.handleTagResource,
		"UntagResource":                 h.handleUntagResource,
		"IncreaseStreamRetentionPeriod": h.handleIncreaseStreamRetentionPeriod,
		"DecreaseStreamRetentionPeriod": h.handleDecreaseStreamRetentionPeriod,
		"DescribeLimits":                h.handleDescribeLimits,
		"DescribeAccountSettings":       h.handleDescribeAccountSettings,
		"UpdateAccountSettings":         h.handleUpdateAccountSettings,
		"UpdateMaxRecordSize":           h.handleUpdateMaxRecordSize,
		"UpdateStreamWarmThroughput":    h.handleUpdateStreamWarmThroughput,
		"MergeShards":                   h.handleMergeShards,
		"SplitShard":                    h.handleSplitShard,
		"StartStreamEncryption":         h.handleStartStreamEncryption,
		"StopStreamEncryption":          h.handleStopStreamEncryption,
		"DeleteResourcePolicy":          h.handleDeleteResourcePolicy,
		"GetResourcePolicy":             h.handleGetResourcePolicy,
		"PutResourcePolicy":             h.handlePutResourcePolicy,
		"ListTagsForResource":           h.handleListTagsForResource,
		"UpdateStreamMode":              h.handleUpdateStreamMode,
		"RegisterStreamConsumer":        h.handleRegisterStreamConsumer,
		"DescribeStreamConsumer":        h.handleDescribeStreamConsumer,
		"ListStreamConsumers":           h.handleListStreamConsumers,
		"DeregisterStreamConsumer":      h.handleDeregisterStreamConsumer,
		"UpdateShardCount":              h.handleUpdateShardCount,
		"EnableEnhancedMonitoring":      h.handleEnableEnhancedMonitoring,
		"DisableEnhancedMonitoring":     h.handleDisableEnhancedMonitoring,
	}
}

// kinesisRoute dispatches a Kinesis action to the appropriate handler method.
// It resolves the per-request AWS region from the SigV4 credential scope and
// attaches it to the context so the backend routes the operation to the right
// region's resources.
func (h *Handler) kinesisRoute(ctx context.Context, r *http.Request, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, ErrUnknownAction
	}

	region := httputils.ExtractRegionFromRequest(r, h.defaultRegion())
	ctx = contextWithRegion(ctx, region)

	result, err := fn(ctx, r, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// handleError writes a Kinesis error response using the standard error details mapping.
func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	errType, message, status := errorDetails(err)

	return c.JSON(status, jsonKinesisError{Type: errType, Message: message})
}

type jsonKinesisError struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// errTypeResourceNotFound is the Kinesis error type string for resource not found errors.
const errTypeResourceNotFound = "ResourceNotFoundException"

// kmsErrorDetails maps the KMS-specific sentinels StartStreamEncryption can
// surface (see stream_encryption.go's resolveKMSKey) to their AWS error type,
// message, and HTTP status. Split out of errorDetails to keep its cyclomatic
// complexity down; returns ok=false when err doesn't match any of them.
func kmsErrorDetails(err error) (string, string, int, bool) {
	switch {
	case errors.Is(err, ErrKMSNotFound):
		return "KMSNotFoundException",
			"The specified KMS key was not found.",
			http.StatusBadRequest, true
	case errors.Is(err, ErrKMSDisabled):
		return "KMSDisabledException",
			"The specified KMS key is disabled.",
			http.StatusBadRequest, true
	case errors.Is(err, ErrKMSInvalidState):
		return "KMSInvalidStateException",
			"The specified KMS key is not in a valid state for this operation.",
			http.StatusBadRequest, true
	case errors.Is(err, ErrKMSAccessDenied):
		return "KMSAccessDeniedException",
			"Access to the specified KMS key is denied.",
			http.StatusBadRequest, true
	default:
		return "", "", 0, false
	}
}

// resourceErrorDetails maps the resource-existence/conflict sentinels
// (streams, consumers, resource policies) to their AWS error type, message,
// and HTTP status. Split out of errorDetails to keep its cyclomatic
// complexity down; returns ok=false when err doesn't match any of them.
func resourceErrorDetails(err error) (string, string, int, bool) {
	switch {
	case errors.Is(err, ErrStreamNotFound):
		return errTypeResourceNotFound,
			"Stream not found.",
			http.StatusBadRequest, true
	case errors.Is(err, ErrStreamAlreadyExists):
		return "ResourceInUseException",
			"A stream with this name already exists.",
			http.StatusBadRequest, true
	case errors.Is(err, ErrConsumerNotFound):
		return errTypeResourceNotFound,
			"Consumer not found.",
			http.StatusBadRequest, true
	case errors.Is(err, ErrConsumerAlreadyExists):
		return "ResourceInUseException",
			"A consumer with this name already exists.",
			http.StatusBadRequest, true
	case errors.Is(err, ErrResourcePolicyNotFound):
		return errTypeResourceNotFound,
			"Resource policy not found.",
			http.StatusBadRequest, true
	default:
		return "", "", 0, false
	}
}

// errorDetails maps an error to its Kinesis JSON error type, message, and HTTP status.
func errorDetails(err error) (string, string, int) {
	if errType, message, status, ok := kmsErrorDetails(err); ok {
		return errType, message, status
	}
	if errType, message, status, ok := resourceErrorDetails(err); ok {
		return errType, message, status
	}

	switch {
	case errors.Is(err, ErrProvisionedThroughputExceeded):
		return "ProvisionedThroughputExceededException",
			"Rate exceeded for shard.",
			http.StatusBadRequest
	case errors.Is(err, ErrTagLimitExceeded):
		return "LimitExceededException",
			"Tag limit exceeded. A stream can have at most 50 tags.",
			http.StatusBadRequest
	case errors.Is(err, ErrLimitExceeded):
		return "LimitExceededException",
			"Limit exceeded.",
			http.StatusBadRequest
	case errors.Is(err, ErrInvalidArgument):
		return "InvalidArgumentException",
			"Invalid argument.",
			http.StatusBadRequest
	case errors.Is(err, ErrShardCountScaling):
		return errTypeValidation, err.Error(), http.StatusBadRequest
	case errors.Is(err, ErrValidation):
		return errTypeValidation, err.Error(), http.StatusBadRequest
	case errors.Is(err, ErrShardIteratorExpired):
		return "ExpiredIteratorException",
			"The shard iterator has expired.",
			http.StatusBadRequest
	case errors.Is(err, ErrUnknownAction):
		return "UnknownOperationException",
			"The requested operation is not recognized.",
			http.StatusBadRequest
	default:
		return "InternalFailureException",
			"An internal error occurred.",
			http.StatusInternalServerError
	}
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	if r, ok := h.Backend.(resetter); ok {
		r.Reset()
	}
}

// Purge implements service.Purgeable by removing all Kinesis streams older than cutoff.
func (h *Handler) Purge(ctx context.Context, cutoff time.Time) {
	if p, ok := h.Backend.(purger); ok {
		p.Purge(ctx, cutoff)
	}
}
