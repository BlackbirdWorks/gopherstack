package kinesisvideo

import (
	"encoding/json"
	"errors"
	"maps"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	kinesisVideoService       = "kinesisvideo"
	kinesisVideoMatchPriority = service.PriorityPathVersioned
)

// Operation names, matching the AWS API exactly.
const (
	opCreateStream        = "CreateStream"
	opDescribeStream      = "DescribeStream"
	opListStreams         = "ListStreams"
	opUpdateStream        = "UpdateStream"
	opDeleteStream        = "DeleteStream"
	opUpdateDataRetention = "UpdateDataRetention"
	opGetDataEndpoint     = "GetDataEndpoint"

	opTagStream         = "TagStream"
	opUntagStream       = "UntagStream"
	opListTagsForStream = "ListTagsForStream"

	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
	opListTagsForResource = "ListTagsForResource"

	opCreateSignalingChannel   = "CreateSignalingChannel"
	opDescribeSignalingChannel = "DescribeSignalingChannel"
	opListSignalingChannels    = "ListSignalingChannels"
	opUpdateSignalingChannel   = "UpdateSignalingChannel"
	opDeleteSignalingChannel   = "DeleteSignalingChannel"

	opDescribeImageGenerationConfiguration = "DescribeImageGenerationConfiguration"
	opUpdateImageGenerationConfiguration   = "UpdateImageGenerationConfiguration"
	opDescribeNotificationConfiguration    = "DescribeNotificationConfiguration"
	opUpdateNotificationConfiguration      = "UpdateNotificationConfiguration"
)

// URI paths. AWS emits camelCase action paths for every operation except the
// generic-tagging trio, which use PascalCase (confirmed against
// aws-sdk-go-v2/service/kinesisvideo@v1.41.1 serializers.go -- /TagResource,
// /UntagResource, /ListTagsForResource vs. e.g. /createStream).
const (
	pathCreateStream        = "/createStream"
	pathDescribeStream      = "/describeStream"
	pathListStreams         = "/listStreams"
	pathUpdateStream        = "/updateStream"
	pathDeleteStream        = "/deleteStream"
	pathUpdateDataRetention = "/updateDataRetention"
	pathGetDataEndpoint     = "/getDataEndpoint"

	pathTagStream         = "/tagStream"
	pathUntagStream       = "/untagStream"
	pathListTagsForStream = "/listTagsForStream"

	pathTagResource         = "/TagResource"
	pathUntagResource       = "/UntagResource"
	pathListTagsForResource = "/ListTagsForResource"

	pathCreateSignalingChannel   = "/createSignalingChannel"
	pathDescribeSignalingChannel = "/describeSignalingChannel"
	pathListSignalingChannels    = "/listSignalingChannels"
	pathUpdateSignalingChannel   = "/updateSignalingChannel"
	pathDeleteSignalingChannel   = "/deleteSignalingChannel"

	pathDescribeImageGenerationConfiguration = "/describeImageGenerationConfiguration"
	pathUpdateImageGenerationConfiguration   = "/updateImageGenerationConfiguration"
	pathDescribeNotificationConfiguration    = "/describeNotificationConfiguration"
	pathUpdateNotificationConfiguration      = "/updateNotificationConfiguration"
)

// kinesisVideoUniquePaths are claimed unconditionally: none of them are
// reused by any other service in this repo (verified by grep across
// services/*/*.go).
var kinesisVideoUniquePaths = map[string]string{ //nolint:gochecknoglobals // package-level routing table
	pathCreateStream:        opCreateStream,
	pathDescribeStream:      opDescribeStream,
	pathListStreams:         opListStreams,
	pathUpdateStream:        opUpdateStream,
	pathDeleteStream:        opDeleteStream,
	pathUpdateDataRetention: opUpdateDataRetention,
	pathGetDataEndpoint:     opGetDataEndpoint,

	pathTagStream:         opTagStream,
	pathUntagStream:       opUntagStream,
	pathListTagsForStream: opListTagsForStream,

	pathCreateSignalingChannel:   opCreateSignalingChannel,
	pathDescribeSignalingChannel: opDescribeSignalingChannel,
	pathListSignalingChannels:    opListSignalingChannels,
	pathUpdateSignalingChannel:   opUpdateSignalingChannel,
	pathDeleteSignalingChannel:   opDeleteSignalingChannel,

	pathDescribeImageGenerationConfiguration: opDescribeImageGenerationConfiguration,
	pathUpdateImageGenerationConfiguration:   opUpdateImageGenerationConfiguration,
	pathDescribeNotificationConfiguration:    opDescribeNotificationConfiguration,
	pathUpdateNotificationConfiguration:      opUpdateNotificationConfiguration,
}

// kinesisVideoSharedPaths are the generic-tagging paths several restjson1
// services claim verbatim (see services/rolesanywhere and services/xray).
// They are SigV4-scoped instead of claimed unconditionally, per
// .claude/memories -- route-matcher-prefix-collision.
var kinesisVideoSharedPaths = map[string]string{ //nolint:gochecknoglobals // package-level routing table
	pathTagResource:         opTagResource,
	pathUntagResource:       opUntagResource,
	pathListTagsForResource: opListTagsForResource,
}

// handlerFunc is the uniform signature for all dispatch operations.
type handlerFunc func(c *echo.Context, body []byte) error

// Handler is the HTTP handler for the Kinesis Video Streams control-plane REST API.
type Handler struct {
	Backend       StorageBackend
	ops           map[string]handlerFunc
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new Kinesis Video Streams handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "KinesisVideo" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(kinesisVideoUniquePaths)+len(kinesisVideoSharedPaths))
	for _, op := range kinesisVideoUniquePaths {
		ops = append(ops, op)
	}

	for _, op := range kinesisVideoSharedPaths {
		ops = append(ops, op)
	}

	return ops
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return kinesisVideoService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches Kinesis Video Streams REST API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		if _, ok := kinesisVideoUniquePaths[path]; ok {
			return true
		}

		if _, ok := kinesisVideoSharedPaths[path]; ok {
			svc := httputils.ExtractServiceFromRequest(c.Request())

			return svc == "" || svc == kinesisVideoService
		}

		return false
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return kinesisVideoMatchPriority }

// ExtractOperation extracts the operation name from the request path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	if op, ok := kinesisVideoUniquePaths[path]; ok {
		return op
	}

	if op, ok := kinesisVideoSharedPaths[path]; ok {
		return op
	}

	return ""
}

// ExtractResource extracts the stream or channel name/ARN from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	for _, key := range []string{"StreamName", "StreamARN", "ChannelName", "ChannelARN", "ResourceARN"} {
		if v, ok := data[key]; ok {
			if s, isStr := v.(string); isStr {
				return s
			}
		}
	}

	return ""
}

// Handler returns the Echo handler function for Kinesis Video Streams requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		path := c.Request().URL.Path

		fn, ok := h.ops[path]
		if !ok {
			return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "unknown operation")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "kinesisvideo: failed to read request body", "error", err)

			return h.writeError(
				c,
				http.StatusInternalServerError,
				"InternalFailureException",
				"failed to read request body",
			)
		}

		log.DebugContext(ctx, "kinesisvideo request", "path", path)

		return fn(c, body)
	}
}

// buildOps constructs the operation dispatch map keyed by URI path.
func (h *Handler) buildOps() map[string]handlerFunc {
	ops := make(map[string]handlerFunc)

	maps.Copy(ops, h.buildStreamOps())

	maps.Copy(ops, h.buildTagOps())

	maps.Copy(ops, h.buildSignalingOps())

	maps.Copy(ops, h.buildConfigOps())

	return ops
}

// listAndConvert runs a paginated backend list call and converts each result
// item to its wire DTO. Shared by handleListStreams and
// handleListSignalingChannels, whose only difference is the resource and DTO
// type parameters.
func listAndConvert[T, D any](
	list func() ([]*T, string, error),
	toDTO func(*T) D,
) ([]D, string, error) {
	items, next, err := list()
	if err != nil {
		return nil, "", err
	}

	dtos := make([]D, 0, len(items))
	for _, item := range items {
		dtos = append(dtos, toDTO(item))
	}

	return dtos, next, nil
}

// writeJSON writes a 200 JSON response.
func (h *Handler) writeJSON(c *echo.Context, v any) error {
	return c.JSON(http.StatusOK, v)
}

// writeError writes a Kinesis Video Streams JSON error response with the AWS __type field.
func (h *Handler) writeError(c *echo.Context, status int, errType, message string) error {
	return c.JSON(status, errorResponse{Type: errType, Message: message})
}

// writeBackendError maps a backend error to an HTTP error response with the appropriate AWS error type.
func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):
		return h.writeError(c, http.StatusBadRequest, "ResourceInUseException", err.Error())
	case errors.Is(err, awserr.ErrConflict):
		return h.writeError(c, http.StatusBadRequest, "VersionMismatchException", err.Error())
	case errors.Is(err, awserr.ErrInvalidParameter):
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", err.Error())
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalFailureException", err.Error())
	}
}
