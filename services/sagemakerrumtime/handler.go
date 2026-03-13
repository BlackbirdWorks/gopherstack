package sagemakerrumtime

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	sagemakerRuntimeService       = "sagemaker-runtime"
	sagemakerRuntimePathPrefix    = "/endpoints/"
	sagemakerRuntimeMatchPriority = service.PriorityPathVersioned
)

// Event stream frame constants (AWS binary event stream protocol).
const (
	eventStreamPreludeLen = 12 // 4 (total-len) + 4 (headers-len) + 4 (prelude-CRC)
	eventStreamMsgCRCLen  = 4

	// eventStreamHeaderValueTypeString is the AWS event stream type byte for string headers.
	eventStreamHeaderValueTypeString = 7
	// eventStreamHeaderValueLenBytes is the number of bytes used to encode a header value length.
	eventStreamHeaderValueLenBytes = 2
)

// Handler is the Echo HTTP handler for AWS SageMaker Runtime operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new SageMaker Runtime handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "SageMakerRuntime" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"InvokeEndpoint",
		"InvokeEndpointAsync",
		"InvokeEndpointWithResponseStream",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return sagemakerRuntimeService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches SageMaker Runtime requests.
// It matches requests to paths beginning with /endpoints/ for the sagemaker-runtime SigV4 service.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()

		if !strings.HasPrefix(r.URL.Path, sagemakerRuntimePathPrefix) {
			return false
		}

		svc := httputils.ExtractServiceFromRequest(r)

		return svc == sagemakerRuntimeService
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return sagemakerRuntimeMatchPriority }

// ExtractOperation returns the operation name derived from the request path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return pathToOperation(c.Request().URL.Path)
}

// ExtractResource extracts the endpoint name from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return extractEndpointName(c.Request().URL.Path)
}

// Handler returns the Echo handler function for SageMaker Runtime requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		log := logger.Load(r.Context())

		if r.Method != http.MethodPost {
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("ValidationException", "method not allowed"))
		}

		body, err := httputils.ReadBody(r)
		if err != nil {
			log.ErrorContext(r.Context(), "sagemakerrumtime: failed to read request body", "error", err)

			return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
		}

		endpointName := extractEndpointName(r.URL.Path)
		if endpointName == "" {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "missing EndpointName in path"))
		}

		op := pathToOperation(r.URL.Path)

		switch op {
		case "InvokeEndpoint":
			return h.handleInvokeEndpoint(c, endpointName, body)
		case "InvokeEndpointAsync":
			return h.handleInvokeEndpointAsync(c, endpointName, body)
		case "InvokeEndpointWithResponseStream":
			return h.handleInvokeEndpointWithResponseStream(c, endpointName, body)
		default:
			return c.JSON(
				http.StatusNotFound,
				errorResponse("UnknownOperationException", "unknown operation: "+r.URL.Path),
			)
		}
	}
}

// handleInvokeEndpoint handles POST /endpoints/{EndpointName}/invocations.
func (h *Handler) handleInvokeEndpoint(
	c *echo.Context,
	endpointName string,
	body []byte,
) error {
	out := []byte(`{"Body":"mock response from Gopherstack"}`)

	h.Backend.RecordInvocation("InvokeEndpoint", endpointName, string(body), string(out))

	c.Response().Header().Set("Content-Type", "application/json")
	c.Response().Header().Set("X-Amzn-Invoked-Production-Variant", "AllTraffic")

	return c.JSONBlob(http.StatusOK, out)
}

// handleInvokeEndpointAsync handles POST /endpoints/{EndpointName}/async-invocations.
func (h *Handler) handleInvokeEndpointAsync(
	c *echo.Context,
	endpointName string,
	body []byte,
) error {
	out := []byte(`{"InferenceId":"mock-inference-id","OutputLocation":"s3://mock-bucket/output"}`)

	h.Backend.RecordInvocation("InvokeEndpointAsync", endpointName, string(body), string(out))

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(http.StatusAccepted, out)
}

// handleInvokeEndpointWithResponseStream handles POST /endpoints/{EndpointName}/invocations-response-stream.
// It returns a well-formed AWS event stream frame containing a single payload event.
func (h *Handler) handleInvokeEndpointWithResponseStream(
	c *echo.Context,
	endpointName string,
	body []byte,
) error {
	out := []byte(`{"Body":"mock streaming response from Gopherstack"}`)

	h.Backend.RecordInvocation("InvokeEndpointWithResponseStream", endpointName, string(body), string(out))

	frame := encodeEventStreamMsg([][2]string{
		{":message-type", "event"},
		{":event-type", "PayloadPart"},
		{":content-type", "application/octet-stream"},
	}, out)

	c.Response().Header().Set("Content-Type", "application/vnd.amazon.eventstream")
	c.Response().WriteHeader(http.StatusOK)
	_, _ = c.Response().Write(frame)

	return nil
}

// extractEndpointName extracts the endpoint name from the URL path.
// Path format: /endpoints/{EndpointName}/...
func extractEndpointName(path string) string {
	rest, ok := strings.CutPrefix(path, sagemakerRuntimePathPrefix)
	if !ok {
		return ""
	}

	endpointName, _, _ := strings.Cut(rest, "/")

	return endpointName
}

// pathToOperation maps a URL path suffix to an operation name.
func pathToOperation(path string) string {
	switch {
	case strings.HasSuffix(path, "/invocations-response-stream"):
		return "InvokeEndpointWithResponseStream"
	case strings.HasSuffix(path, "/async-invocations"):
		return "InvokeEndpointAsync"
	case strings.HasSuffix(path, "/invocations"):
		return "InvokeEndpoint"
	default:
		return "Unknown"
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}

// encodeEventStreamMsg encodes a single AWS event stream binary message.
// Format: totalLen(4) | headersLen(4) | preludeCRC(4) | headers | payload | msgCRC(4).
func encodeEventStreamMsg(hdrs [][2]string, payload []byte) []byte {
	hdrBytes := buildEventStreamHeaders(hdrs)
	headerLen := len(hdrBytes)
	payloadLen := len(payload)

	// Guard against integer overflow: payloadLen must fit within the remaining frame space.
	if headerLen > math.MaxInt32-eventStreamPreludeLen-payloadLen-eventStreamMsgCRCLen {
		return nil
	}

	totalLen := eventStreamPreludeLen + headerLen + payloadLen + eventStreamMsgCRCLen
	buf := make([]byte, totalLen)

	//nolint:gosec // totalLen is bounded by the overflow check above
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalLen))
	//nolint:gosec // headerLen is bounded by the overflow check above
	binary.BigEndian.PutUint32(buf[4:8], uint32(headerLen))

	preludeCRC := crc32.ChecksumIEEE(buf[0:8])
	binary.BigEndian.PutUint32(buf[8:eventStreamPreludeLen], preludeCRC)

	copy(buf[eventStreamPreludeLen:eventStreamPreludeLen+headerLen], hdrBytes)
	copy(buf[eventStreamPreludeLen+headerLen:eventStreamPreludeLen+headerLen+payloadLen], payload)

	msgCRC := crc32.ChecksumIEEE(buf[0 : eventStreamPreludeLen+headerLen+payloadLen])
	binary.BigEndian.PutUint32(buf[eventStreamPreludeLen+headerLen+payloadLen:], msgCRC)

	return buf
}

// buildEventStreamHeaders encodes name/value header pairs in AWS event stream binary format.
func buildEventStreamHeaders(hdrs [][2]string) []byte {
	var buf [512]byte
	n := 0

	for _, kv := range hdrs {
		name, value := kv[0], kv[1]
		buf[n] = byte(len(name)) //nolint:gosec // header name bounded by AWS event stream protocol
		n++
		n += copy(buf[n:], name)
		buf[n] = eventStreamHeaderValueTypeString
		n++
		//nolint:gosec // header value length fits in uint16 by AWS event stream protocol
		binary.BigEndian.PutUint16(buf[n:n+eventStreamHeaderValueLenBytes], uint16(len(value)))
		n += eventStreamHeaderValueLenBytes
		n += copy(buf[n:], value)
	}

	return buf[:n]
}
