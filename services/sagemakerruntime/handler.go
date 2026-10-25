package sagemakerruntime

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

const (
	streamChunkBytes = 32 * 1024
	errTypeKey       = "__type"
)

const (
	opInvokeEndpoint                   = "InvokeEndpoint"
	opInvokeEndpointAsync              = "InvokeEndpointAsync"
	opInvokeEndpointWithResponseStream = "InvokeEndpointWithResponseStream"
)

const (
	sagemakerRuntimeService       = "sagemaker-runtime"
	sagemakerRuntimePathPrefix    = "/endpoints/"
	sagemakerRuntimeMatchPriority = service.PriorityPathVersioned
	defaultContentType            = "application/octet-stream"
	defaultVariant                = "AllTraffic"
	newSessionRequest             = "NEW_SESSION"
	// newSessionExpiresLayout matches the real AWS wire format for the
	// Expires= attribute of X-Amzn-SageMaker-New-Session-Id, per the SDK
	// model's NewSessionResponseHeader pattern:
	// "^[a-zA-Z0-9](-*[a-zA-Z0-9])*;\sExpires=[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
	// (an RFC 3339 timestamp with no fractional seconds), NOT an RFC 1123
	// HTTP-date -- confirmed against botocore's sagemaker-runtime
	// service-2.json.
	newSessionExpiresLayout     = "2006-01-02T15:04:05Z"
	headerTargetModel           = "X-Amzn-Sagemaker-Target-Model"
	headerCustomAttributes      = "X-Amzn-Sagemaker-Custom-Attributes"
	headerNewSessionID          = "X-Amzn-Sagemaker-New-Session-Id"
	headerClosedSessionID       = "X-Amzn-Sagemaker-Closed-Session-Id"
	headerSessionID             = "X-Amzn-Sagemaker-Session-Id"
	headerInferenceID           = "X-Amzn-Sagemaker-Inference-Id"
	headerOutputLocation        = "X-Amzn-Sagemaker-Outputlocation"
	headerFailureLocation       = "X-Amzn-Sagemaker-Failurelocation"
	headerInputLocation         = "X-Amzn-Sagemaker-Inputlocation"
	headerFilename              = "X-Amzn-Sagemaker-Filename"
	headerS3OutputPathExtension = "X-Amzn-Sagemaker-S3outputpathextension"
	headerAsyncAccept           = "X-Amzn-Sagemaker-Accept"
	headerStreamContentType     = "X-Amzn-Sagemaker-Content-Type"
	headerTargetVariant         = "X-Amzn-Sagemaker-Target-Variant"
	headerInvokedVariant        = "X-Amzn-Invoked-Production-Variant"
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
	peers   *regionpeers.Set[Handler]
	Backend *InMemoryBackend
}

// NewHandler creates a new SageMaker Runtime handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Shutdown implements service.Shutdowner. SageMaker Runtime has no background
// goroutines so this is a no-op.
func (h *Handler) Shutdown(_ context.Context) {}

// Name returns the service name.
func (h *Handler) Name() string { return "SageMakerRuntime" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opInvokeEndpoint,
		opInvokeEndpointAsync,
		opInvokeEndpointWithResponseStream,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return sagemakerRuntimeService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches SageMaker Runtime requests.
// It matches all requests to paths beginning with /endpoints/. The real AWS
// SageMaker Runtime endpoint hostname is "runtime.sagemaker.<region>.amazonaws.com"
// (see aws-sdk-go-v2/service/sagemakerruntime's endpoint resolver), not
// "sagemaker-runtime.<region>..." -- the latter would never match real traffic.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Host, "runtime.sagemaker.") ||
			strings.HasPrefix(c.Request().URL.Path, sagemakerRuntimePathPrefix)
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

// homeHandler serves requests for the home region.
func (h *Handler) homeHandler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		log := logger.Load(r.Context())

		if r.Method != http.MethodPost {
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("ValidationError", "method not allowed"))
		}

		body, err := httputils.ReadBody(r)
		if err != nil {
			log.ErrorContext(r.Context(), "sagemakerruntime: failed to read request body", "error", err)

			return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
		}

		endpointName := extractEndpointName(r.URL.Path)
		if endpointName == "" {
			// NOTE: the sagemakerruntime SDK's typed client error is
			// types.ValidationError (__type "ValidationError"), unlike most
			// other JSON-protocol services which use "ValidationException".
			// See aws-sdk-go-v2/service/sagemakerruntime/types/errors.go.
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationError", "missing EndpointName in path"))
		}

		if msg, ok := h.Backend.validateEndpoint(r.Context(), endpointName); !ok {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationError", msg))
		}

		op := pathToOperation(r.URL.Path)

		if msg := h.validateInvoke(r, endpointName, op); msg != "" {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationError", msg))
		}

		switch op {
		case opInvokeEndpoint:
			return h.handleInvokeEndpoint(c, endpointName, body)
		case opInvokeEndpointAsync:
			return h.handleInvokeEndpointAsync(c, endpointName, body)
		case opInvokeEndpointWithResponseStream:
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
	inv, err := h.Backend.invoker()
	if err != nil {
		return writeInvokeError(c, err)
	}

	req := c.Request()
	res, err := inv.InvokeEndpoint(
		req.Context(), invokeRequest(c, endpointName, body, req.Header.Get("Accept"), req.Header.Get("Content-Type")),
	)
	if err != nil {
		return writeInvokeError(c, err)
	}

	h.Backend.RecordInvocation(opInvokeEndpoint, endpointName, string(body), string(res.Body))

	c.Response().Header().Set(headerInvokedVariant, res.InvokedVariant)
	setOptionalHeader(c, headerCustomAttributes, res.CustomAttributes)
	setSessionResponseHeader(c, h.Backend, endpointName)

	ct := res.ContentType
	if ct == "" {
		ct = responseContentType(req.Header.Get("Accept"))
	}

	return c.Blob(http.StatusOK, ct, res.Body)
}

// handleInvokeEndpointAsync handles POST /endpoints/{EndpointName}/async-invocations.
func (h *Handler) handleInvokeEndpointAsync(
	c *echo.Context,
	endpointName string,
	body []byte,
) error {
	// Body and InputLocation are mutually exclusive; the SDK's client-side validator
	// only checks EndpointName, so the server must reject both or neither.
	hdr := c.Request().Header

	hasInputLocation := hdr.Get(headerInputLocation) != ""
	if (len(body) > 0) == hasInputLocation {
		return c.JSON(http.StatusBadRequest, errorResponse(
			"ValidationError",
			"Body and InputLocation are mutually exclusive. Provide exactly one of them.",
		))
	}

	inv, err := h.Backend.invoker()
	if err != nil {
		return writeInvokeError(c, err)
	}

	timeout, _ := strconv.Atoi(hdr.Get(headerInvocationTimeout))

	acc, err := inv.SubmitAsyncInference(c.Request().Context(), sagemaker.AsyncRequest{
		EndpointName: endpointName, InferenceID: hdr.Get(headerInferenceID), InputLocation: hdr.Get(headerInputLocation),
		ContentType: hdr.Get(
			"Content-Type",
		), Accept: hdr.Get("Accept"), CustomAttributes: hdr.Get(headerCustomAttributes),
		Filename: hdr.Get(headerFilename), OutputPathExtension: hdr.Get(headerS3OutputPathExtension),
		Body: body, TimeoutSeconds: int32(timeout), //nolint:gosec // validated to 1..3600 by validateRequestHeaders
	})
	if err != nil {
		return writeInvokeError(c, err)
	}

	h.Backend.RecordAsyncInvocation(
		endpointName,
		acc.InferenceID,
		string(body),
		acc.OutputLocation,
		acc.FailureLocation,
	)

	out, err := json.Marshal(map[string]string{"InferenceId": acc.InferenceID})
	if err != nil {
		return err
	}

	h.Backend.RecordInvocation(opInvokeEndpointAsync, endpointName, string(body), string(out))

	c.Response().Header().Set("Content-Type", "application/json")
	c.Response().Header().Set(headerOutputLocation, acc.OutputLocation)
	c.Response().Header().Set(headerFailureLocation, acc.FailureLocation)

	return c.JSONBlob(http.StatusAccepted, out)
}

// handleInvokeEndpointWithResponseStream handles POST /endpoints/{EndpointName}/invocations-response-stream.
// Each chunk the model container produces becomes one PayloadPart event.
func (h *Handler) handleInvokeEndpointWithResponseStream(
	c *echo.Context,
	endpointName string,
	body []byte,
) error {
	inv, err := h.Backend.invoker()
	if err != nil {
		return writeInvokeError(c, err)
	}

	req := c.Request()

	stream, err := inv.InvokeEndpointStream(
		req.Context(),
		invokeRequest(c, endpointName, body, req.Header.Get(headerAsyncAccept), req.Header.Get("Content-Type")),
	)
	if err != nil {
		return writeInvokeError(c, err)
	}

	defer stream.Body.Close()

	resp := c.Response()
	resp.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
	ct := stream.ContentType
	if ct == "" {
		ct = responseContentType(req.Header.Get(headerAsyncAccept))
	}

	resp.Header().Set(headerStreamContentType, ct)
	setOptionalHeader(c, headerCustomAttributes, stream.CustomAttributes)
	resp.Header().Set(headerInvokedVariant, stream.InvokedVariant)
	// InvokeEndpointWithResponseStreamOutput has no ClosedSessionId member, so any
	// expiry eviction here is a side effect only.
	_ = h.Backend.TouchSession(req.Header.Get(headerSessionID))
	resp.WriteHeader(http.StatusOK)

	rc := http.NewResponseController(resp)
	buf := make([]byte, streamChunkBytes)

	var full []byte

	for {
		n, rerr := stream.Body.Read(buf)
		if n > 0 {
			full = append(full, buf[:n]...)
			_, _ = resp.Write(encodeEventStreamMsg([][2]string{
				{":message-type", "event"},
				{":event-type", "PayloadPart"},
				{":content-type", "application/octet-stream"},
			}, buf[:n]))
			_ = rc.Flush()
		}

		if rerr != nil {
			break
		}
	}

	h.Backend.RecordInvocation(opInvokeEndpointWithResponseStream, endpointName, string(body), string(full))

	return nil
}

func setOptionalHeader(c *echo.Context, name, value string) {
	if value != "" {
		c.Response().Header().Set(name, value)
	}
}

func setSessionResponseHeader(c *echo.Context, backend *InMemoryBackend, endpointName string) {
	sessionID := c.Request().Header.Get(headerSessionID)

	switch sessionID {
	case "":
		return
	case newSessionRequest:
		session := backend.StartSession(endpointName)
		expires := session.ExpiresAt.UTC().Format(newSessionExpiresLayout)
		c.Response().Header().Set(headerNewSessionID, session.ID+"; Expires="+expires)
	default:
		// A non-NEW_SESSION SessionId identifies an existing stateful
		// session. If that session has expired (ExpiresAt in the past),
		// AWS's model container would have already torn it down; we
		// emulate that by reporting ClosedSessionId on this response
		// instead of silently keeping the session alive forever (see
		// PARITY.md's now-fixed gap on ExpiresAt enforcement).
		if outcome := backend.TouchSession(sessionID); outcome.ClosedSessionID != "" {
			c.Response().Header().Set(headerClosedSessionID, outcome.ClosedSessionID)
		}
	}
}

func responseContentType(accept string) string {
	if accept != "" {
		return accept
	}

	return defaultContentType
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
		return opInvokeEndpointWithResponseStream
	case strings.HasSuffix(path, "/async-invocations"):
		return opInvokeEndpointAsync
	case strings.HasSuffix(path, "/invocations"):
		return opInvokeEndpoint
	default:
		return "Unknown"
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{errTypeKey: code, "message": msg}
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
		nameLen := len(name)
		if nameLen > math.MaxUint8 {
			continue
		}

		buf[n] = byte(nameLen)
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

// validateInvoke checks header constraints and invoke targets; "" means valid. It may default
// the TargetVariant request header from an inference component.
func (h *Handler) validateInvoke(r *http.Request, endpointName, op string) string {
	if msg := validateRequestHeaders(r.Header); msg != "" {
		return msg
	}

	if op == opInvokeEndpointAsync {
		return ""
	}

	t := h.Backend.validateInvokeTargets(
		r.Context(), endpointName, r.Header.Get(headerInferenceComponent),
		r.Header.Get(headerTargetContainer), r.Header.Get(headerTargetVariant),
	)
	if t.errMsg != "" {
		return t.errMsg
	}

	if t.variant != "" && r.Header.Get(headerTargetVariant) == "" {
		r.Header.Set(headerTargetVariant, t.variant)
	}

	return ""
}
