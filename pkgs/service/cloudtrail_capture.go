package service

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// minCredentialParts is the minimum number of slash-delimited fields in an AWS
// SigV4 Credential value: AKID / date / region / service / aws4_request.
const minCredentialParts = 5

// httpErrorStatusThreshold is the HTTP status at and above which a response
// is treated as an error for CloudTrail capture purposes.
const httpErrorStatusThreshold = 400

// CloudTrailEventInput carries the fields the central service registry
// extracts from a single mutating API call so the CloudTrail backend can
// record a real management event. It is defined here (rather than reusing the
// cloudtrail package's own Event type) because services/cloudtrail already
// imports pkgs/service for its Provider/Registerable wiring, so the reverse
// import would create an import cycle.
type CloudTrailEventInput struct {
	// EventName is the AWS API operation name, e.g. "CreateBucket", "RunInstances".
	EventName string
	// EventSource is the AWS service's CloudTrail event source, e.g. "s3.amazonaws.com".
	EventSource string
	// AwsRegion is the region the request targeted.
	AwsRegion string
	// Username identifies the caller. Empty when no signed identity was present.
	Username string
	// AccessKeyID is the SigV4 access key used to sign the request, if any.
	AccessKeyID string
	// ResourceName is the primary resource the operation acted on, if known.
	ResourceName string
	// ErrorCode is the AWS error code for a failed (HTTP >= 400) call, e.g.
	// "TrailNotFoundException". Empty on success, and best-effort on
	// failure: only populated when the response body is JSON carrying a
	// "__type"/"Code" field (the shape most of this repo's JSON-protocol
	// services use) -- see extractErrorInfo.
	ErrorCode string
	// ErrorMessage is the corresponding human-readable error message, with
	// the same best-effort caveat as ErrorCode.
	ErrorMessage string
}

// CloudTrailRecorder is implemented by the CloudTrail service backend to
// accept management events captured centrally by the service registry.
// The registry auto-discovers the live backend via SetCloudTrailRecorder;
// no service package other than cloudtrail need know about this interface.
type CloudTrailRecorder interface {
	// RecordManagementEvent records a single mutating API call as a CloudTrail
	// management event so it is later returned by LookupEvents.
	RecordManagementEvent(ev CloudTrailEventInput)
}

// readOnlyOperationPrefixes lists AWS API operation name prefixes that are
// conventionally read-only. CloudTrail trails only log management events by
// default (data/read events require an explicit read event selector), so
// operations matching one of these prefixes are excluded from capture.
var readOnlyOperationPrefixes = []string{ //nolint:gochecknoglobals // static lookup table
	"Get", "List", "Describe", "Head", "Lookup", "Check", "Query", "Search",
	"Test", "Validate", "Preview", "Estimate", "Simulate",
}

// isReadOnlyOperation reports whether op looks like a read-only API call based
// on its AWS-conventional naming prefix.
func isReadOnlyOperation(op string) bool {
	for _, p := range readOnlyOperationPrefixes {
		if strings.HasPrefix(op, p) {
			return true
		}
	}

	return false
}

// isUnknownOperation reports whether the observer could not determine a real
// operation name for the request (services return "Unknown"/"unknown" in that case).
func isUnknownOperation(op string) bool {
	return op == "" || strings.EqualFold(op, "unknown")
}

// extractAccessKeyID extracts the AWS access key ID from the SigV4
// Authorization header's Credential value. Returns "" when absent or malformed.
func extractAccessKeyID(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if auth == "" || !strings.Contains(auth, "Credential=") {
		return ""
	}

	_, after, found := strings.Cut(auth, "Credential=")
	if !found {
		return ""
	}

	credOnly, _, _ := strings.Cut(after, ",")
	parts := strings.Split(credOnly, "/")

	if len(parts) < minCredentialParts {
		return ""
	}

	return parts[0]
}

// eventSourceFor derives the CloudTrail EventSource for a request, e.g.
// "s3.amazonaws.com", from the SigV4 signing name. Falls back to the
// service's registered name when no Authorization header is present.
func eventSourceFor(r *http.Request, svc Registerable) string {
	if signingName := httputils.ExtractServiceFromRequest(r); signingName != "" {
		return signingName + ".amazonaws.com"
	}

	return strings.ToLower(svc.Name()) + ".amazonaws.com"
}

// wrapCloudTrailCapture wraps an already fully-assembled service handler chain
// so that, once it returns, a CloudTrail management event is recorded for
// mutating operations. It is applied as the outermost layer around a single
// service's handler in Registry.Register, so it observes request/response
// state exactly as the existing telemetry wrapper does — reusing each
// service's own ExtractOperation/ExtractResource (required by the
// Registerable/ResourceObserver contract every service already implements)
// gives accurate operation names across every wire protocol (JSON, query,
// REST) without any per-service capture code.
//
// A failed (4xx/5xx) mutating call is recorded the same as a successful one
// — real CloudTrail records failed calls too, with populated errorCode/
// errorMessage — so the response is tee'd through a captureResponseWriter to
// recover a best-effort ErrorCode/ErrorMessage (see extractErrorInfo). This
// only swaps *echo.Response's own *inner* http.ResponseWriter (the raw
// net/http writer it wraps), never echo.Context's Response() value itself:
// pkgs/telemetry.WrapEchoHandler (which runs inside this wrapper, since
// Registry.Register applies CloudTrail capture as the outermost layer around
// an already telemetry-wrapped handler) type-asserts c.Response() to
// *echo.Response directly, and must keep seeing that concrete type.
func wrapCloudTrailCapture(rec CloudTrailRecorder, svc Registerable, next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		resp, ok := c.Response().(*echo.Response)

		var capture *captureResponseWriter
		if ok {
			capture = &captureResponseWriter{ResponseWriter: resp.ResponseWriter}
			resp.ResponseWriter = capture
		}

		err := next(c)

		if ok {
			resp.ResponseWriter = capture.ResponseWriter
		}

		op := svc.ExtractOperation(c)
		if isUnknownOperation(op) || isReadOnlyOperation(op) {
			return err
		}

		req := c.Request()
		accessKeyID := extractAccessKeyID(req)
		username := accessKeyID

		var errorCode, errorMessage string
		if capture != nil {
			errorCode, errorMessage = extractErrorInfo(capture.status, capture.body.Bytes())
		}

		rec.RecordManagementEvent(CloudTrailEventInput{
			EventName:    op,
			EventSource:  eventSourceFor(req, svc),
			AwsRegion:    httputils.ExtractRegionFromRequest(req, config.DefaultRegion),
			Username:     username,
			AccessKeyID:  accessKeyID,
			ResourceName: svc.ExtractResource(c),
			ErrorCode:    errorCode,
			ErrorMessage: errorMessage,
		})

		return err
	}
}

// captureResponseWriter tees every Write into an in-memory buffer while
// passing bytes through to the real ResponseWriter unchanged, so
// wrapCloudTrailCapture can inspect the response body after the wrapped
// handler returns without altering what the client receives. Unwrap lets
// echo.Response's own Flush/Hijack (used by streaming/websocket handlers)
// reach the real underlying writer through this wrapper -- see
// net/http.NewResponseController's Unwrap-chaining contract.
type captureResponseWriter struct {
	http.ResponseWriter
	body   bytes.Buffer
	status int
}

func (w *captureResponseWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *captureResponseWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}

	w.body.Write(b)

	return w.ResponseWriter.Write(b)
}

func (w *captureResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// jsonErrorEnvelope covers the JSON error-body shapes used across
// gopherstack's JSON-protocol services: {"__type":...,"message":...} (AWS
// JSON-RPC 1.1, e.g. this package's own JSONErrorResponse) and
// {"Code":...,"Message":...} (used by some REST-JSON handlers).
type jsonErrorEnvelope struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
	Code    string `json:"Code"`
	Msg     string `json:"Message"`
}

// extractErrorInfo returns a best-effort ErrorCode/ErrorMessage for a failed
// response. This is necessarily partial: gopherstack has ~160 heterogeneous
// service handlers, and error bodies aren't standardized across protocols
// (JSON-RPC's inline "__type"/"message", some REST-JSON handlers' "Code"/
// "Message", query-protocol XML <Error> envelopes, CBOR's X-Amzn-Errortype
// header). A non-JSON or unrecognized body yields empty strings rather than
// a guess -- see PARITY.md's gaps entry for the services this covers vs.
// doesn't.
func extractErrorInfo(status int, body []byte) (string, string) {
	if status < httpErrorStatusThreshold {
		return "", ""
	}

	var env jsonErrorEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return "", ""
	}

	code := env.Type
	if code == "" {
		code = env.Code
	}

	message := env.Message
	if message == "" {
		message = env.Msg
	}

	return code, message
}
