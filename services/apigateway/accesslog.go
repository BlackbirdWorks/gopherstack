package apigateway

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// accessLogWriter wraps http.ResponseWriter to capture the status code for access logging.
// It delegates all writes to the underlying ResponseWriter without modification.
type accessLogWriter struct {
	http.ResponseWriter
	statusCode int
}

func newAccessLogWriter(w http.ResponseWriter) *accessLogWriter {
	return &accessLogWriter{ResponseWriter: w, statusCode: http.StatusOK}
}

func (a *accessLogWriter) WriteHeader(code int) {
	a.statusCode = code
	a.ResponseWriter.WriteHeader(code)
}

// emitAccessLog emits a single access log entry to CloudWatch Logs when the stage has
// AccessLogSettings configured and a LogEmitter is wired up on the handler.
// The format string supports $context.* substitution variables per the AWS specification.
func (h *Handler) emitAccessLog(
	apiID, stageName string,
	r *http.Request,
	alw *accessLogWriter,
	requestID string,
	latency time.Duration,
) {
	if h.logsEmitter == nil {
		return
	}

	stage, err := h.Backend.GetStage(apiID, stageName)
	if err != nil || stage == nil || stage.AccessLogSettings == nil {
		return
	}

	destARN := stage.AccessLogSettings.DestinationARN
	if destARN == "" {
		return
	}

	format := stage.AccessLogSettings.Format
	if format == "" {
		format = defaultAccessLogFormat
	}

	line := formatAccessLogEntry(format, r, alw, requestID, latency, apiID, stageName)
	groupName := logGroupFromARN(destARN)

	events := []LogEvent{
		{
			Timestamp: time.Now().UnixMilli(),
			Message:   line,
		},
	}

	_ = h.logsEmitter.PutLogEvents(groupName, accessLogStreamName(apiID, stageName), events)
}

// defaultAccessLogFormat is the default access log format when none is specified.
const defaultAccessLogFormat = "$context.requestId $context.httpMethod $context.path " +
	"$context.status $context.responseLength $context.responseLatencyMs"

// formatAccessLogEntry substitutes $context.* placeholders in the format string.
func formatAccessLogEntry(
	format string,
	r *http.Request,
	alw *accessLogWriter,
	requestID string,
	latency time.Duration,
	apiID, stageName string,
) string {
	replacer := strings.NewReplacer(
		"$context.requestId", requestID,
		"$context.httpMethod", r.Method,
		"$context.path", r.URL.Path,
		"$context.resourcePath", r.URL.Path,
		"$context.status", strconv.Itoa(alw.statusCode),
		"$context.responseLength", "-",
		"$context.responseLatencyMs", strconv.FormatInt(latency.Milliseconds(), 10),
		"$context.protocol", "HTTP/1.1",
		"$context.stage", stageName,
		"$context.apiId", apiID,
		"$context.sourceIp", extractClientIP(r),
		"$context.identity.sourceIp", extractClientIP(r),
		"$context.requestTime", time.Now().Format("02/Jan/2006:15:04:05 -0700"),
	)

	return replacer.Replace(format)
}

// logGroupFromARN extracts the CloudWatch log group name from a log group ARN.
// Handles:
//   - "arn:aws:logs:region:account:log-group:/my/group" → "/my/group"
//   - "/my/group" (already a name)
func logGroupFromARN(arn string) string {
	const logGroupPrefix = ":log-group:"
	if idx := strings.LastIndex(arn, logGroupPrefix); idx >= 0 {
		return arn[idx+len(logGroupPrefix):]
	}

	return arn
}

// accessLogStreamName returns a consistent stream name for the given API/stage.
func accessLogStreamName(apiID, stageName string) string {
	return apiID + "/" + stageName
}

const requestIDNodeMask = 0xffffffffffff // 48-bit node component of request ID

// generateRequestID creates a random hex request ID similar to the AWS API Gateway format.
func generateRequestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	a := binary.BigEndian.Uint32(b[0:4])
	c := binary.BigEndian.Uint16(b[4:6])
	d := binary.BigEndian.Uint16(b[6:8])
	e := binary.BigEndian.Uint16(b[8:10])
	f := binary.BigEndian.Uint64(b[8:16]) & requestIDNodeMask

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", a, c, d, e, f)
}
