package ociapigateway

import (
	"context"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	logCategoryAccess    = "access"
	logCategoryExecution = "execution"
	logTypeAccess        = "com.oraclecloud.apigateway.access"
	logTypeExecution     = "com.oraclecloud.apigateway.execution"
	levelInfo            = "INFO"
	levelWarn            = "WARN"
	levelError           = "ERROR"
	headerOpcRequestID   = "Opc-Request-Id"
	keyDeploymentID      = "deploymentId"
	keyMessage           = "message"
	keyGatewayID         = "gatewayId"
)

// LogSink receives the access and execution log records of a deployment; wired to OCI Logging SERVICE logs.
type LogSink func(deploymentID, category, eventType string, at time.Time, data map[string]any)

type logPolicyDoc struct {
	IsEnabled *bool  `json:"isEnabled"`
	LogLevel  string `json:"logLevel"`
}

type loggingDoc struct {
	AccessLog    *logPolicyDoc `json:"accessLog"`
	ExecutionLog *logPolicyDoc `json:"executionLog"`
}

func levelRank(level string) int {
	switch level {
	case levelError:
		return 2 //nolint:mnd // ERROR is the highest level
	case levelWarn:
		return 1
	default:
		return 0
	}
}

func validateLogging(d *loggingDoc) *APIError {
	if d == nil || d.ExecutionLog == nil || d.ExecutionLog.LogLevel == "" {
		return nil
	}

	switch d.ExecutionLog.LogLevel {
	case levelInfo, levelWarn, levelError:
		return nil
	default:
		return errParam("loggingPolicies.executionLog.logLevel must be INFO, WARN or ERROR")
	}
}

// SetLogSink wires deployment access and execution logs to OCI Logging.
func (b *InMemoryBackend) SetLogSink(fn LogSink) {
	b.mu.Lock("SetLogSink")
	b.logSink = fn
	b.mu.Unlock()
}

type reqLog struct {
	sink    LogSink
	now     func() time.Time
	base    map[string]any
	depID   string
	opcID   string
	minRank int
}

type logKey struct{}

func logFrom(ctx context.Context) *reqLog {
	l, _ := ctx.Value(logKey{}).(*reqLog)

	return l
}

// startLog attaches a request logger to the request, or returns it unchanged when no sink is wired.
func (b *InMemoryBackend) startLog(r *http.Request, tgt target) (*http.Request, *reqLog) {
	if tgt.logSink == nil {
		return r, nil
	}

	opc := r.Header.Get(headerOpcRequestID)
	if opc == "" {
		opc = strings.ToUpper(strings.ReplaceAll(uuid.NewString(), "-", ""))
	}

	l := &reqLog{
		sink: tgt.logSink, now: b.clock, depID: tgt.depID, opcID: opc,
		base: map[string]any{
			keyGatewayID:         tgt.gateway,
			"gatewayDisplayName": tgt.gwName,
			keyDeploymentID:      tgt.depID,
			"opcRequestId":       opc,
		},
	}

	if lp := tgt.spec.doc.LoggingPolicies; lp != nil && lp.ExecutionLog != nil {
		l.minRank = levelRank(lp.ExecutionLog.LogLevel)
	}

	return r.WithContext(context.WithValue(r.Context(), logKey{}, l)), l
}

// routeLevel applies a route's own execution log level over the deployment's.
func (l *reqLog) routeLevel(lp *loggingDoc) {
	if l != nil && lp != nil && lp.ExecutionLog != nil && lp.ExecutionLog.LogLevel != "" {
		l.minRank = levelRank(lp.ExecutionLog.LogLevel)
	}
}

func (l *reqLog) exec(level, code, msg string, extra ...any) {
	if l == nil || levelRank(level) < l.minRank {
		return
	}

	data := map[string]any{"code": code, "level": level, keyMessage: msg}
	maps.Copy(data, l.base)

	for i := 0; i+1 < len(extra); i += 2 {
		if k, ok := extra[i].(string); ok {
			data[k] = extra[i+1]
		}
	}

	l.sink(l.depID, logCategoryExecution, logTypeExecution, l.now(), data)
}

// statusRecorder captures what the gateway answered, for the access log.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}

	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(p []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}

	n, err := s.ResponseWriter.Write(p)
	s.bytes += n

	return n, err
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func (l *reqLog) access(r *http.Request, rec *statusRecorder, started time.Time) {
	if l == nil {
		return
	}

	status := rec.status
	if status == 0 {
		status = http.StatusOK
	}

	data := map[string]any{
		"httpMethod":      r.Method,
		"requestUri":      r.RequestURI,
		"serverProtocol":  r.Proto,
		"bodyBytesSent":   rec.bytes,
		"httpUserAgent":   r.UserAgent(),
		keyMessage:        r.Method + " " + r.RequestURI + " " + r.Proto,
		"remoteAddr":      peerAddr(r).String(),
		"requestDuration": l.now().Sub(started).Seconds(),
		"status":          status,
	}

	if ref := r.Referer(); ref != "" {
		data["httpReferrer"] = ref
	}

	maps.Copy(data, l.base)

	l.sink(l.depID, logCategoryAccess, logTypeAccess, l.now(), data)
}
