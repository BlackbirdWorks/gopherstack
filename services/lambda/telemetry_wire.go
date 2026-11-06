package lambda

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Telemetry API 2022-07-01 and Logs API 2020-08-15 subscribe contracts:
// https://docs.aws.amazon.com/lambda/latest/dg/telemetry-api-reference.html
// https://docs.aws.amazon.com/lambda/latest/dg/runtimes-logs-api.html
const (
	telemetryPath = "/2022-07-01/telemetry"
	logsPath      = "/2020-08-15/logs"

	apiTelemetry = "telemetry"
	apiLogs      = "logs"

	typePlatform  = "platform"
	typeFunction  = "function"
	typeExtension = "extension"

	protoHTTP = "HTTP"
	protoTCP  = "TCP"

	schemaTelemetry20250129 = "2025-01-29"
	schemaTelemetry20221213 = "2022-12-13"
	schemaTelemetry20220701 = "2022-07-01"
	schemaLogs20200815      = "2020-08-15"
	schemaLogs20210318      = "2021-03-18"

	reservedTelemetryPort = 9001
	maxPort               = 65535

	minMaxItems, maxMaxItems, defaultMaxItems    = 1000, 10000, 1000
	minMaxBytes, maxMaxBytes, defaultMaxBytes    = 262144, 1048576, 262144
	minTimeoutMs, maxTimeoutMs, defaultTimeoutMs = 25, 30000, 1000
	eventTimeLayout                              = "2006-01-02T15:04:05.000Z"
	roundScale                                   = 100
	bytesPerMB                                   = 1 << 20
	telemetryValidationErrorType                 = "ValidationError"
	logsValidationErrorType                      = "Logs.ValidationError"
	recordFrameBytes                             = 32
	sandboxHost                                  = "sandbox.localdomain"
	maxSubscribeBodyBytes                        = 1 << 20
)

const (
	dropReason = "Some logs were dropped because the downstream consumer is slower than " +
		"the logs production rate"
	logsDropReason = "Consumer seems to have fallen behind as it has not acknowledged " +
		"receipt of logs."
)

type bufferingRequest struct {
	MaxItems  *int `json:"maxItems"`
	MaxBytes  *int `json:"maxBytes"`
	TimeoutMs *int `json:"timeoutMs"`
}

type destinationRequest struct {
	Protocol string `json:"protocol"`
	URI      string `json:"URI"`
}

type subscribeRequest struct {
	Buffering     *bufferingRequest   `json:"buffering"`
	Destination   *destinationRequest `json:"destination"`
	SchemaVersion string              `json:"schemaVersion"`
	Types         []string            `json:"types"`
}

type bufferingConfig struct {
	maxItems  int
	maxBytes  int
	timeoutMs int
}

type destination struct {
	protocol string
	host     string
	port     string
	path     string
}

// subscription is a validated Subscribe request.
type subscription struct {
	types  map[string]bool
	dest   destination
	api    string
	schema string
	buf    bufferingConfig
}

func (s subscription) typeList() []string {
	out := make([]string, 0, len(s.types))

	for _, t := range []string{typePlatform, typeFunction, typeExtension} {
		if s.types[t] {
			out = append(out, t)
		}
	}

	return out
}

// validationErrorType is the errorType of a failed Subscribe request.
func validationErrorType(api string) string {
	if api == apiLogs {
		return logsValidationErrorType
	}

	return telemetryValidationErrorType
}

// validateSubscription checks req against the documented bounds; problems are joined with "; ".
// validationError is the errorMessage of a rejected Subscribe request.
type validationError string

func (e validationError) Error() string { return string(e) }

func validateSubscription(api string, req *subscribeRequest) (subscription, error) {
	sub := subscription{api: api, types: map[string]bool{}}

	var problems []string

	problems = append(problems, validateSchema(api, req.SchemaVersion, &sub)...)

	problems = append(problems, validateDestination(req.Destination, &sub.dest)...)

	for _, t := range req.Types {
		switch t {
		case typePlatform, typeFunction, typeExtension:
			sub.types[t] = true
		default:
			problems = append(problems, fmt.Sprintf("unsupported type %q", t))
		}
	}

	if len(req.Types) == 0 {
		problems = append(problems, "types should not be empty")
	}

	problems = append(problems, validateBuffering(req.Buffering, &sub.buf)...)

	if len(problems) > 0 {
		return subscription{}, validationError(strings.Join(problems, "; "))
	}

	return sub, nil
}

func validateSchema(api, version string, sub *subscription) []string {
	switch api {
	case apiLogs:
		switch version {
		case "":
			sub.schema = schemaLogs20200815
		case schemaLogs20200815, schemaLogs20210318:
			sub.schema = version
		default:
			return []string{fmt.Sprintf("unsupported schemaVersion %q", version)}
		}
	default:
		switch version {
		case "":
			return []string{"schemaVersion is required"}
		case schemaTelemetry20250129, schemaTelemetry20221213, schemaTelemetry20220701:
			sub.schema = version
		default:
			return []string{fmt.Sprintf("unsupported schemaVersion %q", version)}
		}
	}

	return nil
}

func validateBuffering(req *bufferingRequest, out *bufferingConfig) []string {
	*out = bufferingConfig{maxItems: defaultMaxItems, maxBytes: defaultMaxBytes, timeoutMs: defaultTimeoutMs}

	if req == nil {
		return nil
	}

	var problems []string

	check := func(name string, v *int, lo, hi int, dst *int) {
		if v == nil {
			return
		}

		if *v < lo || *v > hi {
			problems = append(problems, fmt.Sprintf("buffering.%s must be between %d and %d", name, lo, hi))

			return
		}

		*dst = *v
	}

	check("maxItems", req.MaxItems, minMaxItems, maxMaxItems, &out.maxItems)
	check("maxBytes", req.MaxBytes, minMaxBytes, maxMaxBytes, &out.maxBytes)
	check("timeoutMs", req.TimeoutMs, minTimeoutMs, maxTimeoutMs, &out.timeoutMs)

	return problems
}

func isSandboxHost(h string) bool {
	switch strings.ToLower(h) {
	case sandboxHost, "sandbox", "localhost", "127.0.0.1", "::1":
		return true
	}

	return false
}

func validateDestination(req *destinationRequest, out *destination) []string {
	if req == nil {
		return []string{"destination is required"}
	}

	switch req.Protocol {
	case protoHTTP, protoTCP:
		out.protocol = req.Protocol
	case "":
		return []string{"destination.protocol is required"}
	default:
		return []string{fmt.Sprintf("destination.protocol must be HTTP or TCP, got %q", req.Protocol)}
	}

	if req.URI == "" {
		return []string{"destination.URI is required"}
	}

	return validateDestinationURI(req.URI, out)
}

func validateDestinationURI(uri string, out *destination) []string {
	u, err := url.Parse(uri)
	if err != nil || u.Host == "" {
		return []string{"destination.URI is not a valid URI"}
	}

	want := "http"
	if out.protocol == protoTCP {
		want = "tcp"
	}

	if u.Scheme != want {
		return []string{fmt.Sprintf("destination.URI scheme must be %s for protocol %s", want, out.protocol)}
	}

	if u.Port() == "" {
		return []string{"URI port is not provided"}
	}

	port, convErr := strconv.Atoi(u.Port())

	switch {
	case convErr != nil || port < 1 || port > maxPort:
		return []string{"URI port is invalid"}
	case port == reservedTelemetryPort:
		return []string{"URI port 9001 is reserved"}
	case !isSandboxHost(u.Hostname()):
		return []string{"URI host must be " + sandboxHost + " (destinations must be inside the execution environment)"}
	}

	out.host, out.port = u.Hostname(), u.Port()
	if out.protocol == protoHTTP {
		out.path = u.RequestURI()
	}

	return nil
}

// target is host:port with the sandbox host replaced by the subscribing container's address.
func (d destination) target(containerHost string) string {
	h := d.host
	if containerHost != "" {
		h = containerHost
	}

	return net.JoinHostPort(h, d.port)
}

// wireEvent is one telemetry/log event as delivered to a subscriber.
type wireEvent struct {
	Record any    `json:"record"`
	Time   string `json:"time"`
	Type   string `json:"type"`
}

// MarshalJSON writes the fields in the documented order: time, type, record.
func (e wireEvent) MarshalJSON() ([]byte, error) {
	rec, err := json.Marshal(e.Record)
	if err != nil {
		return nil, err
	}

	t, _ := json.Marshal(e.Time)
	typ, _ := json.Marshal(e.Type)

	out := make([]byte, 0, len(rec)+len(t)+len(typ)+recordFrameBytes)
	out = append(out, `{"time":`...)
	out = append(out, t...)
	out = append(out, `,"type":`...)
	out = append(out, typ...)
	out = append(out, `,"record":`...)
	out = append(out, rec...)

	return append(out, '}'), nil
}

func eventTime(t time.Time) string { return t.UTC().Format(eventTimeLayout) }

func roundMs(d time.Duration) float64 {
	return math.Round(float64(d)/float64(time.Millisecond)*roundScale) / roundScale
}

type telSpan struct {
	Name       string  `json:"name"`
	Start      string  `json:"start"`
	DurationMs float64 `json:"durationMs"`
}

type telTracing struct {
	SpanID string `json:"spanId,omitempty"`
	Type   string `json:"type"`
	Value  string `json:"value"`
}

type recInitStart struct {
	InitializationType string `json:"initializationType"`
	Phase              string `json:"phase"`
	FunctionName       string `json:"functionName"`
	FunctionVersion    string `json:"functionVersion"`
	InstanceID         string `json:"instanceId,omitempty"`
	InstanceMaxMemory  int    `json:"instanceMaxMemory,omitempty"`
}

type recInitRuntimeDone struct {
	InitializationType string    `json:"initializationType"`
	Phase              string    `json:"phase,omitempty"`
	Status             string    `json:"status"`
	ErrorType          string    `json:"errorType,omitempty"`
	Spans              []telSpan `json:"spans,omitempty"`
}

type recInitMetrics struct {
	DurationMs float64 `json:"durationMs"`
}

type recInitReport struct {
	InitializationType string         `json:"initializationType"`
	Status             string         `json:"status"`
	Phase              string         `json:"phase"`
	ErrorType          string         `json:"errorType,omitempty"`
	Spans              []telSpan      `json:"spans,omitempty"`
	Metrics            recInitMetrics `json:"metrics"`
}

type recStart struct {
	Tracing   *telTracing `json:"tracing,omitempty"`
	RequestID string      `json:"requestId"`
	Version   string      `json:"version,omitempty"`
}

type recRuntimeDoneMetrics struct {
	DurationMs    float64 `json:"durationMs"`
	ProducedBytes int     `json:"producedBytes"`
}

type recRuntimeDone struct {
	Tracing   *telTracing            `json:"tracing,omitempty"`
	Metrics   *recRuntimeDoneMetrics `json:"metrics,omitempty"`
	RequestID string                 `json:"requestId"`
	Status    string                 `json:"status"`
	ErrorType string                 `json:"errorType,omitempty"`
	Spans     []telSpan              `json:"spans,omitempty"`
}

type recReportMetrics struct {
	InitDurationMs  *float64 `json:"initDurationMs,omitempty"`
	DurationMs      float64  `json:"durationMs"`
	BilledDuration  int64    `json:"billedDurationMs"`
	MaxMemoryUsedMB int      `json:"maxMemoryUsedMB,omitempty"`
	MemorySizeMB    int      `json:"memorySizeMB"`
}

type recReport struct {
	Tracing   *telTracing      `json:"tracing,omitempty"`
	RequestID string           `json:"requestId"`
	Status    string           `json:"status"`
	ErrorType string           `json:"errorType,omitempty"`
	Spans     []telSpan        `json:"spans,omitempty"`
	Metrics   recReportMetrics `json:"metrics"`
}

type recExtension struct {
	Name   string   `json:"name"`
	State  string   `json:"state"`
	Events []string `json:"events"`
}

type recSubscription struct {
	Name  string   `json:"name"`
	State string   `json:"state"`
	Types []string `json:"types"`
}

type recLogsDropped struct {
	Reason         string `json:"reason"`
	DroppedRecords int    `json:"droppedRecords"`
	DroppedBytes   int    `json:"droppedBytes"`
}

type recRequestID struct {
	RequestID string `json:"requestId"`
}

type recLogsRuntimeDone struct {
	RequestID string `json:"requestId"`
	Status    string `json:"status"`
}
