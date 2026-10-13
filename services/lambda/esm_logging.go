package lambda

import (
	"context"
	"encoding/json"
	"maps"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// Kafka ESM system log levels and event types per docs.aws.amazon.com/lambda/latest/dg/esm-logging.html.
const (
	esmLogDebug = "DEBUG"
	esmLogInfo  = "INFO"
	esmLogWarn  = "WARN"

	logKeyTimestamp    = "timestamp"
	logKeyErrorMessage = "errorMessage"

	esmLogEventProcessing = "ESM_PROCESSING_EVENT"
	esmLogEventPoller     = "POLLER_STATUS_EVENT"
	esmLogEventKafka      = "KAFKA_STATUS_EVENT"
)

const (
	rankDebug = iota
	rankInfo
	rankWarn
)

func esmLogRank(level string) (int, bool) {
	switch level {
	case esmLogDebug:
		return rankDebug, true
	case esmLogInfo:
		return rankInfo, true
	case esmLogWarn:
		return rankWarn, true
	}

	return 0, false
}

// esmLogger writes a Kafka mapping's system logs to its function's log group.
type esmLogger struct {
	b            *InMemoryBackend
	level        string
	functionName string
	uuid         string
	resourceARN  string
	sourceARN    string
}

func newESMLogger(b *InMemoryBackend, spec kafkaWorkerSpec) esmLogger {
	fn, _ := functionNameAndQualifierFromARN(spec.FunctionARN)

	return esmLogger{
		b: b, level: spec.LogLevel, functionName: fn, uuid: spec.UUID,
		resourceARN: spec.ResourceARN, sourceARN: spec.EventSourceARN,
	}
}

func (l esmLogger) enabled(at string) bool {
	cfg, ok := esmLogRank(l.level)
	rank, _ := esmLogRank(at)

	return ok && rank >= cfg
}

// emit writes one system log record when the mapping's level admits at.
func (l esmLogger) emit(ctx context.Context, at, eventType string, extra map[string]any) {
	if !l.enabled(at) {
		return
	}

	l.b.mu.RLock("esmLogger.emit")
	cwl := l.b.cwLogs
	l.b.mu.RUnlock()

	if cwl == nil || l.functionName == "" {
		return
	}

	rec := map[string]any{
		"eventType":        eventType,
		logKeyTimestamp:    time.Now().UnixMilli(),
		"resourceArn":      l.resourceARN,
		"eventProcessorId": l.uuid + "/0",
		"logLevel":         at,
	}

	if l.sourceARN != "" {
		rec["eventSourceArn"] = l.sourceARN
	}

	maps.Copy(rec, extra)

	body, err := json.Marshal(rec)
	if err != nil {
		return
	}

	group := "/aws/lambda/" + l.functionName
	stream := time.Now().UTC().Format("2006/01/02") + "/" + l.uuid

	if err = cwl.EnsureLogGroupAndStream(group, stream); err == nil {
		err = cwl.PutLogLines(group, stream, []string{string(body)})
	}

	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "esm: system log write failed", "uuid", l.uuid, "error", err)
	}
}

func (l esmLogger) warn(ctx context.Context, msg, code string) {
	e := map[string]any{logKeyErrorMessage: msg}
	if code != "" {
		e["errorCode"] = code
	}

	l.emit(ctx, esmLogWarn, esmLogEventProcessing, map[string]any{"error": e})
}

func (l esmLogger) consumerBuilt(ctx context.Context, spec kafkaWorkerSpec) {
	l.emit(ctx, esmLogInfo, esmLogEventPoller, map[string]any{"kafkaEventSourceConnection": map[string]any{
		"brokerEndpoints": spec.BootstrapServers,
		"consumerId":      spec.UUID + "-0",
		"topics":          spec.Consumer.Topics,
		"consumerGroupId": spec.Consumer.GroupID,
	}})
}

// committed logs the highest committed offset per partition of recs.
func (l esmLogger) committed(ctx context.Context, recs []KafkaRecord) {
	if !l.enabled(esmLogDebug) {
		return
	}

	last := map[string]int64{}
	order := []string{}

	for _, r := range recs {
		k := r.Topic + "-" + strconv.Itoa(int(r.Partition))
		if _, seen := last[k]; !seen {
			order = append(order, k)
		}

		last[k] = max(last[k], r.Offset)
	}

	for _, k := range order {
		l.emit(ctx, esmLogDebug, esmLogEventKafka, map[string]any{"kafkaPartitionOffsets": map[string]any{
			"partition":       k,
			"consumedOffset":  last[k],
			"processedOffset": last[k],
			"committedOffset": last[k] + 1,
		}})
	}
}

func esmSystemLogLevel(m *EventSourceMapping) string {
	if m.LoggingConfig == nil {
		return ""
	}

	return m.LoggingConfig.SystemLogLevel
}
