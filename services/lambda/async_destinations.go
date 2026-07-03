package lambda

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// AsyncDestinationDelivery delivers an async-invocation outcome to a configured
// target ARN (an SQS queue, SNS topic, EventBridge bus, or Lambda function). It
// is implemented by an adapter in the CLI wiring layer that routes by the target
// ARN's service. When no delivery implementation is wired, DeadLetterConfig and
// DestinationConfig are stored but not delivered (the pre-parity behaviour).
type AsyncDestinationDelivery interface {
	// DeliverToTarget sends payload to the given target ARN. attributes are
	// surfaced as message attributes for SQS/SNS targets (e.g. the DLQ metadata
	// AWS attaches: RequestID, ErrorCode, ErrorMessage).
	DeliverToTarget(ctx context.Context, targetARN string, payload []byte, attributes map[string]string) error
}

// SetAsyncDestinationDelivery wires the cross-service delivery implementation used
// to route DeadLetterConfig and DestinationConfig (OnSuccess/OnFailure) outcomes.
func (b *InMemoryBackend) SetAsyncDestinationDelivery(d AsyncDestinationDelivery) {
	b.mu.Lock("SetAsyncDestinationDelivery")
	defer b.mu.Unlock()
	b.asyncDelivery = d
}

// asyncDestinationRecordVersion is the schema version of the destination record.
const asyncDestinationRecordVersion = "1.0"

// asyncOutcome captures everything needed to build and route the destination
// records for a completed async invocation.
type asyncOutcome struct {
	functionName    string
	requestID       string
	functionError   string
	requestPayload  []byte
	responsePayload []byte
	invokeCount     int
	statusCode      int
	success         bool
}

// asyncDestinationRecord is the JSON invocation record AWS Lambda delivers to
// OnSuccess/OnFailure destinations.
type asyncDestinationRecord struct {
	RequestPayload  json.RawMessage      `json:"requestPayload,omitempty"`
	ResponsePayload json.RawMessage      `json:"responsePayload,omitempty"`
	Timestamp       string               `json:"timestamp"`
	Version         string               `json:"version"`
	RequestContext  asyncRequestContext  `json:"requestContext"`
	ResponseContext asyncResponseContext `json:"responseContext"`
}

type asyncRequestContext struct {
	RequestID              string `json:"requestId"`
	FunctionArn            string `json:"functionArn"`
	Condition              string `json:"condition"`
	ApproximateInvokeCount int    `json:"approximateInvokeCount"`
}

type asyncResponseContext struct {
	ExecutedVersion string `json:"executedVersion"`
	FunctionError   string `json:"functionError,omitempty"`
	StatusCode      int    `json:"statusCode"`
}

// dispatchAsyncOutcome routes a completed async invocation to its configured
// DeadLetterConfig and DestinationConfig destinations. It is a no-op when no
// delivery implementation is wired or no destinations are configured.
func (b *InMemoryBackend) dispatchAsyncOutcome(ctx context.Context, out asyncOutcome) {
	delivery, functionArn, dlqTarget, destTarget := b.resolveAsyncTargets(out)
	if delivery == nil {
		return
	}

	log := logger.Load(ctx)

	// The legacy DeadLetterConfig only fires on failure and receives the raw event
	// payload plus AWS's DLQ metadata attributes.
	if !out.success && dlqTarget != "" {
		attrs := map[string]string{
			"RequestID":    out.requestID,
			"ErrorCode":    "200",
			"ErrorMessage": "RetriesExhausted",
		}
		if err := delivery.DeliverToTarget(ctx, dlqTarget, out.requestPayload, attrs); err != nil {
			log.WarnContext(ctx, "lambda: dead-letter delivery failed",
				"function", out.functionName, "target", dlqTarget, "error", err)
		}
	}

	if destTarget == "" {
		return
	}

	body, marshalErr := json.Marshal(buildAsyncDestinationRecord(out, functionArn))
	if marshalErr != nil {
		log.WarnContext(ctx, "lambda: failed to marshal destination record",
			"function", out.functionName, "error", marshalErr)

		return
	}

	if err := delivery.DeliverToTarget(ctx, destTarget, body, nil); err != nil {
		log.WarnContext(ctx, "lambda: destination delivery failed",
			"function", out.functionName, "target", destTarget, "error", err)
	}
}

// resolveAsyncTargets reads the delivery implementation and the configured DLQ /
// destination target ARNs for the outcome under the read lock.
func (b *InMemoryBackend) resolveAsyncTargets(out asyncOutcome) (
	AsyncDestinationDelivery, string, string, string,
) {
	b.mu.RLock("resolveAsyncTargets")
	defer b.mu.RUnlock()

	delivery := b.asyncDelivery
	fn := b.functions[out.functionName]
	eic := b.eventInvokeConfigs[out.functionName]

	var functionArn, dlqTarget string

	if fn != nil {
		functionArn = fn.FunctionArn
		if fn.DeadLetterConfig != nil {
			dlqTarget = fn.DeadLetterConfig.TargetArn
		}
	}

	return delivery, functionArn, dlqTarget, resolveDestinationTarget(eic, out.success)
}

// resolveDestinationTarget returns the OnSuccess or OnFailure destination ARN for
// the outcome, or "" when none is configured.
func resolveDestinationTarget(eic *FunctionEventInvokeConfig, success bool) string {
	if eic == nil || eic.DestinationConfig == nil {
		return ""
	}

	if success && eic.DestinationConfig.OnSuccess != nil {
		return eic.DestinationConfig.OnSuccess.Destination
	}

	if !success && eic.DestinationConfig.OnFailure != nil {
		return eic.DestinationConfig.OnFailure.Destination
	}

	return ""
}

// buildAsyncDestinationRecord assembles the AWS async-invocation destination record.
func buildAsyncDestinationRecord(out asyncOutcome, functionArn string) asyncDestinationRecord {
	condition := "Success"
	if !out.success {
		condition = "RetriesExhausted"
	}

	statusCode := out.statusCode
	if statusCode == 0 {
		statusCode = 200
	}

	invokeCount := max(out.invokeCount, 1)

	return asyncDestinationRecord{
		Version:   asyncDestinationRecordVersion,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		RequestContext: asyncRequestContext{
			RequestID:              out.requestID,
			FunctionArn:            functionArn,
			Condition:              condition,
			ApproximateInvokeCount: invokeCount,
		},
		RequestPayload: rawJSONOrNull(out.requestPayload),
		ResponseContext: asyncResponseContext{
			StatusCode:      statusCode,
			ExecutedVersion: versionLatest,
			FunctionError:   out.functionError,
		},
		ResponsePayload: rawJSONOrNull(out.responsePayload),
	}
}

// rawJSONOrNull returns b as raw JSON when it is valid JSON, otherwise a JSON
// string containing the raw bytes so the record always marshals cleanly.
func rawJSONOrNull(b []byte) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("null")
	}

	if json.Valid(b) {
		return json.RawMessage(b)
	}

	quoted, err := json.Marshal(string(b))
	if err != nil {
		return json.RawMessage("null")
	}

	return quoted
}
