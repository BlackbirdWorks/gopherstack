package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	esmMetricDropped            = "DroppedEventCount"
	esmMetricOnFailureDelivered = "OnFailureDestinationDeliveredEventCount"

	// failureConditionRetriesExhausted is the only condition the stream failure-destination docs show.
	failureConditionRetriesExhausted = "RetryAttemptsExhausted"
	failureRecordVersion             = "1.0"
	failureTimestampMillisLayout     = "2006-01-02T15:04:05.000Z"
	failureTimestampSecondsLayout    = "2006-01-02T15:04:05Z"
	kafkaDestinationPrefix           = "kafka://"
	s3ARNPrefix                      = "arn:aws:s3:::"
	kafkaInfiniteRetryCap            = 10
	bisectParts                      = 2
	functionErrorUnhandled           = "Unhandled"
	statusOK                         = 200
)

var (
	errNoFailureDelivery    = errors.New("no delivery implementation wired for on-failure destination")
	errUnsupportedFailureTo = errors.New("unsupported on-failure destination")
)

// ESMS3Destination writes on-failure invocation records to an S3 bucket.
type ESMS3Destination interface {
	PutObject(ctx context.Context, bucket, key string, body []byte) error
}

// SetESMS3Destination wires the S3 writer used for event source mapping on-failure destinations.
func (b *InMemoryBackend) SetESMS3Destination(d ESMS3Destination) {
	b.mu.Lock("SetESMS3Destination")
	defer b.mu.Unlock()

	b.esmS3 = d
}

// retryPolicy is the error-handling configuration of a stream or Kafka mapping.
type retryPolicy struct {
	maxAge     time.Duration
	maxRetries int
	limited    bool
	bisect     bool
}

func policyFor(m *EventSourceMapping) retryPolicy {
	p := retryPolicy{bisect: m.BisectBatchOnFunctionError}

	if m.MaximumRetryAttempts != nil && *m.MaximumRetryAttempts >= 0 {
		p.maxRetries, p.limited = *m.MaximumRetryAttempts, true
	}

	if m.MaximumRecordAgeInSeconds > 0 {
		p.maxAge = time.Duration(m.MaximumRecordAgeInSeconds) * time.Second
	}

	return p
}

// failureDestination returns the configured OnFailure destination ARN, or "".
func failureDestination(m *EventSourceMapping) string {
	if m.DestinationConfig == nil || m.DestinationConfig.OnFailure == nil {
		return ""
	}

	return m.DestinationConfig.OnFailure.Destination
}

// invokeOutcome is the result of one synchronous batch invocation.
type invokeOutcome struct {
	err             error
	requestID       string
	functionError   string
	executedVersion string
	statusCode      int
}

func (o invokeOutcome) failed() bool { return o.err != nil || o.functionError != "" }

func (o invokeOutcome) message() string {
	if o.err != nil {
		return o.err.Error()
	}

	return "function error: " + o.functionError
}

type failureRequestContext struct {
	RequestID              string `json:"requestId"`
	FunctionArn            string `json:"functionArn"`
	Condition              string `json:"condition"`
	ApproximateInvokeCount int    `json:"approximateInvokeCount"`
}

type failureResponseContext struct {
	ExecutedVersion string `json:"executedVersion,omitempty"`
	FunctionError   string `json:"functionError,omitempty"`
	StatusCode      int    `json:"statusCode"`
}

type streamBatchInfo struct {
	ShardID                         string `json:"shardId"`
	StartSequenceNumber             string `json:"startSequenceNumber"`
	EndSequenceNumber               string `json:"endSequenceNumber"`
	ApproximateArrivalOfFirstRecord string `json:"approximateArrivalOfFirstRecord"`
	ApproximateArrivalOfLastRecord  string `json:"approximateArrivalOfLastRecord"`
	StreamArn                       string `json:"streamArn"`
	BatchSize                       int    `json:"batchSize"`
}

type kafkaRecordInfo struct {
	Offset    string `json:"offset"`
	Timestamp string `json:"timestamp"`
}

type kafkaBatchInfo struct {
	RecordInfo       *kafkaRecordInfo `json:"recordInfo,omitempty"`
	EventSourceArn   string           `json:"eventSourceArn,omitempty"`
	BootstrapServers string           `json:"bootstrapServers"`
	BatchSize        int              `json:"batchSize"`
	PayloadSize      int              `json:"payloadSize"`
}

// failureRecord is the invocation record delivered to an event source mapping OnFailure destination.
type failureRecord struct {
	Payload          any                    `json:"payload,omitempty"`
	KinesisBatchInfo *streamBatchInfo       `json:"KinesisBatchInfo,omitempty"`
	DDBBatchInfo     *streamBatchInfo       `json:"DDBStreamBatchInfo,omitempty"`
	KafkaBatchInfo   *kafkaBatchInfo        `json:"KafkaBatchInfo,omitempty"`
	Version          string                 `json:"version"`
	Timestamp        string                 `json:"timestamp"`
	RequestContext   failureRequestContext  `json:"requestContext"`
	ResponseContext  failureResponseContext `json:"responseContext"`
}

func newFailureRecord(m *EventSourceMapping, attempts int, last invokeOutcome) failureRecord {
	requestID := last.requestID
	if requestID == "" {
		requestID = uuid.New().String()
	}

	return failureRecord{
		Version:   failureRecordVersion,
		Timestamp: time.Now().UTC().Format(failureTimestampMillisLayout),
		RequestContext: failureRequestContext{
			RequestID:              requestID,
			FunctionArn:            m.FunctionARN,
			Condition:              failureConditionRetriesExhausted,
			ApproximateInvokeCount: attempts,
		},
		ResponseContext: failureResponseContext{
			StatusCode:      last.statusCode,
			ExecutedVersion: last.executedVersion,
			FunctionError:   last.functionError,
		},
	}
}

// deliverFailureRecord sends body to an SQS, SNS or S3 destination; key/shard name the S3 object.
func (b *InMemoryBackend) deliverFailureRecord(
	ctx context.Context, uuidStr, shard, dest string, body []byte,
) error {
	b.mu.RLock("deliverFailureRecord")
	delivery, s3w := b.asyncDelivery, b.esmS3
	b.mu.RUnlock()

	switch {
	case strings.HasPrefix(dest, s3ARNPrefix):
		if s3w == nil {
			return errNoFailureDelivery
		}

		bucket, _, _ := strings.Cut(strings.TrimPrefix(dest, s3ARNPrefix), "/")
		now := time.Now().UTC()
		key := fmt.Sprintf("aws/lambda/%s/%s/%s/%s-%s", uuidStr, shard, now.Format("2006/01/02"),
			now.Format("2006-01-02T15.04.05"), uuid.New().String())

		return s3w.PutObject(ctx, bucket, key, body)
	case strings.HasPrefix(dest, "arn:aws:sqs:"), strings.HasPrefix(dest, "arn:aws:sns:"):
		if delivery == nil {
			return errNoFailureDelivery
		}

		return delivery.DeliverToTarget(ctx, dest, body, nil)
	default:
		return fmt.Errorf("%w: %s", errUnsupportedFailureTo, dest)
	}
}

func isS3Destination(dest string) bool { return strings.HasPrefix(dest, s3ARNPrefix) }

func logDeliveryFailure(ctx context.Context, uuidStr, dest string, err error) {
	logger.Load(ctx).WarnContext(ctx, "esm: on-failure delivery failed", "uuid", uuidStr, "destination", dest,
		"error", err)
}

func marshalFailure(rec failureRecord) ([]byte, error) {
	body, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("marshal failure record: %w", err)
	}

	return body, nil
}

// validateFailureDestination rejects kafka:// destinations on non-Kafka sources or naming a source topic.
func validateFailureDestination(sourceARN string, selfManaged bool, topics []string, dc *ESMDestinationConfig) error {
	if dc == nil || dc.OnFailure == nil || !strings.HasPrefix(dc.OnFailure.Destination, kafkaDestinationPrefix) {
		return nil
	}

	if !selfManaged && !strings.Contains(sourceARN, ":kafka:") {
		return fmt.Errorf("%w: a Kafka topic on-failure destination applies to Kafka event sources only",
			ErrInvalidParameterValue)
	}

	topic := strings.TrimPrefix(dc.OnFailure.Destination, kafkaDestinationPrefix)
	if topic == "" {
		return fmt.Errorf("%w: Kafka on-failure destination needs a topic name", ErrInvalidParameterValue)
	}

	if slices.Contains(topics, topic) {
		return fmt.Errorf("%w: the on-failure topic cannot be a source topic", ErrInvalidParameterValue)
	}

	return nil
}
