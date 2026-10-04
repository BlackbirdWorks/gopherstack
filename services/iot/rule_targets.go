package iot

import (
	"context"
	"errors"
	"time"
)

// ErrActionTargetUnavailable means the service an action delivers to is not wired.
var ErrActionTargetUnavailable = errors.New("rule action target unavailable")

// LogEvent is one CloudWatch Logs event written by the cloudwatchLogs action.
type LogEvent struct {
	Message   string
	Timestamp int64
}

// SNSPublisher publishes to an SNS topic; jsonFormat sends MessageStructure=json.
type SNSPublisher interface {
	PublishToTopic(ctx context.Context, region, topicARN, message string, jsonFormat bool) error
}

// KinesisRecordPutter writes one record to a Kinesis data stream.
type KinesisRecordPutter interface {
	PutRecord(ctx context.Context, region, stream, partitionKey string, data []byte) error
}

// FirehoseRecordPutter writes records to a Firehose delivery stream.
type FirehoseRecordPutter interface {
	PutRecords(ctx context.Context, region, stream string, records [][]byte) error
}

// DynamoItemWriter applies the dynamoDB and dynamoDBv2 actions; items use the DynamoDB wire format.
type DynamoItemWriter interface {
	PutItem(ctx context.Context, region, table string, item map[string]any) error
	SetAttribute(ctx context.Context, region, table string, key map[string]any, attr string, val map[string]any) error
	DeleteItem(ctx context.Context, region, table string, key map[string]any) error
}

// ObjectPutter stores an S3 object.
type ObjectPutter interface {
	PutObject(ctx context.Context, region, bucket, key string, data []byte, cannedACL string) error
}

// MetricPutter writes one CloudWatch metric data point.
type MetricPutter interface {
	PutMetric(ctx context.Context, region, namespace, name, unit string, value float64, ts time.Time) error
}

// AlarmStateSetter sets a CloudWatch alarm's state.
type AlarmStateSetter interface {
	SetAlarmState(ctx context.Context, region, alarm, state, reason string) error
}

// LogEventPutter writes events to a CloudWatch Logs stream, creating the stream when missing.
type LogEventPutter interface {
	PutLogEvents(ctx context.Context, region, group, stream string, events []LogEvent) error
}

// ExecutionStarter starts a Step Functions execution of the named state machine.
type ExecutionStarter interface {
	StartExecution(ctx context.Context, region, account, stateMachine, execName, input string) error
}

// ChannelMessagePutter ingests messages into an IoT Analytics channel.
type ChannelMessagePutter interface {
	PutChannelMessages(ctx context.Context, region, channel string, payloads [][]byte) error
}

// ActionTargets are the services rule actions deliver to; a nil member leaves that action undelivered.
type ActionTargets struct {
	SNS           SNSPublisher
	Kinesis       KinesisRecordPutter
	Firehose      FirehoseRecordPutter
	DynamoDB      DynamoItemWriter
	S3            ObjectPutter
	Metrics       MetricPutter
	Alarms        AlarmStateSetter
	Logs          LogEventPutter
	StepFunctions ExecutionStarter
	Analytics     ChannelMessagePutter
	DynamoReader  DynamoItemReader
	Secrets       SecretReader
	Shadows       ShadowReader
	Lambda        LambdaRequester
	Credentials   RoleCredentialIssuer
}

// SetActionTargets wires the services non-SQS/Lambda rule actions deliver to.
func (b *InMemoryBackend) SetActionTargets(t *ActionTargets) {
	b.mu.Lock("SetActionTargets")
	defer b.mu.Unlock()

	b.targets = t
}

func (b *InMemoryBackend) actionTargets() *ActionTargets {
	b.mu.RLock("actionTargets")
	defer b.mu.RUnlock()

	if b.targets == nil {
		return &ActionTargets{}
	}

	return b.targets
}
