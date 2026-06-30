package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	
	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/blackbirdworks/gopherstack/services/ecs"
	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/blackbirdworks/gopherstack/services/firehose"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/blackbirdworks/gopherstack/services/sagemaker"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

// === SNS Adapters ===

type snsFirehoseAdapter struct {
	backend *firehose.InMemoryBackend
}

func (a *snsFirehoseAdapter) PutRecordBatch(streamName string, records [][]byte) (int, error) {
	return a.backend.PutRecordBatch(context.Background(), streamName, records)
}

// === EventBridge Delivery Adapters ===

type ebKinesisStreamAdapter struct {
	backend *kinesis.InMemoryBackend
}

func (a *ebKinesisStreamAdapter) PutRecord(ctx context.Context, streamArn string, partitionKey string, data string) error {
	parts := strings.Split(streamArn, "/")
	streamName := parts[len(parts)-1]
	_, err := a.backend.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   streamName,
		PartitionKey: partitionKey,
		Data:         []byte(data),
	})
	return err
}

type ebFirehoseAdapter struct {
	backend *firehose.InMemoryBackend
}

func (a *ebFirehoseAdapter) PutRecord(ctx context.Context, streamArn string, data string) error {
	parts := strings.Split(streamArn, "/")
	streamName := parts[len(parts)-1]
	return a.backend.PutRecord(ctx, streamName, []byte(data))
}

type ebECSAdapter struct {
	backend *ecs.InMemoryBackend
}

func (a *ebECSAdapter) RunTask(ctx context.Context, clusterArn string, payload []byte) error {
	var input ecs.RunTaskInput
	if err := json.Unmarshal(payload, &input); err != nil {
		return err
	}
	input.Cluster = clusterArn
	_, err := a.backend.RunTask(input)
	return err
}

type ebCloudWatchLogsAdapter struct {
	backend *cloudwatchlogs.InMemoryBackend
}

func (a *ebCloudWatchLogsAdapter) PutLogEvents(ctx context.Context, logGroupName, logStreamName string, logEvents []any) error {
	var inputs []cloudwatchlogs.InputLogEvent
	for _, ev := range logEvents {
		inputs = append(inputs, cloudwatchlogs.InputLogEvent{
			Message:   fmt.Sprint(ev),
			Timestamp: time.Now().UnixMilli(),
		})
	}
	_, err := a.backend.PutLogEvents(ctx, logGroupName, logStreamName, "", inputs)
	return err
}

// === Scheduler Runner Adapters ===

type schedEventBusAdapter struct {
	backend *eventbridge.InMemoryBackend
}

func (a *schedEventBusAdapter) PutSchedulerEvent(ctx context.Context, busARN, source, detailType, detail string) error {
	parts := strings.Split(busARN, "/")
	busName := parts[len(parts)-1]
	
	now := time.Now()
	entries := []eventbridge.EventEntry{
		{
			EventBusName: busName,
			Source:       source,
			DetailType:   detailType,
			Detail:       detail,
			Time:         &now,
		},
	}
	a.backend.PutEvents(ctx, entries)
	return nil
}

type schedKinesisAdapter struct {
	backend *kinesis.InMemoryBackend
}

func (a *schedKinesisAdapter) PutSchedulerRecord(ctx context.Context, streamARN, partitionKey string, data []byte) error {
	parts := strings.Split(streamARN, "/")
	streamName := parts[len(parts)-1]
	_, err := a.backend.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   streamName,
		PartitionKey: partitionKey,
		Data:         data,
	})
	return err
}

type schedSageMakerAdapter struct {
	backend *sagemaker.InMemoryBackend
}

func (a *schedSageMakerAdapter) StartPipelineExecution(ctx context.Context, pipelineARN string, params map[string]string) error {
	return nil
}

type schedECSAdapter struct {
	backend *ecs.InMemoryBackend
}

func (a *schedECSAdapter) RunSchedulerTask(ctx context.Context, taskDefARN, launchType string, taskCount int) error {
	_, err := a.backend.RunTask(ecs.RunTaskInput{
		TaskDefinition: taskDefARN,
		LaunchType:     launchType,
		Count:          taskCount,
	})
	return err
}

// === Pipes Runner Adapters ===

type pipesKinesisAdapter struct {
	backend *kinesis.InMemoryBackend
}

func (a *pipesKinesisAdapter) PutRecord(ctx context.Context, streamARN, partitionKey string, data []byte) error {
	parts := strings.Split(streamARN, "/")
	streamName := parts[len(parts)-1]
	_, err := a.backend.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   streamName,
		PartitionKey: partitionKey,
		Data:         data,
	})
	return err
}

type pipesEventBridgeAdapter struct {
	backend *eventbridge.InMemoryBackend
}

func (a *pipesEventBridgeAdapter) PutEvents(ctx context.Context, eventBusARN string, events []map[string]any) error {
	parts := strings.Split(eventBusARN, "/")
	busName := parts[len(parts)-1]
	
	now := time.Now()
	var entries []eventbridge.EventEntry
	for _, ev := range events {
		source, _ := ev["source"].(string)
		detailType, _ := ev["detail-type"].(string)
		b, _ := json.Marshal(ev)
		entries = append(entries, eventbridge.EventEntry{
			EventBusName: busName,
			Source:       source,
			DetailType:   detailType,
			Detail:       string(b),
			Time:         &now,
		})
	}
	a.backend.PutEvents(ctx, entries)
	return nil
}

type pipesCloudWatchLogsAdapter struct {
	backend *cloudwatchlogs.InMemoryBackend
}

func (a *pipesCloudWatchLogsAdapter) PutLogEvents(ctx context.Context, logGroupARN, logStreamName string, messages []string) error {
	parts := strings.Split(logGroupARN, ":")
	var logGroupName string
	if len(parts) >= 7 {
		logGroupName = parts[6]
	}
	
	var inputs []cloudwatchlogs.InputLogEvent
	for _, msg := range messages {
		inputs = append(inputs, cloudwatchlogs.InputLogEvent{
			Message:   msg,
			Timestamp: time.Now().UnixMilli(),
		})
	}
	_, err := a.backend.PutLogEvents(ctx, logGroupName, logStreamName, "", inputs)
	return err
}

type pipesFirehoseAdapter struct {
	backend *firehose.InMemoryBackend
}

func (a *pipesFirehoseAdapter) PutRecord(ctx context.Context, deliveryStreamARN string, data []byte) error {
	parts := strings.Split(deliveryStreamARN, "/")
	streamName := parts[len(parts)-1]
	return a.backend.PutRecord(ctx, streamName, data)
}

type pipesSQSSenderAdapter struct {
	backend *sqsbackend.InMemoryBackend
}

func (a *pipesSQSSenderAdapter) SendMessage(ctx context.Context, queueARN, body, groupID, dedupID string) error {
	_, err := a.backend.SendMessage(&sqsbackend.SendMessageInput{
		QueueURL:               queueARN,
		MessageBody:            body,
		MessageGroupID:         groupID,
		MessageDeduplicationID: dedupID,
	})
	return err
}
