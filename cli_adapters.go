package main

import (
	"context"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/services/azureservicebus"
	"github.com/blackbirdworks/gopherstack/services/ecs"
	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// Scheduler target adapters; each resolves the target backend from the ARN's region.

type schedEventBusAdapter struct {
	backend *eventbridge.InMemoryBackend
}

func (a *schedEventBusAdapter) PutSchedulerEvent(
	ctx context.Context,
	busARN, source, detailType, detail string,
) error {
	parts := strings.Split(busARN, "/")
	busName := parts[len(parts)-1]
	ctx = inRegion(ctx, arnRegion(busARN))

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
	_, err := a.backend.PutEvents(ctx, entries)

	return err
}

type schedKinesisAdapter struct {
	backend *kinesis.InMemoryBackend
}

func (a *schedKinesisAdapter) PutSchedulerRecord(
	ctx context.Context,
	streamARN, partitionKey string,
	data []byte,
) error {
	ctx, streamName := kinesisRefContext(ctx, streamARN)
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

func (a *schedSageMakerAdapter) StartPipelineExecution(
	ctx context.Context,
	pipelineARN string,
	params map[string]string,
) error {
	parts := strings.Split(pipelineARN, "/")
	pipelineName := parts[len(parts)-1]
	ctx = inRegion(ctx, arnRegion(pipelineARN))

	pipelineParams := make([]sagemaker.PipelineParameter, 0, len(params))
	for name, value := range params {
		pipelineParams = append(pipelineParams, sagemaker.PipelineParameter{Name: name, Value: value})
	}

	_, err := a.backend.StartPipelineExecutionFull(ctx, sagemaker.StartPipelineExecutionOptions{
		PipelineName:       pipelineName,
		PipelineParameters: pipelineParams,
	})

	return err
}

// azureServiceBusEntitiesAdapter adapts services/azureservicebus's
// StorageBackend to services/azurearm.ServiceBusEntities, translating
// azurearm's primitive-typed calls into azureservicebus.EntityConfig -- see
// azurearm/interfaces.go's ServiceBusEntities doc comment for why azurearm
// itself never imports azureservicebus directly. Wired by cli.go's
// wireAzureARMResourceProviders (AZURE.md section 10.10's M9 entry).
type azureServiceBusEntitiesAdapter struct {
	backend azureservicebus.StorageBackend
}

func (a *azureServiceBusEntitiesAdapter) CreateQueue(
	name string,
	lockDuration, defaultMessageTTL time.Duration,
	maxDeliveryCount int,
) error {
	_, err := a.backend.CreateQueue(name, azureservicebus.EntityConfig{
		LockDuration:      lockDuration,
		DefaultMessageTTL: defaultMessageTTL,
		MaxDeliveryCount:  maxDeliveryCount,
	})

	return err
}

func (a *azureServiceBusEntitiesAdapter) DeleteQueue(name string) error {
	return a.backend.DeleteQueue(name)
}
func (a *azureServiceBusEntitiesAdapter) QueueExists(name string) bool {
	return a.backend.QueueExists(name)
}

func (a *azureServiceBusEntitiesAdapter) CreateTopic(name string, defaultMessageTTL time.Duration) error {
	_, err := a.backend.CreateTopic(name, azureservicebus.EntityConfig{DefaultMessageTTL: defaultMessageTTL})

	return err
}

func (a *azureServiceBusEntitiesAdapter) DeleteTopic(name string) error {
	return a.backend.DeleteTopic(name)
}
func (a *azureServiceBusEntitiesAdapter) TopicExists(name string) bool {
	return a.backend.TopicExists(name)
}

func (a *azureServiceBusEntitiesAdapter) CreateSubscription(
	topic, name string,
	lockDuration time.Duration,
	maxDeliveryCount int,
) error {
	_, err := a.backend.CreateSubscription(topic, name, azureservicebus.EntityConfig{
		LockDuration:     lockDuration,
		MaxDeliveryCount: maxDeliveryCount,
	})

	return err
}

func (a *azureServiceBusEntitiesAdapter) DeleteSubscription(topic, name string) error {
	return a.backend.DeleteSubscription(topic, name)
}

func (a *azureServiceBusEntitiesAdapter) SubscriptionExists(topic, name string) bool {
	return a.backend.SubscriptionExists(topic, name)
}

type schedECSAdapter struct {
	handler *ecs.Handler
	home    *ecs.InMemoryBackend
}

func (a *schedECSAdapter) RunSchedulerTask(
	ctx context.Context,
	taskDefARN, launchType string,
	taskCount int,
) error {
	bk := regionalECSBackend(a.handler, a.home)(originRegion(ctx, taskDefARN))
	_, _, err := bk.RunTask(ecs.RunTaskInput{
		TaskDefinition: taskDefARN,
		LaunchType:     launchType,
		Count:          taskCount,
	})

	return err
}
