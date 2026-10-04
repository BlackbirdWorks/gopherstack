package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ddbmodels "github.com/blackbirdworks/gopherstack/services/dynamodb/models"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	sagemakerbackend "github.com/blackbirdworks/gopherstack/services/sagemaker"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

const targetWait = 5 * time.Second

func targetRegionCtx(t *testing.T, region string) context.Context {
	t.Helper()

	return awsmeta.Set(t.Context(), &awsmeta.Metadata{Region: region, Account: crossAcct})
}

func targetSQS(t *testing.T, byName map[string]service.Registerable) *sqsbackend.InMemoryBackend {
	t.Helper()

	h, ok := byName["SQS"].(*sqsbackend.Handler)
	require.True(t, ok)

	bk, ok := h.Backend.(*sqsbackend.InMemoryBackend)
	require.True(t, ok)

	return bk
}

func targetKinesis(t *testing.T, byName map[string]service.Registerable) *kinesisbackend.InMemoryBackend {
	t.Helper()

	h, ok := byName["Kinesis"].(*kinesisbackend.Handler)
	require.True(t, ok)

	bk, ok := h.Backend.(*kinesisbackend.InMemoryBackend)
	require.True(t, ok)

	return bk
}

func targetEventBridge(t *testing.T, byName map[string]service.Registerable) *ebbackend.InMemoryBackend {
	t.Helper()

	h, ok := byName["EventBridge"].(*ebbackend.Handler)
	require.True(t, ok)

	bk, ok := h.Backend.(*ebbackend.InMemoryBackend)
	require.True(t, ok)

	return bk
}

func targetLogs(t *testing.T, byName map[string]service.Registerable) *cwlogsbackend.InMemoryBackend {
	t.Helper()

	h, ok := byName["CloudWatchLogs"].(*cwlogsbackend.Handler)
	require.True(t, ok)

	bk, ok := h.Backend.(*cwlogsbackend.InMemoryBackend)
	require.True(t, ok)

	return bk
}

// newRegionalQueues creates the same-named queue in home and eu and returns the eu ARN.
func newRegionalQueues(t *testing.T, bk *sqsbackend.InMemoryBackend, name string) string {
	t.Helper()

	attrs := map[string]string(nil)
	if len(name) > 5 && name[len(name)-5:] == ".fifo" {
		attrs = map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"}
	}

	for _, region := range []string{crossHome, euRegion} {
		_, err := bk.CreateQueue(&sqsbackend.CreateQueueInput{QueueName: name, Region: region, Attributes: attrs})
		require.NoError(t, err)
	}

	return arn.Build("sqs", euRegion, crossAcct, name)
}

func queueBodies(t *testing.T, bk *sqsbackend.InMemoryBackend, region, name string) []string {
	t.Helper()

	out, err := bk.ReceiveMessage(&sqsbackend.ReceiveMessageInput{
		QueueURL:            "http://local/" + crossAcct + "/" + name,
		Region:              region,
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   sqsbackend.NoVisibilityTimeout,
	})
	require.NoError(t, err)

	bodies := make([]string, 0, len(out.Messages))
	for _, m := range out.Messages {
		bodies = append(bodies, m.Body)
	}

	return bodies
}

func TestInitializeServices_SQSTargetsUseQueueARNRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)
	bk := targetSQS(t, byName)
	sender := &sqsSenderAdapter{backend: bk}
	pipesSender := &pipesSQSSenderAdapter{backend: bk}

	tests := []struct {
		send func(ctx context.Context, queueARN string) error
		name string
	}{
		{
			name: "eventbridge-queue",
			send: func(ctx context.Context, a string) error { return sender.SendMessageToQueue(ctx, a, "m") },
		},
		{
			name: "eventbridge-dlq-attrs",
			send: func(ctx context.Context, a string) error {
				return sender.SendMessageWithAttributes(ctx, a, "m", map[string]string{"k": "v"})
			},
		},
		{
			name: "scheduler-fifo.fifo",
			send: func(ctx context.Context, a string) error { return sender.SendMessageToFIFOQueue(ctx, a, "m", "g") },
		},
		{
			name: "pipes-target",
			send: func(ctx context.Context, a string) error { return pipesSender.SendMessage(ctx, a, "m", "", "") },
		},
		{
			name: "stepfunctions-execution-region",
			send: func(_ context.Context, a string) error {
				_, _, err := sfnbackend.NewSQSIntegration(bk).SFNSendMessage(
					targetRegionCtx(t, euRegion), arnToSQSQueueURL(a), "m", "", "", 0)

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			queueARN := newRegionalQueues(t, bk, tc.name)

			require.NoError(t, tc.send(t.Context(), queueARN))
			assert.Equal(t, []string{"m"}, queueBodies(t, bk, euRegion, tc.name))
			assert.Empty(t, queueBodies(t, bk, crossHome, tc.name), "home queue must not receive")
		})
	}
}

func TestInitializeServices_PipesSQSSourceReadsQueueARNRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)
	bk := targetSQS(t, byName)
	queueARN := newRegionalQueues(t, bk, "pipe-src")

	for _, region := range []string{crossHome, euRegion} {
		_, err := bk.SendMessage(&sqsbackend.SendMessageInput{
			QueueURL: arnToSQSQueueURL(queueARN), Region: region, MessageBody: "from-" + region,
		})
		require.NoError(t, err)
	}

	reader := &pipesSQSReaderAdapter{backend: bk}

	msgs, err := reader.ReceivePipeMessages(queueARN, 10)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "from-"+euRegion, msgs[0].Body)

	require.NoError(t, reader.DeletePipeMessages(queueARN, []string{msgs[0].ReceiptHandle}))
}

func newRegionalStreams(t *testing.T, bk *kinesisbackend.InMemoryBackend, name string) string {
	t.Helper()

	shards := map[string]int{crossHome: 1, euRegion: 2}
	for region, n := range shards {
		ctx := targetRegionCtx(t, region)

		require.NoError(t, bk.CreateStream(ctx, &kinesisbackend.CreateStreamInput{
			StreamName: name, Region: region, ShardCount: n,
		}))
		require.Eventually(t, func() bool {
			out, err := bk.DescribeStream(ctx, &kinesisbackend.DescribeStreamInput{StreamName: name})

			return err == nil && out.StreamStatus == "ACTIVE"
		}, targetWait, 10*time.Millisecond)
	}

	return arn.Build("kinesis", euRegion, crossAcct, "stream/"+name)
}

func streamRecordCount(t *testing.T, bk *kinesisbackend.InMemoryBackend, ref string) int {
	t.Helper()

	reader := &kinesisStreamReaderAdapter{backend: bk}

	shards, err := reader.ListShards(ref)
	require.NoError(t, err)

	total := 0

	for _, shard := range shards {
		it, itErr := reader.GetShardIterator(ref, shard)
		require.NoError(t, itErr)

		recs, _, recErr := reader.GetRecords(it, 100)
		require.NoError(t, recErr)

		total += len(recs)
	}

	return total
}

func TestInitializeServices_KinesisTargetsUseStreamARNRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)
	bk := targetKinesis(t, byName)

	tests := []struct {
		send func(ctx context.Context, streamARN string) error
		name string
	}{
		{
			name: "eventbridge",
			send: func(ctx context.Context, a string) error {
				return (&ebKinesisStreamAdapter{backend: bk}).PutRecord(ctx, a, "pk", "data")
			},
		},
		{
			name: "scheduler",
			send: func(ctx context.Context, a string) error {
				return (&schedKinesisAdapter{backend: bk}).PutSchedulerRecord(ctx, a, "pk", []byte("data"))
			},
		},
		{
			name: "pipes",
			send: func(ctx context.Context, a string) error {
				return (&pipesKinesisPutterAdapter{backend: bk}).PutRecord(ctx, a, "pk", []byte("data"))
			},
		},
		{
			name: "logs-subscription",
			send: func(ctx context.Context, a string) error {
				return (&cwlogsSubscriptionDeliverer{kinesis: bk}).DeliverLogEvents(ctx, a, []byte("data"))
			},
		},
		{
			name: "dynamodb-streaming",
			send: func(_ context.Context, a string) error {
				(&ddbKinesisEmitterAdapter{backend: bk}).EmitDynamoDBStreamRecord(a, "t", ddbmodels.StreamRecord{})

				return nil
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			streamARN := newRegionalStreams(t, bk, "ks-"+tc.name)
			homeARN := arn.Build("kinesis", crossHome, crossAcct, "stream/ks-"+tc.name)

			require.NoError(t, tc.send(t.Context(), streamARN))
			require.Eventually(t, func() bool {
				return streamRecordCount(t, bk, streamARN) == 1
			}, targetWait, 10*time.Millisecond)

			assert.Zero(t, streamRecordCount(t, bk, homeARN), "home stream must not receive")
		})
	}
}

func TestInitializeServices_PipesKinesisSourceReadsStreamARNRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)
	bk := targetKinesis(t, byName)
	streamARN := newRegionalStreams(t, bk, "pipe-ks")
	reader := &pipesKinesisReaderAdapter{backend: bk}

	shards, err := reader.GetShardIDs(streamARN)
	require.NoError(t, err)
	require.Len(t, shards, 2, "must describe the eu stream, not the home one")

	_, err = reader.GetShardIterator(streamARN, shards[0], "TRIM_HORIZON", "")
	require.NoError(t, err)
}

func newRegionalEventRule(
	t *testing.T, bk *ebbackend.InMemoryBackend, region, bus, source string, target ebbackend.Target,
) {
	t.Helper()

	ctx := targetRegionCtx(t, region)

	_, err := bk.PutRule(ctx, ebbackend.PutRuleInput{
		Name: "r-" + source, EventBusName: bus, EventPattern: `{"source":["` + source + `"]}`, State: "ENABLED",
	})
	require.NoError(t, err)

	failed, err := bk.PutTargets(ctx, "r-"+source, bus, []ebbackend.Target{target})
	require.NoError(t, err)
	require.Empty(t, failed)
}

func TestInitializeServices_EventBridgePutEventsUseBusARNRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)
	eb := targetEventBridge(t, byName)
	sqsBk := targetSQS(t, byName)

	tests := []struct {
		put  func(ctx context.Context, busARN, bus, source string) error
		name string
	}{
		{
			name: "scheduler",
			put: func(ctx context.Context, busARN, _, source string) error {
				return (&schedEventBusAdapter{backend: eb}).PutSchedulerEvent(ctx, busARN, source, "T", "{}")
			},
		},
		{
			name: "pipes",
			put: func(ctx context.Context, busARN, _, source string) error {
				return (&pipesEventBridgePutterAdapter{backend: eb}).PutEvents(ctx, busARN,
					[]map[string]any{{"Source": source, "DetailType": "T", "Detail": "{}"}})
			},
		},
		{
			name: "bus-arn-entry",
			put: func(ctx context.Context, busARN, _, source string) error {
				_, err := eb.PutEvents(ctx, []ebbackend.EventEntry{
					{EventBusName: busARN, Source: source, DetailType: "T", Detail: "{}"},
				})

				return err
			},
		},
		{
			name: "stepfunctions-execution-region",
			put: func(_ context.Context, _, bus, source string) error {
				_, err := eb.SFNPutEvents(targetRegionCtx(t, euRegion), []map[string]any{
					{"Source": source, "DetailType": "T", "Detail": "{}", "EventBusName": bus},
				})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bus := "bus-" + tc.name
			source := "src." + tc.name
			queue := "eb-" + tc.name
			euARN := newRegionalQueues(t, sqsBk, queue)
			homeARN := arn.Build("sqs", crossHome, crossAcct, queue)

			for _, region := range []string{crossHome, euRegion} {
				_, err := eb.CreateEventBus(targetRegionCtx(t, region), ebbackend.CreateEventBusParams{Name: bus})
				require.NoError(t, err)
			}

			newRegionalEventRule(t, eb, euRegion, bus, source, ebbackend.Target{ID: "t", Arn: euARN})
			newRegionalEventRule(t, eb, crossHome, bus, source, ebbackend.Target{ID: "t", Arn: homeARN})

			busARN := arn.Build("events", euRegion, crossAcct, "event-bus/"+bus)
			require.NoError(t, tc.put(t.Context(), busARN, bus, source))
			require.Eventually(t, func() bool {
				out, err := sqsBk.ReceiveMessage(&sqsbackend.ReceiveMessageInput{
					QueueURL: arnToSQSQueueURL(euARN), Region: euRegion,
					MaxNumberOfMessages: 10, VisibilityTimeout: sqsbackend.NoVisibilityTimeout,
				})

				return err == nil && len(out.Messages) == 1
			}, targetWait, 10*time.Millisecond)

			assert.Empty(t, queueBodies(t, sqsBk, crossHome, queue), "home rule must not match")
		})
	}
}

func newRegionalLogGroups(t *testing.T, bk *cwlogsbackend.InMemoryBackend, group string) string {
	t.Helper()

	for _, region := range []string{crossHome, euRegion} {
		ctx := cwlogsbackend.WithRegion(t.Context(), region)

		_, err := bk.CreateLogGroup(ctx, group, "", "")
		require.NoError(t, err)

		for _, stream := range []string{"s", "EventBridge"} {
			_, err = bk.CreateLogStream(ctx, group, stream)
			require.NoError(t, err)
		}
	}

	return arn.Build("logs", euRegion, crossAcct, "log-group:"+group)
}

func logEventCount(t *testing.T, bk *cwlogsbackend.InMemoryBackend, region, group, stream string) int {
	t.Helper()

	events, _, _, err := bk.GetLogEvents(
		cwlogsbackend.WithRegion(t.Context(), region), group, stream, nil, nil, 100, "", true)
	require.NoError(t, err)

	return len(events)
}

func TestInitializeServices_LogsTargetsUseLogGroupARNRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)
	logs := targetLogs(t, byName)
	eb := targetEventBridge(t, byName)

	tests := []struct {
		deliver func(t *testing.T, groupARN, group string)
		name    string
		stream  string
	}{
		{
			name: "pipes", stream: "s",
			deliver: func(t *testing.T, groupARN, _ string) {
				t.Helper()

				require.NoError(t, (&pipesCloudWatchLogsPutterAdapter{backend: logs}).
					PutLogEvents(t.Context(), groupARN, "s", []string{"m"}))
			},
		},
		{
			name: "eventbridge-target", stream: "EventBridge",
			deliver: func(t *testing.T, groupARN, _ string) {
				t.Helper()

				euCtx := targetRegionCtx(t, euRegion)

				_, err := eb.CreateEventBus(euCtx, ebbackend.CreateEventBusParams{Name: "logs-bus"})
				require.NoError(t, err)

				newRegionalEventRule(t, eb, euRegion, "logs-bus", "src.logs",
					ebbackend.Target{ID: "t", Arn: groupARN})

				_, err = eb.PutEvents(euCtx, []ebbackend.EventEntry{
					{EventBusName: "logs-bus", Source: "src.logs", DetailType: "T", Detail: "{}"},
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			group := "/target/" + tc.name
			groupARN := newRegionalLogGroups(t, logs, group)

			tc.deliver(t, groupARN, group)

			require.Eventually(t, func() bool {
				return logEventCount(t, logs, euRegion, group, tc.stream) == 1
			}, targetWait, 10*time.Millisecond)

			assert.Zero(t, logEventCount(t, logs, crossHome, group, tc.stream), "home group must not receive")
		})
	}
}

func TestInitializeServices_SchedulerSageMakerPipelineUsesARNRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	h, ok := byName["SageMaker"].(*sagemakerbackend.Handler)
	require.True(t, ok)

	for _, region := range []string{crossHome, euRegion} {
		_, err := h.Backend.CreatePipelineFull(targetRegionCtx(t, region), sagemakerbackend.CreatePipelineOptions{
			PipelineName: "p", PipelineDefinition: "{}", RoleArn: "arn:aws:iam::" + crossAcct + ":role/r",
		})
		require.NoError(t, err)
	}

	pipelineARN := arn.Build("sagemaker", euRegion, crossAcct, "pipeline/p")
	require.NoError(t, (&schedSageMakerAdapter{backend: h.Backend}).
		StartPipelineExecution(t.Context(), pipelineARN, nil))

	count := func(region string) int {
		execs, _ := h.Backend.ListPipelineExecutions(targetRegionCtx(t, region),
			sagemakerbackend.ListPipelineExecutionsParams{PipelineName: "p"})

		return len(execs)
	}

	assert.Equal(t, 1, count(euRegion))
	assert.Zero(t, count(crossHome))
}
