package main

// Exercises wirePipesRunner (cli.go) end to end against REAL service backends
// -- not test doubles -- to prove that the composition-root wiring this pass
// added actually delivers, closing the PARITY.md gap where SNS/SQS/Kinesis/
// EventBridge/CloudWatchLogs/Firehose targets and Kinesis/DynamoDB-Streams
// sources were modeled in the wire shapes but never wired to a real backend
// (every invokeXTarget call returned ErrTargetInvokerUnwired, and Kinesis/
// DynamoDB-Streams-sourced pipes were never polled at all).

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	pipesbackend "github.com/blackbirdworks/gopherstack/services/pipes"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

// pipesWiringRig bundles every backend wirePipesRunner wires, all built with
// real in-memory backends (no mocks), to prove the actual cli.go composition
// wiring -- not just the pipes package's own unit-tested Runner logic against
// fakes -- delivers end to end.
type pipesWiringRig struct {
	pipesBk    *pipesbackend.InMemoryBackend
	sqsBk      *sqsbackend.InMemoryBackend
	lambdaBk   *lambdabackend.InMemoryBackend
	snsBk      *snsbackend.InMemoryBackend
	kinesisBk  *kinesisbackend.InMemoryBackend
	ebBk       *ebbackend.InMemoryBackend
	cwlogsBk   *cwlogsbackend.InMemoryBackend
	firehoseBk *firehosebackend.InMemoryBackend
	ddbBk      *ddbbackend.InMemoryDB
	runner     *pipesbackend.Runner
}

func newPipesWiringRig(t *testing.T) *pipesWiringRig {
	t.Helper()

	pipesBk := pipesbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	pipesH := pipesbackend.NewHandler(pipesBk)

	sqsBk := sqsbackend.NewInMemoryBackend()
	sqsH := sqsbackend.NewHandler(sqsBk)

	lambdaBk := lambdabackend.NewInMemoryBackend(
		nil, nil, lambdabackend.DefaultSettings(), config.DefaultAccountID, config.DefaultRegion,
	)
	lambdaH := lambdabackend.NewHandler(lambdaBk)

	snsBk := snsbackend.NewInMemoryBackend()
	snsH := snsbackend.NewHandler(snsBk)

	kinesisBk := kinesisbackend.NewInMemoryBackend()
	kinesisH := kinesisbackend.NewHandler(kinesisBk)

	ebBk := ebbackend.NewInMemoryBackend()
	ebH := ebbackend.NewHandler(ebBk)

	cwlogsBk := cwlogsbackend.NewInMemoryBackend()
	cwlogsH := cwlogsbackend.NewHandler(cwlogsBk)

	firehoseBk := firehosebackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	firehoseH := firehosebackend.NewHandler(firehoseBk)

	ddbBk := ddbbackend.NewInMemoryDB()
	ddbH := ddbbackend.NewHandler(ddbBk)

	// This is the exact call cli.go's setupServices makes.
	wirePipesRunner(pipesH, sqsH, lambdaH, nil, snsH, kinesisH, ebH, cwlogsH, firehoseH, ddbH)

	// cli.go also wires SNS fan-out to SQS; the SNS target subtest observes
	// delivery through a real SQS subscription rather than SNS's archive,
	// since archiving is only legal on FIFO topics and FIFO publishes require
	// a MessageGroupId the pipes SNS adapter does not send.
	wireSNSToSQS(snsH, sqsH)

	runner := pipesH.GetRunner()

	ctx, cancel := context.WithCancel(context.Background())
	runner.Start(ctx)
	t.Cleanup(func() {
		cancel()
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer waitCancel()
		runner.Wait(waitCtx)
	})

	return &pipesWiringRig{
		pipesBk:    pipesBk,
		sqsBk:      sqsBk,
		lambdaBk:   lambdaBk,
		snsBk:      snsBk,
		kinesisBk:  kinesisBk,
		ebBk:       ebBk,
		cwlogsBk:   cwlogsBk,
		firehoseBk: firehoseBk,
		ddbBk:      ddbBk,
		runner:     runner,
	}
}

// createSQSSourcedPipe creates a RUNNING pipe from a freshly created real SQS
// source queue to targetARN, and returns the source queue's URL so the caller
// can seed a message that the real Runner (already started and ticking) will
// pick up.
func (r *pipesWiringRig) createSQSSourcedPipe(t *testing.T, pipeName, targetARN string) string {
	t.Helper()

	queueName := pipeName + "-src"
	qOut, err := r.sqsBk.CreateQueue(&sqsbackend.CreateQueueInput{QueueName: queueName})
	require.NoError(t, err)

	sourceARN := arn.Build("sqs", config.DefaultRegion, config.DefaultAccountID, queueName)

	_, err = r.pipesBk.CreatePipe(context.Background(), pipesbackend.CreatePipeInput{
		Name:         pipeName,
		RoleARN:      "arn:aws:iam::" + config.DefaultAccountID + ":role/r",
		Source:       sourceARN,
		Target:       targetARN,
		DesiredState: "RUNNING",
	})
	require.NoError(t, err)
	r.waitPipeRunning(t, pipeName)

	return qOut.QueueURL
}

func (r *pipesWiringRig) waitPipeRunning(t *testing.T, name string) {
	t.Helper()

	require.Eventually(t, func() bool {
		p, err := r.pipesBk.GetPipe(context.Background(), name)

		return err == nil && p.CurrentState == "RUNNING"
	}, 2*time.Second, 5*time.Millisecond, "pipe %q did not reach RUNNING", name)
}

func (r *pipesWiringRig) sendSourceMessage(t *testing.T, queueURL, body string) {
	t.Helper()

	_, err := r.sqsBk.SendMessage(&sqsbackend.SendMessageInput{
		QueueURL:    queueURL,
		MessageBody: body,
	})
	require.NoError(t, err)
}

// TestWirePipesRunner_SQSSourceTargets proves each newly-wired target invoker
// (SNS, SQS, Kinesis, EventBridge, CloudWatch Logs, Firehose) actually
// delivers an SQS-sourced pipe's event to the real backing service, instead
// of returning pipes.ErrTargetInvokerUnwired as it did before this pass wired
// wirePipesTargets in cli.go.
func TestWirePipesRunner_SQSSourceTargets(t *testing.T) {
	t.Parallel()

	t.Run("SNS", func(t *testing.T) {
		t.Parallel()
		rig := newPipesWiringRig(t)

		// Delivery is observed through a real SQS subscription: a bare
		// Publish() with no subscriptions leaves nothing to assert on, and
		// SNS's message archive is only legal on FIFO topics (whose publishes
		// require a MessageGroupId the pipes SNS adapter does not send).
		topic, err := rig.snsBk.CreateTopic("pipes-sns-target", nil)
		require.NoError(t, err)

		sinkOut, err := rig.sqsBk.CreateQueue(&sqsbackend.CreateQueueInput{QueueName: "pipes-sns-sink"})
		require.NoError(t, err)

		sinkARN := arn.Build("sqs", config.DefaultRegion, config.DefaultAccountID, "pipes-sns-sink")
		_, err = rig.snsBk.Subscribe(topic.TopicArn, "sqs", sinkARN, "")
		require.NoError(t, err)

		qURL := rig.createSQSSourcedPipe(t, "sns-target-pipe", topic.TopicArn)
		rig.sendSourceMessage(t, qURL, "hello-sns-target")

		require.Eventually(t, func() bool {
			out, recvErr := rig.sqsBk.ReceiveMessage(&sqsbackend.ReceiveMessageInput{
				QueueURL:            sinkOut.QueueURL,
				MaxNumberOfMessages: 10,
			})
			if recvErr != nil {
				return false
			}
			for _, m := range out.Messages {
				if containsSubstring(m.Body, "hello-sns-target") {
					return true
				}
			}

			return false
		}, 3*time.Second, 20*time.Millisecond,
			"SQS-sourced pipe must deliver to the real SNS backend via wirePipesRunner's SNS adapter")
	})

	t.Run("SQS", func(t *testing.T) {
		t.Parallel()
		rig := newPipesWiringRig(t)

		targetOut, err := rig.sqsBk.CreateQueue(&sqsbackend.CreateQueueInput{QueueName: "pipes-sqs-target"})
		require.NoError(t, err)
		targetARN := arn.Build("sqs", config.DefaultRegion, config.DefaultAccountID, "pipes-sqs-target")

		qURL := rig.createSQSSourcedPipe(t, "sqs-target-pipe", targetARN)
		rig.sendSourceMessage(t, qURL, "hello-sqs-target")

		require.Eventually(t, func() bool {
			out, recvErr := rig.sqsBk.ReceiveMessage(&sqsbackend.ReceiveMessageInput{
				QueueURL:            targetOut.QueueURL,
				MaxNumberOfMessages: 1,
			})

			return recvErr == nil && len(out.Messages) == 1 &&
				containsSubstring(out.Messages[0].Body, "hello-sqs-target")
		}, 3*time.Second, 20*time.Millisecond,
			"SQS-sourced pipe must deliver to the real SQS target backend via wirePipesRunner's SQS adapter")
	})

	t.Run("Kinesis", func(t *testing.T) {
		t.Parallel()
		rig := newPipesWiringRig(t)

		require.NoError(t, rig.kinesisBk.CreateStream(context.Background(), &kinesisbackend.CreateStreamInput{
			StreamName: "pipes-kinesis-target",
			ShardCount: 1,
		}))
		targetARN := arn.Build("kinesis", config.DefaultRegion, config.DefaultAccountID, "stream/pipes-kinesis-target")

		qURL := rig.createSQSSourcedPipe(t, "kinesis-target-pipe", targetARN)
		rig.sendSourceMessage(t, qURL, "hello-kinesis-target")

		require.Eventually(t, func() bool {
			return kinesisStreamContains(t, rig.kinesisBk, "pipes-kinesis-target", "hello-kinesis-target")
		}, 3*time.Second, 20*time.Millisecond,
			"SQS-sourced pipe must deliver to the real Kinesis target backend via wirePipesRunner's Kinesis adapter")
	})

	t.Run("EventBridge", func(t *testing.T) {
		t.Parallel()
		rig := newPipesWiringRig(t)

		busARN := arn.Build("events", config.DefaultRegion, config.DefaultAccountID, "event-bus/default")

		archive, err := rig.ebBk.CreateArchive(context.Background(), ebbackend.CreateArchiveInput{
			ArchiveName:    "pipes-eb-archive",
			EventSourceArn: busARN,
		})
		require.NoError(t, err)

		qURL := rig.createSQSSourcedPipe(t, "eb-target-pipe", busARN)
		rig.sendSourceMessage(t, qURL, "hello-eb-target")

		require.Eventually(
			t,
			func() bool {
				got, descErr := rig.ebBk.DescribeArchive(context.Background(), archive.ArchiveName)

				return descErr == nil && got.EventCount >= 1
			},
			3*time.Second,
			20*time.Millisecond,
			"SQS-sourced pipe must deliver to the real EventBridge target backend via wirePipesRunner's EventBridge adapter",
		)
	})

	t.Run("CloudWatchLogs", func(t *testing.T) {
		t.Parallel()
		rig := newPipesWiringRig(t)

		_, err := rig.cwlogsBk.CreateLogGroup(context.Background(), "pipes-cwlogs-target", "", "")
		require.NoError(t, err)
		_, err = rig.cwlogsBk.CreateLogStream(context.Background(), "pipes-cwlogs-target", "pipes-stream")
		require.NoError(t, err)

		targetARN := arn.Build("logs", config.DefaultRegion, config.DefaultAccountID, "log-group:pipes-cwlogs-target")

		_, err = rig.pipesBk.CreatePipe(context.Background(), pipesbackend.CreatePipeInput{
			Name:         "cwlogs-target-pipe",
			RoleARN:      "arn:aws:iam::" + config.DefaultAccountID + ":role/r",
			Source:       arn.Build("sqs", config.DefaultRegion, config.DefaultAccountID, "cwlogs-target-pipe-src"),
			Target:       targetARN,
			DesiredState: "RUNNING",
			TargetParameters: &pipesbackend.TargetParameters{
				CloudWatchLogsParameters: &pipesbackend.CloudWatchLogsTargetParameters{
					LogStreamName: "pipes-stream",
				},
			},
		})
		require.NoError(t, err)
		qOut, err := rig.sqsBk.CreateQueue(&sqsbackend.CreateQueueInput{QueueName: "cwlogs-target-pipe-src"})
		require.NoError(t, err)
		rig.waitPipeRunning(t, "cwlogs-target-pipe")
		rig.sendSourceMessage(t, qOut.QueueURL, "hello-cwlogs-target")

		require.Eventually(t, func() bool {
			events, _, _, getErr := rig.cwlogsBk.GetLogEvents(
				context.Background(), "pipes-cwlogs-target", "pipes-stream", nil, nil, 0, "", true,
			)
			if getErr != nil {
				return false
			}
			for _, e := range events {
				if containsSubstring(e.Message, "hello-cwlogs-target") {
					return true
				}
			}

			return false
		}, 3*time.Second, 20*time.Millisecond,
			"SQS-sourced pipe must deliver to the real CloudWatch Logs target backend via wirePipesRunner's adapter")
	})

	t.Run("Firehose", func(t *testing.T) {
		t.Parallel()
		rig := newPipesWiringRig(t)

		s3mock := &wiringMockS3Storer{}
		rig.firehoseBk.SetS3Backend(s3mock)

		_, err := rig.firehoseBk.CreateDeliveryStream(context.Background(), firehosebackend.CreateDeliveryStreamInput{
			Name: "pipes-firehose-target",
			S3Destination: &firehosebackend.S3DestinationDescription{
				BucketARN: "arn:aws:s3:::pipes-firehose-bucket",
				BufferingHints: &firehosebackend.BufferingHints{
					SizeInMBs:         128,
					IntervalInSeconds: 900,
				},
			},
		})
		require.NoError(t, err)
		targetARN := arn.Build(
			"firehose", config.DefaultRegion, config.DefaultAccountID, "deliverystream/pipes-firehose-target",
		)

		qURL := rig.createSQSSourcedPipe(t, "firehose-target-pipe", targetARN)
		rig.sendSourceMessage(t, qURL, "hello-firehose-target")

		require.Eventually(t, func() bool {
			rig.firehoseBk.FlushAll(context.Background())

			for _, c := range s3mock.calls {
				if containsSubstring(string(c.body), "hello-firehose-target") {
					return true
				}
			}

			return false
		}, 3*time.Second, 20*time.Millisecond,
			"SQS-sourced pipe must deliver to the real Firehose target backend via wirePipesRunner's Firehose adapter")
	})
}

// TestWirePipesRunner_KinesisSource proves the real Kinesis backend adapter
// wired by wirePipesRunner actually polls a Kinesis-sourced RUNNING pipe,
// closing the "Kinesis sources are modeled but never polled" PARITY.md gap.
func TestWirePipesRunner_KinesisSource(t *testing.T) {
	t.Parallel()
	rig := newPipesWiringRig(t)

	require.NoError(t, rig.kinesisBk.CreateStream(context.Background(), &kinesisbackend.CreateStreamInput{
		StreamName: "pipes-kinesis-source",
		ShardCount: 1,
	}))
	sourceARN := arn.Build("kinesis", config.DefaultRegion, config.DefaultAccountID, "stream/pipes-kinesis-source")

	targetOut, err := rig.sqsBk.CreateQueue(&sqsbackend.CreateQueueInput{QueueName: "pipes-kinesis-source-target"})
	require.NoError(t, err)
	targetARN := arn.Build("sqs", config.DefaultRegion, config.DefaultAccountID, "pipes-kinesis-source-target")

	_, err = rig.pipesBk.CreatePipe(context.Background(), pipesbackend.CreatePipeInput{
		Name:         "kinesis-source-pipe",
		RoleARN:      "arn:aws:iam::" + config.DefaultAccountID + ":role/r",
		Source:       sourceARN,
		Target:       targetARN,
		DesiredState: "RUNNING",
	})
	require.NoError(t, err)
	rig.waitPipeRunning(t, "kinesis-source-pipe")

	_, err = rig.kinesisBk.PutRecord(context.Background(), &kinesisbackend.PutRecordInput{
		StreamName:   "pipes-kinesis-source",
		PartitionKey: "pk",
		Data:         []byte("hello-kinesis-source"),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, recvErr := rig.sqsBk.ReceiveMessage(&sqsbackend.ReceiveMessageInput{
			QueueURL:            targetOut.QueueURL,
			MaxNumberOfMessages: 1,
		})

		return recvErr == nil && len(out.Messages) == 1 &&
			containsSubstring(out.Messages[0].Body, "aws:kinesis")
	}, 3*time.Second, 20*time.Millisecond,
		"a real Kinesis PutRecord must be polled by the pipes Runner and delivered to the target "+
			"via wirePipesRunner's Kinesis source adapter")
}

// TestWirePipesRunner_DynamoDBStreamSource proves the real DynamoDB backend
// adapter wired by wirePipesRunner actually polls a DynamoDB-Streams-sourced
// RUNNING pipe, closing the same PARITY.md gap for the DynamoDB Streams
// source type.
func TestWirePipesRunner_DynamoDBStreamSource(t *testing.T) {
	t.Parallel()
	rig := newPipesWiringRig(t)

	ctx := context.Background()

	_, err := rig.ddbBk.CreateTable(ctx, &ddbsdk.CreateTableInput{
		TableName: aws.String("pipes-ddb-source"),
		KeySchema: []ddbsdktypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbsdktypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbsdktypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbsdktypes.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &ddbsdktypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)
	require.NoError(t, rig.ddbBk.EnableStream(ctx, "pipes-ddb-source", "NEW_IMAGE"))

	table, ok := rig.ddbBk.GetTable("pipes-ddb-source")
	require.True(t, ok)
	sourceARN := table.StreamARN
	require.NotEmpty(t, sourceARN)

	targetOut, err := rig.sqsBk.CreateQueue(&sqsbackend.CreateQueueInput{QueueName: "pipes-ddb-source-target"})
	require.NoError(t, err)
	targetARN := arn.Build("sqs", config.DefaultRegion, config.DefaultAccountID, "pipes-ddb-source-target")

	_, err = rig.pipesBk.CreatePipe(ctx, pipesbackend.CreatePipeInput{
		Name:         "ddb-source-pipe",
		RoleARN:      "arn:aws:iam::" + config.DefaultAccountID + ":role/r",
		Source:       sourceARN,
		Target:       targetARN,
		DesiredState: "RUNNING",
	})
	require.NoError(t, err)
	rig.waitPipeRunning(t, "ddb-source-pipe")

	_, err = rig.ddbBk.PutItem(ctx, &ddbsdk.PutItemInput{
		TableName: aws.String("pipes-ddb-source"),
		Item: map[string]ddbsdktypes.AttributeValue{
			"pk": &ddbsdktypes.AttributeValueMemberS{Value: "hello-ddb-source"},
		},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, recvErr := rig.sqsBk.ReceiveMessage(&sqsbackend.ReceiveMessageInput{
			QueueURL:            targetOut.QueueURL,
			MaxNumberOfMessages: 1,
		})

		return recvErr == nil && len(out.Messages) == 1 &&
			containsSubstring(out.Messages[0].Body, "hello-ddb-source")
	}, 3*time.Second, 20*time.Millisecond,
		"a real DynamoDB PutItem must be polled from the stream by the pipes Runner and delivered to the "+
			"target via wirePipesRunner's DynamoDB Streams source adapter")
}

// TestWirePipesRunner_DLQDelivery proves that a failed target delivery (a
// Lambda invocation against a function that was never created, which fails
// deterministically) is redirected to the real SQS DLQ, proving both the
// Lambda target invoker and the SQS DLQ sender were wired. The DLQ is
// configured nested under SourceParameters.KinesisStreamParameters -- the
// only location the real Pipes API allows one (see
// aws-sdk-go-v2/service/pipes/types; there is no top-level DeadLetterConfig,
// and SQS sources have no DLQ config at all).
func TestWirePipesRunner_DLQDelivery(t *testing.T) {
	t.Parallel()
	rig := newPipesWiringRig(t)

	dlqOut, err := rig.sqsBk.CreateQueue(&sqsbackend.CreateQueueInput{QueueName: "pipes-dlq"})
	require.NoError(t, err)
	dlqARN := arn.Build("sqs", config.DefaultRegion, config.DefaultAccountID, "pipes-dlq")

	lambdaARN := arn.Build("lambda", config.DefaultRegion, config.DefaultAccountID, "function:pipes-dlq-fn")

	require.NoError(t, rig.kinesisBk.CreateStream(context.Background(), &kinesisbackend.CreateStreamInput{
		StreamName: "pipes-dlq-source",
		ShardCount: 1,
	}))
	sourceARN := arn.Build("kinesis", config.DefaultRegion, config.DefaultAccountID, "stream/pipes-dlq-source")

	_, err = rig.pipesBk.CreatePipe(context.Background(), pipesbackend.CreatePipeInput{
		Name:         "dlq-pipe",
		RoleARN:      "arn:aws:iam::" + config.DefaultAccountID + ":role/r",
		Source:       sourceARN,
		Target:       lambdaARN,
		DesiredState: "RUNNING",
		SourceParameters: &pipesbackend.SourceParameters{
			KinesisStreamParameters: &pipesbackend.KinesisStreamSourceParameters{
				StartingPosition: "TRIM_HORIZON",
				DeadLetterConfig: &pipesbackend.DeadLetterConfig{Arn: dlqARN},
			},
		},
	})
	require.NoError(t, err)
	rig.waitPipeRunning(t, "dlq-pipe")

	_, err = rig.kinesisBk.PutRecord(context.Background(), &kinesisbackend.PutRecordInput{
		StreamName:   "pipes-dlq-source",
		PartitionKey: "pk",
		Data:         []byte("hello-dlq"),
	})
	require.NoError(t, err)

	wantData := base64.StdEncoding.EncodeToString([]byte("hello-dlq"))
	require.Eventually(t, func() bool {
		out, recvErr := rig.sqsBk.ReceiveMessage(&sqsbackend.ReceiveMessageInput{
			QueueURL:            dlqOut.QueueURL,
			MaxNumberOfMessages: 1,
		})

		return recvErr == nil && len(out.Messages) == 1 &&
			containsSubstring(out.Messages[0].Body, wantData)
	}, 3*time.Second, 20*time.Millisecond,
		"a failed Lambda target delivery for a Kinesis-sourced pipe must be redirected to the real SQS "+
			"DLQ configured under SourceParameters.KinesisStreamParameters.DeadLetterConfig")
}

// kinesisStreamContains reads every record currently on the stream's shards
// from TRIM_HORIZON and reports whether any record's data contains want.
func kinesisStreamContains(t *testing.T, bk *kinesisbackend.InMemoryBackend, streamName, want string) bool {
	t.Helper()

	ctx := context.Background()

	describeOut, err := bk.DescribeStream(ctx, &kinesisbackend.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		return false
	}

	for _, shard := range describeOut.Shards {
		iterOut, iterErr := bk.GetShardIterator(ctx, &kinesisbackend.GetShardIteratorInput{
			StreamName:        streamName,
			ShardID:           shard.ShardID,
			ShardIteratorType: "TRIM_HORIZON",
		})
		if iterErr != nil {
			continue
		}

		recOut, recErr := bk.GetRecords(ctx, &kinesisbackend.GetRecordsInput{
			ShardIterator: iterOut.ShardIterator,
			Limit:         100,
		})
		if recErr != nil {
			continue
		}

		for _, r := range recOut.Records {
			if containsSubstring(string(r.Data), want) {
				return true
			}
		}
	}

	return false
}

func containsSubstring(haystack, needle string) bool {
	return len(needle) == 0 || (len(haystack) >= len(needle) && indexOfSubstring(haystack, needle) >= 0)
}

func indexOfSubstring(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}

	return -1
}
