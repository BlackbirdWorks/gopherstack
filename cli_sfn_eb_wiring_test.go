package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// TestWireStepFunctionsServiceIntegrations_ECSGlueEventBridge exercises the exact
// composition-root wiring cli.go's initializeServices performs (wireStepFunctionsServiceIntegrations)
// against REAL ECS, Glue, and EventBridge backends — not test doubles — proving that Step Functions
// Task states with ECS RunTask, Glue StartJobRun, and EventBridge PutEvents resources actually reach
// those backends, instead of failing with *IntegrationNotConfigured as they did before
// SetECSIntegration/SetGlueIntegration/SetEventBridgeIntegration were wired in the running binary.
func TestWireStepFunctionsServiceIntegrations_ECSGlueEventBridge(t *testing.T) {
	t.Parallel()

	sfnBk := sfnbackend.NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
	sfnH := sfnbackend.NewHandler(sfnBk)

	ecsBk := ecsbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion, nil)
	ecsH := ecsbackend.NewHandler(ecsBk)

	glueBk := gluebackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	glueH := gluebackend.NewHandler(glueBk)

	ebBk := ebbackend.NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
	ebH := ebbackend.NewHandler(ebBk)

	// This is the exact call cli.go's initializeServices makes (sqs/sns/ddb omitted: not
	// under test here).
	wireStepFunctionsServiceIntegrations(sfnH, nil, nil, nil, ecsH, glueH, ebH)

	ctx := context.Background()

	t.Run("ECS RunTask", func(t *testing.T) {
		t.Parallel()

		_, err := ecsBk.RegisterTaskDefinition(ecsbackend.RegisterTaskDefinitionInput{
			Family: "wiring-td",
			ContainerDefinitions: []ecsbackend.ContainerDefinition{
				{Name: "c", Image: "busybox"},
			},
		})
		require.NoError(t, err)

		def := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::ecs:runTask","End":true}}}`
		sm, err := sfnBk.CreateStateMachine(ctx, "sfn-ecs-wiring", def, "arn:role", "STANDARD")
		require.NoError(t, err)

		exec, err := sfnBk.StartExecution(
			sm.StateMachineArn, "exec-ecs",
			`{"TaskDefinition":"wiring-td","Cluster":"default","Count":1}`,
		)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			desc, descErr := sfnBk.DescribeExecution(exec.ExecutionArn)

			return descErr == nil && desc.Status != "RUNNING"
		}, 5*time.Second, 50*time.Millisecond)

		desc, err := sfnBk.DescribeExecution(exec.ExecutionArn)
		require.NoError(t, err)
		require.Equal(t, "SUCCEEDED", desc.Status,
			"ECS Task state must have reached the real ECS backend via SetECSIntegration; cause=%s", desc.Cause)
		assert.Contains(t, desc.Output, "Tasks")

		tasks, err := ecsBk.ListTasks("default")
		require.NoError(t, err)
		assert.NotEmpty(t, tasks, "RunTask must have created a real ECS task")
	})

	t.Run("Glue StartJobRun", func(t *testing.T) {
		t.Parallel()

		_, err := glueBk.CreateJob(gluebackend.Job{
			Name:    "wiring-job",
			Role:    "arn:role",
			Command: gluebackend.JobCommand{Name: "glueetl"},
		})
		require.NoError(t, err)

		def := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::glue:startJobRun","End":true}}}`
		sm, err := sfnBk.CreateStateMachine(ctx, "sfn-glue-wiring", def, "arn:role", "STANDARD")
		require.NoError(t, err)

		exec, err := sfnBk.StartExecution(sm.StateMachineArn, "exec-glue", `{"JobName":"wiring-job"}`)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			desc, descErr := sfnBk.DescribeExecution(exec.ExecutionArn)

			return descErr == nil && desc.Status != "RUNNING"
		}, 5*time.Second, 50*time.Millisecond)

		desc, err := sfnBk.DescribeExecution(exec.ExecutionArn)
		require.NoError(t, err)
		require.Equal(t, "SUCCEEDED", desc.Status,
			"Glue Task state must have reached the real Glue backend via SetGlueIntegration; cause=%s", desc.Cause)
		assert.Contains(t, desc.Output, "JobRunId")
	})

	t.Run("EventBridge PutEvents", func(t *testing.T) {
		t.Parallel()

		def := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::events:putEvents","End":true}}}`
		sm, err := sfnBk.CreateStateMachine(ctx, "sfn-eb-wiring", def, "arn:role", "STANDARD")
		require.NoError(t, err)

		input := `{"Entries":[{"Source":"wiring.test","DetailType":"Wiring","Detail":"{}"}]}`
		exec, err := sfnBk.StartExecution(sm.StateMachineArn, "exec-eb", input)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			desc, descErr := sfnBk.DescribeExecution(exec.ExecutionArn)

			return descErr == nil && desc.Status != "RUNNING"
		}, 5*time.Second, 50*time.Millisecond)

		desc, err := sfnBk.DescribeExecution(exec.ExecutionArn)
		require.NoError(t, err)
		require.Equal(t, "SUCCEEDED", desc.Status,
			"EventBridge Task state must have reached the real EventBridge backend via "+
				"SetEventBridgeIntegration; cause=%s", desc.Cause)

		log := ebBk.GetEventLog(ctx)
		found := false
		for _, e := range log {
			if e.Source == "wiring.test" {
				found = true

				break
			}
		}
		assert.True(t, found, "PutEvents must have reached the real EventBridge backend")
	})
}

// TestWireEventBridgeDelivery_KinesisFirehoseECSStepFunctionsCloudWatchLogs exercises the
// exact composition-root wiring cli.go's initializeServices performs (wireEventBridgeDelivery)
// against REAL Kinesis, Firehose, ECS, Step Functions, and CloudWatch Logs backends, proving
// that EventBridge rule targets of these types are actually delivered to, instead of silently
// no-opping (dt.KinesisStream/dt.KinesisFirehose/dt.ECS/dt.StepFunctions/dt.CloudWatchLogs were
// never populated before this wiring existed).
func TestWireEventBridgeDelivery_KinesisFirehoseECSStepFunctionsCloudWatchLogs(t *testing.T) {
	t.Parallel()

	ebBk := ebbackend.NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
	ebH := ebbackend.NewHandler(ebBk)

	kinesisBk := kinesisbackend.NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
	kinesisH := kinesisbackend.NewHandler(kinesisBk)

	firehoseBk := firehosebackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	firehoseH := firehosebackend.NewHandler(firehoseBk)

	ecsBk := ecsbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion, nil)
	ecsH := ecsbackend.NewHandler(ecsBk)

	sfnBk := sfnbackend.NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
	sfnH := sfnbackend.NewHandler(sfnBk)

	cwlogsBk := cwlogsbackend.NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
	cwlogsH := cwlogsbackend.NewHandler(cwlogsBk)

	// This is the exact call cli.go's initializeServices makes (lambda/sqs/sns omitted: not
	// under test here — covered by TestWireSNSToLambdaFirehose_EndToEndDelivery-style tests).
	wireEventBridgeDelivery(ebH, nil, nil, nil, kinesisH, firehoseH, ecsH, sfnH, cwlogsH)

	ctx := context.Background()
	region := config.DefaultRegion
	accountID := config.DefaultAccountID

	// --- Fixtures for each target type. ---

	require.NoError(t, kinesisBk.CreateStream(ctx, &kinesisbackend.CreateStreamInput{StreamName: "wiring-stream"}))
	streamDesc, err := kinesisBk.DescribeStream(ctx, &kinesisbackend.DescribeStreamInput{StreamName: "wiring-stream"})
	require.NoError(t, err)
	require.NotEmpty(t, streamDesc.Shards)

	fhStream, err := firehoseBk.CreateDeliveryStream(ctx, firehosebackend.CreateDeliveryStreamInput{
		Name: "wiring-fh",
		S3Destination: &firehosebackend.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::wiring-bucket",
			BufferingHints: &firehosebackend.BufferingHints{
				SizeInMBs:         128,
				IntervalInSeconds: 900,
			},
		},
	})
	require.NoError(t, err)
	s3mock := &wiringMockS3Storer{}
	firehoseBk.SetS3Backend(s3mock)

	_, err = ecsBk.RegisterTaskDefinition(ecsbackend.RegisterTaskDefinitionInput{
		Family:               "wiring-eb-td",
		ContainerDefinitions: []ecsbackend.ContainerDefinition{{Name: "c", Image: "busybox"}},
	})
	require.NoError(t, err)
	ecsClusterARN := arn.Build("ecs", region, accountID, "cluster/default")

	smDef := `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`
	sm, err := sfnBk.CreateStateMachine(ctx, "wiring-eb-sm", smDef, "arn:role", "STANDARD")
	require.NoError(t, err)

	_, err = cwlogsBk.CreateLogGroup(ctx, "wiring-lg", "", "")
	require.NoError(t, err)
	_, err = cwlogsBk.CreateLogStream(ctx, "wiring-lg", "EventBridge")
	require.NoError(t, err)
	logGroupARN := arn.Build("logs", region, accountID, "log-group:wiring-lg")

	// --- Rule matching a single source, with one target per service under test. ---

	_, err = ebBk.PutRule(ctx, ebbackend.PutRuleInput{
		Name:         "wiring-rule",
		EventPattern: `{"source":["wiring.delivery"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	kinesisARN := arn.Build("kinesis", region, accountID, "stream/wiring-stream")

	_, err = ebBk.PutTargets(ctx, "wiring-rule", "", []ebbackend.Target{
		{ID: "kinesis", Arn: kinesisARN, Input: `{"k":"v"}`},
		{ID: "firehose", Arn: fhStream.ARN, Input: `{"f":"v"}`},
		{ID: "ecs", Arn: ecsClusterARN, Input: `{"TaskDefinition":"wiring-eb-td","Cluster":"default"}`},
		{ID: "sfn", Arn: sm.StateMachineArn, Input: `{"s":"v"}`},
		{ID: "cwlogs", Arn: logGroupARN, Input: `{"l":"v"}`},
	})
	require.NoError(t, err)

	_, err = ebBk.PutEvents(ctx, []ebbackend.EventEntry{
		{Source: "wiring.delivery", DetailType: "Wiring", Detail: "{}"},
	})
	require.NoError(t, err)

	// --- Assert each target actually received the delivery. ---

	t.Run("Kinesis", func(t *testing.T) {
		t.Parallel()

		it, itErr := kinesisBk.GetShardIterator(ctx, &kinesisbackend.GetShardIteratorInput{
			StreamName:        "wiring-stream",
			ShardID:           streamDesc.Shards[0].ShardID,
			ShardIteratorType: "TRIM_HORIZON",
		})
		require.NoError(t, itErr)

		var recs *kinesisbackend.GetRecordsOutput
		require.Eventually(t, func() bool {
			var getErr error
			recs, getErr = kinesisBk.GetRecords(ctx, &kinesisbackend.GetRecordsInput{ShardIterator: it.ShardIterator})

			return getErr == nil && len(recs.Records) > 0
		}, 5*time.Second, 50*time.Millisecond, "EventBridge must have delivered to the real Kinesis backend")
		assert.Contains(t, string(recs.Records[0].Data), `"k":"v"`)
	})

	t.Run("Firehose", func(t *testing.T) {
		t.Parallel()

		require.Eventually(t, func() bool {
			firehoseBk.FlushAll(ctx)

			return len(s3mock.calls) > 0
		}, 5*time.Second, 50*time.Millisecond, "EventBridge must have delivered to the real Firehose backend")
		assert.Contains(t, string(s3mock.calls[len(s3mock.calls)-1].body), `"f":"v"`)
	})

	t.Run("ECS", func(t *testing.T) {
		t.Parallel()

		require.Eventually(t, func() bool {
			tasks, listErr := ecsBk.ListTasks("default")

			return listErr == nil && len(tasks) > 0
		}, 5*time.Second, 50*time.Millisecond, "EventBridge must have delivered to the real ECS backend")
	})

	t.Run("StepFunctions", func(t *testing.T) {
		t.Parallel()

		require.Eventually(t, func() bool {
			execs, _, listErr := sfnBk.ListExecutions(sm.StateMachineArn, "", "", 10)

			return listErr == nil && len(execs) > 0
		}, 5*time.Second, 50*time.Millisecond, "EventBridge must have delivered to the real Step Functions backend")
	})

	t.Run("CloudWatchLogs", func(t *testing.T) {
		t.Parallel()

		require.Eventually(t, func() bool {
			events, _, _, getErr := cwlogsBk.GetLogEvents(ctx, "wiring-lg", "EventBridge", nil, nil, 10, "", true)

			return getErr == nil && len(events) > 0
		}, 5*time.Second, 50*time.Millisecond, "EventBridge must have delivered to the real CloudWatch Logs backend")
	})

	t.Run("APIDestinationsSelfWired", func(t *testing.T) {
		t.Parallel()

		// EventBridge resolves its own API destinations; assert the backend satisfies the
		// resolver interface used by dt.APIDestinations (no separate adapter needed).
		_, ok := ebBk.ResolveAPIDestination("arn:aws:events:" + region + ":" + accountID + ":api-destination/none/1")
		assert.False(t, ok, "unknown API destination should resolve to false, not panic")
	})
}
