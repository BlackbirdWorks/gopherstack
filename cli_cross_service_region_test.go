package main

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

const (
	crossHome   = "us-east-1"
	crossAcct   = "000000000000"
	crossTDName = "worker"
)

func crossRegionServices(t *testing.T) map[string]service.Registerable {
	t.Helper()

	cli := &CLI{AccountID: crossAcct, Region: crossHome}
	portAlloc, err := portalloc.New(19600, 19700)
	require.NoError(t, err)

	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(&service.AppContext{
		Logger: slog.Default(), Config: cli, JanitorCtx: t.Context(), PortAlloc: portAlloc,
	})
	require.NoError(t, err)

	return serviceByName(services)
}

func metricNamespaceNames(t *testing.T, bk cwbackend.StorageBackend, namespace, name string) int {
	t.Helper()

	p, err := bk.ListMetrics(namespace, name, nil, "", "", 0)
	require.NoError(t, err)

	return len(p.Data)
}

func TestInitializeServices_CrossServiceMetricsLandInEmittingRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	cwH, ok := byName["CloudWatch"].(*cwbackend.Handler)
	require.True(t, ok)

	sqsH, ok := byName["SQS"].(*sqsbackend.Handler)
	require.True(t, ok)

	sqsBk, ok := sqsH.Backend.(*sqsbackend.InMemoryBackend)
	require.True(t, ok)

	logsH, ok := byName["CloudWatchLogs"].(*cwlogsbackend.Handler)
	require.True(t, ok)

	logsBk, ok := logsH.Backend.(*cwlogsbackend.InMemoryBackend)
	require.True(t, ok)

	tests := []struct {
		emit      func(t *testing.T, region string)
		name      string
		namespace string
		metric    string
		region    string
	}{
		{
			name: "sqs-eu", namespace: "AWS/SQS", metric: "NumberOfMessagesSent", region: euRegion,
			emit: func(t *testing.T, region string) {
				t.Helper()

				q, err := sqsBk.CreateQueue(&sqsbackend.CreateQueueInput{QueueName: "q-eu", Region: region})
				require.NoError(t, err)

				_, err = sqsBk.SendMessage(&sqsbackend.SendMessageInput{
					QueueURL: q.QueueURL, Region: region, MessageBody: "hi",
				})
				require.NoError(t, err)
			},
		},
		{
			name: "sqs-usw2", namespace: "AWS/SQS", metric: "NumberOfMessagesSent", region: "us-west-2",
			emit: func(t *testing.T, region string) {
				t.Helper()

				q, err := sqsBk.CreateQueue(&sqsbackend.CreateQueueInput{QueueName: "q-usw2", Region: region})
				require.NoError(t, err)

				_, err = sqsBk.SendMessage(&sqsbackend.SendMessageInput{
					QueueURL: q.QueueURL, Region: region, MessageBody: "hi",
				})
				require.NoError(t, err)
			},
		},
		{
			name: "logs-eu", namespace: "Custom/EU", metric: "Hits", region: euRegion,
			emit: func(t *testing.T, region string) {
				t.Helper()

				ctx := cwlogsbackend.WithRegion(t.Context(), region)
				_, err := logsBk.CreateLogGroup(ctx, "/app/eu", "", "")
				require.NoError(t, err)
				_, err = logsBk.CreateLogStream(ctx, "/app/eu", "s")
				require.NoError(t, err)
				mt := []cwlogsbackend.MetricTransformation{
					{MetricNamespace: "Custom/EU", MetricName: "Hits", MetricValue: "1"},
				}
				require.NoError(t, logsBk.PutMetricFilter(ctx, "/app/eu", "f", "ERROR", mt))

				_, err = logsBk.PutLogEvents(ctx, "/app/eu", "s", "", []cwlogsbackend.InputLogEvent{
					{Message: "ERROR boom", Timestamp: time.Now().UnixMilli()},
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.emit(t, tc.region)

			regional := cwH.BackendFor(tc.region)
			require.Eventually(t, func() bool {
				return metricNamespaceNames(t, regional, tc.namespace, tc.metric) > 0
			}, 5*time.Second, 10*time.Millisecond)

			assert.Zero(t, metricNamespaceNames(t, cwH.Backend, tc.namespace, tc.metric),
				"metric must not land in the home region")
		})
	}
}

func TestInitializeServices_CrossServiceECSTargetsUseOriginRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	ecsH, ok := byName["ECS"].(*ecsbackend.Handler)
	require.True(t, ok)

	homeECS, ok := ecsH.Backend.(*ecsbackend.InMemoryBackend)
	require.True(t, ok)

	euECS, ok := ecsH.BackendFor(euRegion).(*ecsbackend.InMemoryBackend)
	require.True(t, ok)

	_, err := euECS.CreateCluster(ecsbackend.CreateClusterInput{ClusterName: "default"})
	require.NoError(t, err)

	td, err := euECS.RegisterTaskDefinition(ecsbackend.RegisterTaskDefinitionInput{
		Family:               crossTDName,
		ContainerDefinitions: []ecsbackend.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	clusterARN := "arn:aws:ecs:" + euRegion + ":" + crossAcct + ":cluster/default"
	euCtx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Region: euRegion, Account: crossAcct})

	tests := []struct {
		run  func() error
		name string
	}{
		{
			name: "sfn-cluster-arn",
			run: func() error {
				_, runErr := (&sfnECSAdapter{handler: ecsH, home: homeECS}).SFNRunTask(t.Context(),
					map[string]any{"Cluster": clusterARN, "TaskDefinition": td.TaskDefinitionArn})

				return runErr
			},
		},
		{
			name: "sfn-execution-region",
			run: func() error {
				_, runErr := (&sfnECSAdapter{handler: ecsH, home: homeECS}).SFNRunTask(euCtx,
					map[string]any{"Cluster": "default", "TaskDefinition": crossTDName})

				return runErr
			},
		},
		{
			name: "scheduler-task-definition-arn",
			run: func() error {
				return (&schedECSAdapter{handler: ecsH, home: homeECS}).
					RunSchedulerTask(t.Context(), td.TaskDefinitionArn, "FARGATE", 1)
			},
		},
	}

	for i, tc := range tests {
		require.NoError(t, tc.run(), tc.name)

		tasks, listErr := euECS.ListTasks("default")
		require.NoError(t, listErr)
		assert.Len(t, tasks, i+1, tc.name)
	}

	_, listErr := homeECS.ListTasks("default")
	assert.Error(t, listErr, "home region must not have received any task")
}

func TestInitializeServices_CrossServiceGlueAndTaggingUseOriginRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)
	euCtx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Region: euRegion, Account: crossAcct})

	t.Run("sfn-glue-job-run", func(t *testing.T) {
		t.Parallel()

		glueH, ok := byName["Glue"].(*gluebackend.Handler)
		require.True(t, ok)

		homeGlue, ok := glueH.Backend.(*gluebackend.InMemoryBackend)
		require.True(t, ok)

		euGlue, ok := glueH.BackendFor(euRegion).(*gluebackend.InMemoryBackend)
		require.True(t, ok)

		_, err := euGlue.CreateJob(gluebackend.Job{
			Name: "etl", Role: "arn:aws:iam::" + crossAcct + ":role/r",
			Command: gluebackend.JobCommand{Name: "glueetl"},
		})
		require.NoError(t, err)

		runID, err := (&sfnGlueAdapter{handler: glueH, home: homeGlue}).SFNStartJobRun(euCtx, "etl", nil)
		require.NoError(t, err)

		_, err = euGlue.GetJobRun("etl", runID)
		require.NoError(t, err)

		_, err = homeGlue.GetJobRun("etl", runID)
		require.Error(t, err)
	})

	t.Run("tagging-ecs", func(t *testing.T) {
		t.Parallel()

		ecsH, ok := byName["ECS"].(*ecsbackend.Handler)
		require.True(t, ok)

		euECS, ok := ecsH.BackendFor(euRegion).(*ecsbackend.InMemoryBackend)
		require.True(t, ok)

		c, err := euECS.CreateCluster(ecsbackend.CreateClusterInput{ClusterName: "tagged"})
		require.NoError(t, err)

		rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
		require.True(t, ok)

		_, err = rgtH.Backend.TagResources(euCtx, &rgtapibackend.TagResourcesInput{
			ResourceARNList: []string{c.ClusterArn}, Tags: map[string]string{"Team": "eu"},
		})
		require.NoError(t, err)

		in := &rgtapibackend.GetResourcesInput{
			TagFilters: []rgtapibackend.TagFilter{{Key: "Team", Values: []string{"eu"}}},
		}

		euOut, err := rgtH.Backend.GetResources(euCtx, in)
		require.NoError(t, err)

		homeOut, err := rgtH.Backend.GetResources(t.Context(), in)
		require.NoError(t, err)

		assert.Contains(t, taggedARNs(euOut), c.ClusterArn)
		assert.NotContains(t, taggedARNs(homeOut), c.ClusterArn)
	})
}

func taggedARNs(out *rgtapibackend.GetResourcesOutput) []string {
	arns := make([]string, 0, len(out.ResourceTagMappingList))
	for _, m := range out.ResourceTagMappingList {
		arns = append(arns, m.ResourceARN)
	}

	return arns
}
