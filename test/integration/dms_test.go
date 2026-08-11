//go:build integration
// +build integration

package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	dmssdk "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	dmstypes "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createDMSClient returns a DMS client pointed at the shared test container.
func createDMSClient(t *testing.T) *dmssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return dmssdk.NewFromConfig(cfg, func(o *dmssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_DMS_EndpointLifecycle tests the full DMS endpoint CRUD lifecycle.
func TestIntegration_DMS_EndpointLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name               string
		endpointIdentifier string
		endpointType       string
		engineName         string
		serverName         string
	}{
		{
			name:               "source_mysql",
			endpointIdentifier: "integ-src-" + uuid.NewString()[:8],
			endpointType:       "source",
			engineName:         "mysql",
			serverName:         "source.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createDMSClient(t)

			// Create endpoint.
			createOut, err := client.CreateEndpoint(ctx, &dmssdk.CreateEndpointInput{
				EndpointIdentifier: aws.String(tt.endpointIdentifier),
				EndpointType:       dmstypes.ReplicationEndpointTypeValueSource,
				EngineName:         aws.String(tt.engineName),
				ServerName:         aws.String(tt.serverName),
				Port:               aws.Int32(3306),
				DatabaseName:       aws.String("testdb"),
				Username:           aws.String("admin"),
				Password:           aws.String("password"),
			})
			require.NoError(t, err, "CreateEndpoint should succeed")
			require.NotNil(t, createOut.Endpoint)
			endpointARN := aws.ToString(createOut.Endpoint.EndpointArn)
			require.NotEmpty(t, endpointARN)

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteEndpoint(cleanupCtx, &dmssdk.DeleteEndpointInput{
					EndpointArn: aws.String(endpointARN),
				})
			})

			// Describe endpoints — should contain the created one.
			descOut, err := client.DescribeEndpoints(ctx, &dmssdk.DescribeEndpointsInput{
				Filters: []dmstypes.Filter{
					{
						Name:   aws.String("endpoint-id"),
						Values: []string{tt.endpointIdentifier},
					},
				},
			})
			require.NoError(t, err, "DescribeEndpoints should succeed")
			require.NotEmpty(t, descOut.Endpoints)
			assert.Equal(t, tt.endpointIdentifier, aws.ToString(descOut.Endpoints[0].EndpointIdentifier))

			// Modify endpoint.
			modifyOut, err := client.ModifyEndpoint(ctx, &dmssdk.ModifyEndpointInput{
				EndpointArn: aws.String(endpointARN),
				ServerName:  aws.String("newserver.example.com"),
			})
			require.NoError(t, err, "ModifyEndpoint should succeed")
			assert.Equal(t, "newserver.example.com", aws.ToString(modifyOut.Endpoint.ServerName))

			// Delete endpoint.
			_, err = client.DeleteEndpoint(ctx, &dmssdk.DeleteEndpointInput{
				EndpointArn: aws.String(endpointARN),
			})
			require.NoError(t, err, "DeleteEndpoint should succeed")
		})
	}
}

// TestIntegration_DMS_ReplicationTaskLifecycle tests the full DMS replication task CRUD lifecycle.
func TestIntegration_DMS_ReplicationTaskLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name           string
		taskIdentifier string
	}{
		{
			name:           "full_lifecycle",
			taskIdentifier: "integ-task-" + uuid.NewString()[:8],
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createDMSClient(t)

			// Create a replication instance first.
			instanceID := "integ-ri-" + uuid.NewString()[:8]
			riOut, err := client.CreateReplicationInstance(ctx, &dmssdk.CreateReplicationInstanceInput{
				ReplicationInstanceIdentifier: aws.String(instanceID),
				ReplicationInstanceClass:      aws.String("dms.t3.micro"),
				AllocatedStorage:              aws.Int32(50),
			})
			require.NoError(t, err, "CreateReplicationInstance should succeed")
			riARN := aws.ToString(riOut.ReplicationInstance.ReplicationInstanceArn)

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteReplicationInstance(cleanupCtx, &dmssdk.DeleteReplicationInstanceInput{
					ReplicationInstanceArn: aws.String(riARN),
				})
			})

			// Create source endpoint.
			srcID := "integ-src-" + uuid.NewString()[:8]
			srcOut, err := client.CreateEndpoint(ctx, &dmssdk.CreateEndpointInput{
				EndpointIdentifier: aws.String(srcID),
				EndpointType:       dmstypes.ReplicationEndpointTypeValueSource,
				EngineName:         aws.String("mysql"),
				ServerName:         aws.String("source.example.com"),
				Port:               aws.Int32(3306),
				DatabaseName:       aws.String("srcdb"),
				Username:           aws.String("admin"),
				Password:           aws.String("password"),
			})
			require.NoError(t, err, "CreateEndpoint (source) should succeed")
			srcARN := aws.ToString(srcOut.Endpoint.EndpointArn)

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteEndpoint(cleanupCtx, &dmssdk.DeleteEndpointInput{
					EndpointArn: aws.String(srcARN),
				})
			})

			// Create target endpoint.
			tgtID := "integ-tgt-" + uuid.NewString()[:8]
			tgtOut, err := client.CreateEndpoint(ctx, &dmssdk.CreateEndpointInput{
				EndpointIdentifier: aws.String(tgtID),
				EndpointType:       dmstypes.ReplicationEndpointTypeValueTarget,
				EngineName:         aws.String("postgres"),
				ServerName:         aws.String("target.example.com"),
				Port:               aws.Int32(5432),
				DatabaseName:       aws.String("tgtdb"),
				Username:           aws.String("admin"),
				Password:           aws.String("password"),
			})
			require.NoError(t, err, "CreateEndpoint (target) should succeed")
			tgtARN := aws.ToString(tgtOut.Endpoint.EndpointArn)

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteEndpoint(cleanupCtx, &dmssdk.DeleteEndpointInput{
					EndpointArn: aws.String(tgtARN),
				})
			})

			// Create replication task.
			taskOut, err := client.CreateReplicationTask(ctx, &dmssdk.CreateReplicationTaskInput{
				ReplicationTaskIdentifier: aws.String(tt.taskIdentifier),
				SourceEndpointArn:         aws.String(srcARN),
				TargetEndpointArn:         aws.String(tgtARN),
				ReplicationInstanceArn:    aws.String(riARN),
				MigrationType:             dmstypes.MigrationTypeValueFullLoad,
				TableMappings:             aws.String(`{"rules":[]}`),
			})
			require.NoError(t, err, "CreateReplicationTask should succeed")
			require.NotNil(t, taskOut.ReplicationTask)

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteReplicationTask(cleanupCtx, &dmssdk.DeleteReplicationTaskInput{
					ReplicationTaskArn: taskOut.ReplicationTask.ReplicationTaskArn,
				})
			})

			// Describe replication tasks — should contain the created one.
			descOut, err := client.DescribeReplicationTasks(ctx, &dmssdk.DescribeReplicationTasksInput{
				Filters: []dmstypes.Filter{
					{
						Name:   aws.String("replication-task-id"),
						Values: []string{tt.taskIdentifier},
					},
				},
			})
			require.NoError(t, err, "DescribeReplicationTasks should succeed")
			require.NotEmpty(t, descOut.ReplicationTasks)
			assert.Equal(t, tt.taskIdentifier, aws.ToString(descOut.ReplicationTasks[0].ReplicationTaskIdentifier))

			// Delete replication task.
			_, err = client.DeleteReplicationTask(ctx, &dmssdk.DeleteReplicationTaskInput{
				ReplicationTaskArn: taskOut.ReplicationTask.ReplicationTaskArn,
			})
			require.NoError(t, err, "DeleteReplicationTask should succeed")

			// Verify task is gone.
			descAfter, err := client.DescribeReplicationTasks(ctx, &dmssdk.DescribeReplicationTasksInput{
				Filters: []dmstypes.Filter{
					{
						Name:   aws.String("replication-task-id"),
						Values: []string{tt.taskIdentifier},
					},
				},
			})
			require.NoError(t, err)
			assert.Empty(t, descAfter.ReplicationTasks, "deleted task should not appear")
		})
	}
}
