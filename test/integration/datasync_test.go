package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	datasyncsdk "github.com/aws/aws-sdk-go-v2/service/datasync"
	datasynctypes "github.com/aws/aws-sdk-go-v2/service/datasync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createDataSyncClient returns a DataSync client pointed at the shared test container.
func createDataSyncClient(t *testing.T) *datasyncsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return datasyncsdk.NewFromConfig(cfg, func(o *datasyncsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_DataSync_AgentLifecycle drives create→describe→list→delete of an agent.
func TestIntegration_DataSync_AgentLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name      string
		agentName string
	}{
		{name: "full_lifecycle", agentName: "integ-agent"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createDataSyncClient(t)

			createOut, err := client.CreateAgent(ctx, &datasyncsdk.CreateAgentInput{
				ActivationKey: aws.String("ACTIVATION-KEY-12345"),
				AgentName:     aws.String(tt.agentName),
			})
			require.NoError(t, err, "CreateAgent should succeed")
			agentArn := aws.ToString(createOut.AgentArn)
			require.NotEmpty(t, agentArn, "agent ARN must be returned")

			t.Cleanup(func() {
				_, _ = client.DeleteAgent(ctx, &datasyncsdk.DeleteAgentInput{AgentArn: aws.String(agentArn)})
			})

			descOut, err := client.DescribeAgent(ctx, &datasyncsdk.DescribeAgentInput{AgentArn: aws.String(agentArn)})
			require.NoError(t, err, "DescribeAgent should succeed")
			assert.Equal(t, tt.agentName, aws.ToString(descOut.Name))

			listOut, err := client.ListAgents(ctx, &datasyncsdk.ListAgentsInput{})
			require.NoError(t, err, "ListAgents should succeed")

			found := false
			for _, a := range listOut.Agents {
				if aws.ToString(a.AgentArn) == agentArn {
					found = true

					break
				}
			}

			assert.True(t, found, "created agent should appear in list")

			_, err = client.DeleteAgent(ctx, &datasyncsdk.DeleteAgentInput{AgentArn: aws.String(agentArn)})
			require.NoError(t, err, "DeleteAgent should succeed")
		})
	}
}

// TestIntegration_DataSync_TaskLifecycle drives two NFS locations→task create→describe→delete.
func TestIntegration_DataSync_TaskLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name     string
		taskName string
	}{
		{name: "full_lifecycle", taskName: "integ-task"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createDataSyncClient(t)

			mkLocation := func(host string) string {
				out, err := client.CreateLocationNfs(ctx, &datasyncsdk.CreateLocationNfsInput{
					ServerHostname: aws.String(host),
					Subdirectory:   aws.String("/export"),
					OnPremConfig: &datasynctypes.OnPremConfig{
						AgentArns: []string{"arn:aws:datasync:us-east-1:000000000000:agent/agent-integ"},
					},
				})
				require.NoError(t, err, "CreateLocationNfs should succeed")

				return aws.ToString(out.LocationArn)
			}

			srcArn := mkLocation("src.example.com")
			dstArn := mkLocation("dst.example.com")

			createOut, err := client.CreateTask(ctx, &datasyncsdk.CreateTaskInput{
				SourceLocationArn:      aws.String(srcArn),
				DestinationLocationArn: aws.String(dstArn),
				Name:                   aws.String(tt.taskName),
			})
			require.NoError(t, err, "CreateTask should succeed")
			taskArn := aws.ToString(createOut.TaskArn)
			require.NotEmpty(t, taskArn, "task ARN must be returned")

			t.Cleanup(func() {
				_, _ = client.DeleteTask(ctx, &datasyncsdk.DeleteTaskInput{TaskArn: aws.String(taskArn)})
			})

			descOut, err := client.DescribeTask(ctx, &datasyncsdk.DescribeTaskInput{TaskArn: aws.String(taskArn)})
			require.NoError(t, err, "DescribeTask should succeed")
			assert.Equal(t, tt.taskName, aws.ToString(descOut.Name))
			assert.Equal(t, srcArn, aws.ToString(descOut.SourceLocationArn))

			_, err = client.DeleteTask(ctx, &datasyncsdk.DeleteTaskInput{TaskArn: aws.String(taskArn)})
			require.NoError(t, err, "DeleteTask should succeed")
		})
	}
}
