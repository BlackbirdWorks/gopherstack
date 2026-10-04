package ecs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// StartTaskInput.Overrides doc (ecs@v1.96.0): per-run container/role overrides.
func TestStartTask_OverridesPersisted_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		override *ecstypes.TaskOverride
		wantRole string
		wantCmd  []string
	}{
		{
			name: "container-command-env",
			override: &ecstypes.TaskOverride{ContainerOverrides: []ecstypes.ContainerOverride{{
				Name:        aws.String("app"),
				Command:     []string{"echo", "hi"},
				Environment: []ecstypes.KeyValuePair{{Name: aws.String("K"), Value: aws.String("V")}},
			}}},
			wantCmd: []string{"echo", "hi"},
		},
		{
			name:     "task-role",
			override: &ecstypes.TaskOverride{TaskRoleArn: aws.String("arn:aws:iam::000000000000:role/over")},
			wantRole: "arn:aws:iam::000000000000:role/over",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestECSClient(t, newTestHandler(t))
			ctx := t.Context()

			_, err := client.CreateCluster(ctx, &ecssdk.CreateClusterInput{ClusterName: aws.String("c")})
			require.NoError(t, err)

			td, err := client.RegisterTaskDefinition(ctx, &ecssdk.RegisterTaskDefinitionInput{
				Family:      aws.String("f"),
				TaskRoleArn: aws.String("arn:aws:iam::000000000000:role/def"),
				ContainerDefinitions: []ecstypes.ContainerDefinition{{
					Name: aws.String("app"), Image: aws.String("busybox"), Memory: aws.Int32(64),
				}},
			})
			require.NoError(t, err)

			reg, err := client.RegisterContainerInstance(ctx, &ecssdk.RegisterContainerInstanceInput{
				Cluster:                  aws.String("c"),
				InstanceIdentityDocument: aws.String(fakeInstanceIdentityDocument("i-start")),
			})
			require.NoError(t, err)

			out, err := client.StartTask(ctx, &ecssdk.StartTaskInput{
				Cluster:            aws.String("c"),
				TaskDefinition:     td.TaskDefinition.TaskDefinitionArn,
				ContainerInstances: []string{aws.ToString(reg.ContainerInstance.ContainerInstanceArn)},
				Overrides:          tt.override,
			})
			require.NoError(t, err)
			require.Len(t, out.Tasks, 1)
			require.NotNil(t, out.Tasks[0].Overrides)

			desc, err := client.DescribeTasks(ctx, &ecssdk.DescribeTasksInput{
				Cluster: aws.String("c"), Tasks: []string{aws.ToString(out.Tasks[0].TaskArn)},
			})
			require.NoError(t, err)
			require.Len(t, desc.Tasks, 1)
			require.NotNil(t, desc.Tasks[0].Overrides)

			ov := desc.Tasks[0].Overrides
			assert.Equal(t, tt.wantRole, aws.ToString(ov.TaskRoleArn))

			if tt.wantCmd != nil {
				require.Len(t, ov.ContainerOverrides, 1)
				assert.Equal(t, tt.wantCmd, ov.ContainerOverrides[0].Command)
				require.Len(t, ov.ContainerOverrides[0].Environment, 1)
				assert.Equal(t, "V", aws.ToString(ov.ContainerOverrides[0].Environment[0].Value))
			}
		})
	}
}
