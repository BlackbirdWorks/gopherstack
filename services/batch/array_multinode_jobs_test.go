package batch_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

type jobEnv struct {
	client *batchsdk.Client
	bk     *batch.InMemoryBackend
	jan    *batch.Janitor
}

func newJobEnv(t *testing.T) *jobEnv {
	t.Helper()

	ctx := t.Context()
	bk := batch.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestBatchClient(t, batch.NewHandler(bk))

	_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String("ce"),
		Type:                   types.CETypeManaged,
	})
	require.NoError(t, err)

	_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
		JobQueueName: aws.String("q"),
		Priority:     aws.Int32(1),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
			{Order: aws.Int32(1), ComputeEnvironment: aws.String("ce")},
		},
	})
	require.NoError(t, err)

	_, err = client.RegisterJobDefinition(ctx, &batchsdk.RegisterJobDefinitionInput{
		JobDefinitionName: aws.String("jd"),
		Type:              types.JobDefinitionTypeContainer,
		ContainerProperties: &types.ContainerProperties{
			Image: aws.String("busybox"),
			ResourceRequirements: []types.ResourceRequirement{
				{Type: types.ResourceTypeVcpu, Value: aws.String("1")},
				{Type: types.ResourceTypeMemory, Value: aws.String("128")},
			},
		},
	})
	require.NoError(t, err)

	return &jobEnv{client: client, bk: bk, jan: batch.NewJanitor(bk, time.Minute, 24*time.Hour, 24*time.Hour)}
}

func (e *jobEnv) submit(t *testing.T, in *batchsdk.SubmitJobInput) (string, error) {
	t.Helper()

	in.JobQueue = aws.String("q")
	if in.JobDefinition == nil {
		in.JobDefinition = aws.String("jd")
	}

	if in.JobName == nil {
		in.JobName = aws.String("job")
	}

	out, err := e.client.SubmitJob(t.Context(), in)
	if err != nil {
		return "", err
	}

	return aws.ToString(out.JobId), nil
}

func (e *jobEnv) describe(t *testing.T, id string) types.JobDetail {
	t.Helper()

	out, err := e.client.DescribeJobs(t.Context(), &batchsdk.DescribeJobsInput{Jobs: []string{id}})
	require.NoError(t, err)
	require.Len(t, out.Jobs, 1)

	return out.Jobs[0]
}

func (e *jobEnv) sweep(t *testing.T, n int) {
	t.Helper()

	for range n {
		e.jan.SweepOnce(t.Context())
	}
}

func TestArrayJob_SizeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		size    int32
		wantErr bool
	}{
		{name: "one", size: 1, wantErr: true},
		{name: "two", size: 2},
		{name: "max", size: 10000},
		{name: "too big", size: 10001, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newJobEnv(t)
			_, err := env.submit(
				t,
				&batchsdk.SubmitJobInput{ArrayProperties: &types.ArrayProperties{Size: aws.Int32(tt.size)}},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestArrayJob_ChildrenLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantParent types.JobStatus
		wantChild0 types.JobStatus
		terminate  bool
	}{
		{name: "all succeed", wantParent: types.JobStatusSucceeded, wantChild0: types.JobStatusSucceeded},
		{
			name:       "terminated child fails parent",
			terminate:  true,
			wantParent: types.JobStatusFailed,
			wantChild0: types.JobStatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newJobEnv(t)
			ctx := t.Context()

			parent, err := env.submit(
				t,
				&batchsdk.SubmitJobInput{ArrayProperties: &types.ArrayProperties{Size: aws.Int32(3)}},
			)
			require.NoError(t, err)

			detail := env.describe(t, parent)
			assert.Equal(t, int32(3), aws.ToInt32(detail.ArrayProperties.Size))
			assert.Equal(t, int32(3), detail.ArrayProperties.StatusSummary["SUBMITTED"])

			kids, err := env.client.ListJobs(ctx, &batchsdk.ListJobsInput{
				ArrayJobId: aws.String(parent), JobStatus: types.JobStatusSubmitted,
			})
			require.NoError(t, err)
			require.Len(t, kids.JobSummaryList, 3)

			for i, k := range kids.JobSummaryList {
				assert.Equal(t, parent+":"+string(rune('0'+i)), aws.ToString(k.JobId))
				assert.Equal(t, int32(i), aws.ToInt32(k.ArrayProperties.Index))
			}

			queued, err := env.client.ListJobs(ctx, &batchsdk.ListJobsInput{
				JobQueue: aws.String("q"), JobStatus: types.JobStatusSubmitted,
			})
			require.NoError(t, err)
			assert.Len(t, queued.JobSummaryList, 1, "queue listing shows the parent only")

			if tt.terminate {
				_, err = env.client.TerminateJob(ctx, &batchsdk.TerminateJobInput{
					JobId: aws.String(parent + ":0"), Reason: aws.String("stop"),
				})
				require.NoError(t, err)
			}

			env.sweep(t, 3)

			assert.Equal(t, tt.wantChild0, env.describe(t, parent+":0").Status)
			assert.Equal(t, int32(0), aws.ToInt32(env.describe(t, parent+":0").ArrayProperties.Index))
			assert.Equal(t, tt.wantParent, env.describe(t, parent).Status)
		})
	}
}

func TestArrayJob_TerminateCascadesToChildren(t *testing.T) {
	t.Parallel()

	env := newJobEnv(t)

	parent, err := env.submit(t, &batchsdk.SubmitJobInput{ArrayProperties: &types.ArrayProperties{Size: aws.Int32(2)}})
	require.NoError(t, err)

	_, err = env.client.TerminateJob(t.Context(), &batchsdk.TerminateJobInput{
		JobId: aws.String(parent), Reason: aws.String("stop"),
	})
	require.NoError(t, err)

	for _, id := range []string{parent + ":0", parent + ":1"} {
		d := env.describe(t, id)
		assert.Equal(t, types.JobStatusFailed, d.Status)
		assert.Equal(t, "stop", aws.ToString(d.StatusReason))
	}
}

func TestArrayJob_Dependencies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		deps func(upstream string) []types.JobDependency
		// states after one sweep: child 0 / child 1 of the dependent array
		wantAfterOne [2]types.JobStatus
	}{
		{
			name: "sequential",
			deps: func(string) []types.JobDependency {
				return []types.JobDependency{{Type: types.ArrayJobDependencySequential}}
			},
			wantAfterOne: [2]types.JobStatus{types.JobStatusRunning, types.JobStatusSubmitted},
		},
		{
			name: "n to n",
			deps: func(up string) []types.JobDependency {
				return []types.JobDependency{{JobId: aws.String(up), Type: types.ArrayJobDependencyNToN}}
			},
			wantAfterOne: [2]types.JobStatus{types.JobStatusSubmitted, types.JobStatusSubmitted},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newJobEnv(t)

			upstream, err := env.submit(t, &batchsdk.SubmitJobInput{
				JobName: aws.String("up"), ArrayProperties: &types.ArrayProperties{Size: aws.Int32(2)},
			})
			require.NoError(t, err)

			down, err := env.submit(t, &batchsdk.SubmitJobInput{
				JobName: aws.String("down"), ArrayProperties: &types.ArrayProperties{Size: aws.Int32(2)},
				DependsOn: tt.deps(upstream),
			})
			require.NoError(t, err)

			env.sweep(t, 1)

			assert.Equal(t, tt.wantAfterOne[0], env.describe(t, down+":0").Status)
			assert.Equal(t, tt.wantAfterOne[1], env.describe(t, down+":1").Status)

			env.sweep(t, 8)

			assert.Equal(t, types.JobStatusSucceeded, env.describe(t, down).Status)
		})
	}
}

func TestArrayJob_NToNPropagatesFailure(t *testing.T) {
	t.Parallel()

	env := newJobEnv(t)

	upstream, err := env.submit(t, &batchsdk.SubmitJobInput{
		JobName: aws.String("up"), ArrayProperties: &types.ArrayProperties{Size: aws.Int32(2)},
	})
	require.NoError(t, err)

	down, err := env.submit(t, &batchsdk.SubmitJobInput{
		JobName: aws.String("down"), ArrayProperties: &types.ArrayProperties{Size: aws.Int32(2)},
		DependsOn: []types.JobDependency{{JobId: aws.String(upstream), Type: types.ArrayJobDependencyNToN}},
	})
	require.NoError(t, err)

	_, err = env.client.TerminateJob(t.Context(), &batchsdk.TerminateJobInput{
		JobId: aws.String(upstream + ":0"), Reason: aws.String("stop"),
	})
	require.NoError(t, err)

	env.sweep(t, 4)

	assert.Equal(t, types.JobStatusFailed, env.describe(t, down+":0").Status)
	assert.Equal(t, types.JobStatusSucceeded, env.describe(t, down+":1").Status)
}

func TestListJobs_ExactlyOneSelector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      batchsdk.ListJobsInput
		name    string
		wantErr bool
	}{
		{name: "none", wantErr: true},
		{name: "queue", in: batchsdk.ListJobsInput{JobQueue: aws.String("q")}},
		{name: "array", in: batchsdk.ListJobsInput{ArrayJobId: aws.String("x")}},
		{name: "node", in: batchsdk.ListJobsInput{MultiNodeJobId: aws.String("x")}},
		{
			name:    "two",
			in:      batchsdk.ListJobsInput{JobQueue: aws.String("q"), ArrayJobId: aws.String("x")},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newJobEnv(t)
			_, err := env.client.ListJobs(t.Context(), &tt.in)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func multiNodeDef(t *testing.T, env *jobEnv) {
	t.Helper()

	_, err := env.client.RegisterJobDefinition(t.Context(), &batchsdk.RegisterJobDefinitionInput{
		JobDefinitionName: aws.String("mnp"),
		Type:              types.JobDefinitionTypeMultinode,
		NodeProperties: &types.NodeProperties{
			NumNodes: aws.Int32(3),
			MainNode: aws.Int32(0),
			NodeRangeProperties: []types.NodeRangeProperty{{
				TargetNodes: aws.String("0:"),
				Container: &types.ContainerProperties{
					Image: aws.String("busybox"),
					Environment: []types.KeyValuePair{
						{Name: aws.String("A"), Value: aws.String("1")},
						{Name: aws.String("B"), Value: aws.String("2")},
					},
					ResourceRequirements: []types.ResourceRequirement{
						{Type: types.ResourceTypeVcpu, Value: aws.String("1")},
						{Type: types.ResourceTypeMemory, Value: aws.String("128")},
					},
				},
			}},
		},
	})
	require.NoError(t, err)
}

func TestMultiNodeJob_NodesAndOverrides(t *testing.T) {
	t.Parallel()

	env := newJobEnv(t)
	multiNodeDef(t, env)

	id, err := env.submit(t, &batchsdk.SubmitJobInput{
		JobDefinition: aws.String("mnp"),
		NodeOverrides: &types.NodeOverrides{
			NumNodes: aws.Int32(5),
			NodePropertyOverrides: []types.NodePropertyOverride{{
				TargetNodes: aws.String("1:2"),
				ContainerOverrides: &types.ContainerOverrides{
					Environment: []types.KeyValuePair{
						{Name: aws.String("B"), Value: aws.String("override")},
						{Name: aws.String("C"), Value: aws.String("3")},
					},
				},
			}},
		},
	})
	require.NoError(t, err)

	parent := env.describe(t, id)
	require.NotNil(t, parent.NodeProperties)
	assert.Equal(t, int32(5), aws.ToInt32(parent.NodeProperties.NumNodes))

	ranges := make([]string, 0, len(parent.NodeProperties.NodeRangeProperties))
	for _, r := range parent.NodeProperties.NodeRangeProperties {
		ranges = append(ranges, aws.ToString(r.TargetNodes))
	}

	assert.Equal(t, []string{"0:0", "1:2", "3:4"}, ranges)

	nodes, err := env.client.ListJobs(t.Context(), &batchsdk.ListJobsInput{
		MultiNodeJobId: aws.String(id), JobStatus: types.JobStatusSubmitted,
	})
	require.NoError(t, err)
	require.Len(t, nodes.JobSummaryList, 5)
	assert.True(t, aws.ToBool(nodes.JobSummaryList[0].NodeProperties.IsMainNode))
	assert.Equal(t, int32(5), aws.ToInt32(nodes.JobSummaryList[0].NodeProperties.NumNodes))
	assert.Equal(t, int32(3), aws.ToInt32(nodes.JobSummaryList[3].NodeProperties.NodeIndex))

	node := env.describe(t, id+"#1")
	require.NotNil(t, node.NodeDetails)
	assert.Equal(t, int32(1), aws.ToInt32(node.NodeDetails.NodeIndex))
	assert.False(t, aws.ToBool(node.NodeDetails.IsMainNode))
	require.NotNil(t, node.Container)

	env1 := map[string]string{}
	for _, kv := range node.Container.Environment {
		env1[aws.ToString(kv.Name)] = aws.ToString(kv.Value)
	}

	assert.Equal(t, map[string]string{"A": "1", "B": "override", "C": "3"}, env1)
}

func TestSubmitJob_OverrideValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   func(*batchsdk.SubmitJobInput)
		name string
	}{
		{name: "reserved env", in: func(in *batchsdk.SubmitJobInput) {
			in.ContainerOverrides = &types.ContainerOverrides{
				Environment: []types.KeyValuePair{{Name: aws.String("AWS_BATCH_X"), Value: aws.String("1")}},
			}
		}},
		{name: "node overrides on container job", in: func(in *batchsdk.SubmitJobInput) {
			in.NodeOverrides = &types.NodeOverrides{NumNodes: aws.Int32(2)}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newJobEnv(t)
			in := &batchsdk.SubmitJobInput{}
			tt.in(in)

			_, err := env.submit(t, in)
			require.Error(t, err)
		})
	}
}

func TestSubmitJob_NumNodesOverrideNeedsOpenRange(t *testing.T) {
	t.Parallel()

	env := newJobEnv(t)

	_, err := env.client.RegisterJobDefinition(t.Context(), &batchsdk.RegisterJobDefinitionInput{
		JobDefinitionName: aws.String("closed"),
		Type:              types.JobDefinitionTypeMultinode,
		NodeProperties: &types.NodeProperties{
			NumNodes: aws.Int32(2), MainNode: aws.Int32(0),
			NodeRangeProperties: []types.NodeRangeProperty{{
				TargetNodes: aws.String("0:1"),
				Container:   &types.ContainerProperties{Image: aws.String("busybox")},
			}},
		},
	})
	require.NoError(t, err)

	_, err = env.submit(t, &batchsdk.SubmitJobInput{
		JobDefinition: aws.String("closed"),
		NodeOverrides: &types.NodeOverrides{NumNodes: aws.Int32(4)},
	})
	require.Error(t, err)
}

func TestDescribeJobs_EksAndEcsProperties(t *testing.T) {
	t.Parallel()

	env := newJobEnv(t)
	ctx := t.Context()

	_, err := env.client.RegisterJobDefinition(ctx, &batchsdk.RegisterJobDefinitionInput{
		JobDefinitionName:    aws.String("eks"),
		Type:                 types.JobDefinitionTypeContainer,
		PlatformCapabilities: []types.PlatformCapability{types.PlatformCapabilityEc2},
		EksProperties: &types.EksProperties{PodProperties: &types.EksPodProperties{
			Containers: []types.EksContainer{{
				Name: aws.String("main"), Image: aws.String("busybox"), Command: []string{"old"},
				Env: []types.EksContainerEnvironmentVariable{{Name: aws.String("K"), Value: aws.String("v")}},
			}},
		}},
	})
	require.NoError(t, err)

	_, err = env.client.RegisterJobDefinition(ctx, &batchsdk.RegisterJobDefinitionInput{
		JobDefinitionName:    aws.String("ecs"),
		Type:                 types.JobDefinitionTypeContainer,
		PlatformCapabilities: []types.PlatformCapability{types.PlatformCapabilityFargate},
		EcsProperties: &types.EcsProperties{TaskProperties: []types.EcsTaskProperties{{
			Containers: []types.TaskContainerProperties{{
				Name: aws.String("app"), Image: aws.String("busybox"), Command: []string{"old"},
			}},
		}}},
	})
	require.NoError(t, err)

	eksID, err := env.submit(t, &batchsdk.SubmitJobInput{
		JobDefinition: aws.String("eks"),
		EksPropertiesOverride: &types.EksPropertiesOverride{PodProperties: &types.EksPodPropertiesOverride{
			Containers: []types.EksContainerOverride{{
				Name: aws.String("main"), Command: []string{"new"},
				Env: []types.EksContainerEnvironmentVariable{{Name: aws.String("X"), Value: aws.String("y")}},
			}},
		}},
	})
	require.NoError(t, err)

	pod := env.describe(t, eksID).EksProperties.PodProperties
	require.Len(t, pod.Containers, 1)
	assert.Equal(t, []string{"new"}, pod.Containers[0].Command)
	assert.Len(t, pod.Containers[0].Env, 2)

	ecsID, err := env.submit(t, &batchsdk.SubmitJobInput{
		JobDefinition: aws.String("ecs"),
		EcsPropertiesOverride: &types.EcsPropertiesOverride{TaskProperties: []types.TaskPropertiesOverride{{
			Containers: []types.TaskContainerOverrides{{Name: aws.String("app"), Command: []string{"new"}}},
		}}},
	})
	require.NoError(t, err)

	task := env.describe(t, ecsID).EcsProperties.TaskProperties
	require.Len(t, task, 1)
	require.Len(t, task[0].Containers, 1)
	assert.Equal(t, []string{"new"}, task[0].Containers[0].Command)
}

func TestRegisterJobDefinition_EcsPropertiesValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ecs  *types.EcsProperties
		name string
	}{
		{name: "no tasks", ecs: &types.EcsProperties{}},
		{name: "no containers", ecs: &types.EcsProperties{TaskProperties: []types.EcsTaskProperties{{}}}},
		{name: "no image", ecs: &types.EcsProperties{TaskProperties: []types.EcsTaskProperties{{
			Containers: []types.TaskContainerProperties{{Name: aws.String("a")}},
		}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newJobEnv(t)

			_, err := env.client.RegisterJobDefinition(t.Context(), &batchsdk.RegisterJobDefinitionInput{
				JobDefinitionName: aws.String("bad"),
				Type:              types.JobDefinitionTypeContainer,
				EcsProperties:     tt.ecs,
			})
			require.Error(t, err)
		})
	}
}
