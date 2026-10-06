package emr_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	emrsdk "github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunJobFlow_DocumentedDefaults(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		release     string
		customAmi   string
		wantScale   types.ScaleDownBehavior
		wantRepo    types.RepoUpgradeOnBoot
		wantVisible bool
	}{
		{
			name:        "modern release",
			release:     "emr-6.10.0",
			wantScale:   types.ScaleDownBehaviorTerminateAtInstanceHour,
			wantVisible: true,
		},
		{
			name:        "custom ami",
			release:     "emr-6.10.0",
			customAmi:   "ami-123",
			wantScale:   types.ScaleDownBehaviorTerminateAtInstanceHour,
			wantRepo:    types.RepoUpgradeOnBootSecurity,
			wantVisible: true,
		},
		{
			name:        "pre 5.1",
			release:     "emr-5.0.0",
			wantScale:   types.ScaleDownBehaviorTerminateAtTaskCompletion,
			wantVisible: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newListFiltersClient(t)
			in := &emrsdk.RunJobFlowInput{
				Name: aws.String("c"), ReleaseLabel: aws.String(tc.release),
				Instances: &types.JobFlowInstancesConfig{KeepJobFlowAliveWhenNoSteps: aws.Bool(true)},
			}
			if tc.customAmi != "" {
				in.CustomAmiId = aws.String(tc.customAmi)
			}

			r, err := c.RunJobFlow(t.Context(), in)
			require.NoError(t, err)

			d, err := c.DescribeCluster(t.Context(), &emrsdk.DescribeClusterInput{ClusterId: r.JobFlowId})
			require.NoError(t, err)
			assert.Equal(t, tc.wantScale, d.Cluster.ScaleDownBehavior)
			assert.Equal(t, tc.wantRepo, d.Cluster.RepoUpgradeOnBoot)
			assert.Equal(t, tc.wantVisible, aws.ToBool(d.Cluster.VisibleToAllUsers))
			assert.EqualValues(t, 1, aws.ToInt32(d.Cluster.StepConcurrencyLevel))
		})
	}
}

func TestModifyCluster_OmittedStepConcurrencyKeepsLevel(t *testing.T) {
	t.Parallel()

	c := newListFiltersClient(t)
	r, err := c.RunJobFlow(t.Context(), &emrsdk.RunJobFlowInput{
		Name: aws.String("c"), StepConcurrencyLevel: aws.Int32(5),
		Instances: &types.JobFlowInstancesConfig{KeepJobFlowAliveWhenNoSteps: aws.Bool(true)},
	})
	require.NoError(t, err)

	out, err := c.ModifyCluster(t.Context(), &emrsdk.ModifyClusterInput{ClusterId: r.JobFlowId})
	require.NoError(t, err)
	assert.EqualValues(t, 5, aws.ToInt32(out.StepConcurrencyLevel))
}
