package dlm_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dlmsdk "github.com/aws/aws-sdk-go-v2/service/dlm"
	dlmtypes "github.com/aws/aws-sdk-go-v2/service/dlm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultPolicyExcludeBootVolumes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		exclude   bool
		wantSnaps int
	}{
		{name: "boot_excluded", exclude: true, wantSnaps: 1},
		{name: "boot_included", exclude: false, wantSnaps: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newExecEnv(t)

			insts, err := env.ec2.RunInstances("ami-fake", "t3.micro", "", 1)
			require.NoError(t, err)

			require.NoError(t, env.ec2.AttachLaunchVolumes(insts[0].ID, nil, nil))
			_, err = env.ec2.AttachVolume(env.volume(t, nil), insts[0].ID, "/dev/sdf")
			require.NoError(t, err)

			out, err := env.dlmClient.CreateLifecyclePolicy(t.Context(), &dlmsdk.CreateLifecyclePolicyInput{
				Description:      aws.String("default"),
				State:            dlmtypes.SettablePolicyStateValuesEnabled,
				ExecutionRoleArn: aws.String(execRole),
				DefaultPolicy:    dlmtypes.DefaultPolicyTypeValuesVolume,
				Exclusions:       &dlmtypes.Exclusions{ExcludeBootVolumes: aws.Bool(tt.exclude)},
			})
			require.NoError(t, err)

			got, err := env.dlmClient.GetLifecyclePolicy(
				t.Context(), &dlmsdk.GetLifecyclePolicyInput{PolicyId: out.PolicyId},
			)
			require.NoError(t, err)

			env.dlmBk.RunSchedules(t.Context(), aws.ToTime(got.Policy.DateCreated).Add(48*time.Hour))
			assert.Len(t, env.managedSnapshots(t, aws.ToString(out.PolicyId)), tt.wantSnaps)
		})
	}
}

func TestDefaultPolicyExtendDeletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		extend       bool
		deleteVolume bool
		disable      bool
		wantSnaps    int
	}{
		{name: "source_deleted_keeps_newest", extend: false, deleteVolume: true, wantSnaps: 1},
		{name: "source_deleted_extended", extend: true, deleteVolume: true, wantSnaps: 0},
		{name: "disabled_stops_deleting", extend: false, disable: true, wantSnaps: 1},
		{name: "disabled_extended_deletes", extend: true, disable: true, wantSnaps: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newExecEnv(t)
			volID := env.volume(t, nil)

			out, err := env.dlmClient.CreateLifecyclePolicy(t.Context(), &dlmsdk.CreateLifecyclePolicyInput{
				Description:      aws.String("default"),
				ExecutionRoleArn: aws.String(execRole),
				State:            dlmtypes.SettablePolicyStateValuesEnabled,
				DefaultPolicy:    dlmtypes.DefaultPolicyTypeValuesVolume,
				ExtendDeletion:   aws.Bool(tt.extend),
				RetainInterval:   aws.Int32(2),
			})
			require.NoError(t, err)

			got, err := env.dlmClient.GetLifecyclePolicy(
				t.Context(), &dlmsdk.GetLifecyclePolicyInput{PolicyId: out.PolicyId},
			)
			require.NoError(t, err)

			created := aws.ToTime(got.Policy.DateCreated)
			policyID := aws.ToString(out.PolicyId)

			env.dlmBk.RunSchedules(t.Context(), created.Add(time.Hour))
			require.Len(t, env.managedSnapshots(t, policyID), 1)

			if tt.deleteVolume {
				require.NoError(t, env.ec2.DeleteVolume(volID))
			}

			if tt.disable {
				_, err = env.dlmClient.UpdateLifecyclePolicy(t.Context(), &dlmsdk.UpdateLifecyclePolicyInput{
					PolicyId: out.PolicyId, State: dlmtypes.SettablePolicyStateValuesDisabled,
				})
				require.NoError(t, err)
			}

			env.dlmBk.RunSchedules(t.Context(), created.Add(4*24*time.Hour))
			assert.Len(t, env.managedSnapshots(t, policyID), tt.wantSnaps)
		})
	}
}
