package opsworks_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

func TestInstanceLifecycle_Delay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  string
		delay time.Duration
		wait  time.Duration
	}{
		{name: "no delay", delay: 0, wait: 0, want: "online"},
		{name: "requested", delay: 8 * time.Second, wait: time.Second, want: "requested"},
		{name: "pending", delay: 8 * time.Second, wait: 3 * time.Second, want: "pending"},
		{name: "booting", delay: 8 * time.Second, wait: 5 * time.Second, want: "booting"},
		{name: "running setup", delay: 8 * time.Second, wait: 7 * time.Second, want: "running_setup"},
		{name: "online", delay: 8 * time.Second, wait: 8 * time.Second, want: "online"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := opsworks.NewInMemoryBackend("000000000000", rtTestRegion)
				b.SetLifecycleDelay(tt.delay)

				stack, instanceID := seedInstance(t, b)

				require.NoError(t, b.StartInstance(instanceID))
				time.Sleep(tt.wait)

				got, err := b.DescribeInstances("", "", []string{instanceID})
				require.NoError(t, err)
				assert.Equal(t, tt.want, got[0].Status)

				summary, err := b.DescribeStackSummary(stack)
				require.NoError(t, err)
				assert.EqualValues(t, 1, summaryCount(summary, tt.want))
			})
		})
	}
}

func TestInstanceLifecycle_StoppingBlocksDelete(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := opsworks.NewInMemoryBackend("000000000000", rtTestRegion)
		b.SetLifecycleDelay(8 * time.Second)

		_, instanceID := seedInstance(t, b)
		require.NoError(t, b.StartInstance(instanceID))
		time.Sleep(8 * time.Second)
		require.NoError(t, b.StopInstance(instanceID))

		got, err := b.DescribeInstances("", "", []string{instanceID})
		require.NoError(t, err)
		assert.Equal(t, "stopping", got[0].Status)
		require.Error(t, b.DeleteInstance(instanceID, false, false))

		time.Sleep(8 * time.Second)
		require.NoError(t, b.DeleteInstance(instanceID, false, false))
	})
}

func TestDeploymentLifecycle_Delay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		wait       time.Duration
	}{
		{name: "running", wait: time.Second, wantStatus: "running"},
		{name: "successful", wait: 5 * time.Second, wantStatus: "successful"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := opsworks.NewInMemoryBackend("000000000000", rtTestRegion)
				b.SetLifecycleDelay(5 * time.Second)

				stack, _ := seedInstance(t, b)
				dep, err := b.CreateDeployment(stack, "", "update_custom_cookbooks", "", opsworks.DeploymentOptions{})
				require.NoError(t, err)

				time.Sleep(tt.wait)

				got, err := b.DescribeDeployments("", "", []string{dep.DeploymentID})
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, got[0].Status)

				cmds, err := b.DescribeCommands(dep.DeploymentID, "", nil)
				require.NoError(t, err)
				require.NotEmpty(t, cmds)
				assert.Equal(t, tt.wantStatus, cmds[0].Status)
			})
		})
	}
}

func seedInstance(t *testing.T, b *opsworks.InMemoryBackend) (string, string) {
	t.Helper()

	stack, err := b.CreateStack(
		"s",
		rtTestRegion,
		testInstanceProfileArn,
		testServiceRoleArn,
		opsworks.CreateStackOptions{},
	)
	require.NoError(t, err)

	layer, err := b.CreateLayer(stack.StackID, "custom", "l", "l", opsworks.LayerSettings{})
	require.NoError(t, err)

	inst, err := b.CreateInstance(stack.StackID, []string{layer.LayerID}, "c5.large", opsworks.CreateInstanceOptions{})
	require.NoError(t, err)

	return stack.StackID, inst.InstanceID
}

func summaryCount(s *opsworks.StackSummary, status string) int32 {
	c := s.InstancesCount

	switch status {
	case "online":
		return c.Online
	case "requested":
		return c.Requested
	case "pending":
		return c.Pending
	case "booting":
		return c.Booting
	case "running_setup":
		return c.RunningSetup
	default:
		return -1
	}
}
