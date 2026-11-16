package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAKSPoolFromSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		snapshot string
		mod      func(*armcontainerservice.ManagedClusterAgentPoolProfileProperties)
		wantCode string
	}{
		{name: "inherits shape", snapshot: "snap"},
		{
			name: "same family size", snapshot: "snap",
			mod: func(p *armcontainerservice.ManagedClusterAgentPoolProfileProperties) {
				p.VMSize = new("Standard_D8s_v5")
			},
		},
		{
			name: "other family", snapshot: "snap", wantCode: "InvalidParameter",
			mod: func(p *armcontainerservice.ManagedClusterAgentPoolProfileProperties) {
				p.VMSize = new("Standard_E4s_v5")
			},
		},
		{
			name: "other os", snapshot: "snap", wantCode: "InvalidParameter",
			mod: func(p *armcontainerservice.ManagedClusterAgentPoolProfileProperties) {
				p.OSType = new(armcontainerservice.OSTypeWindows)
			},
		},
		{name: "missing snapshot", snapshot: "nope", wantCode: "ResourceNotFound"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newAKSStack(t)
			s.mustPut(t, "snapsrc", nil)

			poolID := "/subscriptions/" + aciSub + "/resourceGroups/" + aksRG +
				"/providers/Microsoft.ContainerService/managedClusters/snapsrc/agentPools/sys"
			_, err := s.factory.NewSnapshotsClient().CreateOrUpdate(t.Context(), aksRG, "snap",
				armcontainerservice.Snapshot{
					Location: new(aksLoc),
					Properties: &armcontainerservice.SnapshotProperties{
						CreationData: &armcontainerservice.CreationData{SourceResourceID: new(poolID)},
					},
				}, nil)
			require.NoError(t, err)

			props := &armcontainerservice.ManagedClusterAgentPoolProfileProperties{
				Mode: new(armcontainerservice.AgentPoolModeUser), Count: new(int32(1)),
				CreationData: &armcontainerservice.CreationData{SourceResourceID: new(
					"/subscriptions/" + aciSub + "/resourceGroups/" + aksRG +
						"/providers/Microsoft.ContainerService/snapshots/" + tt.snapshot)},
			}
			if tt.mod != nil {
				tt.mod(props)
			}

			poller, err := s.factory.NewAgentPoolsClient().
				BeginCreateOrUpdate(t.Context(), aksRG, "snapsrc", "fromsnap",
					armcontainerservice.AgentPool{Properties: props}, nil)
			if err == nil {
				var res armcontainerservice.AgentPoolsClientCreateOrUpdateResponse

				res, err = poller.PollUntilDone(t.Context(), pollFast())
				if err == nil {
					want := "Standard_D4s_v5"
					if props.VMSize != nil {
						want = *props.VMSize
					}

					assert.Equal(t, want, *res.Properties.VMSize)
					assert.Equal(t, armcontainerservice.OSTypeLinux, *res.Properties.OSType)
					assert.NotNil(t, res.Properties.CreationData)
				}
			}

			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestAKSPoolFromSnapshotSurvivesSnapshotDelete(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	s.mustPut(t, "snapdel", nil)

	poolID := "/subscriptions/" + aciSub + "/resourceGroups/" + aksRG +
		"/providers/Microsoft.ContainerService/managedClusters/snapdel/agentPools/sys"
	snapID := "/subscriptions/" + aciSub + "/resourceGroups/" + aksRG +
		"/providers/Microsoft.ContainerService/snapshots/snap"

	_, err := s.factory.NewSnapshotsClient().CreateOrUpdate(t.Context(), aksRG, "snap",
		armcontainerservice.Snapshot{
			Location: new(aksLoc),
			Properties: &armcontainerservice.SnapshotProperties{
				CreationData: &armcontainerservice.CreationData{SourceResourceID: new(poolID)},
			},
		}, nil)
	require.NoError(t, err)

	pool := func(count int32) armcontainerservice.AgentPool {
		return armcontainerservice.AgentPool{Properties: &armcontainerservice.ManagedClusterAgentPoolProfileProperties{
			Mode: new(armcontainerservice.AgentPoolModeUser), Count: new(count),
			CreationData: &armcontainerservice.CreationData{SourceResourceID: new(snapID)},
		}}
	}

	put := func(count int32) error {
		poller, perr := s.factory.NewAgentPoolsClient().
			BeginCreateOrUpdate(t.Context(), aksRG, "snapdel", "fromsnap", pool(count), nil)
		if perr != nil {
			return perr
		}

		_, perr = poller.PollUntilDone(t.Context(), pollFast())

		return perr
	}

	require.NoError(t, put(1))

	_, err = s.factory.NewSnapshotsClient().Delete(t.Context(), aksRG, "snap", nil)
	require.NoError(t, err)

	assert.NoError(t, put(2))
}
