package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAKSSnapshots(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	s.mustPut(t, "snapsrc", nil)

	poolID := "/subscriptions/" + aciSub + "/resourceGroups/" + aksRG +
		"/providers/Microsoft.ContainerService/managedClusters/snapsrc/agentPools/sys"
	snaps := s.factory.NewSnapshotsClient()

	tests := []struct {
		name     string
		source   string
		loc      *string
		wantCode string
	}{
		{name: "from pool", source: poolID, loc: new(aksLoc)},
		{name: "no source", loc: new(aksLoc), wantCode: "InvalidParameter"},
		{
			name:     "not a pool",
			source:   "/subscriptions/x/resourceGroups/y",
			loc:      new(aksLoc),
			wantCode: "InvalidParameter",
		},
		{name: "missing pool", source: poolID + "x", loc: new(aksLoc), wantCode: "ResourceNotFound"},
		{name: "no location", source: poolID, wantCode: "LocationRequired"},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			name := "snap" + string(rune('a'+i))

			got, err := snaps.CreateOrUpdate(t.Context(), aksRG, name, armcontainerservice.Snapshot{
				Location: tt.loc, Tags: map[string]*string{"k": new("v")},
				Properties: &armcontainerservice.SnapshotProperties{
					CreationData: &armcontainerservice.CreationData{SourceResourceID: new(tt.source)},
				},
			}, nil)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, armcontainerservice.SnapshotTypeNodePool, *got.Properties.SnapshotType)
			assert.Equal(t, "Standard_D4s_v5", *got.Properties.VMSize)
			assert.Equal(t, armcontainerservice.OSTypeLinux, *got.Properties.OSType)
			assert.NotEmpty(t, *got.Properties.NodeImageVersion)
			assert.NotEmpty(t, *got.Properties.KubernetesVersion)

			upd, err := snaps.UpdateTags(t.Context(), aksRG, name, armcontainerservice.TagsObject{
				Tags: map[string]*string{"k": new("v2")},
			}, nil)
			require.NoError(t, err)
			assert.Equal(t, "v2", *upd.Tags["k"])

			var listed int

			for pg := snaps.NewListByResourceGroupPager(aksRG, nil); pg.More(); {
				page, perr := pg.NextPage(t.Context())
				require.NoError(t, perr)

				listed += len(page.Value)
			}

			assert.Equal(t, 1, listed)

			_, err = snaps.Delete(t.Context(), aksRG, name, nil)
			require.NoError(t, err)

			_, err = snaps.Get(t.Context(), aksRG, name, nil)
			require.Error(t, err)
		})
	}
}

func TestAKSSnapshotsSurviveRestore(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	s.mustPut(t, "snapsrc", nil)

	_, err := s.factory.NewSnapshotsClient().CreateOrUpdate(t.Context(), aksRG, "keep", armcontainerservice.Snapshot{
		Location: new(aksLoc),
		Properties: &armcontainerservice.SnapshotProperties{CreationData: &armcontainerservice.CreationData{
			SourceResourceID: new("/subscriptions/" + aciSub + "/resourceGroups/" + aksRG +
				"/providers/Microsoft.ContainerService/managedClusters/snapsrc/agentPools/sys"),
		}},
	}, nil)
	require.NoError(t, err)

	other := newAKSStack(t)
	require.NoError(t, other.handler.Restore(t.Context(), s.handler.Snapshot(t.Context())))

	got, err := other.factory.NewSnapshotsClient().Get(t.Context(), aksRG, "keep", nil)
	require.NoError(t, err)
	assert.Equal(t, "keep", *got.Name)
}
