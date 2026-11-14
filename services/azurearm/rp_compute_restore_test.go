package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeSDK_RestorePointCollections(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	ctx := t.Context()
	colls := s.cf.NewRestorePointCollectionsClient()
	points := s.cf.NewRestorePointsClient()

	vm := s.mustVM(t, "vm1", linuxVM(s.newNIC(t, "nic1")))

	source := func(id string) armcompute.RestorePointCollection {
		return armcompute.RestorePointCollection{
			Location: new("westus"),
			Properties: &armcompute.RestorePointCollectionProperties{
				Source: &armcompute.RestorePointCollectionSourceProperties{ID: new(id)},
			},
		}
	}

	_, err := colls.CreateOrUpdate(ctx, netRG, "rpc1", armcompute.RestorePointCollection{Location: new("westus")}, nil)
	require.Error(t, err)
	assert.Equal(t, "InvalidParameter", sdkErrCode(err))

	_, err = colls.CreateOrUpdate(ctx, netRG, "rpc1", source(*vm.ID+"x"), nil)
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	coll, err := colls.CreateOrUpdate(ctx, netRG, "rpc1", source(*vm.ID), nil)
	require.NoError(t, err)
	assert.NotEmpty(t, *coll.Properties.RestorePointCollectionID)
	assert.Equal(t, *vm.ID, *coll.Properties.Source.ID)
	assert.Equal(t, "westus", *coll.Properties.Source.Location)

	_, err = colls.CreateOrUpdate(ctx, netRG, "rpc1", source(*vm.ID+"y"), nil)
	require.Error(t, err)

	tests := []struct {
		mod      func(*armcompute.RestorePoint)
		name     string
		wantCode string
	}{
		{name: "bad mode", wantCode: "InvalidParameter", mod: func(r *armcompute.RestorePoint) {
			r.Properties.ConsistencyMode = new(armcompute.ConsistencyModeTypes("Perfect"))
		}},
		{name: "exclude unknown disk", wantCode: "InvalidParameter", mod: func(r *armcompute.RestorePoint) {
			r.Properties.ExcludeDisks = []*armcompute.APIEntityReference{{ID: new("/disks/ghost")}}
		}},
		{name: "ghost source point", wantCode: "ResourceNotFound", mod: func(r *armcompute.RestorePoint) {
			r.Properties.SourceRestorePoint = &armcompute.APIEntityReference{ID: new(*coll.ID + "/restorePoints/ghost")}
		}},
	}

	for _, tt := range tests {
		rp := armcompute.RestorePoint{Properties: &armcompute.RestorePointProperties{}}
		tt.mod(&rp)

		_, rerr := points.BeginCreate(ctx, netRG, "rpc1", "rp-bad", rp, nil)
		require.Error(t, rerr, tt.name)
		assert.Equal(t, tt.wantCode, sdkErrCode(rerr), tt.name)
	}

	cp, err := points.BeginCreate(ctx, netRG, "rpc1", "rp1", armcompute.RestorePoint{}, nil)
	require.NoError(t, waitPoller(t, cp, err))

	got, err := points.Get(ctx, netRG, "rpc1", "rp1", nil)
	require.NoError(t, err)
	assert.Equal(t, armcompute.ConsistencyModeTypesCrashConsistent, *got.Properties.ConsistencyMode)
	assert.Equal(t, vm.Properties.VMID, got.Properties.SourceMetadata.VMID)
	assert.Equal(t, "westus", *got.Properties.SourceMetadata.Location)
	assert.NotNil(t, got.Properties.SourceMetadata.StorageProfile.OSDisk)
	assert.Nil(t, got.Properties.SourceMetadata.OSProfile.AdminPassword)
	assert.NotNil(t, got.Properties.TimeCreated)

	_, err = points.BeginCreate(ctx, netRG, "rpc1", "rp1", armcompute.RestorePoint{}, nil)
	require.Error(t, err)
	assert.Equal(t, "Conflict", sdkErrCode(err))

	copyPoint := armcompute.RestorePoint{Properties: &armcompute.RestorePointProperties{
		SourceRestorePoint: &armcompute.APIEntityReference{ID: got.ID},
	}}
	cp, err = points.BeginCreate(ctx, netRG, "rpc1", "rp2", copyPoint, nil)
	require.NoError(t, waitPoller(t, cp, err))

	expanded, err := colls.Get(ctx, netRG, "rpc1", &armcompute.RestorePointCollectionsClientGetOptions{
		Expand: new(armcompute.RestorePointCollectionExpandOptionsRestorePoints),
	})
	require.NoError(t, err)
	assert.Len(t, expanded.Properties.RestorePoints, 2)

	plain, err := colls.Get(ctx, netRG, "rpc1", nil)
	require.NoError(t, err)
	assert.Empty(t, plain.Properties.RestorePoints)

	upd, err := colls.Update(ctx, netRG, "rpc1", armcompute.RestorePointCollectionUpdate{
		Tags: map[string]*string{"k": new("v")},
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, "v", *upd.Tags["k"])
	assert.Equal(t, *vm.ID, *upd.Properties.Source.ID)

	n := 0

	for pager := colls.NewListPager(netRG, nil); pager.More(); {
		page, perr := pager.NextPage(ctx)
		require.NoError(t, perr)

		n += len(page.Value)
	}

	assert.Equal(t, 1, n)

	dp, err := points.BeginDelete(ctx, netRG, "rpc1", "rp2", nil)
	require.NoError(t, waitPoller(t, dp, err))

	snap := s.handler.Snapshot(ctx)
	fresh := newCmpStack(t)
	require.NoError(t, fresh.handler.Restore(ctx, snap))

	back, err := fresh.cf.NewRestorePointsClient().Get(ctx, netRG, "rpc1", "rp1", nil)
	require.NoError(t, err)
	assert.Equal(t, *got.Properties.SourceMetadata.VMID, *back.Properties.SourceMetadata.VMID)

	cd, err := colls.BeginDelete(ctx, netRG, "rpc1", nil)
	require.NoError(t, waitPoller(t, cd, err))

	_, err = points.Get(ctx, netRG, "rpc1", "rp1", nil)
	require.Error(t, err)
}
