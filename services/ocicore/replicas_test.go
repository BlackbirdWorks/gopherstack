package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBootVolumeReplicas(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	phx := "Abcd:PHX-AD-1"
	bv := e.mkBoot(t)

	tests := []struct {
		name     string
		details  []core.BootVolumeReplicaDetails
		wantCode int
	}{
		{
			"same ad",
			[]core.BootVolumeReplicaDetails{{AvailabilityDomain: bv.AvailabilityDomain}},
			http.StatusBadRequest,
		},
		{"two replicas", []core.BootVolumeReplicaDetails{
			{AvailabilityDomain: new(phx)}, {AvailabilityDomain: new("Abcd:PHX-AD-2")},
		}, http.StatusBadRequest},
		{"bad ad", []core.BootVolumeReplicaDetails{{AvailabilityDomain: new("nowhere")}}, http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.block.UpdateBootVolume(t.Context(), core.UpdateBootVolumeRequest{
				BootVolumeId:       bv.Id,
				BootVolumeReplicas: tc.details,
			})
			assert.Equal(t, tc.wantCode, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	up, err := e.block.UpdateBootVolume(ctx, core.UpdateBootVolumeRequest{
		BootVolumeId:       bv.Id,
		BootVolumeReplicas: []core.BootVolumeReplicaDetails{{AvailabilityDomain: new(phx), DisplayName: new("bdr")}},
	})
	require.NoError(t, err)
	require.Len(t, up.BootVolumeReplicas, 1)

	info := up.BootVolumeReplicas[0]
	rep, err := e.block.GetBootVolumeReplica(
		ctx,
		core.GetBootVolumeReplicaRequest{BootVolumeReplicaId: info.BootVolumeReplicaId},
	)
	require.NoError(t, err)
	assert.Equal(t, core.BootVolumeReplicaLifecycleStateProvisioning, rep.LifecycleState)
	assert.Equal(t, *bv.Id, *rep.BootVolumeId)
	assert.Equal(t, "bdr", *rep.DisplayName)
	assert.Equal(t, *bv.ImageId, *rep.ImageId)

	e.settle()

	l, err := e.block.ListBootVolumeReplicas(ctx, core.ListBootVolumeReplicasRequest{
		CompartmentId: new(testComp), AvailabilityDomain: new(phx),
	})
	require.NoError(t, err)
	require.Len(t, l.Items, 1)
	assert.Equal(t, core.BootVolumeReplicaLifecycleStateAvailable, l.Items[0].LifecycleState)

	l, err = e.block.ListBootVolumeReplicas(
		ctx,
		core.ListBootVolumeReplicasRequest{VolumeGroupReplicaId: new("ocid1.x.oc1.iad.y")},
	)
	require.NoError(t, err)
	assert.Empty(t, l.Items)

	_, err = e.block.CreateBootVolume(ctx, core.CreateBootVolumeRequest{
		CompartmentId: new(testComp), AvailabilityDomain: new(testAD2),
		SourceDetails: core.BootVolumeSourceFromBootVolumeReplicaDetails{Id: info.BootVolumeReplicaId},
	})
	require.Error(t, err, "a restore must target the replica availability domain")
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	restored, err := e.block.CreateBootVolume(ctx, core.CreateBootVolumeRequest{
		CompartmentId: new(testComp), AvailabilityDomain: new(phx),
		SourceDetails: core.BootVolumeSourceFromBootVolumeReplicaDetails{Id: info.BootVolumeReplicaId},
	})
	require.NoError(t, err)
	assert.Equal(t, *bv.ImageId, *restored.ImageId)

	_, err = e.block.UpdateBootVolume(ctx, core.UpdateBootVolumeRequest{
		BootVolumeId:       bv.Id,
		BootVolumeReplicas: []core.BootVolumeReplicaDetails{},
	})
	require.NoError(t, err)
	e.settle()

	rep, err = e.block.GetBootVolumeReplica(
		ctx,
		core.GetBootVolumeReplicaRequest{BootVolumeReplicaId: info.BootVolumeReplicaId},
	)
	require.NoError(t, err)
	assert.Equal(t, core.BootVolumeReplicaLifecycleStateTerminated, rep.LifecycleState)
}

func TestVolumeGroupReplicas(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	phx := "Abcd:PHX-AD-1"
	v1, v2 := e.mkVol(t, testAD, 60), e.mkVol(t, testAD, 70)
	bv := e.mkBoot(t)

	g, err := e.block.CreateVolumeGroup(ctx, core.CreateVolumeGroupRequest{
		CompartmentId: new(testComp), AvailabilityDomain: new(testAD),
		SourceDetails:       core.VolumeGroupSourceFromVolumesDetails{VolumeIds: []string{*v1.Id, *bv.Id}},
		VolumeGroupReplicas: []core.VolumeGroupReplicaDetails{{AvailabilityDomain: new(phx), DisplayName: new("gdr")}},
	})
	require.NoError(t, err)
	require.Len(t, g.VolumeGroupReplicas, 1)

	grID := g.VolumeGroupReplicas[0].VolumeGroupReplicaId
	gr, err := e.block.GetVolumeGroupReplica(ctx, core.GetVolumeGroupReplicaRequest{VolumeGroupReplicaId: grID})
	require.NoError(t, err)
	assert.Equal(t, "gdr", *gr.DisplayName)
	assert.Equal(t, *g.Id, *gr.VolumeGroupId)
	assert.Equal(t, core.VolumeGroupReplicaLifecycleStateProvisioning, gr.LifecycleState)
	require.Len(t, gr.MemberReplicas, 2)

	for _, m := range gr.MemberReplicas {
		assert.Equal(t, core.MemberReplicaMembershipStateAddPending, m.MembershipState)
	}

	e.settle()

	gr, err = e.block.GetVolumeGroupReplica(ctx, core.GetVolumeGroupReplicaRequest{VolumeGroupReplicaId: grID})
	require.NoError(t, err)
	assert.Equal(t, core.VolumeGroupReplicaLifecycleStateAvailable, gr.LifecycleState)
	assert.Equal(t, 60+*bv.SizeInGBs, *gr.SizeInGBs)

	for _, m := range gr.MemberReplicas {
		assert.Equal(t, core.MemberReplicaMembershipStateStable, m.MembershipState)
	}

	byGroup, err := e.block.ListBlockVolumeReplicas(
		ctx,
		core.ListBlockVolumeReplicasRequest{VolumeGroupReplicaId: grID},
	)
	require.NoError(t, err)
	assert.Len(t, byGroup.Items, 1)

	bootByGroup, err := e.block.ListBootVolumeReplicas(
		ctx,
		core.ListBootVolumeReplicasRequest{VolumeGroupReplicaId: grID},
	)
	require.NoError(t, err)
	assert.Len(t, bootByGroup.Items, 1)

	l, err := e.block.ListVolumeGroupReplicas(ctx, core.ListVolumeGroupReplicasRequest{
		CompartmentId: new(testComp), AvailabilityDomain: new(phx),
	})
	require.NoError(t, err)
	assert.Len(t, l.Items, 1)

	_, err = e.block.UpdateVolume(
		ctx,
		core.UpdateVolumeRequest{VolumeId: v1.Id, BlockVolumeReplicas: []core.BlockVolumeReplicaDetails{}},
	)
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "the group owns the member replica")

	_, err = e.block.UpdateVolumeGroup(ctx, core.UpdateVolumeGroupRequest{
		VolumeGroupId: g.Id,
		VolumeIds:     []string{*v1.Id, *bv.Id, *v2.Id},
	})
	require.NoError(t, err)

	gr, err = e.block.GetVolumeGroupReplica(ctx, core.GetVolumeGroupReplicaRequest{VolumeGroupReplicaId: grID})
	require.NoError(t, err)
	assert.Len(t, gr.MemberReplicas, 3, "a new member is replicated")

	e.settle()

	restored, err := e.block.CreateVolumeGroup(ctx, core.CreateVolumeGroupRequest{
		CompartmentId: new(testComp), AvailabilityDomain: new(phx),
		SourceDetails: core.VolumeGroupSourceFromVolumeGroupReplicaDetails{VolumeGroupReplicaId: grID},
	})
	require.NoError(t, err)
	assert.Len(t, restored.VolumeIds, 3)

	_, err = e.block.UpdateVolumeGroup(ctx, core.UpdateVolumeGroupRequest{
		VolumeGroupId:       g.Id,
		VolumeGroupReplicas: []core.VolumeGroupReplicaDetails{},
	})
	require.NoError(t, err)
	e.settle()

	gr, err = e.block.GetVolumeGroupReplica(ctx, core.GetVolumeGroupReplicaRequest{VolumeGroupReplicaId: grID})
	require.NoError(t, err)
	assert.Equal(t, core.VolumeGroupReplicaLifecycleStateTerminated, gr.LifecycleState)
	assert.Empty(t, gr.MemberReplicas)
}
