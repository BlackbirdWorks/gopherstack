package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) bootAttachOf(t *testing.T, inst core.Instance) core.BootVolumeAttachment {
	t.Helper()

	l, err := e.compute.ListBootVolumeAttachments(t.Context(), core.ListBootVolumeAttachmentsRequest{
		CompartmentId: new(testComp), AvailabilityDomain: inst.AvailabilityDomain, InstanceId: inst.Id,
	})
	require.NoError(t, err)
	require.NotEmpty(t, l.Items)

	return l.Items[0]
}

func (e *env) instanceAction(t *testing.T, inst core.Instance, action string) error {
	t.Helper()

	_, err := e.compute.InstanceAction(
		t.Context(),
		core.InstanceActionRequest{InstanceId: inst.Id, Action: core.InstanceActionActionEnum(action)},
	)

	return err
}

func TestAttachAndDetachBootVolume(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)
	inst := e.mustLaunch(t, f.pub)
	e.settle()

	att := e.bootAttachOf(t, inst)
	bvID := att.BootVolumeId

	_, err := e.compute.DetachBootVolume(ctx, core.DetachBootVolumeRequest{BootVolumeAttachmentId: att.Id})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode(), "the instance is running")

	require.NoError(t, e.instanceAction(t, inst, "STOP"))
	e.settle()

	_, err = e.compute.DetachBootVolume(ctx, core.DetachBootVolumeRequest{BootVolumeAttachmentId: att.Id})
	require.NoError(t, err)
	e.settle()

	got, err := e.compute.GetBootVolumeAttachment(
		ctx,
		core.GetBootVolumeAttachmentRequest{BootVolumeAttachmentId: att.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, core.BootVolumeAttachmentLifecycleStateDetached, got.LifecycleState)

	assert.Equal(
		t,
		"IncorrectState",
		svcErr(t, e.instanceAction(t, inst, "START")).GetCode(),
		"no boot volume to start from",
	)

	other := e.mustLaunch(t, f.pub)
	e.settle()

	cases := []struct {
		name     string
		details  core.AttachBootVolumeDetails
		wantCode int
	}{
		{"missing instance", core.AttachBootVolumeDetails{BootVolumeId: bvID}, http.StatusBadRequest},
		{
			"unknown volume",
			core.AttachBootVolumeDetails{BootVolumeId: new("ocid1.bootvolume.oc1.iad.nope"), InstanceId: inst.Id},
			http.StatusNotFound,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, aerr := e.compute.AttachBootVolume(
				t.Context(),
				core.AttachBootVolumeRequest{AttachBootVolumeDetails: tc.details},
			)
			assert.Equal(t, tc.wantCode, svcErr(t, aerr).GetHTTPStatusCode())
		})
	}

	_, err = e.compute.AttachBootVolume(
		ctx,
		core.AttachBootVolumeRequest{BootVolumeId: bvID, InstanceId: other.Id, DisplayName: new("second")},
	)
	require.NoError(t, err)

	_, err = e.compute.AttachBootVolume(ctx, core.AttachBootVolumeRequest{
		BootVolumeId: bvID, InstanceId: inst.Id,
	})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "attached to the other instance")
}

func TestReplaceInstanceBootVolume(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)
	inst := e.mustLaunch(t, f.pub)
	e.settle()

	old := e.bootAttachOf(t, inst).BootVolumeId
	spare := e.mkBoot(t)
	spareClone, err := e.block.CreateBootVolume(
		ctx,
		core.CreateBootVolumeRequest{
			CompartmentId: new(testComp),
			SourceDetails: core.BootVolumeSourceFromBootVolumeDetails{Id: spare.Id},
		},
	)
	require.NoError(t, err)
	e.settle()

	_, err = e.compute.UpdateInstance(ctx, core.UpdateInstanceRequest{
		InstanceId: inst.Id,
		SourceDetails: core.UpdateInstanceSourceViaBootVolumeDetails{
			BootVolumeId: spareClone.Id, IsPreserveBootVolumeEnabled: new(true),
		},
	})
	require.NoError(t, err)
	e.settle()

	cur := e.bootAttachOf(t, e.getInstance(t, inst.Id))
	assert.Equal(t, *spareClone.Id, *cur.BootVolumeId)

	oldVol, err := e.block.GetBootVolume(ctx, core.GetBootVolumeRequest{BootVolumeId: old})
	require.NoError(t, err)
	assert.Equal(t, core.BootVolumeLifecycleStateAvailable, oldVol.LifecycleState, "preserved")

	img := e.imageID(t, "Canonical Ubuntu", "22.04", false)
	_, err = e.compute.UpdateInstance(ctx, core.UpdateInstanceRequest{
		InstanceId:    inst.Id,
		SourceDetails: core.UpdateInstanceSourceViaImageDetails{ImageId: img},
	})
	require.NoError(t, err)
	e.settle()

	after := e.getInstance(t, inst.Id)
	assert.Equal(t, *img, *after.ImageId)

	replaced, err := e.block.GetBootVolume(ctx, core.GetBootVolumeRequest{BootVolumeId: spareClone.Id})
	require.NoError(t, err)
	assert.Equal(t, core.BootVolumeLifecycleStateTerminated, replaced.LifecycleState, "not preserved by default")
}
