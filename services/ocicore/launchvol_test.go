package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLaunchVolumeAttachmentsInvalid(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	f := e.fixture(t)
	existing := e.mkVol(t, testAD, 60)
	other := e.mkVol(t, testAD2, 60)

	create := func(size int64) core.LaunchCreateVolumeFromAttributes {
		return core.LaunchCreateVolumeFromAttributes{SizeInGBs: new(size), DisplayName: new("data")}
	}

	tests := []struct {
		name     string
		atts     []core.LaunchAttachVolumeDetails
		wantCode int
	}{
		{"too small", []core.LaunchAttachVolumeDetails{
			core.LaunchAttachParavirtualizedVolumeDetails{LaunchCreateVolumeDetails: create(10)},
		}, http.StatusBadRequest},
		{"both sources", []core.LaunchAttachVolumeDetails{
			core.LaunchAttachParavirtualizedVolumeDetails{VolumeId: existing.Id, LaunchCreateVolumeDetails: create(60)},
		}, http.StatusBadRequest},
		{
			"neither source",
			[]core.LaunchAttachVolumeDetails{core.LaunchAttachParavirtualizedVolumeDetails{}},
			http.StatusBadRequest,
		},
		{"other ad", []core.LaunchAttachVolumeDetails{
			core.LaunchAttachParavirtualizedVolumeDetails{VolumeId: other.Id},
		}, http.StatusBadRequest},
		{
			"duplicate device",
			[]core.LaunchAttachVolumeDetails{
				core.LaunchAttachIScsiVolumeDetails{
					Device:                    new("/dev/oracleoci/oraclevdb"),
					LaunchCreateVolumeDetails: create(50),
				},
				core.LaunchAttachParavirtualizedVolumeDetails{
					Device: new("/dev/oracleoci/oraclevdb"), LaunchCreateVolumeDetails: create(50),
				},
			},
			http.StatusConflict,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.launch(
				t,
				f.pub,
				func(d *core.LaunchInstanceDetails) { d.LaunchVolumeAttachments = tc.atts },
			)
			assert.Equal(t, tc.wantCode, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestLaunchVolumeAttachments(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)
	existing := e.mkVol(t, testAD, 60)

	create := func(size int64) core.LaunchCreateVolumeFromAttributes {
		return core.LaunchCreateVolumeFromAttributes{SizeInGBs: new(size), DisplayName: new("data")}
	}

	inst, err := e.launch(t, f.pub, func(d *core.LaunchInstanceDetails) {
		d.LaunchVolumeAttachments = []core.LaunchAttachVolumeDetails{
			core.LaunchAttachIScsiVolumeDetails{
				LaunchCreateVolumeDetails: create(70), UseChap: new(true), Device: new("/dev/oracleoci/oraclevdc"),
			},
			core.LaunchAttachParavirtualizedVolumeDetails{VolumeId: existing.Id},
		}
	})
	require.NoError(t, err)
	e.settle()

	l, err := e.compute.ListVolumeAttachments(ctx, core.ListVolumeAttachmentsRequest{
		CompartmentId: new(testComp), InstanceId: inst.Id,
	})
	require.NoError(t, err)
	require.Len(t, l.Items, 2)

	var created string

	for _, a := range l.Items {
		if a.GetVolumeId() != nil && *a.GetVolumeId() != *existing.Id {
			created = *a.GetVolumeId()

			iscsi, ok := a.(core.IScsiVolumeAttachment)
			require.True(t, ok)
			assert.NotEmpty(t, *iscsi.ChapSecret)
			assert.Equal(t, "/dev/oracleoci/oraclevdc", *iscsi.Device)
		}
	}

	vol, err := e.block.GetVolume(ctx, core.GetVolumeRequest{VolumeId: &created})
	require.NoError(t, err)
	assert.EqualValues(t, 70, *vol.SizeInGBs)
	assert.Equal(t, core.VolumeLifecycleStateAvailable, vol.LifecycleState)

	_, err = e.compute.TerminateInstance(ctx, core.TerminateInstanceRequest{
		InstanceId: inst.Id, PreserveDataVolumesCreatedAtLaunch: new(false),
	})
	require.NoError(t, err)
	e.settle()

	vol, err = e.block.GetVolume(ctx, core.GetVolumeRequest{VolumeId: &created})
	require.NoError(t, err)
	assert.Equal(
		t,
		core.VolumeLifecycleStateTerminated,
		vol.LifecycleState,
		"launch volumes go with the instance on request",
	)

	kept, err := e.block.GetVolume(ctx, core.GetVolumeRequest{VolumeId: existing.Id})
	require.NoError(t, err)
	assert.Equal(
		t,
		core.VolumeLifecycleStateAvailable,
		kept.LifecycleState,
		"attached existing volumes are never deleted",
	)
}

func TestTerminatePreservesLaunchVolumesByDefault(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)

	inst, err := e.launch(t, f.pub, func(d *core.LaunchInstanceDetails) {
		d.LaunchVolumeAttachments = []core.LaunchAttachVolumeDetails{core.LaunchAttachParavirtualizedVolumeDetails{
			LaunchCreateVolumeDetails: core.LaunchCreateVolumeFromAttributes{SizeInGBs: new(int64(50))},
		}}
	})
	require.NoError(t, err)
	e.settle()

	_, err = e.compute.TerminateInstance(ctx, core.TerminateInstanceRequest{InstanceId: inst.Id})
	require.NoError(t, err)
	e.settle()

	l, err := e.block.ListVolumes(
		ctx,
		core.ListVolumesRequest{CompartmentId: new(testComp), LifecycleState: core.VolumeLifecycleStateAvailable},
	)
	require.NoError(t, err)
	assert.Len(t, l.Items, 1)
}
