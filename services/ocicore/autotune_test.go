package ocicore_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAutotunePoliciesCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		policies []core.AutotunePolicy
		vpus     int64
		wantCode int
	}{
		{"detached", []core.AutotunePolicy{core.DetachedVolumeAutotunePolicy{}}, 10, 0},
		{"perf", []core.AutotunePolicy{core.PerformanceBasedAutotunePolicy{MaxVpusPerGB: new(int64(30))}}, 10, 0},
		{"both", []core.AutotunePolicy{
			core.DetachedVolumeAutotunePolicy{},
			core.PerformanceBasedAutotunePolicy{MaxVpusPerGB: new(int64(20))},
		}, 10, 0},
		{"max below vpus", []core.AutotunePolicy{
			core.PerformanceBasedAutotunePolicy{MaxVpusPerGB: new(int64(10))},
		}, 20, http.StatusBadRequest},
		{"max bad step", []core.AutotunePolicy{
			core.PerformanceBasedAutotunePolicy{MaxVpusPerGB: new(int64(25))},
		}, 10, http.StatusBadRequest},
		{"duplicate", []core.AutotunePolicy{
			core.DetachedVolumeAutotunePolicy{}, core.DetachedVolumeAutotunePolicy{},
		}, 10, http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			r, err := e.block.CreateVolume(t.Context(), core.CreateVolumeRequest{
				CompartmentId: new(testComp), AvailabilityDomain: new(testAD), SizeInGBs: new(int64(60)),
				VpusPerGB: new(tc.vpus), AutotunePolicies: tc.policies,
			})

			if tc.wantCode != 0 {
				var se common.ServiceError
				require.ErrorAs(t, err, &se)
				assert.Equal(t, tc.wantCode, se.GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
			assert.Len(t, r.AutotunePolicies, len(tc.policies))
			assert.Equal(t, tc.vpus, *r.AutoTunedVpusPerGB)
		})
	}
}

func TestAutotuneDetachedVolumeAfterFourteenDays(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.fixture(t)
	inst := e.mustLaunch(t, f.pub)
	e.settle()

	vol, err := e.block.CreateVolume(ctx, core.CreateVolumeRequest{
		CompartmentId: new(testComp), AvailabilityDomain: inst.AvailabilityDomain, SizeInGBs: new(int64(60)),
		VpusPerGB:        new(int64(20)),
		AutotunePolicies: []core.AutotunePolicy{core.DetachedVolumeAutotunePolicy{}},
	})
	require.NoError(t, err)
	e.settle()

	att, err := e.compute.AttachVolume(ctx, core.AttachVolumeRequest{
		AttachVolumeDetails: core.AttachParavirtualizedVolumeDetails{InstanceId: inst.Id, VolumeId: vol.Id},
	})
	require.NoError(t, err)
	e.settle()

	e.clock.Advance(15 * 24 * time.Hour)

	got, err := e.block.GetVolume(ctx, core.GetVolumeRequest{VolumeId: vol.Id})
	require.NoError(t, err)
	assert.EqualValues(t, 20, *got.AutoTunedVpusPerGB, "an attached volume keeps its performance")

	_, err = e.compute.DetachVolume(ctx, core.DetachVolumeRequest{VolumeAttachmentId: att.VolumeAttachment.GetId()})
	require.NoError(t, err)
	e.settle()

	got, err = e.block.GetVolume(ctx, core.GetVolumeRequest{VolumeId: vol.Id})
	require.NoError(t, err)
	assert.EqualValues(t, 20, *got.AutoTunedVpusPerGB, "the drop waits 14 days")

	e.clock.Advance(13 * 24 * time.Hour)

	got, err = e.block.GetVolume(ctx, core.GetVolumeRequest{VolumeId: vol.Id})
	require.NoError(t, err)
	assert.EqualValues(t, 20, *got.AutoTunedVpusPerGB)

	e.clock.Advance(2 * 24 * time.Hour)

	got, err = e.block.GetVolume(ctx, core.GetVolumeRequest{VolumeId: vol.Id})
	require.NoError(t, err)
	assert.EqualValues(t, 0, *got.AutoTunedVpusPerGB, "lower cost once detached for 14 days")

	up, err := e.block.UpdateVolume(ctx, core.UpdateVolumeRequest{
		VolumeId:         vol.Id,
		AutotunePolicies: []core.AutotunePolicy{},
	})
	require.NoError(t, err)
	assert.Empty(t, up.AutotunePolicies)
	assert.Nil(t, up.AutoTunedVpusPerGB)
}

func TestAutotuneBootVolume(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	bv := e.mkBoot(t)

	up, err := e.block.UpdateBootVolume(ctx, core.UpdateBootVolumeRequest{
		BootVolumeId:     bv.Id,
		AutotunePolicies: []core.AutotunePolicy{core.PerformanceBasedAutotunePolicy{MaxVpusPerGB: new(int64(40))}},
	})
	require.NoError(t, err)
	require.Len(t, up.AutotunePolicies, 1)
	assert.Equal(t, *bv.VpusPerGB, *up.AutoTunedVpusPerGB)

	pol, ok := up.AutotunePolicies[0].(core.PerformanceBasedAutotunePolicy)
	require.True(t, ok)
	assert.EqualValues(t, 40, *pol.MaxVpusPerGB)

	clone, err := e.block.CreateBootVolume(ctx, core.CreateBootVolumeRequest{
		CompartmentId: new(testComp), SourceDetails: core.BootVolumeSourceFromBootVolumeDetails{Id: bv.Id},
		AutotunePolicies: []core.AutotunePolicy{core.DetachedVolumeAutotunePolicy{}},
	})
	require.NoError(t, err)
	assert.Len(t, clone.AutotunePolicies, 1)
}
