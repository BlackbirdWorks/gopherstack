package ocicore_test

import (
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduledBackups(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	vol := e.mkVol(t, testAD, 60)

	pol, err := e.block.CreateVolumeBackupPolicy(ctx, core.CreateVolumeBackupPolicyRequest{
		CompartmentId: new(testComp),
		Schedules: []core.VolumeBackupSchedule{{
			BackupType: core.VolumeBackupScheduleBackupTypeIncremental, Period: core.VolumeBackupSchedulePeriodDay,
			OffsetType: core.VolumeBackupScheduleOffsetTypeStructured, HourOfDay: new(3),
			RetentionSeconds: new(36 * 3600), TimeZone: core.VolumeBackupScheduleTimeZoneUtc,
		}},
	})
	require.NoError(t, err)

	_, err = e.block.CreateVolumeBackupPolicyAssignment(ctx, core.CreateVolumeBackupPolicyAssignmentRequest{
		AssetId: vol.Id, PolicyId: pol.Id,
	})
	require.NoError(t, err)

	list := func() []core.VolumeBackup {
		r, lerr := e.block.ListVolumeBackups(
			ctx,
			core.ListVolumeBackupsRequest{CompartmentId: new(testComp), VolumeId: vol.Id},
		)
		require.NoError(t, lerr)

		return r.Items
	}

	assert.Empty(t, list(), "nothing is due at assignment time")

	e.clock.Advance(25 * time.Hour)

	first := list()
	require.Len(t, first, 1)
	assert.Equal(t, core.VolumeBackupSourceTypeScheduled, first[0].SourceType)
	assert.Equal(t, core.VolumeBackupTypeFull, first[0].Type, "the first backup of a volume is full")

	e.clock.Advance(24 * time.Hour)

	second := list()
	require.Len(t, second, 2)

	var states []core.VolumeBackupLifecycleStateEnum

	for _, b := range second {
		if *b.Id == *first[0].Id {
			states = append(states, b.LifecycleState)
		} else {
			assert.Equal(t, core.VolumeBackupTypeIncremental, b.Type)
		}
	}

	assert.Equal(t, []core.VolumeBackupLifecycleStateEnum{core.VolumeBackupLifecycleStateAvailable}, states,
		"36 hours of retention have not passed yet")

	e.clock.Advance(24 * time.Hour)

	got, err := e.block.GetVolumeBackup(ctx, core.GetVolumeBackupRequest{VolumeBackupId: first[0].Id})
	require.NoError(t, err)
	assert.Contains(t, []core.VolumeBackupLifecycleStateEnum{
		core.VolumeBackupLifecycleStateTerminating, core.VolumeBackupLifecycleStateTerminated,
	}, got.LifecycleState,
		"the retention of the schedule has passed")
}
