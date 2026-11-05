package ocicore_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) mkVol(t *testing.T, ad string, gbs int64) core.Volume {
	t.Helper()

	r, err := e.block.CreateVolume(t.Context(), core.CreateVolumeRequest{
		CompartmentId: new(testComp), AvailabilityDomain: new(ad), SizeInGBs: new(gbs)})
	require.NoError(t, err)
	e.settle()

	return r.Volume
}

func (e *env) bootVolumeOf(t *testing.T, inst core.Instance) core.BootVolume {
	t.Helper()

	l, err := e.compute.ListBootVolumeAttachments(t.Context(), core.ListBootVolumeAttachmentsRequest{
		CompartmentId: new(testComp), AvailabilityDomain: inst.AvailabilityDomain, InstanceId: inst.Id})
	require.NoError(t, err)
	require.NotEmpty(t, l.Items)

	r, err := e.block.GetBootVolume(t.Context(), core.GetBootVolumeRequest{BootVolumeId: l.Items[0].BootVolumeId})
	require.NoError(t, err)

	return r.BootVolume
}

func (e *env) mkBoot(t *testing.T) core.BootVolume {
	t.Helper()

	f := e.fixture(t)
	inst := e.mustLaunch(t, f.pub)
	e.settle()

	return e.bootVolumeOf(t, inst)
}

func TestBootVolumeBackups(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	bv := e.mkBoot(t)

	first, err := e.block.CreateBootVolumeBackup(ctx, core.CreateBootVolumeBackupRequest{
		BootVolumeId: bv.Id, DisplayName: new("b1")})
	require.NoError(t, err)
	assert.Equal(t, core.BootVolumeBackupTypeFull, first.Type, "the first backup is full")
	assert.Equal(t, core.BootVolumeBackupLifecycleStateCreating, first.LifecycleState)
	assert.Equal(t, *bv.SizeInGBs, *first.SizeInGBs)
	assert.Equal(t, *bv.ImageId, *first.ImageId)
	assert.Equal(t, core.BootVolumeBackupSourceTypeManual, first.SourceType)
	assert.NotEmpty(t, *first.Etag)

	second, err := e.block.CreateBootVolumeBackup(ctx, core.CreateBootVolumeBackupRequest{BootVolumeId: bv.Id})
	require.NoError(t, err)
	assert.Equal(t, core.BootVolumeBackupTypeIncremental, second.Type)

	tests := []struct {
		name     string
		req      core.CreateBootVolumeBackupRequest
		wantCode string
	}{
		{"unknown", core.CreateBootVolumeBackupRequest{BootVolumeId: new("ocid1.bootvolume.oc1.iad.nope")},
			"NotAuthorizedOrNotFound"},
		{"missing", core.CreateBootVolumeBackupRequest{}, "MissingParameter"},
		{
			"bad retention unit",
			core.CreateBootVolumeBackupRequest{BootVolumeId: bv.Id, RetentionPeriod: &core.RetentionDuration{
				RetentionTimeUnit: "WEEKS", RetentionTimeAmount: new(1)}},
			"InvalidParameter",
		},
		{
			"bad retention amount",
			core.CreateBootVolumeBackupRequest{BootVolumeId: bv.Id, RetentionPeriod: &core.RetentionDuration{
				RetentionTimeUnit:   core.RetentionDurationRetentionTimeUnitDays,
				RetentionTimeAmount: new(0),
			}},
			"InvalidParameter",
		},
	}

	for _, tt := range tests {
		_, cerr := e.block.CreateBootVolumeBackup(ctx, tt.req)
		assert.Equal(t, tt.wantCode, svcErr(t, cerr).GetCode())
	}

	e.settle()

	l, err := e.block.ListBootVolumeBackups(ctx, core.ListBootVolumeBackupsRequest{
		CompartmentId: new(
			testComp,
		),
		BootVolumeId:   bv.Id,
		LifecycleState: core.BootVolumeBackupLifecycleStateAvailable,
	})
	require.NoError(t, err)
	assert.Len(t, l.Items, 2)

	l, err = e.block.ListBootVolumeBackups(ctx, core.ListBootVolumeBackupsRequest{
		CompartmentId: new(testComp), DisplayName: new("b1")})
	require.NoError(t, err)
	require.Len(t, l.Items, 1)

	up, err := e.block.UpdateBootVolumeBackup(ctx, core.UpdateBootVolumeBackupRequest{BootVolumeBackupId: first.Id,
		DisplayName: new("renamed"), FreeformTags: map[string]string{"k": "v"}})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *up.DisplayName)

	restored, err := e.block.CreateBootVolume(ctx, core.CreateBootVolumeRequest{
		CompartmentId:      new(testComp),
		AvailabilityDomain: bv.AvailabilityDomain,
		SourceDetails:      core.BootVolumeSourceFromBootVolumeBackupDetails{Id: first.Id},
	})
	require.NoError(t, err)
	assert.Equal(t, core.BootVolumeLifecycleStateRestoring, restored.LifecycleState)
	assert.Equal(t, *bv.ImageId, *restored.ImageId)

	_, err = e.block.CreateBootVolume(ctx, core.CreateBootVolumeRequest{CompartmentId: new(testComp),
		SourceDetails: core.BootVolumeSourceFromBootVolumeBackupDetails{Id: first.Id}})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "restoring needs an availability domain")

	_, err = e.block.DeleteBootVolumeBackup(ctx, core.DeleteBootVolumeBackupRequest{BootVolumeBackupId: second.Id})
	require.NoError(t, err)
	e.settle()

	gone, err := e.block.GetBootVolumeBackup(ctx, core.GetBootVolumeBackupRequest{BootVolumeBackupId: second.Id})
	require.NoError(t, err)
	assert.Equal(t, core.BootVolumeBackupLifecycleStateTerminated, gone.LifecycleState)

	_, err = e.block.GetBootVolumeBackup(
		ctx,
		core.GetBootVolumeBackupRequest{BootVolumeBackupId: new("ocid1.x.oc1.iad.nope")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.block.ChangeBootVolumeBackupCompartment(ctx, core.ChangeBootVolumeBackupCompartmentRequest{
		BootVolumeBackupId: first.Id,
		CompartmentId:      new(testComp + "x"),
	})
	require.NoError(t, err)
}

func TestBackupRetention(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	vol := e.mkVol(t, testAD, 60)
	days := &core.RetentionDuration{
		RetentionTimeUnit:   core.RetentionDurationRetentionTimeUnitDays,
		RetentionTimeAmount: new(2),
	}

	bk, err := e.block.CreateVolumeBackup(ctx, core.CreateVolumeBackupRequest{VolumeId: vol.Id, RetentionPeriod: days,
		IsPreventDeletionEnabled: new(true)})
	require.NoError(t, err)
	assert.True(t, *bk.IsPreventDeletionEnabled)
	require.NotNil(t, bk.TimeRetentionExpiresAt)
	assert.WithinDuration(t, bk.TimeCreated.Add(48*time.Hour), bk.TimeRetentionExpiresAt.Time, time.Second)
	assert.Equal(t, 2, *bk.RetentionPeriod.RetentionTimeAmount)

	_, err = e.block.DeleteVolumeBackup(ctx, core.DeleteVolumeBackupRequest{VolumeBackupId: bk.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "retained")

	e.clock.Advance(49 * time.Hour)

	_, err = e.block.DeleteVolumeBackup(ctx, core.DeleteVolumeBackupRequest{VolumeBackupId: bk.Id})
	require.NoError(t, err, "retention expired")

	lock, err := e.block.CreateVolumeBackup(ctx, core.CreateVolumeBackupRequest{VolumeId: vol.Id, RetentionPeriod: days,
		IsRetentionLockEnabled: new(true)})
	require.NoError(t, err)

	_, err = e.block.UpdateVolumeBackup(ctx, core.UpdateVolumeBackupRequest{VolumeBackupId: lock.Id,
		IsRetentionLockEnabled: new(false)})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "a lock cannot be removed")

	forever, err := e.block.CreateVolumeBackup(ctx, core.CreateVolumeBackupRequest{VolumeId: vol.Id,
		IsIndefiniteRetentionEnabled: new(true)})
	require.NoError(t, err)

	e.clock.Advance(24 * 365 * time.Hour)

	_, err = e.block.DeleteVolumeBackup(ctx, core.DeleteVolumeBackupRequest{VolumeBackupId: forever.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "indefinite retention")

	released, err := e.block.UpdateVolumeBackup(ctx, core.UpdateVolumeBackupRequest{VolumeBackupId: forever.Id,
		IsIndefiniteRetentionEnabled: new(false)})
	require.NoError(t, err)
	assert.False(t, *released.IsIndefiniteRetentionEnabled)

	_, err = e.block.DeleteVolumeBackup(ctx, core.DeleteVolumeBackupRequest{VolumeBackupId: forever.Id})
	require.NoError(t, err)

	_, err = e.block.DeleteVolumeBackup(ctx, core.DeleteVolumeBackupRequest{VolumeBackupId: lock.Id})
	require.NoError(t, err, "the lock period has passed")
}

func TestCrossRegionBackupCopy(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	vol := e.mkVol(t, testAD, 60)
	bv := e.mkBoot(t)

	bk, err := e.block.CreateVolumeBackup(
		ctx,
		core.CreateVolumeBackupRequest{VolumeId: vol.Id, DisplayName: new("src")},
	)
	require.NoError(t, err)

	bootBk, err := e.block.CreateBootVolumeBackup(ctx, core.CreateBootVolumeBackupRequest{BootVolumeId: bv.Id})
	require.NoError(t, err)

	creating := func(region string) core.CopyVolumeBackupRequest {
		return core.CopyVolumeBackupRequest{VolumeBackupId: bk.Id,
			DestinationRegion: new(region)}
	}

	_, err = e.block.CopyVolumeBackup(ctx, creating("eu-frankfurt-1"))
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode(), "the source is still CREATING")

	e.settle()

	tests := []struct {
		name     string
		region   string
		wantCode string
	}{
		{"same region", "us-ashburn-1", "InvalidParameter"},
		{"unknown region", "mars-1", "InvalidParameter"},
		{"ok", "eu-frankfurt-1", ""},
	}

	for _, tt := range tests {
		r, cerr := e.block.CopyVolumeBackup(ctx, creating(tt.region))
		if tt.wantCode != "" {
			assert.Equal(t, tt.wantCode, svcErr(t, cerr).GetCode())

			continue
		}

		require.NoError(t, cerr)
		assert.Equal(t, core.VolumeBackupLifecycleStateCreating, r.LifecycleState)
		assert.Equal(t, *bk.Id, *r.SourceVolumeBackupId)
		assert.Contains(t, *r.Id, ".fra.", "the copy lives in the destination region")
		assert.NotEmpty(t, *r.OpcWorkRequestId)
		assert.Equal(t, "COPY_VOLUME_BACKUP", *e.workRequest(t, r.OpcWorkRequestId).OperationType)
	}

	e.settle()

	copies, err := e.block.ListVolumeBackups(ctx, core.ListVolumeBackupsRequest{CompartmentId: new(testComp),
		SourceVolumeBackupId: bk.Id})
	require.NoError(t, err)
	require.Len(t, copies.Items, 1)
	assert.Equal(
		t,
		core.VolumeBackupLifecycleStateAvailable,
		copies.Items[0].LifecycleState,
		"the copy completes on the clock",
	)
	assert.Equal(t, *vol.Id, *copies.Items[0].VolumeId)

	_, err = e.block.CopyVolumeBackup(ctx, core.CopyVolumeBackupRequest{VolumeBackupId: copies.Items[0].Id,
		DestinationRegion: new("eu-frankfurt-1")})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "a copy cannot return to its own region")

	back, err := e.block.CopyVolumeBackup(ctx, core.CopyVolumeBackupRequest{VolumeBackupId: copies.Items[0].Id,
		DestinationRegion: new("us-ashburn-1"),
		DisplayName:       new("home")})
	require.NoError(t, err)
	assert.Contains(t, *back.Id, ".iad.")
	assert.Equal(t, "home", *back.DisplayName)

	e.settle()

	bootCopy, err := e.block.CopyBootVolumeBackup(ctx, core.CopyBootVolumeBackupRequest{BootVolumeBackupId: bootBk.Id,
		DestinationRegion: new("uk-london-1")})
	require.NoError(t, err)
	assert.Equal(t, *bootBk.Id, *bootCopy.SourceBootVolumeBackupId)
	assert.Contains(t, *bootCopy.Id, ".lhr.")
	assert.Equal(t, *bootBk.ImageId, *bootCopy.ImageId)

	_, err = e.block.CopyBootVolumeBackup(ctx, core.CopyBootVolumeBackupRequest{BootVolumeBackupId: bootBk.Id})
	require.Error(t, err)

	byBoot, err := e.block.ListBootVolumeBackups(ctx, core.ListBootVolumeBackupsRequest{CompartmentId: new(testComp),
		SourceBootVolumeBackupId: bootBk.Id})
	require.NoError(t, err)
	assert.Len(t, byBoot.Items, 1)
}

func TestVolumeGroups(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v1, v2, v3 := e.mkVol(t, testAD, 60), e.mkVol(t, testAD, 70), e.mkVol(t, testAD2, 80)
	bv := e.mkBoot(t)

	create := func(ad string, ids ...string) (core.CreateVolumeGroupResponse, error) {
		return e.block.CreateVolumeGroup(
			ctx,
			core.CreateVolumeGroupRequest{
				CompartmentId: new(testComp), AvailabilityDomain: new(ad), DisplayName: new("grp"),
				SourceDetails: core.VolumeGroupSourceFromVolumesDetails{VolumeIds: ids}},
		)
	}

	g, err := create(testAD, *v1.Id, *v2.Id, *bv.Id)
	require.NoError(t, err)
	assert.Equal(t, core.VolumeGroupLifecycleStateProvisioning, g.LifecycleState)
	assert.ElementsMatch(t, []string{*v1.Id, *v2.Id, *bv.Id}, g.VolumeIds)
	assert.Equal(t, (60+70+*bv.SizeInGBs)*1024, *g.SizeInMBs)
	assert.True(t, *g.IsHydrated)

	got, err := e.block.GetVolume(ctx, core.GetVolumeRequest{VolumeId: v1.Id})
	require.NoError(t, err)
	assert.Equal(t, *g.Id, *got.VolumeGroupId)

	tests := []struct {
		name     string
		ad       string
		wantCode string
		ids      []string
	}{
		{"already grouped", testAD, "Conflict", []string{*v1.Id}},
		{"other availability domain", testAD, "InvalidParameter", []string{*v3.Id}},
		{"duplicate", testAD2, "InvalidParameter", []string{*v3.Id, *v3.Id}},
		{"not a volume", testAD2, "InvalidParameter", []string{"ocid1.vcn.oc1.iad.x"}},
		{"unknown volume", testAD2, "NotAuthorizedOrNotFound", []string{"ocid1.volume.oc1.iad.nope"}},
		{"bad availability domain", "nope", "InvalidParameter", []string{*v3.Id}},
	}

	for _, tt := range tests {
		_, cerr := create(tt.ad, tt.ids...)
		assert.Equal(t, tt.wantCode, svcErr(t, cerr).GetCode())
	}

	_, err = e.block.DeleteVolume(ctx, core.DeleteVolumeRequest{VolumeId: v1.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "a grouped volume cannot be deleted")

	e.settle()

	up, err := e.block.UpdateVolumeGroup(ctx, core.UpdateVolumeGroupRequest{
		VolumeGroupId: g.Id,
		VolumeIds:     []string{*v1.Id},
		DisplayName:   new("small"),
	})
	require.NoError(t, err)
	assert.Equal(t, []string{*v1.Id}, up.VolumeIds)
	assert.EqualValues(t, 60*1024, *up.SizeInMBs)

	freed, err := e.block.GetVolume(ctx, core.GetVolumeRequest{VolumeId: v2.Id})
	require.NoError(t, err)
	assert.Nil(t, freed.VolumeGroupId)

	_, err = e.block.UpdateVolumeGroup(ctx, core.UpdateVolumeGroupRequest{VolumeGroupId: g.Id,
		VolumeIds: []string{*v3.Id}})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	l, err := e.block.ListVolumeGroups(ctx, core.ListVolumeGroupsRequest{CompartmentId: new(testComp),
		AvailabilityDomain: new(testAD), DisplayName: new("small")})
	require.NoError(t, err)
	assert.Len(t, l.Items, 1)

	_, err = e.block.DeleteVolumeGroup(ctx, core.DeleteVolumeGroupRequest{VolumeGroupId: g.Id})
	require.NoError(t, err)
	e.settle()

	after, err := e.block.GetVolume(ctx, core.GetVolumeRequest{VolumeId: v1.Id})
	require.NoError(t, err)
	assert.Nil(t, after.VolumeGroupId, "members survive the group")

	_, err = e.block.DeleteVolume(ctx, core.DeleteVolumeRequest{VolumeId: v1.Id})
	require.NoError(t, err)
}

func TestVolumeGroupBackups(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v1, v2 := e.mkVol(t, testAD, 60), e.mkVol(t, testAD, 70)
	bv := e.mkBoot(t)

	g, err := e.block.CreateVolumeGroup(
		ctx,
		core.CreateVolumeGroupRequest{
			CompartmentId: new(testComp), AvailabilityDomain: new(testAD), DisplayName: new("grp"),
			SourceDetails: core.VolumeGroupSourceFromVolumesDetails{VolumeIds: []string{*v1.Id, *v2.Id, *bv.Id}}},
	)
	require.NoError(t, err)
	e.settle()

	gb, err := e.block.CreateVolumeGroupBackup(ctx, core.CreateVolumeGroupBackupRequest{
		VolumeGroupId: g.Id,
		DisplayName:   new("gb1"),
	})
	require.NoError(t, err)
	assert.Equal(t, core.VolumeGroupBackupTypeFull, gb.Type)
	assert.Len(t, gb.VolumeBackupIds, 3)
	assert.Equal(t, *g.SizeInMBs, *gb.SizeInMBs)
	assert.Equal(t, *g.Id, *gb.VolumeGroupId)
	assert.Equal(t, testComp, *gb.CompartmentId)

	for _, id := range gb.VolumeBackupIds {
		if id[:len("ocid1.volumebackup")] == "ocid1.volumebackup" {
			b, gerr := e.block.GetVolumeBackup(ctx, core.GetVolumeBackupRequest{VolumeBackupId: &id})
			require.NoError(t, gerr)
			assert.Equal(t, *gb.Id, *b.VolumeGroupBackupId)

			_, derr := e.block.DeleteVolumeBackup(ctx, core.DeleteVolumeBackupRequest{VolumeBackupId: &id})
			assert.Equal(t, "Conflict", svcErr(t, derr).GetCode(), "members belong to the group backup")
		}
	}

	e.settle()

	second, err := e.block.CreateVolumeGroupBackup(ctx, core.CreateVolumeGroupBackupRequest{
		VolumeGroupId: g.Id})
	require.NoError(t, err)
	assert.Equal(t, core.VolumeGroupBackupTypeIncremental, second.Type)

	_, err = e.block.CreateVolumeGroupBackup(ctx, core.CreateVolumeGroupBackupRequest{
		VolumeGroupId: new("ocid1.volumegroup.oc1.iad.nope"),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	l, err := e.block.ListVolumeGroupBackups(ctx, core.ListVolumeGroupBackupsRequest{CompartmentId: new(testComp),
		VolumeGroupId: g.Id})
	require.NoError(t, err)
	assert.Len(t, l.Items, 2)

	restored, err := e.block.CreateVolumeGroup(
		ctx,
		core.CreateVolumeGroupRequest{
			CompartmentId: new(testComp), AvailabilityDomain: new(testAD2),
			SourceDetails: core.VolumeGroupSourceFromVolumeGroupBackupDetails{VolumeGroupBackupId: gb.Id}},
	)
	require.NoError(t, err)
	assert.Len(t, restored.VolumeIds, 3)
	assert.False(t, *restored.IsHydrated)
	assert.Equal(t, *g.SizeInMBs, *restored.SizeInMBs)

	for _, id := range restored.VolumeIds {
		assert.NotContains(t, []string{*v1.Id, *v2.Id, *bv.Id}, id, "restored volumes are new")
	}

	clone, err := e.block.CreateVolumeGroup(
		ctx,
		core.CreateVolumeGroupRequest{
			CompartmentId: new(testComp), AvailabilityDomain: new(testAD),
			SourceDetails: core.VolumeGroupSourceFromVolumeGroupDetails{VolumeGroupId: g.Id}},
	)
	require.NoError(t, err)
	assert.Len(t, clone.VolumeIds, 3)

	_, err = e.block.CreateVolumeGroup(
		ctx,
		core.CreateVolumeGroupRequest{
			CompartmentId: new(testComp), AvailabilityDomain: new(testAD2),
			SourceDetails: core.VolumeGroupSourceFromVolumeGroupDetails{VolumeGroupId: g.Id}},
	)
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "clones stay in the availability domain")

	_, err = e.block.CreateVolumeGroup(
		ctx,
		core.CreateVolumeGroupRequest{
			CompartmentId:      new(testComp),
			AvailabilityDomain: new(testAD),
			SourceDetails: core.VolumeGroupSourceFromVolumeGroupReplicaDetails{
				VolumeGroupReplicaId: new("ocid1.volumegroupreplica.oc1.iad.x"),
			}},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	e.settle()

	cp, err := e.block.CopyVolumeGroupBackup(ctx, core.CopyVolumeGroupBackupRequest{VolumeGroupBackupId: gb.Id,
		DestinationRegion: new("us-phoenix-1")})
	require.NoError(t, err)
	assert.Equal(t, *gb.Id, *cp.SourceVolumeGroupBackupId)
	assert.Contains(t, *cp.Id, ".phx.")
	assert.Len(t, cp.VolumeBackupIds, 3)

	up, err := e.block.UpdateVolumeGroupBackup(ctx, core.UpdateVolumeGroupBackupRequest{VolumeGroupBackupId: gb.Id,
		DisplayName:              new("renamed"),
		IsPreventDeletionEnabled: new(true)})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *up.DisplayName)

	_, err = e.block.DeleteVolumeGroupBackup(ctx, core.DeleteVolumeGroupBackupRequest{VolumeGroupBackupId: gb.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "deletion is prevented")

	_, err = e.block.UpdateVolumeGroupBackup(ctx, core.UpdateVolumeGroupBackupRequest{VolumeGroupBackupId: gb.Id,
		IsPreventDeletionEnabled: new(false)})
	require.NoError(t, err)

	_, err = e.block.DeleteVolumeGroupBackup(ctx, core.DeleteVolumeGroupBackupRequest{VolumeGroupBackupId: second.Id})
	require.NoError(t, err)
	e.settle()

	for _, id := range second.VolumeBackupIds {
		if id[:len("ocid1.volumebackup")] == "ocid1.volumebackup" {
			b, gerr := e.block.GetVolumeBackup(ctx, core.GetVolumeBackupRequest{VolumeBackupId: &id})
			require.NoError(t, gerr)
			assert.Equal(
				t,
				core.VolumeBackupLifecycleStateTerminated,
				b.LifecycleState,
				"members follow the group backup",
			)
		}
	}

	_, err = e.block.ChangeVolumeGroupBackupCompartment(ctx, core.ChangeVolumeGroupBackupCompartmentRequest{
		VolumeGroupBackupId: gb.Id,
		CompartmentId:       new(testComp),
	})
	require.NoError(t, err)

	_, err = e.block.ChangeVolumeGroupCompartment(ctx, core.ChangeVolumeGroupCompartmentRequest{VolumeGroupId: g.Id,
		CompartmentId: new(testComp)})
	require.NoError(t, err)
}

func TestVolumeGroupBackupPolicy(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	vol := e.mkVol(t, testAD, 60)

	pols, err := e.block.ListVolumeBackupPolicies(ctx, core.ListVolumeBackupPoliciesRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, pols.Items)

	g, err := e.block.CreateVolumeGroup(
		ctx,
		core.CreateVolumeGroupRequest{
			CompartmentId: new(testComp), AvailabilityDomain: new(testAD), BackupPolicyId: pols.Items[0].Id,
			SourceDetails: core.VolumeGroupSourceFromVolumesDetails{VolumeIds: []string{*vol.Id}}},
	)
	require.NoError(t, err)

	as, err := e.block.GetVolumeBackupPolicyAssetAssignment(
		ctx,
		core.GetVolumeBackupPolicyAssetAssignmentRequest{AssetId: g.Id},
	)
	require.NoError(t, err)
	require.Len(t, as.Items, 1)
	assert.Equal(t, *pols.Items[0].Id, *as.Items[0].PolicyId)

	_, err = e.block.DeleteVolumeGroup(ctx, core.DeleteVolumeGroupRequest{VolumeGroupId: g.Id})
	require.NoError(t, err)

	_, err = e.block.CreateVolumeGroup(
		ctx,
		core.CreateVolumeGroupRequest{
			CompartmentId: new(
				testComp,
			),
			AvailabilityDomain: new(testAD),
			BackupPolicyId:     new("ocid1.volumebackuppolicy.oc1.iad.nope"),
			SourceDetails:      core.VolumeGroupSourceFromVolumesDetails{VolumeIds: []string{*vol.Id}}},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestBlockVolumeReplicas(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	phx := "Abcd:PHX-AD-1"

	vr, err := e.block.CreateVolume(ctx, core.CreateVolumeRequest{CompartmentId: new(testComp),
		AvailabilityDomain: new(testAD), SizeInGBs: new(int64(60)),
		BlockVolumeReplicas: []core.BlockVolumeReplicaDetails{{AvailabilityDomain: new(phx), DisplayName: new("dr")}}})
	require.NoError(t, err)
	require.Len(t, vr.BlockVolumeReplicas, 1)

	info := vr.BlockVolumeReplicas[0]
	assert.Equal(t, phx, *info.AvailabilityDomain)
	assert.Equal(t, "dr", *info.DisplayName)

	rep, err := e.block.GetBlockVolumeReplica(
		ctx,
		core.GetBlockVolumeReplicaRequest{BlockVolumeReplicaId: info.BlockVolumeReplicaId},
	)
	require.NoError(t, err)
	assert.Equal(t, core.BlockVolumeReplicaLifecycleStateProvisioning, rep.LifecycleState)
	assert.Equal(t, *vr.Id, *rep.BlockVolumeId)
	assert.EqualValues(t, 60, *rep.SizeInGBs)
	assert.Equal(t, rep.TimeCreated.Time, rep.TimeLastSynced.Time)

	e.settle()

	rep, err = e.block.GetBlockVolumeReplica(
		ctx,
		core.GetBlockVolumeReplicaRequest{BlockVolumeReplicaId: info.BlockVolumeReplicaId},
	)
	require.NoError(t, err)
	assert.Equal(t, core.BlockVolumeReplicaLifecycleStateAvailable, rep.LifecycleState)
	assert.True(t, rep.TimeLastSynced.After(rep.TimeCreated.Time), "the first sync completes on the clock")

	l, err := e.block.ListBlockVolumeReplicas(ctx, core.ListBlockVolumeReplicasRequest{CompartmentId: new(testComp),
		AvailabilityDomain: new(phx)})
	require.NoError(t, err)
	assert.Len(t, l.Items, 1)

	l, err = e.block.ListBlockVolumeReplicas(ctx, core.ListBlockVolumeReplicasRequest{AvailabilityDomain: new(testAD)})
	require.NoError(t, err)
	assert.Empty(t, l.Items)

	l, err = e.block.ListBlockVolumeReplicas(
		ctx,
		core.ListBlockVolumeReplicasRequest{VolumeGroupReplicaId: new("ocid1.x.oc1.iad.y")},
	)
	require.NoError(t, err)
	assert.Empty(t, l.Items)

	tests := []struct {
		name     string
		wantCode string
		reps     []core.BlockVolumeReplicaDetails
	}{
		{"same availability domain", "InvalidParameter", []core.BlockVolumeReplicaDetails{{AvailabilityDomain: new(testAD)}}},
		{"bad availability domain", "InvalidParameter", []core.BlockVolumeReplicaDetails{{AvailabilityDomain: new("nope")}}},
		{"two replicas", "LimitExceeded", []core.BlockVolumeReplicaDetails{
			{AvailabilityDomain: new(phx)},
			{AvailabilityDomain: new("Abcd:FRA-AD-1")},
		}},
	}

	for _, tt := range tests {
		_, cerr := e.block.CreateVolume(ctx, core.CreateVolumeRequest{CompartmentId: new(testComp),
			AvailabilityDomain: new(testAD), BlockVolumeReplicas: tt.reps})
		assert.Equal(t, tt.wantCode, svcErr(t, cerr).GetCode())
	}

	grown, err := e.block.UpdateVolume(ctx, core.UpdateVolumeRequest{VolumeId: vr.Id,
		SizeInGBs: new(int64(100))})
	require.NoError(t, err)
	assert.EqualValues(t, 100, *grown.SizeInGBs)

	rep, err = e.block.GetBlockVolumeReplica(
		ctx,
		core.GetBlockVolumeReplicaRequest{BlockVolumeReplicaId: info.BlockVolumeReplicaId},
	)
	require.NoError(t, err)
	assert.EqualValues(t, 100, *rep.SizeInGBs, "replicas grow with their volume")

	fra := "Abcd:FRA-AD-1"
	swapped, err := e.block.UpdateVolume(ctx, core.UpdateVolumeRequest{
		VolumeId:            vr.Id,
		BlockVolumeReplicas: []core.BlockVolumeReplicaDetails{{AvailabilityDomain: new(fra)}},
	})
	require.NoError(t, err)
	require.Len(t, swapped.BlockVolumeReplicas, 1)
	assert.Equal(t, fra, *swapped.BlockVolumeReplicas[0].AvailabilityDomain)

	e.settle()

	old, err := e.block.GetBlockVolumeReplica(
		ctx,
		core.GetBlockVolumeReplicaRequest{BlockVolumeReplicaId: info.BlockVolumeReplicaId},
	)
	require.NoError(t, err)
	assert.Equal(t, core.BlockVolumeReplicaLifecycleStateTerminated, old.LifecycleState, "the old replica is replaced")

	restored, err := e.block.CreateVolume(ctx, core.CreateVolumeRequest{CompartmentId: new(testComp),
		AvailabilityDomain: new(fra), SourceDetails: core.VolumeSourceFromBlockVolumeReplicaDetails{
			Id: swapped.BlockVolumeReplicas[0].BlockVolumeReplicaId}})
	require.NoError(t, err)
	assert.Equal(t, core.VolumeLifecycleStateRestoring, restored.LifecycleState)
	assert.EqualValues(t, 100, *restored.SizeInGBs)

	_, err = e.block.CreateVolume(ctx, core.CreateVolumeRequest{CompartmentId: new(testComp),
		AvailabilityDomain: new(testAD), SourceDetails: core.VolumeSourceFromBlockVolumeReplicaDetails{
			Id: swapped.BlockVolumeReplicas[0].BlockVolumeReplicaId}})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "restore in the replica's availability domain")

	none, err := e.block.UpdateVolume(ctx, core.UpdateVolumeRequest{VolumeId: vr.Id,
		BlockVolumeReplicas: []core.BlockVolumeReplicaDetails{}})
	require.NoError(t, err)
	assert.Empty(t, none.BlockVolumeReplicas)

	withRep, err := e.block.CreateVolume(ctx, core.CreateVolumeRequest{
		CompartmentId: new(testComp),
		AvailabilityDomain: new(
			testAD,
		),
		BlockVolumeReplicas: []core.BlockVolumeReplicaDetails{{AvailabilityDomain: new(phx)}},
	})
	require.NoError(t, err)

	_, err = e.block.DeleteVolume(ctx, core.DeleteVolumeRequest{VolumeId: withRep.Id})
	require.NoError(t, err)
	e.settle()

	gone, err := e.block.GetBlockVolumeReplica(ctx, core.GetBlockVolumeReplicaRequest{
		BlockVolumeReplicaId: withRep.BlockVolumeReplicas[0].BlockVolumeReplicaId})
	require.NoError(t, err)
	assert.Equal(
		t,
		core.BlockVolumeReplicaLifecycleStateTerminated,
		gone.LifecycleState,
		"replicas follow their volume",
	)

	_, err = e.block.GetBlockVolumeReplica(
		ctx,
		core.GetBlockVolumeReplicaRequest{BlockVolumeReplicaId: new("ocid1.x.oc1.iad.nope")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestVolumeKmsKeys(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	keys := map[string]string{
		"ocid1.key.oc1.iad.good": "ENABLED", "ocid1.key.oc1.iad.off": "DISABLED", "ocid1.key.oc1.iad.good2": "ENABLED",
	}
	e.backend.SetKeyChecker(func(id string) (string, bool) {
		s, ok := keys[id]
		return s, ok
	})

	good, off, missing := "ocid1.key.oc1.iad.good", "ocid1.key.oc1.iad.off", "ocid1.key.oc1.iad.nope"

	tests := []struct {
		name     string
		key      string
		wantCode string
	}{
		{"enabled", good, ""},
		{"none", "", ""},
		{"disabled", off, "IncorrectState"},
		{"unknown", missing, "NotAuthorizedOrNotFound"},
	}

	for _, tt := range tests {
		r, cerr := e.block.CreateVolume(ctx, core.CreateVolumeRequest{CompartmentId: new(testComp),
			AvailabilityDomain: new(testAD), KmsKeyId: new(tt.key)})
		if tt.wantCode != "" {
			assert.Equal(t, tt.wantCode, svcErr(t, cerr).GetCode())

			continue
		}

		require.NoError(t, cerr)

		if tt.key != "" {
			assert.Equal(t, tt.key, *r.KmsKeyId)
		}
	}

	vol := e.mkVol(t, testAD, 60)

	k, err := e.block.GetVolumeKmsKey(ctx, core.GetVolumeKmsKeyRequest{VolumeId: vol.Id})
	require.NoError(t, err)
	assert.Nil(t, k.KmsKeyId, "Oracle-managed keys give a null kmsKeyId")

	_, err = e.block.UpdateVolumeKmsKey(ctx, core.UpdateVolumeKmsKeyRequest{VolumeId: vol.Id,
		KmsKeyId: new(off)})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())

	_, err = e.block.UpdateVolumeKmsKey(ctx, core.UpdateVolumeKmsKeyRequest{VolumeId: vol.Id,
		KmsKeyId: new(missing)})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	u, err := e.block.UpdateVolumeKmsKey(ctx, core.UpdateVolumeKmsKeyRequest{VolumeId: vol.Id,
		KmsKeyId: new(good)})
	require.NoError(t, err)
	assert.Equal(t, good, *u.KmsKeyId)

	gv, err := e.block.GetVolume(ctx, core.GetVolumeRequest{VolumeId: vol.Id})
	require.NoError(t, err)
	assert.Equal(t, good, *gv.KmsKeyId)

	bk, err := e.block.CreateVolumeBackup(ctx, core.CreateVolumeBackupRequest{VolumeId: vol.Id, KmsKeyId: new(missing)})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode(), "backup keys are checked: %v", bk)

	_, err = e.block.DeleteVolumeKmsKey(ctx, core.DeleteVolumeKmsKeyRequest{VolumeId: vol.Id})
	require.NoError(t, err)

	k, err = e.block.GetVolumeKmsKey(ctx, core.GetVolumeKmsKeyRequest{VolumeId: vol.Id})
	require.NoError(t, err)
	assert.Nil(t, k.KmsKeyId)

	_, err = e.block.GetVolumeKmsKey(ctx, core.GetVolumeKmsKeyRequest{VolumeId: new("ocid1.volume.oc1.iad.nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	bv := e.mkBoot(t)

	_, err = e.block.UpdateBootVolumeKmsKey(ctx, core.UpdateBootVolumeKmsKeyRequest{BootVolumeId: bv.Id,
		KmsKeyId: new(good)})
	require.NoError(t, err)

	bk2, err := e.block.CreateBootVolumeBackup(
		ctx,
		core.CreateBootVolumeBackupRequest{BootVolumeId: bv.Id, KmsKeyId: new(good)},
	)
	require.NoError(t, err)
	assert.Equal(t, good, *bk2.KmsKeyId)

	bkey, err := e.block.GetBootVolumeKmsKey(ctx, core.GetBootVolumeKmsKeyRequest{BootVolumeId: bv.Id})
	require.NoError(t, err)
	assert.Equal(t, good, *bkey.KmsKeyId)

	_, err = e.block.DeleteBootVolumeKmsKey(ctx, core.DeleteBootVolumeKmsKeyRequest{BootVolumeId: bv.Id})
	require.NoError(t, err)

	_, err = e.compute.LaunchInstance(ctx, core.LaunchInstanceRequest{
		CompartmentId:      new(testComp),
		AvailabilityDomain: new(testAD),
		Shape:              new("VM.Standard.E4.Flex"),
		ShapeConfig:        &core.LaunchInstanceShapeConfigDetails{Ocpus: new(float32(1))},
		SourceDetails: core.InstanceSourceViaImageDetails{
			ImageId:  e.imageID(t, "Oracle Linux", "9", false),
			KmsKeyId: new(missing),
		},
		CreateVnicDetails: &core.CreateVnicDetails{SubnetId: e.fixture(t).pub.Id}})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode(), "launch checks the boot volume key")
}
