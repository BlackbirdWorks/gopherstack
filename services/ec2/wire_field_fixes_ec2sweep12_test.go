package ec2_test

import (
	"net/url"
	"regexp"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestCopyVolumes_RealClient covers handler_volumes.go's handleCopyVolumes,
// which pre-fix read a "VolumeId.N" list and a nonexistent "DestinationRegion"
// field -- neither of which the real CopyVolumesInput serializes. The real
// request field is the singular "SourceVolumeId" (serializers.go:69198,
// api_op_CopyVolumes.go); a real client's request never populated VolumeId.N,
// so the handler always saw an empty ID list and every real call failed with
// "at least one VolumeId is required", regardless of what the caller asked to
// copy. The response was equally wrong-shaped: a custom
// sourceVolumeId/destVolumeId item instead of the real full Volume item
// (awsEc2query_deserializeDocumentVolumeList, element "item" -> Volume), so
// even a hand-rolled request that reached the backend would decode to zero
// volumes since "sourceVolumeId"/"destVolumeId" aren't real Volume fields.
func TestCopyVolumes_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	vol, err := b.CreateVolume("us-east-1a", "gp2", 20, "")
	require.NoError(t, err)

	out, err := client.CopyVolumes(t.Context(), &ec2sdk.CopyVolumesInput{
		SourceVolumeId: &vol.ID,
	})
	require.NoError(t, err, "pre-fix this always failed: SourceVolumeId was never read as VolumeId.N")
	require.Len(t, out.Volumes, 1, "pre-fix the response item shape didn't match Volume, decoding to zero volumes")

	copied := out.Volumes[0]
	assert.NotEqual(t, vol.ID, *copied.VolumeId)
	assert.EqualValues(t, vol.Size, *copied.Size)
	assert.Equal(t, "us-east-1a", *copied.AvailabilityZone)

	sizeOverride := int32(50)
	out2, err := client.CopyVolumes(t.Context(), &ec2sdk.CopyVolumesInput{
		SourceVolumeId: &vol.ID,
		Size:           &sizeOverride,
		VolumeType:     types.VolumeTypeGp3,
	})
	require.NoError(t, err)
	require.Len(t, out2.Volumes, 1)
	assert.Equal(t, sizeOverride, *out2.Volumes[0].Size)
	assert.Equal(t, types.VolumeTypeGp3, out2.Volumes[0].VolumeType)
}

// TestLockSnapshot_SurfacesLockFields_RealClient covers handler_snapshots.go's
// handleLockSnapshot, which returned only snapshotId/lockState even though the
// backend's SnapshotLock already computes LockDuration/LockCreatedOn/
// LockExpiresOn (surfaced correctly by the sibling DescribeLockedSnapshots).
// The real LockSnapshotOutput models lockDuration/lockCreatedOn/lockExpiresOn
// (deserializers.go:216295-216340), so a client reading those fields straight
// off the LockSnapshot response -- the natural way to confirm what was just
// locked -- got zero values pre-fix despite the state genuinely existing.
func TestLockSnapshot_SurfacesLockFields_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	vol, err := b.CreateVolume("us-east-1a", "gp2", 10, "")
	require.NoError(t, err)
	snap, err := b.CreateSnapshot(vol.ID, "lock-test")
	require.NoError(t, err)

	out, err := client.LockSnapshot(t.Context(), &ec2sdk.LockSnapshotInput{
		SnapshotId:   &snap.SnapshotID,
		LockMode:     types.LockModeCompliance,
		LockDuration: aws.Int32(30),
	})
	require.NoError(t, err)

	require.NotNil(t, out.LockDuration, "pre-fix LockDuration was never rendered on this op's own response")
	assert.EqualValues(t, 30, *out.LockDuration)
	require.NotNil(t, out.LockCreatedOn, "pre-fix LockCreatedOn was never rendered")
	require.NotNil(t, out.LockExpiresOn, "pre-fix LockExpiresOn was never rendered")
}

// TestUnlockSnapshot_SurfacesSnapshotID_RealClient covers
// handler_snapshots.go's handleUnlockSnapshot, which returned a generic
// stubResponse{Return: true}. The real UnlockSnapshotOutput has no Return
// field at all -- only snapshotId (deserializers.go:223213-223224) -- so a
// client confirming the unlock via the response's SnapshotId got an empty
// string pre-fix, on an operation whose only real confirmation signal is that
// field.
func TestUnlockSnapshot_SurfacesSnapshotID_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	vol, err := b.CreateVolume("us-east-1a", "gp2", 10, "")
	require.NoError(t, err)
	snap, err := b.CreateSnapshot(vol.ID, "unlock-test")
	require.NoError(t, err)
	_, err = b.LockSnapshot(snap.SnapshotID, "compliance", 30)
	require.NoError(t, err)

	out, err := client.UnlockSnapshot(t.Context(), &ec2sdk.UnlockSnapshotInput{
		SnapshotId: &snap.SnapshotID,
	})
	require.NoError(t, err)
	require.NotNil(t, out.SnapshotId, "pre-fix this field was never rendered, only a bare Return bool")
	assert.Equal(t, snap.SnapshotID, *out.SnapshotId)
}

// TestDescribeSnapshotTierStatus_Filters_RealClient covers
// handler_snapshots.go's handleDescribeSnapshotTierStatus, which pre-fix read
// a "SnapshotId.N" list. DescribeSnapshotTierStatusInput has no such field at
// all (api_op_DescribeSnapshotTierStatus.go/serializers.go:80972-80999) --
// only Filters, with a real "snapshot-id" filter key -- so a real client
// filtering by snapshot ID the only way the real API supports had that filter
// silently ignored, seeing every snapshot's tier status instead of just the
// one asked for.
func TestDescribeSnapshotTierStatus_Filters_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	vol, err := b.CreateVolume("us-east-1a", "gp2", 10, "")
	require.NoError(t, err)
	snap1, err := b.CreateSnapshot(vol.ID, "tier-filter-1")
	require.NoError(t, err)
	_, err = b.CreateSnapshot(vol.ID, "tier-filter-2")
	require.NoError(t, err)

	out, err := client.DescribeSnapshotTierStatus(t.Context(), &ec2sdk.DescribeSnapshotTierStatusInput{
		Filters: []types.Filter{{Name: aws.String("snapshot-id"), Values: []string{snap1.SnapshotID}}},
	})
	require.NoError(t, err)
	require.Len(t, out.SnapshotTierStatuses, 1, "pre-fix the snapshot-id filter was silently ignored")
	assert.Equal(t, snap1.SnapshotID, *out.SnapshotTierStatuses[0].SnapshotId)
}

// TestEnableDisableFastSnapshotRestores_SurfacesResults_RealClient covers two
// stacked bugs in handler_snapshots.go's handleEnableFastSnapshotRestores/
// handleDisableFastSnapshotRestores. (1) Request side: both read the snapshot
// ID list under "SnapshotId", but the real wire key is "SourceSnapshotId"
// (serializers.go:84014-84019 Enable, 83152-83157 Disable, both
// FlatKey("SourceSnapshotId") off EnableFastSnapshotRestoresInput.
// SourceSnapshotIds) -- so a real client's request never populated
// "SnapshotId.N" at all, and every real call silently enabled/disabled fast
// restores for zero snapshots while still reporting success. (2) Response
// side: pre-fix returned a bare stubResponse{Return: true}; the real
// EnableFastSnapshotRestoresOutput/DisableFastSnapshotRestoresOutput have no
// Return field at all -- only Successful/Unsuccessful sets
// (deserializers.go:210002-210012, 208034-208044) -- so even a hand-built
// request using the right key would show zero successful entries.
func TestEnableDisableFastSnapshotRestores_SurfacesResults_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	vol, err := b.CreateVolume("us-east-1a", "gp2", 10, "")
	require.NoError(t, err)
	snap, err := b.CreateSnapshot(vol.ID, "fsr-test")
	require.NoError(t, err)

	enableOut, err := client.EnableFastSnapshotRestores(t.Context(), &ec2sdk.EnableFastSnapshotRestoresInput{
		SourceSnapshotIds: []string{snap.SnapshotID},
		AvailabilityZones: []string{"us-east-1a"},
	})
	require.NoError(t, err)
	require.Len(t, enableOut.Successful, 1, "pre-fix Successful was always empty: only a bare Return bool came back")
	assert.Equal(t, snap.SnapshotID, *enableOut.Successful[0].SnapshotId)
	assert.Equal(t, types.FastSnapshotRestoreStateCodeEnabled, enableOut.Successful[0].State)

	disableOut, err := client.DisableFastSnapshotRestores(t.Context(), &ec2sdk.DisableFastSnapshotRestoresInput{
		SourceSnapshotIds: []string{snap.SnapshotID},
		AvailabilityZones: []string{"us-east-1a"},
	})
	require.NoError(t, err)
	require.Len(t, disableOut.Successful, 1)
	assert.Equal(t, types.FastSnapshotRestoreStateCodeDisabled, disableOut.Successful[0].State)
}

// paginationRoundTrip drives action across real (non-forged) NextToken pages
// via dispatchHandler with the given per-page MaxResults, collecting every
// match of idRe group 1. It asserts both that the traversed set equals want
// exactly AND that pages were actually capped and split -- the first page
// holds at most maxResults items, and (since len(want) > maxResults in every
// caller here) more than one page was needed. That second assertion is the
// one that actually distinguishes fixed from unfixed code: an unpaginated
// handler returns everything in a single page with no nextToken at all, which
// would satisfy a check of the traversed set alone without ever truncating.
func paginationRoundTrip(
	t *testing.T,
	h *ec2.Handler,
	action string,
	idRe *regexp.Regexp,
	want map[string]bool,
) {
	t.Helper()

	const maxResults = 3
	require.Greater(t, len(want), maxResults, "test bug: seed more items than maxResults or capping can't be observed")

	nextTokenRe := regexp.MustCompile(`<nextToken>([^<]+)</nextToken>`)
	seen := make(map[string]bool, len(want))
	nextToken := ""
	pages := 0
	firstPageCount := -1

	for {
		vals := url.Values{
			"Action":     {action},
			"Version":    {"2016-11-15"},
			"MaxResults": {strconv.Itoa(maxResults)},
		}
		if nextToken != "" {
			vals.Set("NextToken", nextToken)
		}

		body, err := dispatchHandler(h, vals)
		require.NoError(t, err)
		pages++

		matches := idRe.FindAllStringSubmatch(body, -1)
		if firstPageCount == -1 {
			firstPageCount = len(matches)
		}

		for _, m := range matches {
			seen[m[1]] = true
		}

		tokMatch := nextTokenRe.FindStringSubmatch(body)
		if tokMatch == nil {
			break
		}

		nextToken = tokMatch[1]
	}

	assert.Equal(t, want, seen, "%s pagination must traverse every item exactly once across real tokens", action)
	assert.LessOrEqual(
		t,
		firstPageCount,
		maxResults,
		"%s: first page held %d items, exceeding MaxResults=%d -- pagination is not capping",
		action,
		firstPageCount,
		maxResults,
	)
	assert.Greater(t, pages, 1,
		"%s: everything came back in one page -- MaxResults/NextToken are being ignored", action)
}

// TestVolumesFamilyPagination_RealPageRoundTrip proves the pagination fix for
// three handler_volumes.go ops that declared MaxResults/NextToken on both
// Input and Output (ec2@v1.319.1) but implemented neither pre-fix, always
// returning every matching item in one page. Each seeds more items than its
// MaxResults floor per the earlier ec2sweep11 lesson: a fixture no larger than
// the minimum page size can't fail even against unfixed code.
func TestVolumesFamilyPagination_RealPageRoundTrip(t *testing.T) {
	t.Parallel()

	const seedCount = 7

	t.Run("describe_volume_status", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		h := ec2.NewHandler(b)

		want := make(map[string]bool, seedCount)
		for range seedCount {
			vol, err := b.CreateVolume("us-east-1a", "gp2", 10, "")
			require.NoError(t, err)
			want[vol.ID] = true
		}

		paginationRoundTrip(
			t,
			h,
			"DescribeVolumeStatus",
			regexp.MustCompile(`<volumeId>(vol-[0-9a-f]+)</volumeId>`),
			want,
		)
	})

	t.Run("describe_volumes_modifications", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		h := ec2.NewHandler(b)

		want := make(map[string]bool, seedCount)
		for range seedCount {
			vol, err := b.CreateVolume("us-east-1a", "gp2", 10, "")
			require.NoError(t, err)
			_, err = b.ModifyVolume(vol.ID, "gp3", 20, 3000)
			require.NoError(t, err)
			want[vol.ID] = true
		}

		paginationRoundTrip(
			t,
			h,
			"DescribeVolumesModifications",
			regexp.MustCompile(`<volumeId>(vol-[0-9a-f]+)</volumeId>`),
			want,
		)
	})

	t.Run("describe_replace_root_volume_tasks", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		h := ec2.NewHandler(b)

		insts, err := b.RunInstances("ami-test", "t3.micro", "", 1)
		require.NoError(t, err)

		want := make(map[string]bool, seedCount)
		for range seedCount {
			task, terr := b.CreateReplaceRootVolumeTask(insts[0].ID, "")
			require.NoError(t, terr)
			want[task.ReplaceRootVolumeTaskID] = true
		}

		paginationRoundTrip(
			t, h, "DescribeReplaceRootVolumeTasks",
			regexp.MustCompile(`<replaceRootVolumeTaskId>(replacevol-[0-9a-f]+)</replaceRootVolumeTaskId>`), want,
		)
	})
}

// TestSnapshotsFamilyPagination_RealPageRoundTrip is the handler_snapshots.go
// counterpart of TestVolumesFamilyPagination_RealPageRoundTrip, above.
func TestSnapshotsFamilyPagination_RealPageRoundTrip(t *testing.T) {
	t.Parallel()

	const seedCount = 7

	t.Run("describe_snapshot_tier_status", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		h := ec2.NewHandler(b)

		vol, err := b.CreateVolume("us-east-1a", "gp2", 10, "")
		require.NoError(t, err)

		want := make(map[string]bool, seedCount)
		for range seedCount {
			snap, serr := b.CreateSnapshot(vol.ID, "tier-test")
			require.NoError(t, serr)
			want[snap.SnapshotID] = true
		}

		paginationRoundTrip(
			t,
			h,
			"DescribeSnapshotTierStatus",
			regexp.MustCompile(`<snapshotId>(snap-[0-9a-f]+)</snapshotId>`),
			want,
		)
	})

	t.Run("describe_locked_snapshots", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		h := ec2.NewHandler(b)

		vol, err := b.CreateVolume("us-east-1a", "gp2", 10, "")
		require.NoError(t, err)

		want := make(map[string]bool, seedCount)
		for range seedCount {
			snap, serr := b.CreateSnapshot(vol.ID, "lock-test")
			require.NoError(t, serr)
			_, lerr := b.LockSnapshot(snap.SnapshotID, "compliance", 30)
			require.NoError(t, lerr)
			want[snap.SnapshotID] = true
		}

		paginationRoundTrip(
			t,
			h,
			"DescribeLockedSnapshots",
			regexp.MustCompile(`<snapshotId>(snap-[0-9a-f]+)</snapshotId>`),
			want,
		)
	})

	t.Run("describe_import_snapshot_tasks", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		h := ec2.NewHandler(b)

		want := make(map[string]bool, seedCount)
		for range seedCount {
			task, terr := b.ImportSnapshot("import-test", false, "")
			require.NoError(t, terr)
			want[task.ImportTaskID] = true
		}

		paginationRoundTrip(
			t, h, "DescribeImportSnapshotTasks",
			regexp.MustCompile(`<importTaskId>(import-snap-[0-9a-f]+)</importTaskId>`), want,
		)
	})

	t.Run("describe_fast_snapshot_restores", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		h := ec2.NewHandler(b)

		vol, err := b.CreateVolume("us-east-1a", "gp2", 10, "")
		require.NoError(t, err)

		want := make(map[string]bool, seedCount)
		for range seedCount {
			snap, serr := b.CreateSnapshot(vol.ID, "fsr-test")
			require.NoError(t, serr)
			require.NoError(t, b.EnableFastSnapshotRestores([]string{snap.SnapshotID}, []string{"us-east-1a"}))
			want[snap.SnapshotID] = true
		}

		paginationRoundTrip(
			t,
			h,
			"DescribeFastSnapshotRestores",
			regexp.MustCompile(`<snapshotId>(snap-[0-9a-f]+)</snapshotId>`),
			want,
		)
	})
}
