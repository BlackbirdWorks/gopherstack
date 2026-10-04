package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_CreateSnapshotLocation(t *testing.T) {
	t.Parallel()

	const localZone = "us-east-1-bos-1a"

	tests := []struct {
		name     string
		az       string
		location types.SnapshotLocationEnum
		wantErr  string
	}{
		{"regional_default", "us-east-1a", "", ""},
		{"regional_explicit", "us-east-1a", types.SnapshotLocationEnumRegional, ""},
		{"local_in_local_zone", localZone, types.SnapshotLocationEnumLocal, ""},
		{"local_outside_local_zone", "us-east-1a", types.SnapshotLocationEnumLocal, "InvalidParameterValue"},
		{"unknown_location", "us-east-1a", types.SnapshotLocationEnum("zonal"), "InvalidParameterValue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newMiscClient(t)

			vol, err := b.CreateVolume(tt.az, "gp3", 8, "")
			require.NoError(t, err)

			insts, err := b.RunInstances("ami-123", "t3.micro", "", 1)
			require.NoError(t, err)
			_, err = b.AttachVolume(vol.ID, insts[0].ID, "/dev/sdf")
			require.NoError(t, err)

			single, singleErr := client.CreateSnapshot(t.Context(), &ec2sdk.CreateSnapshotInput{
				VolumeId: aws.String(vol.ID), Location: tt.location,
			})
			multi, multiErr := client.CreateSnapshots(t.Context(), &ec2sdk.CreateSnapshotsInput{
				InstanceSpecification: &types.InstanceSpecification{InstanceId: aws.String(insts[0].ID)},
				Location:              tt.location,
			})

			if tt.wantErr != "" {
				require.ErrorContains(t, singleErr, tt.wantErr)
				require.ErrorContains(t, multiErr, tt.wantErr)

				return
			}

			require.NoError(t, singleErr)
			assert.Equal(t, vol.ID, aws.ToString(single.VolumeId))

			if tt.az == localZone {
				// The instance sits in a regional AZ, so only the volume-level call may use local.
				require.ErrorContains(t, multiErr, "InvalidParameterValue")

				return
			}

			require.NoError(t, multiErr)
			assert.Len(t, multi.Snapshots, 1)
		})
	}
}

func TestRealClient_CreateReplaceRootVolumeTaskInitializationRate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rate    *int64
		wantErr string
	}{
		{"absent", nil, ""},
		{"lower_bound", aws.Int64(100), ""},
		{"upper_bound", aws.Int64(300), ""},
		{"below_range", aws.Int64(99), "InvalidParameterValue"},
		{"above_range", aws.Int64(301), "InvalidParameterValue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newMiscClient(t)

			insts, err := b.RunInstances("ami-123", "t3.micro", "", 1)
			require.NoError(t, err)

			_, err = client.CreateReplaceRootVolumeTask(t.Context(), &ec2sdk.CreateReplaceRootVolumeTaskInput{
				InstanceId:               aws.String(insts[0].ID),
				VolumeInitializationRate: tt.rate,
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestRealClient_GetManagedPrefixListEntriesTargetVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target  *int64
		name    string
		wantErr string
		want    []string
	}{
		{nil, "current_by_default", "", []string{"10.0.0.0/16", "10.2.0.0/16"}},
		{aws.Int64(1), "version_one", "", []string{"10.0.0.0/16"}},
		{aws.Int64(2), "version_two", "", []string{"10.0.0.0/16", "10.1.0.0/16"}},
		{aws.Int64(3), "current_version_explicit", "", []string{"10.0.0.0/16", "10.2.0.0/16"}},
		{aws.Int64(9), "unknown_version", "InvalidParameterValue", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newMiscClient(t)
			ctx := t.Context()

			created, err := client.CreateManagedPrefixList(ctx, &ec2sdk.CreateManagedPrefixListInput{
				PrefixListName: aws.String("pl"), AddressFamily: aws.String("IPv4"), MaxEntries: aws.Int32(10),
				Entries: []types.AddPrefixListEntry{{Cidr: aws.String("10.0.0.0/16")}},
			})
			require.NoError(t, err)

			plID := created.PrefixList.PrefixListId

			_, err = client.ModifyManagedPrefixList(ctx, &ec2sdk.ModifyManagedPrefixListInput{
				PrefixListId: plID, CurrentVersion: aws.Int64(1),
				AddEntries: []types.AddPrefixListEntry{{Cidr: aws.String("10.1.0.0/16")}},
			})
			require.NoError(t, err)

			_, err = client.ModifyManagedPrefixList(ctx, &ec2sdk.ModifyManagedPrefixListInput{
				PrefixListId: plID, CurrentVersion: aws.Int64(2),
				AddEntries:    []types.AddPrefixListEntry{{Cidr: aws.String("10.2.0.0/16")}},
				RemoveEntries: []types.RemovePrefixListEntry{{Cidr: aws.String("10.1.0.0/16")}},
			})
			require.NoError(t, err)

			out, err := client.GetManagedPrefixListEntries(ctx, &ec2sdk.GetManagedPrefixListEntriesInput{
				PrefixListId: plID, TargetVersion: tt.target,
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			got := make([]string, 0, len(out.Entries))
			for _, e := range out.Entries {
				got = append(got, aws.ToString(e.Cidr))
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestRealClient_RestoreManagedPrefixListVersionRestoresEntries(t *testing.T) {
	t.Parallel()

	_, client := newMiscClient(t)
	ctx := t.Context()

	created, err := client.CreateManagedPrefixList(ctx, &ec2sdk.CreateManagedPrefixListInput{
		PrefixListName: aws.String("pl"), AddressFamily: aws.String("IPv4"), MaxEntries: aws.Int32(10),
		Entries: []types.AddPrefixListEntry{{Cidr: aws.String("10.0.0.0/16")}},
	})
	require.NoError(t, err)

	plID := created.PrefixList.PrefixListId

	_, err = client.ModifyManagedPrefixList(ctx, &ec2sdk.ModifyManagedPrefixListInput{
		PrefixListId: plID, AddEntries: []types.AddPrefixListEntry{{Cidr: aws.String("10.1.0.0/16")}},
	})
	require.NoError(t, err)

	restored, err := client.RestoreManagedPrefixListVersion(ctx, &ec2sdk.RestoreManagedPrefixListVersionInput{
		PrefixListId: plID, PreviousVersion: aws.Int64(1), CurrentVersion: aws.Int64(2),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3), aws.ToInt64(restored.PrefixList.Version))

	out, err := client.GetManagedPrefixListEntries(ctx, &ec2sdk.GetManagedPrefixListEntriesInput{PrefixListId: plID})
	require.NoError(t, err)
	require.Len(t, out.Entries, 1)
	assert.Equal(t, "10.0.0.0/16", aws.ToString(out.Entries[0].Cidr))
}

func TestRealClient_CreateImageSnapshotLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		location types.SnapshotLocationEnum
		wantErr  string
	}{
		{"default", "", ""},
		{"regional", types.SnapshotLocationEnumRegional, ""},
		{"local_outside_local_zone", types.SnapshotLocationEnumLocal, "InvalidParameterValue"},
		{"unknown", types.SnapshotLocationEnum("zonal"), "InvalidParameterValue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newMiscClient(t)

			insts, err := b.RunInstances("ami-123", "t3.micro", "", 1)
			require.NoError(t, err)

			out, err := client.CreateImage(t.Context(), &ec2sdk.CreateImageInput{
				InstanceId:       aws.String(insts[0].ID),
				Name:             aws.String("img-" + tt.name),
				SnapshotLocation: tt.location,
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				images, descErr := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{
					Owners: []string{tailAcct},
				})
				require.NoError(t, descErr)
				assert.Empty(t, images.Images, "a rejected CreateImage must not register an AMI")

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(out.ImageId))

			images, descErr := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{
				ImageIds: []string{aws.ToString(out.ImageId)},
			})
			require.NoError(t, descErr)
			assert.Len(t, images.Images, 1)

			owned, ownedErr := client.DescribeImages(
				t.Context(),
				&ec2sdk.DescribeImagesInput{Owners: []string{tailAcct}},
			)
			require.NoError(t, ownedErr)
			assert.Len(t, owned.Images, 1)
		})
	}
}
