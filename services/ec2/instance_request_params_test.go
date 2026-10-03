package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_StopInstancesHibernate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErr    string
		wantReason string
		hibernate  bool
		configured bool
	}{
		{"hibernation_enabled", "", "Client.UserInitiatedHibernate", true, true},
		{"plain_stop_on_enabled", "", "Client.UserInitiatedShutdown", false, true},
		{"hibernate_not_enabled", "UnsupportedHibernationConfiguration", "", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newMiscClient(t)

			runOut, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
				ImageId:            aws.String("ami-123"),
				InstanceType:       types.InstanceTypeT3Micro,
				MinCount:           aws.Int32(1),
				MaxCount:           aws.Int32(1),
				HibernationOptions: &types.HibernationOptionsRequest{Configured: aws.Bool(tt.configured)},
			})
			require.NoError(t, err)

			id := aws.ToString(runOut.Instances[0].InstanceId)

			desc, err := client.DescribeInstances(
				t.Context(),
				&ec2sdk.DescribeInstancesInput{InstanceIds: []string{id}},
			)
			require.NoError(t, err)
			require.Equal(t, tt.configured, aws.ToBool(describedInstance(desc).HibernationOptions.Configured))

			_, err = client.StopInstances(t.Context(), &ec2sdk.StopInstancesInput{
				InstanceIds: []string{id},
				Hibernate:   aws.Bool(tt.hibernate),
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			desc, err = client.DescribeInstances(t.Context(), &ec2sdk.DescribeInstancesInput{InstanceIds: []string{id}})
			require.NoError(t, err)
			assert.Equal(t, tt.wantReason, aws.ToString(describedInstance(desc).StateReason.Code))
		})
	}
}

func describedInstance(out *ec2sdk.DescribeInstancesOutput) types.Instance {
	inst := out.Reservations[0].Instances[0]
	if inst.HibernationOptions == nil {
		inst.HibernationOptions = &types.HibernationOptions{}
	}

	if inst.StateReason == nil {
		inst.StateReason = &types.StateReason{}
	}

	return inst
}

func TestRealClient_ModifyInstanceAttributeBlockDeviceMappings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		device       string
		wantErr      string
		deleteOnTerm bool
		wantVolGone  bool
	}{
		{"enable_delete_on_termination", "/dev/sdf", "", true, true},
		{"keep_volume", "/dev/sdf", "", false, false},
		{"unknown_device", "/dev/sdz", "InvalidParameterValue", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newMiscClient(t)

			insts, err := b.RunInstances("ami-123", "t3.micro", "", 1)
			require.NoError(t, err)

			vol, err := b.CreateVolume("us-east-1a", "gp3", 8, "")
			require.NoError(t, err)
			_, err = b.AttachVolume(vol.ID, insts[0].ID, "/dev/sdf")
			require.NoError(t, err)

			_, err = client.ModifyInstanceAttribute(t.Context(), &ec2sdk.ModifyInstanceAttributeInput{
				InstanceId: aws.String(insts[0].ID),
				BlockDeviceMappings: []types.InstanceBlockDeviceMappingSpecification{
					{
						DeviceName: aws.String(tt.device),
						Ebs: &types.EbsInstanceBlockDeviceSpecification{
							DeleteOnTermination: aws.Bool(tt.deleteOnTerm),
						},
					},
				},
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			attr, err := client.DescribeInstanceAttribute(t.Context(), &ec2sdk.DescribeInstanceAttributeInput{
				InstanceId: aws.String(insts[0].ID),
				Attribute:  types.InstanceAttributeNameBlockDeviceMapping,
			})
			require.NoError(t, err)
			require.Len(t, attr.BlockDeviceMappings, 1)
			assert.Equal(t, tt.deleteOnTerm, aws.ToBool(attr.BlockDeviceMappings[0].Ebs.DeleteOnTermination))

			desc, err := client.DescribeInstances(t.Context(), &ec2sdk.DescribeInstancesInput{
				InstanceIds: []string{insts[0].ID},
			})
			require.NoError(t, err)

			bdm := desc.Reservations[0].Instances[0].BlockDeviceMappings
			require.Len(t, bdm, 1)
			assert.Equal(t, vol.ID, aws.ToString(bdm[0].Ebs.VolumeId))
			assert.Equal(t, tt.deleteOnTerm, aws.ToBool(bdm[0].Ebs.DeleteOnTermination))

			_, err = client.TerminateInstances(t.Context(), &ec2sdk.TerminateInstancesInput{
				InstanceIds: []string{insts[0].ID},
			})
			require.NoError(t, err)

			vols := b.DescribeVolumes([]string{})
			assert.Equal(t, !tt.wantVolGone, len(vols) == 1)
		})
	}
}

func registerMappedImage(t *testing.T, client *ec2sdk.Client, name, snapshotID string) string {
	t.Helper()

	out, err := client.RegisterImage(t.Context(), &ec2sdk.RegisterImageInput{
		Name:           aws.String(name),
		RootDeviceName: aws.String("/dev/xvda"),
		BlockDeviceMappings: []types.BlockDeviceMapping{{
			DeviceName: aws.String("/dev/xvda"),
			Ebs:        &types.EbsBlockDevice{SnapshotId: aws.String(snapshotID)},
		}},
	})
	require.NoError(t, err)

	return aws.ToString(out.ImageId)
}

func TestRealClient_CopyImageEncryptsSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		kmsKey    string
		wantErr   string
		wantKey   string
		encrypted bool
	}{
		{"encrypted_with_key", "alias/my-key", "", "alias/my-key", true},
		{"encrypted_default_key", "", "", "alias/aws/ebs", true},
		{"plain_copy", "", "", "", false},
		{"key_without_encrypted", "alias/my-key", "InvalidParameterCombination", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newMiscClient(t)

			vol, err := b.CreateVolume("us-east-1a", "gp3", 8, "")
			require.NoError(t, err)
			snap, err := b.CreateSnapshot(vol.ID, "root")
			require.NoError(t, err)
			srcImage := registerMappedImage(t, client, "src", snap.SnapshotID)

			in := &ec2sdk.CopyImageInput{
				Name:          aws.String("copy"),
				SourceImageId: aws.String(srcImage),
				SourceRegion:  aws.String("us-east-1"),
				Encrypted:     aws.Bool(tt.encrypted),
			}
			if tt.kmsKey != "" {
				in.KmsKeyId = aws.String(tt.kmsKey)
			}

			out, err := client.CopyImage(t.Context(), in)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			images, err := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{
				ImageIds: []string{aws.ToString(out.ImageId)},
			})
			require.NoError(t, err)
			require.Len(t, images.Images, 1)
			require.Len(t, images.Images[0].BlockDeviceMappings, 1)

			ebs := images.Images[0].BlockDeviceMappings[0].Ebs
			assert.NotEqual(t, snap.SnapshotID, aws.ToString(ebs.SnapshotId))
			assert.Equal(t, tt.encrypted, aws.ToBool(ebs.Encrypted))

			snaps, err := client.DescribeSnapshots(t.Context(), &ec2sdk.DescribeSnapshotsInput{
				SnapshotIds: []string{aws.ToString(ebs.SnapshotId)},
			})
			require.NoError(t, err)
			require.Len(t, snaps.Snapshots, 1)
			assert.Equal(t, tt.encrypted, aws.ToBool(snaps.Snapshots[0].Encrypted))
			assert.Equal(t, tt.wantKey, aws.ToString(snaps.Snapshots[0].KmsKeyId))
		})
	}
}

func TestRealClient_DeregisterImageDeleteAssociatedSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantCode   string
		deleteSnap bool
		shared     bool
		wantGone   bool
	}{
		{"deleted", "success", true, false, true},
		{"kept_by_default", "", false, false, false},
		{"skipped_when_shared", "skipped", true, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newMiscClient(t)

			vol, err := b.CreateVolume("us-east-1a", "gp3", 8, "")
			require.NoError(t, err)
			snap, err := b.CreateSnapshot(vol.ID, "root")
			require.NoError(t, err)

			imageID := registerMappedImage(t, client, "img", snap.SnapshotID)
			if tt.shared {
				registerMappedImage(t, client, "other", snap.SnapshotID)
			}

			out, err := client.DeregisterImage(t.Context(), &ec2sdk.DeregisterImageInput{
				ImageId:                   aws.String(imageID),
				DeleteAssociatedSnapshots: aws.Bool(tt.deleteSnap),
			})
			require.NoError(t, err)

			if tt.wantCode == "" {
				assert.Empty(t, out.DeleteSnapshotResults)
			} else {
				require.Len(t, out.DeleteSnapshotResults, 1)
				assert.Equal(t, snap.SnapshotID, aws.ToString(out.DeleteSnapshotResults[0].SnapshotId))
				assert.Equal(t, tt.wantCode, string(out.DeleteSnapshotResults[0].ReturnCode))
			}

			_, descErr := client.DescribeSnapshots(t.Context(), &ec2sdk.DescribeSnapshotsInput{
				SnapshotIds: []string{snap.SnapshotID},
			})
			assert.Equal(t, tt.wantGone, descErr != nil)
		})
	}
}
