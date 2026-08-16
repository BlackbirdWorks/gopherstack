package ec2_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestCreateVolume_Encrypted(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vol, err := b.CreateVolume("us-east-1a", "gp3", 100, "")
	require.NoError(t, err)

	err = b.SetVolumeEncryption(vol.ID, true, "arn:aws:kms:us-east-1:123456789012:key/my-key")
	require.NoError(t, err)

	vols := b.DescribeVolumes([]string{vol.ID})
	require.Len(t, vols, 1)
	assert.True(t, vols[0].Encrypted)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/my-key", vols[0].KmsKeyID)
}

func TestCreateSnapshot_InheritsEncryption(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vol, err := b.CreateVolume("us-east-1a", "gp3", 100, "")
	require.NoError(t, err)

	err = b.SetVolumeEncryption(vol.ID, true, "alias/aws/ebs")
	require.NoError(t, err)

	snap, err := b.CreateSnapshot(vol.ID, "test snapshot")
	require.NoError(t, err)
	assert.True(t, snap.Encrypted, "snapshot should inherit volume encryption")
	assert.Equal(t, "alias/aws/ebs", snap.KmsKeyID)
}

func TestSetVolumeEncryption_DefaultKMSKey(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vol, err := b.CreateVolume("us-east-1a", "gp2", 20, "")
	require.NoError(t, err)

	// Encrypted=true with no explicit key → default AWS-managed key.
	err = b.SetVolumeEncryption(vol.ID, true, "")
	require.NoError(t, err)

	vols := b.DescribeVolumes([]string{vol.ID})
	require.Len(t, vols, 1)
	assert.True(t, vols[0].Encrypted)
	assert.Equal(t, "alias/aws/ebs", vols[0].KmsKeyID)
}

// ---- Gap 14: ModifyInstanceAttribute persistence ----

func TestSetVolumePerformance(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vol, err := b.CreateVolume("us-east-1a", "gp3", 20, "")
	require.NoError(t, err)

	err = b.SetVolumePerformance(vol.ID, 5000, 300)
	require.NoError(t, err)

	vols := b.DescribeVolumes([]string{vol.ID})
	require.Len(t, vols, 1)
	assert.Equal(t, 5000, vols[0].Iops)
	assert.Equal(t, 300, vols[0].Throughput)
}

// newTestHandler creates a fresh Handler with an InMemoryBackend.

func TestCreateVolume_IOPS_Throughput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		body           string
		wantErrContain string
		wantIops       string
		wantThroughput string
		wantErr        bool
	}{
		{
			name:           "gp3_defaults",
			body:           "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a&Size=20&VolumeType=gp3",
			wantIops:       "3000",
			wantThroughput: "125",
		},
		{
			name: "gp3_custom_iops_throughput",
			body: "Action=CreateVolume&Version=2016-11-15" +
				"&AvailabilityZone=us-east-1a&Size=20&VolumeType=gp3&Iops=6000&Throughput=500",
			wantIops:       "6000",
			wantThroughput: "500",
		},
		{
			name:     "gp2_iops_derived_from_size",
			body:     "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a&Size=100&VolumeType=gp2",
			wantIops: "300",
		},
		{
			name:     "gp2_iops_minimum_100",
			body:     "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a&Size=8&VolumeType=gp2",
			wantIops: "100",
		},
		{
			name:           "io1_requires_iops",
			body:           "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a&Size=100&VolumeType=io1",
			wantErr:        true,
			wantErrContain: "InvalidParameterValue",
		},
		{
			name:     "io1_with_iops",
			body:     "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a&Size=100&VolumeType=io1&Iops=5000",
			wantIops: "5000",
		},
		{
			name:           "io2_requires_iops",
			body:           "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a&Size=100&VolumeType=io2",
			wantErr:        true,
			wantErrContain: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals, err := url.ParseQuery(tt.body)
			require.NoError(t, err)

			resp, dispatchErr := dispatchHandler(h, vals)
			if tt.wantErr {
				require.Error(t, dispatchErr)
				if tt.wantErrContain != "" {
					assert.Contains(t, dispatchErr.Error(), tt.wantErrContain)
				}

				return
			}

			require.NoError(t, dispatchErr)

			if tt.wantIops != "" {
				assert.Contains(t, resp, "<iops>"+tt.wantIops+"</iops>",
					"CreateVolume response should include iops")
			}

			if tt.wantThroughput != "" {
				assert.Contains(t, resp, "<throughput>"+tt.wantThroughput+"</throughput>",
					"CreateVolume response should include throughput")
			}
		})
	}
}

func TestDescribeVolumes_IOPS_Throughput(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create a gp3 volume and verify DescribeVolumes includes IOPS/throughput.
	createVals, err := url.ParseQuery(
		"Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a&Size=20&VolumeType=gp3&Iops=4000&Throughput=200",
	)
	require.NoError(t, err)

	createResp, err := dispatchHandler(h, createVals)
	require.NoError(t, err)

	volID := accuracyExtractXMLValue(createResp, "volumeId")
	require.NotEmpty(t, volID)

	descVals, err := url.ParseQuery(
		"Action=DescribeVolumes&Version=2016-11-15&VolumeId.1=" + volID,
	)
	require.NoError(t, err)

	descResp, err := dispatchHandler(h, descVals)
	require.NoError(t, err)
	assert.Contains(t, descResp, "<iops>4000</iops>", "DescribeVolumes should return iops")
	assert.Contains(t, descResp, "<throughput>200</throughput>", "DescribeVolumes should return throughput")
}

// TestHTTP_CreateVolume_FromSnapshot covers the real AWS CreateVolumeInput.SnapshotId
// wire parameter end to end: previously handleCreateVolume never read SnapshotId at
// all, so "restore a volume from a snapshot" silently created an empty, disconnected
// volume with no error and no size inheritance.
func TestHTTP_CreateVolume_FromSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		snapshotIDFn   func(snapshotID string) string
		wantErrContain string
		wantSnapshotID bool
		wantErr        bool
	}{
		{
			name:           "restores_from_snapshot_id",
			snapshotIDFn:   func(id string) string { return id },
			wantSnapshotID: true,
		},
		{
			name:           "unknown_snapshot_rejected",
			snapshotIDFn:   func(string) string { return "snap-doesnotexist" },
			wantErr:        true,
			wantErrContain: "InvalidSnapshotID.NotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			snapVals, err := url.ParseQuery(
				"Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a&Size=20&VolumeType=gp2",
			)
			require.NoError(t, err)

			volResp, err := dispatchHandler(h, snapVals)
			require.NoError(t, err)
			srcVolID := accuracyExtractXMLValue(volResp, "volumeId")
			require.NotEmpty(t, srcVolID)

			snapResp, err := dispatchHandler(h, url.Values{
				"Action":   []string{"CreateSnapshot"},
				"Version":  []string{"2016-11-15"},
				"VolumeId": []string{srcVolID},
			})
			require.NoError(t, err)
			snapID := accuracyExtractXMLValue(snapResp, "snapshotId")
			require.NotEmpty(t, snapID)

			restoreResp, err := dispatchHandler(h, url.Values{
				"Action":     []string{"CreateVolume"},
				"Version":    []string{"2016-11-15"},
				"SnapshotId": []string{tt.snapshotIDFn(snapID)},
			})

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrContain != "" {
					assert.Contains(t, err.Error(), tt.wantErrContain)
				}

				return
			}

			require.NoError(t, err)

			if tt.wantSnapshotID {
				assert.Contains(t, restoreResp, "<snapshotId>"+snapID+"</snapshotId>",
					"CreateVolume response should echo the source snapshotId")
				// Size must default to the source volume's size (20 GiB), matching
				// the snapshot's VolumeSize, not the mock's unrelated 8 GiB fallback.
				assert.Contains(t, restoreResp, "<size>20</size>")
			}
		})
	}
}

// TestCreateVolume_GP3Coupling covers the AWS gp3 iops/throughput coupling
// validation on CreateVolume.
func TestCreateVolume_GP3Coupling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{
			name: "gp3_defaults_ok",
			body: "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a&Size=20&VolumeType=gp3",
		},
		{
			name: "gp3_custom_within_bounds_ok",
			body: "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a" +
				"&Size=100&VolumeType=gp3&Iops=6000&Throughput=500",
		},
		{
			name: "gp3_iops_above_max_rejected",
			body: "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a" +
				"&Size=100&VolumeType=gp3&Iops=20000&Throughput=125",
			wantErr: true,
		},
		{
			name: "gp3_iops_below_min_rejected",
			body: "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a" +
				"&Size=100&VolumeType=gp3&Iops=1000&Throughput=125",
			wantErr: true,
		},
		{
			name: "gp3_throughput_above_max_rejected",
			body: "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a" +
				"&Size=100&VolumeType=gp3&Iops=16000&Throughput=2000",
			wantErr: true,
		},
		{
			name: "gp3_throughput_to_iops_ratio_violation_rejected",
			// throughput 1000 * 4 = 4000 > iops 3000 -> violates 0.25 MiB/s per IOPS.
			body: "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a" +
				"&Size=100&VolumeType=gp3&Iops=3000&Throughput=1000",
			wantErr: true,
		},
		{
			name: "gp3_iops_to_size_ratio_violation_rejected",
			// size 10 GiB, iops 8000 > 10*500=5000 and above baseline -> violation.
			body: "Action=CreateVolume&Version=2016-11-15&AvailabilityZone=us-east-1a" +
				"&Size=10&VolumeType=gp3&Iops=8000&Throughput=125",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals, err := url.ParseQuery(tt.body)
			require.NoError(t, err)

			resp, dispErr := dispatchHandler(h, vals)

			if tt.wantErr {
				require.Error(t, dispErr)
				assert.ErrorIs(t, dispErr, ec2.ErrInvalidParameter)

				return
			}

			require.NoError(t, dispErr)
			assert.Contains(t, resp, "CreateVolumeResponse")
		})
	}
}

// TestPagination_ForgedTokenRejected asserts that a forged/tampered NextToken is
// rejected with InvalidPaginationToken across the three opaque-token describe
// operations, rather than silently re-paging from offset 0.

func TestVolumeOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		az      string
		volType string
		size    int
		wantErr bool
	}{
		{
			name:    "create_volume_defaults",
			op:      "create",
			wantErr: false,
		},
		{
			name:    "create_volume_custom",
			op:      "create",
			az:      "us-east-1b",
			volType: "gp3",
			size:    100,
			wantErr: false,
		},
		{
			name:    "describe_all",
			op:      "describe_all",
			wantErr: false,
		},
		{
			name:    "delete_volume",
			op:      "delete",
			wantErr: false,
		},
		{
			name:    "delete_nonexistent",
			op:      "delete_nonexistent",
			wantErr: true,
		},
		{
			name:    "attach_detach",
			op:      "attach_detach",
			wantErr: false,
		},
		{
			name:    "delete_attached_volume",
			op:      "delete_attached",
			wantErr: true,
		},
		{
			name:    "attach_nonexistent_volume",
			op:      "attach_nonexistent_vol",
			wantErr: true,
		},
		{
			name:    "attach_nonexistent_instance",
			op:      "attach_nonexistent_inst",
			wantErr: true,
		},
		{
			name:    "detach_not_attached",
			op:      "detach_not_attached",
			wantErr: true,
		},
		{
			name:    "create_from_snapshot",
			op:      "create_from_snapshot",
			wantErr: false,
		},
		{
			name:    "create_from_snapshot_undersized",
			op:      "create_from_snapshot_undersized",
			wantErr: true,
		},
		{
			name:    "create_from_snapshot_not_found",
			op:      "create_from_snapshot_not_found",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			switch tt.op {
			case "create":
				vol, err := b.CreateVolume(tt.az, tt.volType, tt.size, "")
				require.NoError(t, err)
				assert.NotEmpty(t, vol.ID)
				assert.Equal(t, "available", vol.State)
				if tt.az != "" {
					assert.Equal(t, tt.az, vol.AZ)
				}
				if tt.volType != "" {
					assert.Equal(t, tt.volType, vol.VolumeType)
				}
				if tt.size > 0 {
					assert.Equal(t, tt.size, vol.Size)
				}

			case "describe_all":
				_, err := b.CreateVolume("", "", 0, "")
				require.NoError(t, err)
				vols := b.DescribeVolumes(nil)
				assert.NotEmpty(t, vols)

			case "delete":
				vol, err := b.CreateVolume("", "", 0, "")
				require.NoError(t, err)
				err = b.DeleteVolume(vol.ID)
				require.NoError(t, err)
				vols := b.DescribeVolumes([]string{vol.ID})
				assert.Empty(t, vols)

			case "delete_nonexistent":
				err := b.DeleteVolume("vol-nonexistent")
				require.Error(t, err)

			case "attach_detach":
				vol, err := b.CreateVolume("", "", 0, "")
				require.NoError(t, err)
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, err)
				att, err := b.AttachVolume(vol.ID, instances[0].ID, "/dev/sdf")
				require.NoError(t, err)
				assert.Equal(t, "attached", att.State)
				vols := b.DescribeVolumes([]string{vol.ID})
				require.Len(t, vols, 1)
				assert.Equal(t, "in-use", vols[0].State)
				detatt, err := b.DetachVolume(vol.ID, false)
				require.NoError(t, err)
				assert.Equal(t, "detached", detatt.State)

			case "delete_attached":
				vol, err := b.CreateVolume("", "", 0, "")
				require.NoError(t, err)
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, err)
				_, err = b.AttachVolume(vol.ID, instances[0].ID, "/dev/sdf")
				require.NoError(t, err)
				err = b.DeleteVolume(vol.ID)
				require.Error(t, err)

			case "attach_nonexistent_vol":
				instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
				require.NoError(t, err)
				_, err = b.AttachVolume("vol-nonexistent", instances[0].ID, "/dev/sdf")
				require.Error(t, err)

			case "attach_nonexistent_inst":
				vol, err := b.CreateVolume("", "", 0, "")
				require.NoError(t, err)
				_, err = b.AttachVolume(vol.ID, "i-nonexistent", "/dev/sdf")
				require.Error(t, err)

			case "detach_not_attached":
				vol, err := b.CreateVolume("", "", 0, "")
				require.NoError(t, err)
				_, err = b.DetachVolume(vol.ID, false)
				require.Error(t, err)

			case "create_from_snapshot":
				src, err := b.CreateVolume("us-east-1a", "gp2", 20, "")
				require.NoError(t, err)
				require.NoError(t, b.SetVolumeEncryption(src.ID, true, "alias/aws/ebs"))
				snap, err := b.CreateSnapshot(src.ID, "src for restore")
				require.NoError(t, err)

				// Size omitted (0): must default to the snapshot's VolumeSize.
				restored, err := b.CreateVolume("us-east-1a", "gp2", 0, snap.SnapshotID)
				require.NoError(t, err)
				assert.Equal(t, snap.SnapshotID, restored.SnapshotID)
				assert.Equal(t, snap.VolumeSize, restored.Size)
				assert.True(t, restored.Encrypted, "volume restored from an encrypted snapshot must be encrypted")
				assert.Equal(t, "alias/aws/ebs", restored.KmsKeyID)

				// Larger explicit size is allowed and must be honored, not clamped.
				bigger, err := b.CreateVolume("us-east-1a", "gp2", snap.VolumeSize+50, snap.SnapshotID)
				require.NoError(t, err)
				assert.Equal(t, snap.VolumeSize+50, bigger.Size)

			case "create_from_snapshot_undersized":
				src, err := b.CreateVolume("us-east-1a", "gp2", 20, "")
				require.NoError(t, err)
				snap, err := b.CreateSnapshot(src.ID, "src for restore")
				require.NoError(t, err)

				_, err = b.CreateVolume("us-east-1a", "gp2", snap.VolumeSize-1, snap.SnapshotID)
				require.Error(t, err)
				require.ErrorIs(t, err, ec2.ErrInvalidParameter)

			case "create_from_snapshot_not_found":
				_, err := b.CreateVolume("us-east-1a", "gp2", 20, "snap-doesnotexist")
				require.Error(t, err)
				require.ErrorIs(t, err, ec2.ErrSnapshotNotFound)
			}
		})
	}
}
