package ec2_test

import (
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateVolume_Encrypted(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vol, err := b.CreateVolume("us-east-1a", "gp3", 100)
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

	vol, err := b.CreateVolume("us-east-1a", "gp3", 100)
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

	vol, err := b.CreateVolume("us-east-1a", "gp2", 20)
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

	vol, err := b.CreateVolume("us-east-1a", "gp3", 20)
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
