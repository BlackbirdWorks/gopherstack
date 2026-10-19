package s3files_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3filessdk "github.com/aws/aws-sdk-go-v2/service/s3files"
	"github.com/aws/aws-sdk-go-v2/service/s3files/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateMountTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode string
		wantIP   string
		wantAZ   string
		in       s3filessdk.CreateMountTargetInput
	}{
		{
			name:   "auto ip",
			in:     s3filessdk.CreateMountTargetInput{SubnetId: aws.String(testSubnet)},
			wantIP: "10.0.1.4", wantAZ: "use1-az1",
		},
		{
			name: "explicit ip",
			in: s3filessdk.CreateMountTargetInput{
				SubnetId: aws.String("subnet-bbbb"), Ipv4Address: aws.String("10.0.2.50"),
			},
			wantIP: "10.0.2.50", wantAZ: "use1-az2",
		},
		{
			name: "ip outside cidr",
			in: s3filessdk.CreateMountTargetInput{
				SubnetId: aws.String(testSubnet), Ipv4Address: aws.String("10.9.9.9"),
			},
			wantCode: "ValidationException",
		},
		{
			name:     "unknown subnet",
			in:       s3filessdk.CreateMountTargetInput{SubnetId: aws.String("subnet-zzzz")},
			wantCode: "ResourceNotFoundException",
		},
		{
			name: "ipv4 with ipv6 only",
			in: s3filessdk.CreateMountTargetInput{
				SubnetId: aws.String(
					testSubnet,
				), IpAddressType: types.IpAddressTypeIpv6Only, Ipv4Address: aws.String("10.0.1.9"),
			},
			wantCode: "ValidationException",
		},
		{
			name: "too many security groups",
			in: s3filessdk.CreateMountTargetInput{
				SubnetId:       aws.String(testSubnet),
				SecurityGroups: []string{"sg-1", "sg-2", "sg-3", "sg-4", "sg-5", "sg-6"},
			},
			wantCode: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			in := tt.in
			in.FileSystemId = aws.String(f.fileSystem(t))

			out, err := f.client.CreateMountTarget(t.Context(), &in)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			assert.Regexp(t, `^fsmt-[0-9a-f]{17}$`, aws.ToString(out.MountTargetId))
			assert.Equal(t, tt.wantIP, aws.ToString(out.Ipv4Address))
			assert.Equal(t, tt.wantAZ, aws.ToString(out.AvailabilityZoneId))
			assert.Equal(t, "vpc-1", aws.ToString(out.VpcId))
			assert.NotEmpty(t, aws.ToString(out.NetworkInterfaceId))
			assert.Equal(t, types.LifeCycleStateAvailable, out.Status)
		})
	}
}

func TestMountTargetConflictsAndAllocation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	fs1, fs2 := f.fileSystem(t), f.fileSystem(t)

	first, err := f.client.CreateMountTarget(ctx, &s3filessdk.CreateMountTargetInput{
		FileSystemId: aws.String(fs1), SubnetId: aws.String(testSubnet),
	})
	require.NoError(t, err)

	_, err = f.client.CreateMountTarget(ctx, &s3filessdk.CreateMountTargetInput{
		FileSystemId: aws.String(fs1), SubnetId: aws.String(testSubnet),
	})
	assert.Equal(t, "ConflictException", apiCode(err))

	second, err := f.client.CreateMountTarget(ctx, &s3filessdk.CreateMountTargetInput{
		FileSystemId: aws.String(fs2), SubnetId: aws.String(testSubnet),
	})
	require.NoError(t, err)
	assert.NotEqual(t, aws.ToString(first.Ipv4Address), aws.ToString(second.Ipv4Address))

	_, err = f.client.CreateMountTarget(ctx, &s3filessdk.CreateMountTargetInput{
		FileSystemId: aws.String(fs2), SubnetId: aws.String("subnet-bbbb"), Ipv4Address: aws.String("10.0.2.7"),
	})
	require.NoError(t, err)

	_, err = f.client.CreateMountTarget(ctx, &s3filessdk.CreateMountTargetInput{
		FileSystemId: aws.String(fs1), SubnetId: aws.String("subnet-bbbb"), Ipv4Address: aws.String("10.0.2.7"),
	})
	assert.Equal(t, "ConflictException", apiCode(err))
}

func TestMountTargetGetUpdateListDelete(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	fsID := f.fileSystem(t)

	mt, err := f.client.CreateMountTarget(ctx, &s3filessdk.CreateMountTargetInput{
		FileSystemId: aws.String(fsID), SubnetId: aws.String(testSubnet), SecurityGroups: []string{"sg-one"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"sg-one"}, mt.SecurityGroups)

	updated, err := f.client.UpdateMountTarget(ctx, &s3filessdk.UpdateMountTargetInput{
		MountTargetId: mt.MountTargetId, SecurityGroups: []string{"sg-two", "sg-three"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"sg-two", "sg-three"}, updated.SecurityGroups)

	got, err := f.client.GetMountTarget(ctx, &s3filessdk.GetMountTargetInput{MountTargetId: mt.MountTargetId})
	require.NoError(t, err)
	assert.Equal(t, []string{"sg-two", "sg-three"}, got.SecurityGroups)

	_, err = f.client.UpdateMountTarget(ctx, &s3filessdk.UpdateMountTargetInput{
		MountTargetId: mt.MountTargetId, SecurityGroups: []string{"bad"},
	})
	assert.Equal(t, "ValidationException", apiCode(err))

	_, err = f.client.UpdateMountTarget(ctx, &s3filessdk.UpdateMountTargetInput{
		MountTargetId: aws.String("fsmt-missing"), SecurityGroups: []string{"sg-1"},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	other := f.fileSystem(t)
	_, err = f.client.CreateMountTarget(ctx, &s3filessdk.CreateMountTargetInput{
		FileSystemId: aws.String(other), SubnetId: aws.String("subnet-bbbb"),
	})
	require.NoError(t, err)

	byFS, err := f.client.ListMountTargets(ctx, &s3filessdk.ListMountTargetsInput{FileSystemId: aws.String(fsID)})
	require.NoError(t, err)
	require.Len(t, byFS.MountTargets, 1)
	assert.Equal(t, aws.ToString(mt.MountTargetId), aws.ToString(byFS.MountTargets[0].MountTargetId))

	ap, err := f.client.CreateAccessPoint(ctx, &s3filessdk.CreateAccessPointInput{FileSystemId: aws.String(other)})
	require.NoError(t, err)

	byAP, err := f.client.ListMountTargets(ctx, &s3filessdk.ListMountTargetsInput{AccessPointId: ap.AccessPointArn})
	require.NoError(t, err)
	require.Len(t, byAP.MountTargets, 1)
	assert.Equal(t, other, aws.ToString(byAP.MountTargets[0].FileSystemId))

	all, err := f.client.ListMountTargets(ctx, &s3filessdk.ListMountTargetsInput{})
	require.NoError(t, err)
	assert.Len(t, all.MountTargets, 2)

	_, err = f.client.ListMountTargets(ctx, &s3filessdk.ListMountTargetsInput{FileSystemId: aws.String("fs-missing")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.DeleteMountTarget(ctx, &s3filessdk.DeleteMountTargetInput{MountTargetId: mt.MountTargetId})
	require.NoError(t, err)

	_, err = f.client.GetMountTarget(ctx, &s3filessdk.GetMountTargetInput{MountTargetId: mt.MountTargetId})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
