package app

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3files"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const s3FilesTestBucket = "arn:aws:s3:::s3files-wiring"

type s3FilesEnv struct {
	fx      *sfnFixture
	ec2     *ec2.Client
	files   *s3files.Client
	subnet  string
	vpc     string
	roleARN string
}

func newS3FilesEnv(t *testing.T) *s3FilesEnv {
	t.Helper()

	fx := newSFNFixture(t)
	ctx := t.Context()
	env := &s3FilesEnv{fx: fx, ec2: ec2.NewFromConfig(fx.cfg), files: s3files.NewFromConfig(fx.cfg)}

	_, err := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true }).
		CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("s3files-wiring")})
	require.NoError(t, err)

	vpc, err := env.ec2.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.7.0.0/16")})
	require.NoError(t, err)

	env.vpc = aws.ToString(vpc.Vpc.VpcId)

	sn, err := env.ec2.CreateSubnet(ctx, &ec2.CreateSubnetInput{
		VpcId: vpc.Vpc.VpcId, CidrBlock: aws.String("10.7.1.0/24"),
	})
	require.NoError(t, err)

	env.subnet = aws.ToString(sn.Subnet.SubnetId)

	role, err := iam.NewFromConfig(fx.cfg).CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 aws.String("s3files-role"),
		AssumeRolePolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	env.roleARN = aws.ToString(role.Role.Arn)

	return env
}

func (e *s3FilesEnv) fileSystem(t *testing.T, keyID *string) string {
	t.Helper()

	out, err := e.files.CreateFileSystem(t.Context(), &s3files.CreateFileSystemInput{
		Bucket: aws.String(s3FilesTestBucket), RoleArn: aws.String(e.roleARN), KmsKeyId: keyID,
	})
	require.NoError(t, err)

	return aws.ToString(out.FileSystemId)
}

func (e *s3FilesEnv) eni(t *testing.T, id string) []ec2types.NetworkInterface {
	t.Helper()

	out, err := e.ec2.DescribeNetworkInterfaces(t.Context(), &ec2.DescribeNetworkInterfacesInput{
		NetworkInterfaceIds: []string{id},
	})
	if err != nil {
		return nil
	}

	return out.NetworkInterfaces
}

func s3FilesCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

func TestS3FilesMountTargetNetworkWiring(t *testing.T) {
	t.Parallel()

	env := newS3FilesEnv(t)
	ctx := t.Context()

	otherVPC, err := env.ec2.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.8.0.0/16")})
	require.NoError(t, err)

	foreign, err := env.ec2.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
		GroupName: aws.String("foreign"), Description: aws.String("d"), VpcId: otherVPC.Vpc.VpcId,
	})
	require.NoError(t, err)

	own, err := env.ec2.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
		GroupName: aws.String("own"), Description: aws.String("d"), VpcId: aws.String(env.vpc),
	})
	require.NoError(t, err)

	tests := []struct {
		name     string
		wantCode string
		sgs      []string
		wantOwn  bool
	}{
		{name: "default group"},
		{name: "own group", sgs: []string{aws.ToString(own.GroupId)}, wantOwn: true},
		{name: "unknown group", sgs: []string{"sg-0000000000000000f"}, wantCode: "ResourceNotFoundException"},
		{name: "foreign vpc group", sgs: []string{aws.ToString(foreign.GroupId)}, wantCode: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fsID := env.fileSystem(t, nil)

			mt, mtErr := env.files.CreateMountTarget(ctx, &s3files.CreateMountTargetInput{
				FileSystemId: aws.String(fsID), SubnetId: aws.String(env.subnet), SecurityGroups: tt.sgs,
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, s3FilesCode(mtErr))

				return
			}

			require.NoError(t, mtErr)

			enis := env.eni(t, aws.ToString(mt.NetworkInterfaceId))
			require.Len(t, enis, 1)
			assert.Equal(t, env.subnet, aws.ToString(enis[0].SubnetId))
			assert.Contains(t, aws.ToString(enis[0].Description), aws.ToString(mt.MountTargetId))
			assert.Equal(t, aws.ToString(mt.Ipv4Address), aws.ToString(enis[0].PrivateIpAddress))
			require.Len(t, mt.SecurityGroups, 1)
			require.Len(t, enis[0].Groups, 1)
			assert.Equal(t, mt.SecurityGroups[0], aws.ToString(enis[0].Groups[0].GroupId))

			if tt.wantOwn {
				assert.Equal(t, tt.sgs[0], mt.SecurityGroups[0])
			} else {
				dsc, dErr := env.ec2.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{
					GroupIds: mt.SecurityGroups,
				})
				require.NoError(t, dErr)
				assert.Equal(t, "default", aws.ToString(dsc.SecurityGroups[0].GroupName))
				assert.Equal(t, env.vpc, aws.ToString(dsc.SecurityGroups[0].VpcId))
			}

			_, upErr := env.files.UpdateMountTarget(ctx, &s3files.UpdateMountTargetInput{
				MountTargetId: mt.MountTargetId, SecurityGroups: []string{aws.ToString(foreign.GroupId)},
			})
			assert.Equal(t, "ValidationException", s3FilesCode(upErr))

			_, delErr := env.files.DeleteMountTarget(ctx, &s3files.DeleteMountTargetInput{
				MountTargetId: mt.MountTargetId,
			})
			require.NoError(t, delErr)
			assert.Empty(t, env.eni(t, aws.ToString(mt.NetworkInterfaceId)))
		})
	}
}

func TestS3FilesForceDeleteReleasesENIs(t *testing.T) {
	t.Parallel()

	env := newS3FilesEnv(t)
	ctx := t.Context()
	fsID := env.fileSystem(t, nil)

	mt, err := env.files.CreateMountTarget(ctx, &s3files.CreateMountTargetInput{
		FileSystemId: aws.String(fsID), SubnetId: aws.String(env.subnet),
	})
	require.NoError(t, err)
	require.Len(t, env.eni(t, aws.ToString(mt.NetworkInterfaceId)), 1)

	_, err = env.files.DeleteFileSystem(ctx, &s3files.DeleteFileSystemInput{FileSystemId: aws.String(fsID)})
	assert.Equal(t, "ConflictException", s3FilesCode(err))

	_, err = env.files.DeleteFileSystem(ctx, &s3files.DeleteFileSystemInput{
		FileSystemId: aws.String(fsID), ForceDelete: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Empty(t, env.eni(t, aws.ToString(mt.NetworkInterfaceId)))

	_, err = env.files.GetMountTarget(ctx, &s3files.GetMountTargetInput{MountTargetId: mt.MountTargetId})
	assert.Equal(t, "ResourceNotFoundException", s3FilesCode(err))
}

func TestS3FilesRoleAndKeyWiring(t *testing.T) {
	t.Parallel()

	env := newS3FilesEnv(t)
	ctx := t.Context()
	kc := kms.NewFromConfig(env.fx.cfg)

	enabled, err := kc.CreateKey(ctx, &kms.CreateKeyInput{})
	require.NoError(t, err)

	disabled, err := kc.CreateKey(ctx, &kms.CreateKeyInput{})
	require.NoError(t, err)

	_, err = kc.DisableKey(ctx, &kms.DisableKeyInput{KeyId: disabled.KeyMetadata.KeyId})
	require.NoError(t, err)

	tests := []struct {
		name     string
		role     string
		key      *string
		wantCode string
	}{
		{name: "valid key", role: env.roleARN, key: enabled.KeyMetadata.KeyId},
		{name: "no key", role: env.roleARN},
		{name: "unknown role", role: "arn:aws:iam::000000000000:role/ghost", wantCode: "ValidationException"},
		{
			name: "unknown key", role: env.roleARN,
			key: aws.String("1234abcd-12ab-34cd-56ef-1234567890ab"), wantCode: "ValidationException",
		},
		{name: "disabled key", role: env.roleARN, key: disabled.KeyMetadata.KeyId, wantCode: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, cErr := env.files.CreateFileSystem(ctx, &s3files.CreateFileSystemInput{
				Bucket: aws.String(s3FilesTestBucket), RoleArn: aws.String(tt.role), KmsKeyId: tt.key,
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, s3FilesCode(cErr))

				return
			}

			require.NoError(t, cErr)
		})
	}
}
