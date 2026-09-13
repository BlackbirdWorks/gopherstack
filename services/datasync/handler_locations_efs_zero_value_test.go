package datasync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	datasyncsdk "github.com/aws/aws-sdk-go-v2/service/datasync"
	"github.com/aws/aws-sdk-go-v2/service/datasync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/datasync"
)

// TestDescribeLocationEfs_Ec2ConfigSecurityGroupArns_EmptyRoundTrip_RealClient
// drives CreateLocationEfs then DescribeLocationEfs through the real SDK
// client with an empty (non-nil) SecurityGroupArns. types.Ec2Config.
// SecurityGroupArns is "This member is required" (datasync@v1.61.4
// types/types.go:115), but as a []string the client-side required check
// (validators.go:1260-1261) only rejects nil, not an empty slice -- a
// conformant client can legitimately send []string{} and still expect it to
// round-trip in DescribeLocationEfsOutput. The pre-fix handler tagged
// SecurityGroupArns `omitempty`, silently dropping it instead --
// gopherstack-mven required-output nested-domain-struct sweep.
func TestDescribeLocationEfs_Ec2ConfigSecurityGroupArns_EmptyRoundTrip_RealClient(t *testing.T) {
	t.Parallel()

	backend := datasync.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestDataSyncClient(t, datasync.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateLocationEfs(ctx, &datasyncsdk.CreateLocationEfsInput{
		Ec2Config: &types.Ec2Config{
			SubnetArn:         aws.String("arn:aws:ec2:us-east-1:000000000000:subnet/subnet-1"),
			SecurityGroupArns: []string{},
		},
		EfsFilesystemArn: aws.String("arn:aws:elasticfilesystem:us-east-1:000000000000:file-system/fs-1"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeLocationEfs(ctx, &datasyncsdk.DescribeLocationEfsInput{
		LocationArn: created.LocationArn,
	})
	require.NoError(t, err)

	require.NotNil(t, desc.Ec2Config)
	assert.NotNil(t, desc.Ec2Config.SecurityGroupArns,
		"an empty (non-nil) SecurityGroupArns must round-trip as [], not vanish")
	assert.Empty(t, desc.Ec2Config.SecurityGroupArns)
}

// TestDescribeLocationNfs_OnPremConfig_SurvivesEmptyAgentArnsUpdate_RealClient
// drives CreateLocationNfs then UpdateLocationNfs (clearing AgentArns to an
// empty-but-present list) then DescribeLocationNfs through the real SDK
// client. OnPremConfig.AgentArns is "This member is required" whenever
// OnPremConfig is present (datasync@v1.61.4 types/types.go:487), and the
// client-side required check on UpdateLocationNfsInput.OnPremConfig
// (validators.go:1414-1415, 2489-2493) only rejects nil, not an empty
// slice -- so a conformant client can legitimately clear AgentArns to [].
// The pre-fix handler gated the entire OnPremConfig object on
// len(AgentArns) > 0, so DescribeLocationNfs silently dropped OnPremConfig
// altogether afterward instead of returning it with an empty AgentArns --
// gopherstack-mven required-output nested-domain-struct sweep.
func TestDescribeLocationNfs_OnPremConfig_SurvivesEmptyAgentArnsUpdate_RealClient(t *testing.T) {
	t.Parallel()

	backend := datasync.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestDataSyncClient(t, datasync.NewHandler(backend))
	ctx := t.Context()

	agent, err := backend.CreateAgent("agent1", "", nil)
	require.NoError(t, err)
	agentArn := agent.AgentArn

	created, err := client.CreateLocationNfs(ctx, &datasyncsdk.CreateLocationNfsInput{
		ServerHostname: aws.String("nfs.example.com"),
		Subdirectory:   aws.String("/export"),
		OnPremConfig:   &types.OnPremConfig{AgentArns: []string{agentArn}},
	})
	require.NoError(t, err)

	_, err = client.UpdateLocationNfs(ctx, &datasyncsdk.UpdateLocationNfsInput{
		LocationArn:  created.LocationArn,
		OnPremConfig: &types.OnPremConfig{AgentArns: []string{}},
	})
	require.NoError(t, err)

	desc, err := client.DescribeLocationNfs(ctx, &datasyncsdk.DescribeLocationNfsInput{
		LocationArn: created.LocationArn,
	})
	require.NoError(t, err)
	require.NotNil(t, desc.OnPremConfig,
		"OnPremConfig must not vanish just because AgentArns was cleared to an empty list")
	assert.Empty(t, desc.OnPremConfig.AgentArns)
}

// TestDescribeLocationS3_S3Config_SurvivesUnrelatedUpdate_RealClient drives
// CreateLocationS3 then UpdateLocationS3 (touching only S3StorageClass, not
// S3Config) then DescribeLocationS3 through the real SDK client.
// UpdateLocationS3Input.S3Config is optional -- omitting it means "leave
// unchanged" (datasync@v1.61.4 api_op_UpdateLocationS3.go:46-52). The
// pre-fix backend unconditionally overwrote the stored S3Config with
// whatever zero-valued config the handler built for an absent field,
// silently wiping BucketAccessRoleArn (required whenever S3Config is
// present, types.go:915) on every S3Config-omitting update --
// gopherstack-mven required-output nested-domain-struct sweep.
func TestDescribeLocationS3_S3Config_SurvivesUnrelatedUpdate_RealClient(t *testing.T) {
	t.Parallel()

	backend := datasync.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestDataSyncClient(t, datasync.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateLocationS3(ctx, &datasyncsdk.CreateLocationS3Input{
		S3BucketArn: aws.String("arn:aws:s3:::example-bucket"),
		S3Config: &types.S3Config{
			BucketAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/DataSyncS3Role"),
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateLocationS3(ctx, &datasyncsdk.UpdateLocationS3Input{
		LocationArn:    created.LocationArn,
		S3StorageClass: types.S3StorageClassStandardIa,
	})
	require.NoError(t, err)

	desc, err := client.DescribeLocationS3(ctx, &datasyncsdk.DescribeLocationS3Input{
		LocationArn: created.LocationArn,
	})
	require.NoError(t, err)
	require.NotNil(t, desc.S3Config,
		"S3Config must survive an update that doesn't specify it")
	assert.Equal(t, "arn:aws:iam::000000000000:role/DataSyncS3Role", aws.ToString(desc.S3Config.BucketAccessRoleArn))
}
