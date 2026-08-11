package mgn_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mgnsdk "github.com/aws/aws-sdk-go-v2/service/mgn"
	"github.com/aws/aws-sdk-go-v2/service/mgn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateOpsWithTags_RoundTrip drives every mgn Create* op whose real
// Input struct accepts Tags (mgn@v1.48.4: api_op_CreateApplication.go,
// api_op_CreateConnector.go, api_op_CreateLaunchConfigurationTemplate.go,
// api_op_CreateNetworkMigrationDefinition.go,
// api_op_CreateReplicationConfigurationTemplate.go, api_op_CreateWave.go,
// all `Tags map[string]string`) through the real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *mgnsdk.Client) string
		name  string
	}{
		{
			name: "application",
			setup: func(t *testing.T, client *mgnsdk.Client) string {
				t.Helper()
				out, err := client.CreateApplication(t.Context(), &mgnsdk.CreateApplicationInput{
					Name: aws.String("tagged-application"),
					Tags: map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
		{
			name: "wave",
			setup: func(t *testing.T, client *mgnsdk.Client) string {
				t.Helper()
				out, err := client.CreateWave(t.Context(), &mgnsdk.CreateWaveInput{
					Name: aws.String("tagged-wave"),
					Tags: map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
		{
			name: "connector",
			setup: func(t *testing.T, client *mgnsdk.Client) string {
				t.Helper()
				out, err := client.CreateConnector(t.Context(), &mgnsdk.CreateConnectorInput{
					Name:          aws.String("tagged-connector"),
					SsmInstanceID: aws.String("mi-1234567890abcdef0"),
					Tags:          map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
		{
			name: "launch configuration template",
			setup: func(t *testing.T, client *mgnsdk.Client) string {
				t.Helper()
				out, err := client.CreateLaunchConfigurationTemplate(
					t.Context(),
					&mgnsdk.CreateLaunchConfigurationTemplateInput{
						Tags: map[string]string{"env": "prod"},
					},
				)
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
		{
			name: "replication configuration template",
			setup: func(t *testing.T, client *mgnsdk.Client) string {
				t.Helper()
				out, err := client.CreateReplicationConfigurationTemplate(
					t.Context(),
					&mgnsdk.CreateReplicationConfigurationTemplateInput{
						AssociateDefaultSecurityGroup:       aws.Bool(true),
						BandwidthThrottling:                 100,
						CreatePublicIP:                      aws.Bool(false),
						DataPlaneRouting:                    types.ReplicationConfigurationDataPlaneRoutingPrivateIp,
						DefaultLargeStagingDiskType:         types.ReplicationConfigurationDefaultLargeStagingDiskTypeGp3,
						EbsEncryption:                       types.ReplicationConfigurationEbsEncryptionDefault,
						ReplicationServerInstanceType:       aws.String("t3.small"),
						ReplicationServersSecurityGroupsIDs: []string{"sg-1"},
						StagingAreaSubnetId:                 aws.String("subnet-1"),
						StagingAreaTags:                     map[string]string{"k": "v"},
						UseDedicatedReplicationServer:       aws.Bool(false),
						Tags:                                map[string]string{"env": "prod"},
					},
				)
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
		{
			name: "network migration definition",
			setup: func(t *testing.T, client *mgnsdk.Client) string {
				t.Helper()
				out, err := client.CreateNetworkMigrationDefinition(
					t.Context(),
					&mgnsdk.CreateNetworkMigrationDefinitionInput{
						Name:          aws.String("tagged-nm-def"),
						TargetNetwork: &types.TargetNetwork{Topology: types.TargetNetworkTopologyIsolatedVpc},
						TargetS3Configuration: &types.TargetS3Configuration{
							S3Bucket:      aws.String("bucket"),
							S3BucketOwner: aws.String(rtTestAccountID),
						},
						Tags: map[string]string{"env": "prod"},
					},
				)
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)

			arn := tc.setup(t, client)
			require.NotEmpty(t, arn)

			got, err := client.ListTagsForResource(t.Context(), &mgnsdk.ListTagsForResourceInput{
				ResourceArn: aws.String(arn),
			})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "prod"}, got.Tags)
		})
	}
}
