package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	medialivetypes "github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createMediaLiveClient returns a MediaLive client pointed at the shared test container.
func createMediaLiveClient(t *testing.T) *medialivesdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return medialivesdk.NewFromConfig(cfg, func(o *medialivesdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_MediaLive_InputSecurityGroupLifecycle drives create→describe→list→delete of an
// input security group, asserting the whitelist CIDR round-trips.
func TestIntegration_MediaLive_InputSecurityGroupLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name string
		cidr string
	}{
		{name: "full_lifecycle", cidr: "10.0.0.0/16"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMediaLiveClient(t)

			createOut, err := client.CreateInputSecurityGroup(ctx, &medialivesdk.CreateInputSecurityGroupInput{
				WhitelistRules: []medialivetypes.InputWhitelistRuleCidr{
					{Cidr: aws.String(tt.cidr)},
				},
			})
			require.NoError(t, err, "CreateInputSecurityGroup should succeed")
			require.NotNil(t, createOut.SecurityGroup)
			sgID := aws.ToString(createOut.SecurityGroup.Id)
			require.NotEmpty(t, sgID, "security group id must be returned")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteInputSecurityGroup(cleanupCtx, &medialivesdk.DeleteInputSecurityGroupInput{
					InputSecurityGroupId: aws.String(sgID),
				})
			})

			descOut, err := client.DescribeInputSecurityGroup(ctx, &medialivesdk.DescribeInputSecurityGroupInput{
				InputSecurityGroupId: aws.String(sgID),
			})
			require.NoError(t, err, "DescribeInputSecurityGroup should succeed")
			assert.Equal(t, sgID, aws.ToString(descOut.Id))
			require.NotEmpty(t, descOut.WhitelistRules)
			assert.Equal(t, tt.cidr, aws.ToString(descOut.WhitelistRules[0].Cidr))

			listOut, err := client.ListInputSecurityGroups(ctx, &medialivesdk.ListInputSecurityGroupsInput{})
			require.NoError(t, err, "ListInputSecurityGroups should succeed")

			found := false
			for _, sg := range listOut.InputSecurityGroups {
				if aws.ToString(sg.Id) == sgID {
					found = true

					break
				}
			}

			assert.True(t, found, "created security group should appear in list")

			_, err = client.DeleteInputSecurityGroup(ctx, &medialivesdk.DeleteInputSecurityGroupInput{
				InputSecurityGroupId: aws.String(sgID),
			})
			require.NoError(t, err, "DeleteInputSecurityGroup should succeed")
		})
	}
}
