package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	workmailsdk "github.com/aws/aws-sdk-go-v2/service/workmail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createWorkMailClient returns a WorkMail client pointed at the shared test container.
func createWorkMailClient(t *testing.T) *workmailsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return workmailsdk.NewFromConfig(cfg, func(o *workmailsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_WorkMail_OrganizationLifecycle drives create→describe→list→delete of an
// organization, then a nested group create→delete.
func TestIntegration_WorkMail_OrganizationLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name      string
		alias     string
		groupName string
	}{
		{name: "full_lifecycle", alias: "integ-org", groupName: "integ-group"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createWorkMailClient(t)

			createOut, err := client.CreateOrganization(ctx, &workmailsdk.CreateOrganizationInput{
				Alias: aws.String(tt.alias),
			})
			require.NoError(t, err, "CreateOrganization should succeed")
			orgID := aws.ToString(createOut.OrganizationId)
			require.NotEmpty(t, orgID, "organization id must be returned")

			t.Cleanup(func() {
				_, _ = client.DeleteOrganization(ctx, &workmailsdk.DeleteOrganizationInput{
					OrganizationId:  aws.String(orgID),
					DeleteDirectory: true,
				})
			})

			descOut, err := client.DescribeOrganization(ctx, &workmailsdk.DescribeOrganizationInput{
				OrganizationId: aws.String(orgID),
			})
			require.NoError(t, err, "DescribeOrganization should succeed")
			assert.Equal(t, tt.alias, aws.ToString(descOut.Alias))

			grpOut, err := client.CreateGroup(ctx, &workmailsdk.CreateGroupInput{
				OrganizationId: aws.String(orgID),
				Name:           aws.String(tt.groupName),
			})
			require.NoError(t, err, "CreateGroup should succeed")
			groupID := aws.ToString(grpOut.GroupId)
			require.NotEmpty(t, groupID, "group id must be returned")

			listOut, err := client.ListGroups(ctx, &workmailsdk.ListGroupsInput{
				OrganizationId: aws.String(orgID),
			})
			require.NoError(t, err, "ListGroups should succeed")

			found := false
			for _, g := range listOut.Groups {
				if aws.ToString(g.Id) == groupID {
					found = true

					break
				}
			}

			assert.True(t, found, "created group should appear in list")

			_, err = client.DeleteGroup(ctx, &workmailsdk.DeleteGroupInput{
				OrganizationId: aws.String(orgID),
				GroupId:        aws.String(groupID),
			})
			require.NoError(t, err, "DeleteGroup should succeed")
		})
	}
}
