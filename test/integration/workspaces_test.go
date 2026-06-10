package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	workspacessdk "github.com/aws/aws-sdk-go-v2/service/workspaces"
	workspacestypes "github.com/aws/aws-sdk-go-v2/service/workspaces/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createWorkSpacesClient returns a WorkSpaces client pointed at the shared test container.
func createWorkSpacesClient(t *testing.T) *workspacessdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return workspacessdk.NewFromConfig(cfg, func(o *workspacessdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_WorkSpaces_IpGroupLifecycle drives create→describe→delete of an IP access
// control group, asserting the configured CIDR rule round-trips.
func TestIntegration_WorkSpaces_IpGroupLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name      string
		groupName string
		cidr      string
	}{
		{name: "full_lifecycle", groupName: "integ-ipgroup", cidr: "10.0.0.0/16"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createWorkSpacesClient(t)

			createOut, err := client.CreateIpGroup(ctx, &workspacessdk.CreateIpGroupInput{
				GroupName: aws.String(tt.groupName),
				GroupDesc: aws.String("integration test group"),
				UserRules: []workspacestypes.IpRuleItem{
					{IpRule: aws.String(tt.cidr), RuleDesc: aws.String("allow corp")},
				},
			})
			require.NoError(t, err, "CreateIpGroup should succeed")
			groupID := aws.ToString(createOut.GroupId)
			require.NotEmpty(t, groupID, "group id must be returned")

			t.Cleanup(func() {
				_, _ = client.DeleteIpGroup(ctx, &workspacessdk.DeleteIpGroupInput{GroupId: aws.String(groupID)})
			})

			descOut, err := client.DescribeIpGroups(ctx, &workspacessdk.DescribeIpGroupsInput{
				GroupIds: []string{groupID},
			})
			require.NoError(t, err, "DescribeIpGroups should succeed")
			require.Len(t, descOut.Result, 1)
			assert.Equal(t, tt.groupName, aws.ToString(descOut.Result[0].GroupName))

			_, err = client.DeleteIpGroup(ctx, &workspacessdk.DeleteIpGroupInput{GroupId: aws.String(groupID)})
			require.NoError(t, err, "DeleteIpGroup should succeed")
		})
	}
}

// TestIntegration_WorkSpaces_ConnectionAliasLifecycle drives create→describe→delete of a
// connection alias.
func TestIntegration_WorkSpaces_ConnectionAliasLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name             string
		connectionString string
	}{
		{name: "full_lifecycle", connectionString: "integ.example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createWorkSpacesClient(t)

			createOut, err := client.CreateConnectionAlias(ctx, &workspacessdk.CreateConnectionAliasInput{
				ConnectionString: aws.String(tt.connectionString),
			})
			require.NoError(t, err, "CreateConnectionAlias should succeed")
			aliasID := aws.ToString(createOut.AliasId)
			require.NotEmpty(t, aliasID, "alias id must be returned")

			t.Cleanup(func() {
				_, _ = client.DeleteConnectionAlias(ctx, &workspacessdk.DeleteConnectionAliasInput{
					AliasId: aws.String(aliasID),
				})
			})

			descOut, err := client.DescribeConnectionAliases(ctx, &workspacessdk.DescribeConnectionAliasesInput{
				AliasIds: []string{aliasID},
			})
			require.NoError(t, err, "DescribeConnectionAliases should succeed")
			require.Len(t, descOut.ConnectionAliases, 1)
			assert.Equal(t, tt.connectionString, aws.ToString(descOut.ConnectionAliases[0].ConnectionString))

			_, err = client.DeleteConnectionAlias(ctx, &workspacessdk.DeleteConnectionAliasInput{
				AliasId: aws.String(aliasID),
			})
			require.NoError(t, err, "DeleteConnectionAlias should succeed")
		})
	}
}
