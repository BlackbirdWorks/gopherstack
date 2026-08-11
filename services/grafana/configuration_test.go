package grafana_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"
	"github.com/aws/aws-sdk-go-v2/service/grafana/types"
	"github.com/stretchr/testify/require"
)

func TestDescribeAndUpdateWorkspaceConfiguration(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

	desc, err := client.DescribeWorkspaceConfiguration(t.Context(), &grafanasdk.DescribeWorkspaceConfigurationInput{
		WorkspaceId: aws.String(id),
	})
	require.NoError(t, err)
	require.Equal(t, "{}", aws.ToString(desc.Configuration), "unset configuration defaults to an empty JSON object")

	newConfig := `{"panels":[{"id":1}]}`

	_, err = client.UpdateWorkspaceConfiguration(t.Context(), &grafanasdk.UpdateWorkspaceConfigurationInput{
		WorkspaceId:   aws.String(id),
		Configuration: aws.String(newConfig),
	})
	require.NoError(t, err)

	desc2, err := client.DescribeWorkspaceConfiguration(t.Context(), &grafanasdk.DescribeWorkspaceConfigurationInput{
		WorkspaceId: aws.String(id),
	})
	require.NoError(t, err)
	require.Equal(t, newConfig, aws.ToString(desc2.Configuration), "the opaque blob must round-trip byte-for-byte")
}

func TestListVersions_CreatableAndUpgradePath(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	all, err := client.ListVersions(t.Context(), &grafanasdk.ListVersionsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, all.GrafanaVersions)

	oldest := all.GrafanaVersions[0]

	id := createActiveWorkspace(t, client, &grafanasdk.CreateWorkspaceInput{
		AccountAccessType:       types.AccountAccessTypeCurrentAccount,
		AuthenticationProviders: []types.AuthenticationProviderTypes{types.AuthenticationProviderTypesAwsSso},
		PermissionType:          types.PermissionTypeCustomerManaged,
		GrafanaVersion:          aws.String(oldest),
	})

	upgradeable, err := client.ListVersions(t.Context(), &grafanasdk.ListVersionsInput{WorkspaceId: aws.String(id)})
	require.NoError(t, err)
	require.NotEmpty(t, upgradeable.GrafanaVersions)
	require.NotContains(t, upgradeable.GrafanaVersions, oldest,
		"a workspace may only upgrade, never stay on or downgrade to its current version")

	target := upgradeable.GrafanaVersions[len(upgradeable.GrafanaVersions)-1]

	_, err = client.UpdateWorkspaceConfiguration(t.Context(), &grafanasdk.UpdateWorkspaceConfigurationInput{
		WorkspaceId:    aws.String(id),
		Configuration:  aws.String("{}"),
		GrafanaVersion: aws.String(target),
	})
	require.NoError(t, err)

	desc, err := client.DescribeWorkspace(t.Context(), &grafanasdk.DescribeWorkspaceInput{WorkspaceId: aws.String(id)})
	require.NoError(t, err)
	require.Equal(t, types.WorkspaceStatusVersionUpdating, desc.Workspace.Status)

	waitForWorkspaceActive(t, client, id)

	// Downgrade must be rejected outright.
	_, err = client.UpdateWorkspaceConfiguration(t.Context(), &grafanasdk.UpdateWorkspaceConfigurationInput{
		WorkspaceId:    aws.String(id),
		Configuration:  aws.String("{}"),
		GrafanaVersion: aws.String(oldest),
	})
	require.Error(t, err)

	var ve *types.ValidationException
	require.ErrorAs(t, err, &ve)
}
