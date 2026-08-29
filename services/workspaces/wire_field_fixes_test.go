package workspaces_test

import (
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wssdk "github.com/aws/aws-sdk-go-v2/service/workspaces"
	"github.com/aws/aws-sdk-go-v2/service/workspaces/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateAccountLinkInvitation_RealSDKClient_UsesRealFieldNames drives
// account-link creation through the real aws-sdk-go-v2 client. The real
// AccountLink type's identifying/status members are "AccountLinkId"/
// "AccountLinkStatus" -- gopherstack previously emitted "LinkId"/"Status"
// (the request-side field names), which a real typed client silently
// decodes as empty/zero rather than erroring. A wrong wire key is
// unobservable through direct handler calls but not through the real SDK
// client's typed fields, which is why this is checked here instead of only
// via a raw-body test.
func TestCreateAccountLinkInvitation_RealSDKClient_UsesRealFieldNames(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	out, err := client.CreateAccountLinkInvitation(
		t.Context(),
		&wssdk.CreateAccountLinkInvitationInput{TargetAccountId: aws.String("999988887777")},
	)
	require.NoError(t, err)
	require.NotNil(t, out.AccountLink)

	require.NotNil(t, out.AccountLink.AccountLinkId)
	assert.NotEmpty(t, *out.AccountLink.AccountLinkId)
	assert.Equal(
		t,
		types.AccountLinkStatusEnumPendingAcceptanceByTargetAccount,
		out.AccountLink.AccountLinkStatus,
		"PENDING_ACCEPTANCE_BY_TARGET_ACCOUNT is the real enum value; "+
			"the previous PENDING_ACCEPTANCE was not a member of AccountLinkStatusEnum at all",
	)
}

// TestDeleteAccountLinkInvitation_RealSDKClient_NoFabricatedStatus proves
// DeleteAccountLinkInvitation no longer reports a fabricated "DELETED"
// status, which is not a member of the real AccountLinkStatusEnum.
func TestDeleteAccountLinkInvitation_RealSDKClient_NoFabricatedStatus(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	created, err := client.CreateAccountLinkInvitation(
		t.Context(),
		&wssdk.CreateAccountLinkInvitationInput{TargetAccountId: aws.String("111100002222")},
	)
	require.NoError(t, err)

	deleted, err := client.DeleteAccountLinkInvitation(
		t.Context(),
		&wssdk.DeleteAccountLinkInvitationInput{LinkId: created.AccountLink.AccountLinkId},
	)
	require.NoError(t, err)
	require.NotNil(t, deleted.AccountLink)

	if slices.Contains((types.AccountLinkStatusEnum("")).Values(), deleted.AccountLink.AccountLinkStatus) {
		return
	}

	t.Fatalf(
		"AccountLinkStatus %q is not a member of the real AccountLinkStatusEnum",
		deleted.AccountLink.AccountLinkStatus,
	)
}

// TestDescribeApplicationAssociations_RealSDKClient_UsesApplicationId
// proves DescribeApplicationAssociations' response items carry the real
// ApplicationResourceAssociation shape. gopherstack previously reused the
// WorkspaceResourceAssociation-shaped struct here: it emitted "WorkspaceId"
// (not a member of ApplicationResourceAssociation at all) instead of the
// real "ApplicationId", and put the application's own ID under
// "AssociatedResourceId" instead of the workspace's -- backwards from what
// the real type documents ("The identifier of the associated resource").
func TestDescribeApplicationAssociations_RealSDKClient_UsesApplicationId(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	wsID := createSDKWorkspace(t, client)
	appID := "app-real-shape-associations"

	_, err := client.AssociateWorkspaceApplication(
		t.Context(),
		&wssdk.AssociateWorkspaceApplicationInput{
			WorkspaceId:   aws.String(wsID),
			ApplicationId: aws.String(appID),
		},
	)
	require.NoError(t, err)

	out, err := client.DescribeApplicationAssociations(
		t.Context(),
		&wssdk.DescribeApplicationAssociationsInput{
			ApplicationId: aws.String(appID),
			AssociatedResourceTypes: []types.ApplicationAssociatedResourceType{
				types.ApplicationAssociatedResourceTypeWorkspace,
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, out.Associations, 1)

	assoc := out.Associations[0]
	require.NotNil(t, assoc.ApplicationId)
	assert.Equal(t, appID, *assoc.ApplicationId)
	require.NotNil(t, assoc.AssociatedResourceId)
	assert.Equal(t, wsID, *assoc.AssociatedResourceId)
	assert.Equal(t, types.ApplicationAssociatedResourceTypeWorkspace, assoc.AssociatedResourceType)
}

// TestDescribeConnectionAliases_NoFabricatedTopLevelConnectionIdentifier
// checks the raw response body: the real top-level ConnectionAlias shape
// (deserializers.go's awsAwsjson11_deserializeDocumentConnectionAlias) has
// no "ConnectionIdentifier" member -- that field only exists nested inside
// each Associations[] entry (ConnectionAliasAssociation). A real typed
// client silently ignores the extra top-level key, which is why this is a
// raw-body check rather than a typed-field one.
//
// The alias must be associated with a resource first: this backend only
// populates ConnectionIdentifier on Associate (Create leaves it the zero
// value), and the fabricated field's own "omitempty" tag would otherwise
// mask the bug by omitting an empty string either way.
func TestDescribeConnectionAliases_NoFabricatedTopLevelConnectionIdentifier(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerWithBackend(t)

	doTargetRequest(t, h, "RegisterWorkspaceDirectory", map[string]any{"DirectoryId": "d-ca-test"})

	var createOut struct {
		AliasId string `json:"AliasId"` //nolint:revive,staticcheck // matches wire key.
	}
	createRec := doTargetRequest(t, h, "CreateConnectionAlias", map[string]any{
		"ConnectionString": "rt-fixed-shape.example.com",
	})
	decodeJSON(t, createRec.Body.Bytes(), &createOut)

	assocRec := doTargetRequest(t, h, "AssociateConnectionAlias", map[string]any{
		"AliasId":    createOut.AliasId,
		"ResourceId": "d-ca-test",
	})
	require.Equal(t, 200, assocRec.Code, "body: %s", assocRec.Body)

	rec := doTargetRequest(t, h, "DescribeConnectionAliases", map[string]any{})
	require.Equal(t, 200, rec.Code, "body: %s", rec.Body)

	var out struct {
		ConnectionAliases []map[string]any `json:"ConnectionAliases"`
	}
	decodeJSON(t, rec.Body.Bytes(), &out)
	require.Len(t, out.ConnectionAliases, 1)

	// Sanity check the fixture is actually exercising a non-empty
	// ConnectionIdentifier (nested, where it belongs), or the top-level
	// absence check above would prove nothing.
	assocs, ok := out.ConnectionAliases[0]["Associations"].([]any)
	require.True(t, ok, "body: %v", out.ConnectionAliases[0])
	require.Len(t, assocs, 1)
	nested, ok := assocs[0].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, nested["ConnectionIdentifier"])

	assert.NotContains(
		t, out.ConnectionAliases[0], "ConnectionIdentifier",
		"ConnectionIdentifier is not a member of the real top-level ConnectionAlias type",
	)
}

// TestCreateWorkspaces_RealSDKClient_WorkspaceNameThreadedThrough drives a
// real aws-sdk-go-v2 client to prove types.WorkspaceRequest.WorkspaceName
// (aws-sdk-go-v2/service/workspaces@v1.73.1/types/types.go:1874-1879 --
// "required if UserName is [UNDEFINED] for user-decoupled WorkSpaces") is
// actually stored and echoed by CreateWorkspaces/DescribeWorkspaces, and
// that a normal user-assigned WorkSpace's WorkspaceName stays genuinely
// unset ("not applicable if UserName is specified") rather than fabricated
// from UserName.
func TestCreateWorkspaces_RealSDKClient_WorkspaceNameThreadedThrough(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.RegisterWorkspaceDirectory(t.Context(), &wssdk.RegisterWorkspaceDirectoryInput{
		DirectoryId:            aws.String("d-name11111"),
		WorkspaceDirectoryName: aws.String("dir"),
	})
	require.NoError(t, err)

	createOut, err := client.CreateWorkspaces(t.Context(), &wssdk.CreateWorkspacesInput{
		Workspaces: []types.WorkspaceRequest{
			{
				BundleId:      aws.String("wsb-00000000"),
				DirectoryId:   aws.String("d-name11111"),
				UserName:      aws.String("[UNDEFINED]"),
				WorkspaceName: aws.String("decoupled-real-sdk"),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, createOut.PendingRequests, 1)
	assert.Equal(t, "decoupled-real-sdk", aws.ToString(createOut.PendingRequests[0].WorkspaceName),
		"CreateWorkspacesOutput.PendingRequests must echo the caller-supplied WorkspaceName")

	wsID := aws.ToString(createOut.PendingRequests[0].WorkspaceId)

	descOut, err := client.DescribeWorkspaces(t.Context(), &wssdk.DescribeWorkspacesInput{
		WorkspaceIds: []string{wsID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Workspaces, 1)
	assert.Equal(t, "decoupled-real-sdk", aws.ToString(descOut.Workspaces[0].WorkspaceName),
		"DescribeWorkspacesOutput must echo the caller-supplied WorkspaceName, not silently drop it")

	// A normal user-assigned WorkSpace must NOT fabricate a WorkspaceName.
	normalID := createSDKWorkspace(t, client)

	normalOut, err := client.DescribeWorkspaces(t.Context(), &wssdk.DescribeWorkspacesInput{
		WorkspaceIds: []string{normalID},
	})
	require.NoError(t, err)
	require.Len(t, normalOut.Workspaces, 1)
	assert.Nil(t, normalOut.Workspaces[0].WorkspaceName,
		"WorkspaceName is not applicable for a user-assigned WorkSpace and must not be fabricated from UserName")
}

// TestDescribeWorkspaceDirectories_RealSDKClient_SettingsRoundTrip proves
// DescribeWorkspaceDirectories echoes back the directory-level settings set
// via the seven Modify* ops (EndpointEncryptionMode,
// CertificateBasedAuthProperties, SamlProperties, SelfservicePermissions,
// WorkspaceAccessProperties, WorkspaceCreationProperties) plus IpGroupIds
// (AssociateIpGroups) -- all real members of
// types.WorkspaceDirectory (aws-sdk-go-v2/service/workspaces@v1.73.1
// deserializers.go's awsAwsjson11_deserializeDocumentWorkspaceDirectory case
// list). Real AWS has no separate Describe op for any of these settings;
// DescribeWorkspaceDirectories is the only place a real client ever reads
// them back. Before this fix, this backend's dirResp (handler_directories.go)
// carried only DirectoryId/DirectoryName/DirectoryType/Alias/State/SubnetIds
// -- every one of these real fields was silently dropped even though the
// Modify* ops genuinely stored the data (accept-and-drop, not mere
// omission): a real client's typed fields all decoded nil/empty regardless
// of what was configured.
func TestDescribeWorkspaceDirectories_RealSDKClient_SettingsRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.RegisterWorkspaceDirectory(ctx, &wssdk.RegisterWorkspaceDirectoryInput{
		DirectoryId:            aws.String("d-settings11111"),
		WorkspaceDirectoryName: aws.String("settings-dir"),
	})
	require.NoError(t, err)

	groupID, err := client.CreateIpGroup(ctx, &wssdk.CreateIpGroupInput{GroupName: aws.String("settings-group")})
	require.NoError(t, err)

	_, err = client.AssociateIpGroups(ctx, &wssdk.AssociateIpGroupsInput{
		DirectoryId: aws.String("d-settings11111"),
		GroupIds:    []string{aws.ToString(groupID.GroupId)},
	})
	require.NoError(t, err)

	_, err = client.ModifyEndpointEncryptionMode(ctx, &wssdk.ModifyEndpointEncryptionModeInput{
		DirectoryId:            aws.String("d-settings11111"),
		EndpointEncryptionMode: types.EndpointEncryptionModeFipsValidated,
	})
	require.NoError(t, err)

	_, err = client.ModifyCertificateBasedAuthProperties(ctx, &wssdk.ModifyCertificateBasedAuthPropertiesInput{
		ResourceId: aws.String("d-settings11111"),
		CertificateBasedAuthProperties: &types.CertificateBasedAuthProperties{
			Status:                  types.CertificateBasedAuthStatusEnumEnabled,
			CertificateAuthorityArn: aws.String("arn:aws:acm-pca:us-east-1:000000000000:certificate-authority/ca-1"),
		},
	})
	require.NoError(t, err)

	_, err = client.ModifySamlProperties(ctx, &wssdk.ModifySamlPropertiesInput{
		ResourceId: aws.String("d-settings11111"),
		SamlProperties: &types.SamlProperties{
			Status:                  types.SamlStatusEnumEnabled,
			UserAccessUrl:           aws.String("https://idp.example.com/sso"),
			RelayStateParameterName: aws.String("RelayState"),
		},
	})
	require.NoError(t, err)

	_, err = client.ModifySelfservicePermissions(ctx, &wssdk.ModifySelfservicePermissionsInput{
		ResourceId: aws.String("d-settings11111"),
		SelfservicePermissions: &types.SelfservicePermissions{
			RestartWorkspace:   types.ReconnectEnumEnabled,
			IncreaseVolumeSize: types.ReconnectEnumEnabled,
			ChangeComputeType:  types.ReconnectEnumDisabled,
			SwitchRunningMode:  types.ReconnectEnumEnabled,
			RebuildWorkspace:   types.ReconnectEnumDisabled,
		},
	})
	require.NoError(t, err)

	_, err = client.ModifyWorkspaceAccessProperties(ctx, &wssdk.ModifyWorkspaceAccessPropertiesInput{
		ResourceId: aws.String("d-settings11111"),
		WorkspaceAccessProperties: &types.WorkspaceAccessProperties{
			DeviceTypeWindows: types.AccessPropertyValueAllow,
			DeviceTypeOsx:     types.AccessPropertyValueDeny,
		},
	})
	require.NoError(t, err)

	_, err = client.ModifyWorkspaceCreationProperties(ctx, &wssdk.ModifyWorkspaceCreationPropertiesInput{
		ResourceId: aws.String("d-settings11111"),
		WorkspaceCreationProperties: &types.WorkspaceCreationProperties{
			DefaultOu:             aws.String("OU=WorkSpaces,DC=example,DC=com"),
			CustomSecurityGroupId: aws.String("sg-0123456789abcdef0"),
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeWorkspaceDirectories(ctx, &wssdk.DescribeWorkspaceDirectoriesInput{
		DirectoryIds: []string{"d-settings11111"},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Directories, 1)

	dir := descOut.Directories[0]

	assert.Equal(t, []string{aws.ToString(groupID.GroupId)}, dir.IpGroupIds,
		"WorkspaceDirectory.IpGroupIds must round-trip the AssociateIpGroups association")
	assert.Equal(t, types.EndpointEncryptionModeFipsValidated, dir.EndpointEncryptionMode)

	require.NotNil(t, dir.CertificateBasedAuthProperties)
	assert.Equal(t, types.CertificateBasedAuthStatusEnumEnabled, dir.CertificateBasedAuthProperties.Status)
	assert.Equal(t, "arn:aws:acm-pca:us-east-1:000000000000:certificate-authority/ca-1",
		aws.ToString(dir.CertificateBasedAuthProperties.CertificateAuthorityArn))

	require.NotNil(t, dir.SamlProperties)
	assert.Equal(t, types.SamlStatusEnumEnabled, dir.SamlProperties.Status)
	assert.Equal(t, "https://idp.example.com/sso", aws.ToString(dir.SamlProperties.UserAccessUrl))

	require.NotNil(t, dir.SelfservicePermissions)
	assert.Equal(t, types.ReconnectEnumEnabled, dir.SelfservicePermissions.RestartWorkspace)
	assert.Equal(t, types.ReconnectEnumDisabled, dir.SelfservicePermissions.ChangeComputeType)

	require.NotNil(t, dir.WorkspaceAccessProperties)
	assert.Equal(t, types.AccessPropertyValueAllow, dir.WorkspaceAccessProperties.DeviceTypeWindows)
	assert.Equal(t, types.AccessPropertyValueDeny, dir.WorkspaceAccessProperties.DeviceTypeOsx)

	require.NotNil(t, dir.WorkspaceCreationProperties)
	assert.Equal(t, "OU=WorkSpaces,DC=example,DC=com", aws.ToString(dir.WorkspaceCreationProperties.DefaultOu))
	assert.Equal(t, "sg-0123456789abcdef0", aws.ToString(dir.WorkspaceCreationProperties.CustomSecurityGroupId))
}
