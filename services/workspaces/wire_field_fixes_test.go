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
