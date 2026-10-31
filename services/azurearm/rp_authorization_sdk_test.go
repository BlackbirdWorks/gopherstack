package azurearm_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/authorization/armauthorization/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/msi/armmsi"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armlocks"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armpolicy"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const (
	authRG        = "rg-auth"
	authSubScope  = "/subscriptions/" + aciSub
	authRGScope   = authSubScope + "/resourceGroups/" + authRG
	authAcctScope = authRGScope + "/providers/Microsoft.Storage/storageAccounts/acctauth"
	roleOwner     = "8e3af657-a8ff-443c-a75c-2fe8c4bcb635"
	roleContrib   = "b24988ac-6180-42a0-ab88-20f7382dd24c"
	roleReader    = "acdd72a7-3385-48ef-bd42-f606fba81ae7"
	roleBlobOwner = "b7e6dc6d-f1e8-4753-8033-0f276bb0955b"
	guidA         = "11111111-1111-1111-1111-111111111111"
	guidB         = "22222222-2222-2222-2222-222222222222"
	guidC         = "33333333-3333-3333-3333-333333333333"
	guidD         = "44444444-4444-4444-4444-444444444444"
	storageBody   = `{"location":"westus","sku":{"name":"Standard_LRS"},"kind":"StorageV2"}`
)

type authStack struct {
	handler *azurearm.Handler
	hc      *http.Client
	roles   *armauthorization.RoleDefinitionsClient
	assigns *armauthorization.RoleAssignmentsClient
	locks   *armlocks.ManagementLocksClient
	defs    *armpolicy.DefinitionsClient
	sets    *armpolicy.SetDefinitionsClient
	pAssign *armpolicy.AssignmentsClient
	msi     *armmsi.UserAssignedIdentitiesClient
	url     string
}

func newAuthStack(t *testing.T) *authStack {
	t.Helper()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := svc.(*azurearm.Handler)
	require.True(t, ok)

	t.Cleanup(func() { h.Shutdown(context.Background()) })

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	opts := &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: srv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: srv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	}

	af, err := armauthorization.NewClientFactory(aciSub, aciCred{}, opts)
	require.NoError(t, err)

	lf, err := armlocks.NewClientFactory(aciSub, aciCred{}, opts)
	require.NoError(t, err)

	pf, err := armpolicy.NewClientFactory(aciSub, aciCred{}, opts)
	require.NoError(t, err)

	mf, err := armmsi.NewClientFactory(aciSub, aciCred{}, opts)
	require.NoError(t, err)

	s := &authStack{
		handler: h, hc: srv.Client(), url: srv.URL,
		roles:   af.NewRoleDefinitionsClient(),
		assigns: af.NewRoleAssignmentsClient(),
		locks:   lf.NewManagementLocksClient(),
		defs:    pf.NewDefinitionsClient(),
		sets:    pf.NewSetDefinitionsClient(),
		pAssign: pf.NewAssignmentsClient(),
		msi:     mf.NewUserAssignedIdentitiesClient(),
	}

	status, _ := s.raw(t, http.MethodPut, "/subscriptions/"+aciSub+"/resourcegroups/"+authRG, `{"location":"westus"}`)
	require.Equal(t, http.StatusCreated, status)

	return s
}

func (s *authStack) raw(t *testing.T, method, path, body string) (int, map[string]any) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, s.url+path, strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.hc.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var out map[string]any
	_ = json.Unmarshal(data, &out)

	return resp.StatusCode, out
}

func (s *authStack) putAccount(t *testing.T) {
	t.Helper()

	status, _ := s.raw(t, http.MethodPut, authAcctScope+"?api-version=2023-01-01", storageBody)
	require.Equal(t, http.StatusOK, status)
}

func authErrCode(t *testing.T, body map[string]any) string {
	t.Helper()

	e, _ := body["error"].(map[string]any)
	code, _ := e["code"].(string)

	return code
}

func roleID(guid string) string {
	return authSubScope + "/providers/Microsoft.Authorization/roleDefinitions/" + guid
}

func assignParams(
	role, principal string,
	ptype *armauthorization.PrincipalType,
) armauthorization.RoleAssignmentCreateParameters {
	return armauthorization.RoleAssignmentCreateParameters{Properties: &armauthorization.RoleAssignmentProperties{
		RoleDefinitionID: new(roleID(role)), PrincipalID: new(principal), PrincipalType: ptype,
	}}
}

func listRoles(
	t *testing.T,
	c *armauthorization.RoleDefinitionsClient,
	scope string,
	filter *string,
) []*armauthorization.RoleDefinition {
	t.Helper()

	var out []*armauthorization.RoleDefinition

	for pg := c.NewListPager(scope, &armauthorization.RoleDefinitionsClientListOptions{Filter: filter}); pg.More(); {
		page, err := pg.NextPage(t.Context())
		require.NoError(t, err)

		out = append(out, page.Value...)
	}

	return out
}

func TestAuthorizationSDK_BuiltInRoles(t *testing.T) {
	t.Parallel()

	s := newAuthStack(t)
	ctx := t.Context()

	tests := []struct {
		name string
		guid string
		role string
	}{
		{name: "owner", guid: roleOwner, role: "Owner"},
		{name: "contributor", guid: roleContrib, role: "Contributor"},
		{name: "reader", guid: roleReader, role: "Reader"},
		{name: "uaa", guid: "18d7d88d-d35e-4fb5-a5c3-7773c20a72d9", role: "User Access Administrator"},
		{name: "blob owner", guid: roleBlobOwner, role: "Storage Blob Data Owner"},
		{name: "blob contributor", guid: "ba92f5b4-2d11-453d-a403-e96b0029c9fe", role: "Storage Blob Data Contributor"},
		{name: "blob reader", guid: "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1", role: "Storage Blob Data Reader"},
		{name: "kv admin", guid: "00482a5a-887f-4fb3-b363-3b7fe8e74483", role: "Key Vault Administrator"},
		{name: "kv secrets user", guid: "4633458b-17de-408a-b874-0445c86b69e6", role: "Key Vault Secrets User"},
		{name: "kv crypto user", guid: "12338af0-0e69-4776-bea7-57ae8d297424", role: "Key Vault Crypto User"},
		{name: "acrpull", guid: "7f951dda-4ed3-4680-a7ca-43fe172d538d", role: "AcrPull"},
		{name: "acrpush", guid: "8311e382-0749-4cb8-b61a-304f252e45ec", role: "AcrPush"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := s.roles.Get(ctx, authSubScope, tt.guid, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.role, *got.Properties.RoleName)
			assert.Equal(t, "BuiltInRole", *got.Properties.RoleType)
			assert.Equal(t, roleID(tt.guid), *got.ID)
			assert.NotEmpty(t, got.Properties.Permissions)

			byName := listRoles(t, s.roles, authSubScope, new("roleName eq '"+tt.role+"'"))
			require.Len(t, byName, 1)
			assert.Equal(t, tt.guid, *byName[0].Name)
		})
	}

	owner, err := s.roles.Get(ctx, authSubScope, roleOwner, nil)
	require.NoError(t, err)
	assert.Equal(t, []*string{new("*")}, owner.Properties.Permissions[0].Actions)

	contrib, err := s.roles.Get(ctx, authSubScope, roleContrib, nil)
	require.NoError(t, err)
	assert.Contains(t, derefAll(contrib.Properties.Permissions[0].NotActions), "Microsoft.Authorization/*/Write")

	_, err = s.roles.Get(ctx, authSubScope, guidA, nil)
	assert.Equal(t, "RoleDefinitionDoesNotExist", sdkErrCode(err))

	all := listRoles(t, s.roles, authSubScope, new("type eq 'BuiltInRole'"))
	assert.GreaterOrEqual(t, len(all), 70)
	assert.Empty(t, listRoles(t, s.roles, authSubScope, new("type eq 'CustomRole'")))

	_, err = s.roles.NewListPager(authSubScope, &armauthorization.RoleDefinitionsClientListOptions{
		Filter: new("bogus eq 'x'"),
	}).NextPage(ctx)
	assert.Equal(t, "InvalidFilter", sdkErrCode(err))
}

func derefAll(in []*string) []string {
	out := make([]string, 0, len(in))
	for _, p := range in {
		out = append(out, *p)
	}

	return out
}

func customRole(name string, scopes []string, perm *armauthorization.Permission) armauthorization.RoleDefinition {
	ptrs := make([]*string, 0, len(scopes))
	for _, sc := range scopes {
		ptrs = append(ptrs, new(sc))
	}

	return armauthorization.RoleDefinition{Properties: &armauthorization.RoleDefinitionProperties{
		RoleName: new(name), Description: new("custom"), AssignableScopes: ptrs,
		Permissions: []*armauthorization.Permission{perm},
	}}
}

func TestAuthorizationSDK_CustomRoleDefinitions(t *testing.T) {
	t.Parallel()

	s := newAuthStack(t)
	ctx := t.Context()
	read := &armauthorization.Permission{Actions: []*string{new("Microsoft.Storage/*/read")}}

	tests := []struct {
		role     armauthorization.RoleDefinition
		name     string
		guid     string
		wantCode string
	}{
		{name: "ok", guid: guidA, role: customRole("Storage Reader X", []string{authSubScope}, read)},
		{
			name:     "bad guid",
			guid:     "not-a-guid",
			role:     customRole("Y", []string{authSubScope}, read),
			wantCode: "InvalidRequestContent",
		},
		{
			name:     "no name",
			guid:     guidB,
			role:     customRole("", []string{authSubScope}, read),
			wantCode: "InvalidRequestContent",
		},
		{name: "no scopes", guid: guidB, role: customRole("Z", nil, read), wantCode: "InvalidRequestContent"},
		{
			name:     "bad scope",
			guid:     guidB,
			role:     customRole("Z", []string{"/nonsense"}, read),
			wantCode: "InvalidRequestContent",
		},
		{
			name:     "root scope",
			guid:     guidB,
			role:     customRole("Z", []string{"/"}, read),
			wantCode: "InvalidRequestContent",
		},
		{
			name:     "builtin name",
			guid:     guidB,
			role:     customRole("Reader", []string{authSubScope}, read),
			wantCode: "RoleDefinitionWithSameNameExists",
		},
		{
			name:     "dup name",
			guid:     guidB,
			role:     customRole("storage reader x", []string{authSubScope}, read),
			wantCode: "RoleDefinitionWithSameNameExists",
		},
		{
			name:     "builtin guid",
			guid:     roleOwner,
			role:     customRole("Q", []string{authSubScope}, read),
			wantCode: "InvalidRequestContent",
		},
		{
			name: "mgmt group data actions", guid: guidB, wantCode: "InvalidRequestContent",
			role: customRole("MG", []string{"/providers/Microsoft.Management/managementGroups/mg1"},
				&armauthorization.Permission{DataActions: []*string{new("Microsoft.Storage/x/read")}}),
		},
	}

	for _, tt := range tests {
		got, err := s.roles.CreateOrUpdate(ctx, authSubScope, tt.guid, tt.role, nil)
		if tt.wantCode != "" {
			require.Error(t, err, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(err), tt.name)

			continue
		}

		require.NoError(t, err, tt.name)
		assert.Equal(t, "CustomRole", *got.Properties.RoleType)
		assert.Equal(t, roleID(tt.guid), *got.ID)
	}

	updated := customRole("Storage Reader X", []string{authSubScope}, &armauthorization.Permission{
		Actions: []*string{new("Microsoft.Storage/*/read"), new("Microsoft.Compute/*/read")},
	})
	got, err := s.roles.CreateOrUpdate(ctx, authSubScope, guidA, updated, nil)
	require.NoError(t, err)
	assert.Len(t, got.Properties.Permissions[0].Actions, 2)

	customs := listRoles(t, s.roles, authSubScope, new("type eq 'CustomRole'"))
	require.Len(t, customs, 1)
	assert.Equal(t, guidA, *customs[0].Name)

	other := authSubScope + "x"
	assert.Empty(t, listRoles(t, s.roles, other, new("type eq 'CustomRole'")), "not assignable elsewhere")

	_, err = s.assigns.Create(ctx, authRGScope, guidC, assignParams(guidA, guidD, nil), nil)
	require.NoError(t, err)

	_, err = s.roles.Delete(ctx, authSubScope, guidA, nil)
	assert.Equal(t, "RoleDefinitionHasAssignments", sdkErrCode(err))

	_, err = s.assigns.Delete(ctx, authRGScope, guidC, nil)
	require.NoError(t, err)

	del, err := s.roles.Delete(ctx, authSubScope, guidA, nil)
	require.NoError(t, err)
	assert.Equal(t, "Storage Reader X", *del.Properties.RoleName)

	_, err = s.roles.Get(ctx, authSubScope, guidA, nil)
	assert.Equal(t, "RoleDefinitionDoesNotExist", sdkErrCode(err))

	_, err = s.roles.Delete(ctx, authSubScope, guidA, nil)
	require.NoError(t, err, "deleting a missing role is a no-op")

	_, err = s.roles.Delete(ctx, authSubScope, roleOwner, nil)
	assert.Equal(t, "InvalidRequestContent", sdkErrCode(err))
}

func TestAuthorizationSDK_RoleAssignments(t *testing.T) {
	t.Parallel()

	s := newAuthStack(t)
	ctx := t.Context()
	s.putAccount(t)

	user := armauthorization.PrincipalTypeUser

	tests := []struct {
		params   armauthorization.RoleAssignmentCreateParameters
		name     string
		scope    string
		guid     string
		wantCode string
	}{
		{name: "subscription", scope: authSubScope, guid: guidA, params: assignParams(roleReader, guidD, &user)},
		{name: "resource group", scope: authRGScope, guid: guidB, params: assignParams(roleContrib, guidD, nil)},
		{name: "resource", scope: authAcctScope, guid: guidC, params: assignParams(roleBlobOwner, guidD, nil)},
		{
			name:     "duplicate triple",
			scope:    authRGScope,
			guid:     "55555555-5555-5555-5555-555555555555",
			params:   assignParams(roleContrib, guidD, nil),
			wantCode: "RoleAssignmentExists",
		},
		{
			name:     "name used elsewhere",
			scope:    authRGScope,
			guid:     guidA,
			params:   assignParams(roleOwner, guidD, nil),
			wantCode: "RoleAssignmentExists",
		},
		{
			name:     "changed principal",
			scope:    authRGScope,
			guid:     guidB,
			params:   assignParams(roleContrib, guidC, nil),
			wantCode: "RoleAssignmentUpdateNotPermitted",
		},
		{
			name:     "changed role",
			scope:    authRGScope,
			guid:     guidB,
			params:   assignParams(roleOwner, guidD, nil),
			wantCode: "RoleAssignmentUpdateNotPermitted",
		},
		{
			name:     "bad name",
			scope:    authRGScope,
			guid:     "nope",
			params:   assignParams(roleOwner, guidD, nil),
			wantCode: "InvalidRoleAssignmentId",
		},
		{
			name:     "unknown role",
			scope:    authRGScope,
			guid:     "66666666-6666-6666-6666-666666666666",
			params:   assignParams(guidB, guidD, nil),
			wantCode: "RoleDefinitionDoesNotExist",
		},
		{
			name:     "bad principal",
			scope:    authRGScope,
			guid:     "77777777-7777-7777-7777-777777777777",
			params:   assignParams(roleOwner, "x", nil),
			wantCode: "PrincipalNotFound",
		},
		{
			name: "no role", scope: authRGScope, wantCode: "InvalidRequestContent",
			guid: "88888888-8888-8888-8888-888888888888",
			params: armauthorization.RoleAssignmentCreateParameters{
				Properties: &armauthorization.RoleAssignmentProperties{PrincipalID: new(guidD)},
			},
		},
		{
			name: "bad principal type", scope: authRGScope, wantCode: "InvalidRequestContent",
			guid:   "99999999-9999-9999-9999-999999999999",
			params: assignParams(roleOwner, guidD, new(armauthorization.PrincipalType("Robot"))),
		},
		{
			name:     "missing rg",
			scope:    authSubScope + "/resourceGroups/ghost",
			guid:     "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
			params:   assignParams(roleOwner, guidD, nil),
			wantCode: "ResourceGroupNotFound",
		},
		{
			name: "missing resource", scope: authRGScope + "/providers/Microsoft.Storage/storageAccounts/ghost",
			guid:     "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
			params:   assignParams(roleOwner, guidD, nil),
			wantCode: "ResourceNotFound",
		},
	}

	for _, tt := range tests {
		got, err := s.assigns.Create(ctx, tt.scope, tt.guid, tt.params, nil)
		if tt.wantCode != "" {
			require.Error(t, err, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(err), tt.name)

			continue
		}

		require.NoError(t, err, tt.name)
		assert.Equal(t, tt.scope+"/providers/Microsoft.Authorization/roleAssignments/"+tt.guid, *got.ID, tt.name)
		assert.Equal(t, tt.scope, *got.Properties.Scope, tt.name)
		assert.Equal(t, guidD, *got.Properties.PrincipalID, tt.name)
		assert.NotNil(t, got.Properties.CreatedOn, tt.name)
	}

	status, _ := s.raw(
		t,
		http.MethodPut,
		authRGScope+"/providers/Microsoft.Authorization/roleAssignments/"+guidB+"?api-version=2022-04-01",
		`{"properties":{"roleDefinitionId":"`+roleID(
			roleContrib,
		)+`","principalId":"`+guidD+`","description":"updated"}}`,
	)
	assert.Equal(t, http.StatusOK, status, "re-PUT of the same assignment updates it")

	got, err := s.assigns.Get(ctx, authRGScope, guidB, nil)
	require.NoError(t, err)
	assert.Equal(t, "updated", *got.Properties.Description)
	assert.Equal(t, roleID(roleContrib), *got.Properties.RoleDefinitionID)
	assert.Equal(t, armauthorization.PrincipalTypeUser, *got.Properties.PrincipalType)

	_, err = s.assigns.Get(ctx, authRGScope, guidA, nil)
	assert.Equal(t, "RoleAssignmentNotFound", sdkErrCode(err))

	ids := func(opts *armauthorization.RoleAssignmentsClientListForScopeOptions, scope string) []string {
		var out []string

		for pg := s.assigns.NewListForScopePager(scope, opts); pg.More(); {
			page, perr := pg.NextPage(ctx)
			require.NoError(t, perr)

			for _, a := range page.Value {
				out = append(out, *a.Name)
			}
		}

		return out
	}

	assert.ElementsMatch(t, []string{guidB, guidC}, ids(nil, authRGScope), "default: at and below")
	assert.ElementsMatch(
		t,
		[]string{guidA, guidB},
		ids(&armauthorization.RoleAssignmentsClientListForScopeOptions{Filter: new("atScope()")}, authRGScope),
	)
	// The SDK sends $filter unencoded (spaces included), which a Go server rejects; query the wire directly.
	rawNames := func(principal string) []string {
		_, body := s.raw(t, http.MethodGet, authRGScope+"/providers/Microsoft.Authorization/roleAssignments?"+
			"api-version=2022-04-01&$filter=principalId%20eq%20%27"+principal+"%27", "")

		vals, _ := body["value"].([]any)
		out := make([]string, 0, len(vals))

		for _, v := range vals {
			out = append(out, v.(map[string]any)["name"].(string)) //nolint:forcetypeassert // test shape
		}

		return out
	}

	assert.ElementsMatch(t, []string{guidA, guidB, guidC}, rawNames(guidD))
	assert.Empty(t, rawNames(guidC))
	assert.ElementsMatch(
		t,
		[]string{guidA, guidB},
		ids(
			&armauthorization.RoleAssignmentsClientListForScopeOptions{Filter: new("assignedTo('" + guidD + "')")},
			authRGScope,
		),
	)
	assert.ElementsMatch(t, []string{guidA, guidB, guidC}, ids(nil, authSubScope))

	var rgCount int

	for pg := s.assigns.NewListForResourceGroupPager(authRG, nil); pg.More(); {
		page, perr := pg.NextPage(ctx)
		require.NoError(t, perr)

		rgCount += len(page.Value)
	}

	assert.Equal(t, 2, rgCount)

	_, err = s.assigns.Delete(ctx, authAcctScope, guidC, nil)
	require.NoError(t, err)

	_, err = s.assigns.Get(ctx, authAcctScope, guidC, nil)
	assert.Equal(t, "RoleAssignmentNotFound", sdkErrCode(err))

	_, err = s.assigns.Delete(ctx, authAcctScope, guidC, nil)
	require.NoError(t, err, "delete of a missing assignment is a no-op")

	_, err = s.assigns.Create(ctx, authAcctScope, guidC, assignParams(roleBlobOwner, guidD, nil), nil)
	require.NoError(t, err)

	s.raw(t, http.MethodDelete, authAcctScope+"?api-version=2023-01-01", "")
	assert.Empty(t, ids(nil, authAcctScope), "assignments go with their resource")
}

func TestAuthorizationSDK_PrincipalTypeFromManagedIdentity(t *testing.T) {
	t.Parallel()

	s := newAuthStack(t)
	ctx := t.Context()

	id, err := s.msi.CreateOrUpdate(ctx, authRG, "id-app", armmsi.Identity{Location: new("westus")}, nil)
	require.NoError(t, err)

	got, err := s.assigns.Create(
		ctx,
		authRGScope,
		guidA,
		assignParams(roleReader, *id.Properties.PrincipalID, nil),
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, armauthorization.PrincipalTypeServicePrincipal, *got.Properties.PrincipalType)

	got, err = s.assigns.Create(ctx, authRGScope, guidB, assignParams(roleReader, guidD, nil), nil)
	require.NoError(t, err)
	assert.Equal(t, armauthorization.PrincipalTypeUser, *got.Properties.PrincipalType)
}

func TestAuthorizationSDK_Locks(t *testing.T) {
	t.Parallel()

	s := newAuthStack(t)
	ctx := t.Context()
	s.putAccount(t)

	lock := func(level armlocks.LockLevel) armlocks.ManagementLockObject {
		return armlocks.ManagementLockObject{
			Properties: &armlocks.ManagementLockProperties{Level: &level, Notes: new("n")},
		}
	}

	tests := []struct {
		obj      armlocks.ManagementLockObject
		name     string
		scope    string
		lname    string
		wantCode string
	}{
		{name: "subscription", scope: authSubScope, lname: "sublock", obj: lock(armlocks.LockLevelCanNotDelete)},
		{name: "resource group", scope: authRGScope, lname: "rglock", obj: lock(armlocks.LockLevelReadOnly)},
		{name: "resource", scope: authAcctScope, lname: "acctlock", obj: lock(armlocks.LockLevelCanNotDelete)},
		{name: "bad level", scope: authRGScope, lname: "bad", obj: lock("Other"), wantCode: "InvalidRequestContent"},
		{
			name:     "bad name",
			scope:    authRGScope,
			lname:    "a+b",
			obj:      lock(armlocks.LockLevelReadOnly),
			wantCode: "InvalidLockName",
		},
		{
			name:     "missing rg",
			scope:    authSubScope + "/resourceGroups/ghost",
			lname:    "x",
			obj:      lock(armlocks.LockLevelReadOnly),
			wantCode: "ResourceGroupNotFound",
		},
		{
			name:     "missing resource",
			scope:    authRGScope + "/providers/Microsoft.Storage/storageAccounts/ghost",
			lname:    "x",
			obj:      lock(armlocks.LockLevelReadOnly),
			wantCode: "ResourceNotFound",
		},
	}

	for _, tt := range tests {
		got, err := s.locks.CreateOrUpdateByScope(ctx, tt.scope, tt.lname, tt.obj, nil)
		if tt.wantCode != "" {
			require.Error(t, err, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(err), tt.name)

			continue
		}

		require.NoError(t, err, tt.name)
		assert.Equal(t, tt.scope+"/providers/Microsoft.Authorization/locks/"+tt.lname, *got.ID, tt.name)
		assert.Equal(t, *tt.obj.Properties.Level, *got.Properties.Level, tt.name)
	}

	got, err := s.locks.GetByScope(ctx, authRGScope, "rglock", nil)
	require.NoError(t, err)
	assert.Equal(t, armlocks.LockLevelReadOnly, *got.Properties.Level)

	_, err = s.locks.GetByScope(ctx, authRGScope, "ghost", nil)
	assert.Equal(t, "LockNotFound", sdkErrCode(err))

	_, err = s.locks.CreateOrUpdateAtResourceGroupLevel(
		ctx,
		authRG,
		"rglock",
		lock(armlocks.LockLevelCanNotDelete),
		nil,
	)
	require.NoError(t, err)

	_, err = s.locks.CreateOrUpdateAtResourceLevel(
		ctx,
		authRG,
		"Microsoft.Storage",
		"",
		"storageAccounts",
		"acctauth",
		"rl2",
		lock(armlocks.LockLevelCanNotDelete),
		nil,
	)
	require.NoError(t, err)

	names := func(scope string, filter *string) []string {
		var out []string

		opts := &armlocks.ManagementLocksClientListByScopeOptions{Filter: filter}

		for pg := s.locks.NewListByScopePager(scope, opts); pg.More(); {
			page, perr := pg.NextPage(ctx)
			require.NoError(t, perr)

			for _, l := range page.Value {
				out = append(out, *l.Name)
			}
		}

		return out
	}

	assert.ElementsMatch(t, []string{"rglock", "acctlock", "rl2"}, names(authRGScope, nil))
	assert.ElementsMatch(t, []string{"sublock", "rglock"}, names(authRGScope, new("atScope()")))
	assert.ElementsMatch(t, []string{"sublock", "rglock", "acctlock", "rl2"}, names(authSubScope, nil))

	var atSub int

	for pg := s.locks.NewListAtSubscriptionLevelPager(nil); pg.More(); {
		page, perr := pg.NextPage(ctx)
		require.NoError(t, perr)

		atSub += len(page.Value)
	}

	assert.Equal(t, 4, atSub)

	_, err = s.locks.DeleteByScope(ctx, authAcctScope, "rl2", nil)
	require.NoError(t, err)

	_, err = s.locks.DeleteByScope(ctx, authAcctScope, "rl2", nil)
	require.NoError(t, err, "delete of a missing lock is a no-op")
}

func TestAuthorizationSDK_LockEnforcement(t *testing.T) {
	t.Parallel()

	rgPath := "/subscriptions/" + aciSub + "/resourcegroups/" + authRG + "?api-version=2021-04-01"
	acctPath := authAcctScope + "?api-version=2023-01-01"
	otherPath := authRGScope + "/providers/Microsoft.Storage/storageAccounts/other?api-version=2023-01-01"

	tests := []struct {
		name       string
		lockScope  string
		level      string
		method     string
		path       string
		body       string
		wantVerb   string
		wantStatus int
	}{
		{
			name:       "delete rg cannotdelete",
			lockScope:  authRGScope,
			level:      "CanNotDelete",
			method:     http.MethodDelete,
			path:       rgPath,
			wantStatus: http.StatusConflict,
			wantVerb:   "delete",
		},
		{
			name:       "delete rg readonly",
			lockScope:  authRGScope,
			level:      "ReadOnly",
			method:     http.MethodDelete,
			path:       rgPath,
			wantStatus: http.StatusConflict,
			wantVerb:   "delete",
		},
		{
			name:       "delete rg with locked resource",
			lockScope:  authAcctScope,
			level:      "CanNotDelete",
			method:     http.MethodDelete,
			path:       rgPath,
			wantStatus: http.StatusConflict,
			wantVerb:   "delete",
		},
		{
			name:       "delete rg with sub lock",
			lockScope:  authSubScope,
			level:      "CanNotDelete",
			method:     http.MethodDelete,
			path:       rgPath,
			wantStatus: http.StatusConflict,
			wantVerb:   "delete",
		},
		{
			name:       "delete account cannotdelete",
			lockScope:  authAcctScope,
			level:      "CanNotDelete",
			method:     http.MethodDelete,
			path:       acctPath,
			wantStatus: http.StatusConflict,
			wantVerb:   "delete",
		},
		{
			name:       "delete account via rg lock",
			lockScope:  authRGScope,
			level:      "CanNotDelete",
			method:     http.MethodDelete,
			path:       acctPath,
			wantStatus: http.StatusConflict,
			wantVerb:   "delete",
		},
		{
			name:       "update account cannotdelete ok",
			lockScope:  authAcctScope,
			level:      "CanNotDelete",
			method:     http.MethodPut,
			path:       acctPath,
			body:       storageBody,
			wantStatus: http.StatusOK,
		},
		{
			name:       "update account readonly",
			lockScope:  authAcctScope,
			level:      "ReadOnly",
			method:     http.MethodPut,
			path:       acctPath,
			body:       storageBody,
			wantStatus: http.StatusConflict,
			wantVerb:   "write",
		},
		{
			name:       "patch account readonly",
			lockScope:  authAcctScope,
			level:      "ReadOnly",
			method:     http.MethodPatch,
			path:       acctPath,
			body:       `{"tags":{"a":"b"}}`,
			wantStatus: http.StatusConflict,
			wantVerb:   "write",
		},
		{
			name:       "list keys readonly",
			lockScope:  authAcctScope,
			level:      "ReadOnly",
			method:     http.MethodPost,
			path:       authAcctScope + "/listKeys?api-version=2023-01-01",
			wantStatus: http.StatusConflict,
			wantVerb:   "action",
		},
		{
			name:       "create in readonly rg",
			lockScope:  authRGScope,
			level:      "ReadOnly",
			method:     http.MethodPut,
			path:       otherPath,
			body:       storageBody,
			wantStatus: http.StatusConflict,
			wantVerb:   "write",
		},
		{
			name:       "create in cannotdelete rg",
			lockScope:  authRGScope,
			level:      "CanNotDelete",
			method:     http.MethodPut,
			path:       otherPath,
			body:       storageBody,
			wantStatus: http.StatusOK,
		},
		{
			name:       "update locked rg readonly",
			lockScope:  authRGScope,
			level:      "ReadOnly",
			method:     http.MethodPut,
			path:       rgPath,
			body:       `{"location":"westus"}`,
			wantStatus: http.StatusConflict,
			wantVerb:   "write",
		},
		{
			name:       "readonly child does not block parent write",
			lockScope:  authAcctScope,
			level:      "ReadOnly",
			method:     http.MethodPut,
			path:       rgPath,
			body:       `{"location":"westus"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "get under readonly",
			lockScope:  authRGScope,
			level:      "ReadOnly",
			method:     http.MethodGet,
			path:       acctPath,
			wantStatus: http.StatusOK,
		},
		{
			name:       "sibling unaffected",
			lockScope:  authAcctScope,
			level:      "ReadOnly",
			method:     http.MethodPut,
			path:       otherPath,
			body:       storageBody,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newAuthStack(t)
			s.putAccount(t)

			lockPath := tt.lockScope + "/providers/Microsoft.Authorization/locks/l1?api-version=2020-05-01"
			status, _ := s.raw(t, http.MethodPut, lockPath, `{"properties":{"level":"`+tt.level+`"}}`)
			require.Equal(t, http.StatusCreated, status)

			status, body := s.raw(t, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, status)

			if tt.wantVerb == "" {
				return
			}

			assert.Equal(t, "ScopeLocked", authErrCode(t, body))

			msg, _ := body["error"].(map[string]any)["message"].(string)
			assert.Contains(
				t,
				msg,
				"cannot perform "+tt.wantVerb+" operation because following scope(s) are locked: '"+tt.lockScope+"'",
			)
			assert.True(t, strings.HasSuffix(msg, "Please remove the lock and try again."))

			status, _ = s.raw(t, http.MethodGet, authRGScope+"?api-version=2021-04-01", "")
			assert.Equal(t, http.StatusOK, status, "rejected write left the group intact")
		})
	}
}

func TestAuthorizationSDK_LockLifecycle(t *testing.T) {
	t.Parallel()

	s := newAuthStack(t)
	ctx := t.Context()
	s.putAccount(t)

	level := armlocks.LockLevelReadOnly
	_, err := s.locks.CreateOrUpdateByScope(ctx, authRGScope, "ro", armlocks.ManagementLockObject{
		Properties: &armlocks.ManagementLockProperties{Level: &level},
	}, nil)
	require.NoError(t, err)

	rgDelete := "/subscriptions/" + aciSub + "/resourcegroups/" + authRG + "?api-version=2021-04-01"
	status, _ := s.raw(t, http.MethodDelete, rgDelete, "")
	require.Equal(t, http.StatusConflict, status)

	level = armlocks.LockLevelCanNotDelete
	_, err = s.locks.CreateOrUpdateByScope(ctx, authRGScope, "ro", armlocks.ManagementLockObject{
		Properties: &armlocks.ManagementLockProperties{Level: &level},
	}, nil)
	require.NoError(t, err, "locks stay manageable under a ReadOnly or CanNotDelete lock")

	_, err = s.locks.DeleteByScope(ctx, authRGScope, "ro", nil)
	require.NoError(t, err)

	status, _ = s.raw(t, http.MethodDelete, rgDelete, "")
	assert.Equal(t, http.StatusOK, status)

	status, _ = s.raw(t, http.MethodPut, rgDelete, `{"location":"westus"}`)
	require.Equal(t, http.StatusCreated, status)

	var left int

	for pg := s.locks.NewListByScopePager(authSubScope, nil); pg.More(); {
		page, perr := pg.NextPage(ctx)
		require.NoError(t, perr)

		left += len(page.Value)
	}

	assert.Zero(t, left)
}

func TestAuthorizationSDK_ResourceDeleteDropsLocks(t *testing.T) {
	t.Parallel()

	s := newAuthStack(t)
	ctx := t.Context()
	s.putAccount(t)

	level := armlocks.LockLevelCanNotDelete
	obj := armlocks.ManagementLockObject{Properties: &armlocks.ManagementLockProperties{Level: &level}}

	_, err := s.locks.CreateOrUpdateByScope(ctx, authAcctScope, "keep", obj, nil)
	require.NoError(t, err)

	status, _ := s.raw(t, http.MethodDelete, authAcctScope+"?api-version=2023-01-01", "")
	require.Equal(t, http.StatusConflict, status)

	_, err = s.locks.DeleteByScope(ctx, authAcctScope, "keep", nil)
	require.NoError(t, err)

	_, err = s.locks.CreateOrUpdateByScope(ctx, authRGScope, "rg", obj, nil)
	require.NoError(t, err)

	status, _ = s.raw(t, http.MethodDelete, authAcctScope+"?api-version=2023-01-01", "")
	require.Equal(t, http.StatusConflict, status)
}

func TestAuthorizationSDK_PolicyDefinitions(t *testing.T) {
	t.Parallel()

	s := newAuthStack(t)
	ctx := t.Context()

	allowed, err := s.defs.GetBuiltIn(ctx, "e56962a6-4747-49cd-b67b-bf8b01975c4c", nil)
	require.NoError(t, err)
	assert.Equal(t, "Allowed locations", *allowed.Properties.DisplayName)
	assert.Equal(t, armpolicy.PolicyTypeBuiltIn, *allowed.Properties.PolicyType)

	_, err = s.defs.GetBuiltIn(ctx, "ghost", nil)
	assert.Equal(t, "PolicyDefinitionNotFound", sdkErrCode(err))

	var builtins int

	for pg := s.defs.NewListBuiltInPager(&armpolicy.DefinitionsClientListBuiltInOptions{}); pg.More(); {
		page, perr := pg.NextPage(ctx)
		require.NoError(t, perr)

		builtins += len(page.Value)
	}

	assert.GreaterOrEqual(t, builtins, 10)

	rule := map[string]any{
		"if":   map[string]any{"field": "type", "equals": "Microsoft.Storage/storageAccounts"},
		"then": map[string]any{"effect": "audit"},
	}

	mode := "All"
	def := armpolicy.Definition{Properties: &armpolicy.DefinitionProperties{
		DisplayName: new("audit storage"), PolicyRule: rule, Mode: &mode, Description: new("d"),
	}}

	tests := []struct {
		def      armpolicy.Definition
		name     string
		pname    string
		wantCode string
	}{
		{name: "ok", pname: "audit-storage", def: def},
		{name: "bad name", pname: "a.b", def: def, wantCode: "InvalidRequestContent"},
		{
			name: "no rule", pname: "norule", wantCode: "InvalidPolicyRule",
			def: armpolicy.Definition{Properties: &armpolicy.DefinitionProperties{DisplayName: new("x")}},
		},
		{
			name: "no effect", pname: "noeffect", wantCode: "InvalidPolicyRule",
			def: armpolicy.Definition{
				Properties: &armpolicy.DefinitionProperties{PolicyRule: map[string]any{"if": map[string]any{}}},
			},
		},
	}

	for _, tt := range tests {
		got, cerr := s.defs.CreateOrUpdate(ctx, tt.pname, tt.def, nil)
		if tt.wantCode != "" {
			require.Error(t, cerr, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(cerr), tt.name)

			continue
		}

		require.NoError(t, cerr, tt.name)
		assert.Equal(t, armpolicy.PolicyTypeCustom, *got.Properties.PolicyType)
		assert.Equal(t, authSubScope+"/providers/Microsoft.Authorization/policyDefinitions/"+tt.pname, *got.ID)
		assert.Equal(t, "All", *got.Properties.Mode)
	}

	def2 := armpolicy.Definition{Properties: &armpolicy.DefinitionProperties{PolicyRule: rule}}
	got, err := s.defs.CreateOrUpdate(ctx, "default-mode", def2, nil)
	require.NoError(t, err)
	assert.Equal(t, "Indexed", *got.Properties.Mode)

	read, err := s.defs.Get(ctx, "audit-storage", nil)
	require.NoError(t, err)
	assert.Equal(t, "audit storage", *read.Properties.DisplayName)

	_, err = s.defs.Get(ctx, "ghost", nil)
	assert.Equal(t, "PolicyDefinitionNotFound", sdkErrCode(err))

	customOnly := 0

	customFilter := &armpolicy.DefinitionsClientListOptions{Filter: new("policyType eq 'Custom'")}

	for pg := s.defs.NewListPager(customFilter); pg.More(); {
		page, perr := pg.NextPage(ctx)
		require.NoError(t, perr)

		customOnly += len(page.Value)
	}

	assert.Equal(t, 2, customOnly)

	var topped int

	for pg := s.defs.NewListPager(&armpolicy.DefinitionsClientListOptions{Top: new(int32(3))}); pg.More(); {
		page, perr := pg.NextPage(ctx)
		require.NoError(t, perr)

		topped += len(page.Value)
	}

	assert.Equal(t, 3, topped)

	_, err = s.defs.Delete(ctx, "audit-storage", nil)
	require.NoError(t, err)

	_, err = s.defs.Get(ctx, "audit-storage", nil)
	assert.Equal(t, "PolicyDefinitionNotFound", sdkErrCode(err))

	_, err = s.defs.Delete(ctx, "audit-storage", nil)
	require.NoError(t, err)
}

func TestAuthorizationSDK_PolicySetsAndAssignments(t *testing.T) {
	t.Parallel()

	s := newAuthStack(t)
	ctx := t.Context()
	s.putAccount(t)

	rule := map[string]any{
		"if":   map[string]any{"field": "type", "equals": "Microsoft.Storage/storageAccounts"},
		"then": map[string]any{"effect": "audit"},
	}
	_, err := s.defs.CreateOrUpdate(ctx, "audit-storage", armpolicy.Definition{
		Properties: &armpolicy.DefinitionProperties{PolicyRule: rule},
	}, nil)
	require.NoError(t, err)

	defID := authSubScope + "/providers/Microsoft.Authorization/policyDefinitions/audit-storage"
	builtinID := "/providers/Microsoft.Authorization/policyDefinitions/e56962a6-4747-49cd-b67b-bf8b01975c4c"

	setTests := []struct {
		name     string
		wantCode string
		refs     []*armpolicy.DefinitionReference
	}{
		{
			name: "ok",
			refs: []*armpolicy.DefinitionReference{{PolicyDefinitionID: &defID}, {PolicyDefinitionID: &builtinID}},
		},
		{name: "empty", refs: nil, wantCode: "InvalidRequestContent"},
		{
			name: "missing ref", wantCode: "PolicyDefinitionNotFound",
			refs: []*armpolicy.DefinitionReference{
				{PolicyDefinitionID: new(authSubScope + "/providers/Microsoft.Authorization/policyDefinitions/ghost")},
			},
		},
	}

	for _, tt := range setTests {
		got, serr := s.sets.CreateOrUpdate(ctx, "set1", armpolicy.SetDefinition{
			Properties: &armpolicy.SetDefinitionProperties{PolicyDefinitions: tt.refs, DisplayName: new("set")},
		}, nil)
		if tt.wantCode != "" {
			require.Error(t, serr, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(serr), tt.name)

			continue
		}

		require.NoError(t, serr, tt.name)
		assert.Len(t, got.Properties.PolicyDefinitions, 2)
	}

	set, err := s.sets.Get(ctx, "set1", nil)
	require.NoError(t, err)
	assert.Equal(t, armpolicy.PolicyTypeCustom, *set.Properties.PolicyType)

	builtinSet, err := s.sets.GetBuiltIn(ctx, "0a2ebd47-3fb9-4735-a006-b7f31ddadd9f", nil)
	require.NoError(t, err)
	assert.Equal(t, "Allow Usage Cost Resources", *builtinSet.Properties.DisplayName)

	setID := authSubScope + "/providers/Microsoft.Authorization/policySetDefinitions/set1"

	assignTests := []struct {
		name     string
		scope    string
		aname    string
		defID    string
		mode     *armpolicy.EnforcementMode
		wantCode string
	}{
		{name: "custom def at sub", scope: authSubScope, aname: "a1", defID: defID},
		{
			name:  "builtin at rg",
			scope: authRGScope,
			aname: "a2",
			defID: builtinID,
			mode:  new(armpolicy.EnforcementModeDoNotEnforce),
		},
		{name: "set at resource", scope: authAcctScope, aname: "a3", defID: setID},
		{
			name:     "missing def",
			scope:    authSubScope,
			aname:    "a4",
			defID:    authSubScope + "/providers/Microsoft.Authorization/policyDefinitions/ghost",
			wantCode: "PolicyDefinitionNotFound",
		},
		{name: "no def", scope: authSubScope, aname: "a5", defID: "", wantCode: "InvalidRequestContent"},
		{
			name:     "missing rg",
			scope:    authSubScope + "/resourceGroups/ghost",
			aname:    "a6",
			defID:    defID,
			wantCode: "ResourceGroupNotFound",
		},
	}

	for _, tt := range assignTests {
		props := &armpolicy.AssignmentProperties{DisplayName: new(tt.aname), EnforcementMode: tt.mode}
		if tt.defID != "" {
			props.PolicyDefinitionID = &tt.defID
		}

		got, aerr := s.pAssign.Create(ctx, tt.scope, tt.aname, armpolicy.Assignment{Properties: props}, nil)
		if tt.wantCode != "" {
			require.Error(t, aerr, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(aerr), tt.name)

			continue
		}

		require.NoError(t, aerr, tt.name)
		assert.Equal(t, tt.scope+"/providers/Microsoft.Authorization/policyAssignments/"+tt.aname, *got.ID, tt.name)
		assert.Equal(t, tt.scope, *got.Properties.Scope, tt.name)
	}

	got, err := s.pAssign.Get(ctx, authRGScope, "a2", nil)
	require.NoError(t, err)
	assert.Equal(t, armpolicy.EnforcementModeDoNotEnforce, *got.Properties.EnforcementMode)

	sub, err := s.pAssign.Get(ctx, authSubScope, "a1", nil)
	require.NoError(t, err)
	assert.Equal(t, armpolicy.EnforcementModeDefault, *sub.Properties.EnforcementMode)

	_, err = s.pAssign.Get(ctx, authSubScope, "ghost", nil)
	assert.Equal(t, "PolicyAssignmentNotFound", sdkErrCode(err))

	names := func(pager func() ([]*armpolicy.Assignment, error)) []string {
		items, perr := pager()
		require.NoError(t, perr)

		out := make([]string, 0, len(items))
		for _, a := range items {
			out = append(out, *a.Name)
		}

		return out
	}

	rgList := func(filter *string) []string {
		return names(func() ([]*armpolicy.Assignment, error) {
			var all []*armpolicy.Assignment

			opts := &armpolicy.AssignmentsClientListForResourceGroupOptions{Filter: filter}

			for pg := s.pAssign.NewListForResourceGroupPager(authRG, opts); pg.More(); {
				page, perr := pg.NextPage(ctx)
				if perr != nil {
					return nil, perr
				}

				all = append(all, page.Value...)
			}

			return all, nil
		})
	}

	assert.ElementsMatch(t, []string{"a1", "a2", "a3"}, rgList(nil))
	assert.ElementsMatch(t, []string{"a1", "a2"}, rgList(new("atScope()")))
	assert.ElementsMatch(t, []string{"a2"}, rgList(new("atExactScope()")))
	assert.ElementsMatch(t, []string{"a2"}, rgList(new("policyDefinitionId eq '"+builtinID+"'")))

	upd, err := s.pAssign.Update(ctx, authSubScope, "a1", armpolicy.AssignmentUpdate{
		Location: new("westus"), Properties: &armpolicy.AssignmentUpdateProperties{},
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, "westus", *upd.Location)

	_, err = s.pAssign.Update(ctx, authSubScope, "ghost", armpolicy.AssignmentUpdate{}, nil)
	assert.Equal(t, "PolicyAssignmentNotFound", sdkErrCode(err))

	sys := armpolicy.ResourceIdentityTypeSystemAssigned
	withID, err := s.pAssign.Create(ctx, authSubScope, "a7", armpolicy.Assignment{
		Location: new("westus"), Identity: &armpolicy.Identity{Type: &sys},
		Properties: &armpolicy.AssignmentProperties{PolicyDefinitionID: &defID},
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, withID.Identity)
	assert.NotEmpty(t, *withID.Identity.PrincipalID)
	assert.NotEmpty(t, *withID.Identity.TenantID)

	again, err := s.pAssign.Create(ctx, authSubScope, "a7", armpolicy.Assignment{
		Location: new("westus"), Identity: &armpolicy.Identity{Type: &sys},
		Properties: &armpolicy.AssignmentProperties{PolicyDefinitionID: &defID},
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, *withID.Identity.PrincipalID, *again.Identity.PrincipalID, "system identity survives re-PUT")

	_, err = s.pAssign.Delete(ctx, authSubScope, "a7", nil)
	require.NoError(t, err)

	_, err = s.pAssign.Delete(ctx, authSubScope, "a7", nil)
	require.NoError(t, err)

	_, err = s.sets.Delete(ctx, "set1", nil)
	require.NoError(t, err)

	_, err = s.sets.Get(ctx, "set1", nil)
	assert.Equal(t, "PolicySetDefinitionNotFound", sdkErrCode(err))

	s.raw(t, http.MethodDelete, authAcctScope+"?api-version=2023-01-01", "")

	_, err = s.pAssign.Get(ctx, authAcctScope, "a3", nil)
	assert.Equal(t, "PolicyAssignmentNotFound", sdkErrCode(err), "assignments go with their resource")
}

func TestAuthorizationSDK_ProviderRegistered(t *testing.T) {
	t.Parallel()

	s := newAuthStack(t)

	status, body := s.raw(
		t,
		http.MethodGet,
		"/subscriptions/"+aciSub+"/providers/Microsoft.Authorization?api-version=2021-04-01",
		"",
	)
	require.Equal(t, http.StatusOK, status)

	types, _ := body["resourceTypes"].([]any)
	got := make([]string, 0, len(types))

	for _, ty := range types {
		got = append(got, ty.(map[string]any)["resourceType"].(string)) //nolint:forcetypeassert // test shape
	}

	assert.ElementsMatch(t, []string{
		"roleDefinitions", "roleAssignments", "locks", "policyDefinitions", "policySetDefinitions", "policyAssignments",
	}, got)
}

func TestAuthorizationSDK_PersistenceAndReset(t *testing.T) {
	t.Parallel()

	s := newAuthStack(t)
	ctx := t.Context()
	s.putAccount(t)

	_, err := s.roles.CreateOrUpdate(ctx, authSubScope, guidA, customRole("Persisted", []string{authSubScope},
		&armauthorization.Permission{Actions: []*string{new("*/read")}}), nil)
	require.NoError(t, err)

	_, err = s.assigns.Create(ctx, authRGScope, guidB, assignParams(guidA, guidD, nil), nil)
	require.NoError(t, err)

	level := armlocks.LockLevelReadOnly
	_, err = s.locks.CreateOrUpdateByScope(ctx, authAcctScope, "ro", armlocks.ManagementLockObject{
		Properties: &armlocks.ManagementLockProperties{Level: &level},
	}, nil)
	require.NoError(t, err)

	rule := map[string]any{
		"if":   map[string]any{"field": "type", "equals": "x"},
		"then": map[string]any{"effect": "audit"},
	}
	_, err = s.defs.CreateOrUpdate(
		ctx,
		"pd",
		armpolicy.Definition{Properties: &armpolicy.DefinitionProperties{PolicyRule: rule}},
		nil,
	)
	require.NoError(t, err)

	pdID := authSubScope + "/providers/Microsoft.Authorization/policyDefinitions/pd"
	_, err = s.sets.CreateOrUpdate(ctx, "ps", armpolicy.SetDefinition{Properties: &armpolicy.SetDefinitionProperties{
		PolicyDefinitions: []*armpolicy.DefinitionReference{{PolicyDefinitionID: &pdID}},
	}}, nil)
	require.NoError(t, err)

	_, err = s.pAssign.Create(
		ctx,
		authRGScope,
		"pa",
		armpolicy.Assignment{Properties: &armpolicy.AssignmentProperties{PolicyDefinitionID: &pdID}},
		nil,
	)
	require.NoError(t, err)

	snap := s.handler.Snapshot(ctx)
	require.NotEmpty(t, snap)

	s.handler.Reset()

	_, err = s.roles.Get(ctx, authSubScope, guidA, nil)
	assert.Equal(t, "RoleDefinitionDoesNotExist", sdkErrCode(err), "reset clears custom roles")

	require.NoError(t, s.handler.Restore(ctx, snap))

	role, err := s.roles.Get(ctx, authSubScope, guidA, nil)
	require.NoError(t, err)
	assert.Equal(t, "Persisted", *role.Properties.RoleName)

	a, err := s.assigns.Get(ctx, authRGScope, guidB, nil)
	require.NoError(t, err)
	assert.Equal(t, guidD, *a.Properties.PrincipalID)

	l, err := s.locks.GetByScope(ctx, authAcctScope, "ro", nil)
	require.NoError(t, err)
	assert.Equal(t, armlocks.LockLevelReadOnly, *l.Properties.Level)

	status, _ := s.raw(t, http.MethodDelete, authAcctScope+"?api-version=2023-01-01", "")
	assert.Equal(t, http.StatusConflict, status, "restored locks are enforced")

	_, err = s.defs.Get(ctx, "pd", nil)
	require.NoError(t, err)

	_, err = s.sets.Get(ctx, "ps", nil)
	require.NoError(t, err)

	_, err = s.pAssign.Get(ctx, authRGScope, "pa", nil)
	require.NoError(t, err)

	empty := s.handler
	empty.Reset()
	require.NoError(t, empty.Restore(ctx, empty.Snapshot(ctx)))

	_, err = s.pAssign.Get(ctx, authRGScope, "pa", nil)
	assert.Equal(t, "PolicyAssignmentNotFound", sdkErrCode(err))
}

func TestAuthorizationSDK_ListForScopeUnencodedFilter(t *testing.T) {
	t.Parallel()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := svc.(*azurearm.Handler)
	require.True(t, ok)

	port := freeEphemeralPort(t)
	h.Port = port

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	opts := &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {
				Endpoint: fmt.Sprintf("https://127.0.0.1:%d", port),
				Audience: "https://management.core.windows.net/",
			},
		}},
		Transport: &http.Client{Transport: insecureTLSTransport()},
		Retry:     policy.RetryOptions{MaxRetries: -1},
	}

	af, err := armauthorization.NewClientFactory(aciSub, aciCred{}, opts)
	require.NoError(t, err)

	client := af.NewRoleAssignmentsClient()
	ctx := t.Context()

	require.Eventually(t, func() bool {
		_, perr := client.NewListForScopePager(authSubScope, nil).NextPage(ctx)

		return perr == nil
	}, 5*time.Second, 10*time.Millisecond)

	for guid, principal := range map[string]string{guidA: guidC, guidB: guidD} {
		_, err = client.Create(ctx, authSubScope, guid, assignParams(roleReader, principal, nil), nil)
		require.NoError(t, err)
	}

	var names []string

	filter := "principalId eq '" + guidD + "'"

	for pg := client.NewListForScopePager(authSubScope, &armauthorization.RoleAssignmentsClientListForScopeOptions{
		Filter: &filter,
	}); pg.More(); {
		page, perr := pg.NextPage(ctx)
		require.NoError(t, perr)

		for _, a := range page.Value {
			names = append(names, *a.Name)
		}
	}

	assert.Equal(t, []string{guidB}, names)
}
