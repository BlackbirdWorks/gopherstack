package azurearm_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/msi/armmsi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assocNames(t *testing.T, body map[string]any) []string {
	t.Helper()

	rows, _ := body["value"].([]any)
	names := make([]string, 0, len(rows))

	for _, r := range rows {
		m, _ := r.(map[string]any)
		names = append(names, m["name"].(string))
	}

	return names
}

func TestManagedIdentitySDK_ListAssociatedResources(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	one := s.userIdentity(t, "id-one")
	two := s.userIdentity(t, "id-two")

	vmA := linuxVM(s.newNIC(t, "nic-a"))
	vmA.Identity = vmIdentity(armcompute.ResourceIdentityTypeUserAssigned, *one.ID)
	s.mustVM(t, "vm-a", vmA)

	vmB := linuxVM(s.newNIC(t, "nic-b"))
	vmB.Identity = vmIdentity(armcompute.ResourceIdentityTypeSystemAssignedUserAssigned, *one.ID, *two.ID)
	s.mustVM(t, "vm-b", vmB)

	vmC := linuxVM(s.newNIC(t, "nic-c"))
	s.mustVM(t, "vm-c", vmC)

	base := "/subscriptions/" + aciSub + "/resourceGroups/" + netRG +
		"/providers/Microsoft.ManagedIdentity/userAssignedIdentities/"
	const preview = "api-version=2022-01-31-preview"

	tests := []struct {
		name      string
		path      string
		wantNames []string
		wantTotal float64
		wantCode  int
		wantNext  bool
	}{
		{name: "all", path: base + "id-one/listAssociatedResources?" + preview, wantCode: 200,
			wantNames: []string{"vm-a", "vm-b"}, wantTotal: 2},
		{name: "second identity", path: base + "id-two/listAssociatedResources?" + preview, wantCode: 200,
			wantNames: []string{"vm-b"}, wantTotal: 1},
		{name: "older preview", path: base + "id-two/listAssociatedResources?api-version=2021-09-30-preview",
			wantCode: 200, wantNames: []string{"vm-b"}, wantTotal: 1},
		{name: "filter contains", path: base + "id-one/listAssociatedResources?" + preview +
			"&$filter=" + url.QueryEscape("contains(name, 'vm-a')"), wantCode: 200, wantNames: []string{"vm-a"}, wantTotal: 1},
		{name: "filter eq and", path: base + "id-one/listAssociatedResources?" + preview +
			"&$filter=" + url.QueryEscape("name eq 'vm-b' and resourceGroup eq '"+netRG+"'"), wantCode: 200,
			wantNames: []string{"vm-b"}, wantTotal: 1},
		{name: "orderby desc", path: base + "id-one/listAssociatedResources?" + preview + "&$orderby=" +
			url.QueryEscape("name desc"), wantCode: 200, wantNames: []string{"vm-b", "vm-a"}, wantTotal: 2},
		{name: "top and next", path: base + "id-one/listAssociatedResources?" + preview + "&$top=1", wantCode: 200,
			wantNames: []string{"vm-a"}, wantTotal: 2, wantNext: true},
		{name: "skip", path: base + "id-one/listAssociatedResources?" + preview + "&$skip=1", wantCode: 200,
			wantNames: []string{"vm-b"}, wantTotal: 2},
		{
			name:     "bad filter",
			path:     base + "id-one/listAssociatedResources?" + preview + "&$filter=bogus",
			wantCode: 400,
		},
		{name: "unsupported api version", path: base + "id-one/listAssociatedResources?api-version=2023-01-31",
			wantCode: 400},
		{name: "unknown identity", path: base + "ghost-id/listAssociatedResources?" + preview, wantCode: 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, body := s.rawJSON(t, http.MethodPost, tt.path, "")
			require.Equal(t, tt.wantCode, code)

			if tt.wantCode != http.StatusOK {
				return
			}

			assert.Equal(t, tt.wantNames, assocNames(t, body))
			assert.InDelta(t, tt.wantTotal, body["totalCount"], 0)
			assert.Equal(t, tt.wantNext, body["nextLink"] != nil)
		})
	}

	code, body := s.rawJSON(t, http.MethodPost, base+"id-two/listAssociatedResources?"+preview, "")
	require.Equal(t, http.StatusOK, code)

	row, _ := body["value"].([]any)[0].(map[string]any)
	assert.Equal(t, "microsoft.compute/virtualmachines", row["type"])
	assert.Equal(t, netRG, row["resourceGroup"])
	assert.Equal(t, aciSub, row["subscriptionId"])
	assert.Contains(t, row["id"], "/virtualMachines/vm-b")
}

func TestManagedIdentitySDK_SystemAssignedByScope(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	uai := s.userIdentity(t, "id-one")

	sys := linuxVM(s.newNIC(t, "nic-s"))
	sys.Identity = vmIdentity(armcompute.ResourceIdentityTypeSystemAssigned)
	got := s.mustVM(t, "vm-sys", sys)

	usr := linuxVM(s.newNIC(t, "nic-u"))
	usr.Identity = vmIdentity(armcompute.ResourceIdentityTypeUserAssigned, *uai.ID)
	userVM := s.mustVM(t, "vm-usr", usr)

	none := s.mustVM(t, "vm-none", linuxVM(s.newNIC(t, "nic-n")))

	c := s.msi.NewSystemAssignedIdentitiesClient()

	res, err := c.GetByScope(t.Context(), *got.ID, nil)
	require.NoError(t, err)
	assert.Equal(t, *got.Identity.PrincipalID, *res.Properties.PrincipalID)
	assert.Equal(t, *got.Identity.TenantID, *res.Properties.TenantID)
	assert.NotEmpty(t, *res.Properties.ClientID)
	assert.Contains(t, *res.Properties.ClientSecretURL, *res.Properties.ClientID)
	assert.Equal(t, "vm-sys", *res.Name)
	assert.Equal(t, "westus", *res.Location)

	again, err := c.GetByScope(t.Context(), *got.ID, nil)
	require.NoError(t, err)
	assert.Equal(t, *res.Properties.ClientID, *again.Properties.ClientID)

	for name, scope := range map[string]string{
		"user assigned only": *userVM.ID, "no identity": *none.ID,
		"missing resource": *got.ID + "-ghost",
	} {
		_, err = c.GetByScope(t.Context(), scope, nil)
		require.Error(t, err, name)
		assert.Equal(t, "ResourceNotFound", sdkErrCode(err), name)
	}
}

func TestManagedIdentitySDK_IsolationScope(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	c := s.msi.NewUserAssignedIdentitiesClient()
	regional := armmsi.IsolationScopeRegional

	tests := []struct {
		name     string
		scope    *armmsi.IsolationScope
		want     *armmsi.IsolationScope
		wantCode string
	}{
		{name: "regional", scope: &regional, want: &regional},
		{name: "unset", scope: nil, want: nil},
		{name: "invalid", scope: new(armmsi.IsolationScope("Galaxy")), wantCode: "InvalidRequestContent"},
	}

	for _, tt := range tests {
		got, err := c.CreateOrUpdate(t.Context(), netRG, "id-"+tt.name, armmsi.Identity{
			Location: new("westus"), Properties: &armmsi.UserAssignedIdentityProperties{IsolationScope: tt.scope},
		}, nil)

		if tt.wantCode != "" {
			require.Error(t, err, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(err), tt.name)

			continue
		}

		require.NoError(t, err, tt.name)
		assert.Equal(t, tt.want, got.Properties.IsolationScope, tt.name)

		read, gerr := c.Get(t.Context(), netRG, "id-"+tt.name, nil)
		require.NoError(t, gerr)
		assert.Equal(t, tt.want, read.Properties.IsolationScope, tt.name)
	}
}

func TestManagedIdentitySDK_SubscriptionScoped(t *testing.T) {
	t.Parallel()

	s := newNetStack(t)
	const otherSub = "11111111-2222-3333-4444-555555555555"

	s.rawDo(t, http.MethodPut, "/subscriptions/"+otherSub+"/resourcegroups/"+netRG, `{"location":"westus"}`)

	path := func(sub string) string {
		return "/subscriptions/" + sub + "/resourceGroups/" + netRG +
			"/providers/Microsoft.ManagedIdentity/userAssignedIdentities/shared-name?api-version=2023-01-31"
	}

	code, first := s.rawJSON(t, http.MethodPut, path(aciSub), `{"location":"westus"}`)
	require.Equal(t, http.StatusCreated, code)

	code, second := s.rawJSON(t, http.MethodPut, path(otherSub), `{"location":"eastus"}`)
	require.Equal(t, http.StatusCreated, code, "same name in another subscription is a new identity")

	principal := func(m map[string]any) string {
		p, _ := m["properties"].(map[string]any)

		return p["principalId"].(string)
	}

	assert.NotEqual(t, principal(first), principal(second))

	code, got := s.rawJSON(t, http.MethodGet, path(otherSub), "")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "eastus", got["location"])
	assert.Equal(t, principal(second), principal(got))

	code, _ = s.rawJSON(t, http.MethodDelete, path(otherSub), "")
	require.Equal(t, http.StatusOK, code)

	code, got = s.rawJSON(t, http.MethodGet, path(aciSub), "")
	require.Equal(t, http.StatusOK, code, "deleting in one subscription leaves the other")
	assert.Equal(t, principal(first), principal(got))

	code, _ = s.rawJSON(t, http.MethodGet, path(otherSub), "")
	assert.Equal(t, http.StatusNotFound, code)
}

func TestNetworkSDK_VirtualNetworkPutSubnetsKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want int
	}{
		{
			name: "omitted keeps subnets",
			body: `{"location":"westus","properties":{"addressSpace":{"addressPrefixes":["10.0.0.0/16"]}}}`,
			want: 1,
		},
		{
			name: "empty array removes subnets",
			body: `{"location":"westus","properties":{"addressSpace":{"addressPrefixes":["10.0.0.0/16"]},"subnets":[]}}`,
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newNetStack(t)
			s.vnet(t, "vnet-put", []string{"10.0.0.0/16"}, map[string]string{"s1": "10.0.1.0/24"})

			path := netProv("Microsoft.Network", "virtualNetworks", "vnet-put") + "?api-version=2024-05-01"
			code, _ := s.rawJSON(t, http.MethodPut, path, tt.body)
			require.Contains(t, []int{http.StatusOK, http.StatusCreated}, code)

			got, err := s.net.NewVirtualNetworksClient().Get(t.Context(), netRG, "vnet-put", nil)
			require.NoError(t, err)
			assert.Len(t, got.Properties.Subnets, tt.want)
		})
	}
}
