package azurearm_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appservice/armappservice/v6"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v9"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerinstance/armcontainerinstance/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/msi/armmsi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

// fakeFunctionApps accepts every app; the rest of the data-plane surface is never reached by these tests.
type fakeFunctionApps struct{ azurearm.FunctionApps }

func (fakeFunctionApps) PutApp(name string, _ azurearm.FunctionAppConfig) (azurearm.FunctionAppInfo, error) {
	return azurearm.FunctionAppInfo{Created: time.Now(), HostName: name + ".local", SCMHost: name + ".scm.local"}, nil
}

func (fakeFunctionApps) DeleteApp(string) error { return nil }

func (fakeFunctionApps) AppInfo(name string) (azurearm.FunctionAppInfo, bool) {
	return azurearm.FunctionAppInfo{Created: time.Now(), HostName: name + ".local", SCMHost: name + ".scm.local"}, true
}

func slug(s string) string { return strings.ReplaceAll(s, " ", "-") }

func (s *cmpStack) userIdentity(t *testing.T, name string) armmsi.Identity {
	t.Helper()

	got, err := s.msi.NewUserAssignedIdentitiesClient().CreateOrUpdate(t.Context(), netRG, name,
		armmsi.Identity{Location: new("westus")}, nil)
	require.NoError(t, err)

	return got.Identity
}

func vmIdentity(typ armcompute.ResourceIdentityType, uai ...string) *armcompute.VirtualMachineIdentity {
	id := &armcompute.VirtualMachineIdentity{Type: new(typ)}

	if len(uai) > 0 {
		id.UserAssignedIdentities = map[string]*armcompute.UserAssignedIdentitiesValue{}
		for _, u := range uai {
			id.UserAssignedIdentities[u] = &armcompute.UserAssignedIdentitiesValue{}
		}
	}

	return id
}

func TestIdentitySDK_VirtualMachine(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	uai := s.userIdentity(t, "id-one")
	other := s.userIdentity(t, "id-two")
	missing := *uai.ID + "-ghost"

	tests := []struct {
		identity  *armcompute.VirtualMachineIdentity
		name      string
		wantCode  string
		wantType  string
		wantUAIs  []armmsi.Identity
		wantAssig bool
	}{
		{
			name: "system", identity: vmIdentity(armcompute.ResourceIdentityTypeSystemAssigned),
			wantType: "SystemAssigned", wantAssig: true,
		},
		{
			name: "user", identity: vmIdentity(armcompute.ResourceIdentityTypeUserAssigned, *uai.ID),
			wantType: "UserAssigned", wantUAIs: []armmsi.Identity{uai},
		},
		{
			name:      "both",
			identity:  vmIdentity(armcompute.ResourceIdentityTypeSystemAssignedUserAssigned, *uai.ID, *other.ID),
			wantType:  "SystemAssigned, UserAssigned",
			wantAssig: true,
			wantUAIs:  []armmsi.Identity{uai, other},
		},
		{name: "none", identity: vmIdentity(armcompute.ResourceIdentityTypeNone), wantType: ""},
		{name: "no identity", identity: nil, wantType: ""},
		{
			name: "user without identities", identity: vmIdentity(armcompute.ResourceIdentityTypeUserAssigned),
			wantCode: "InvalidParameter",
		},
		{
			name:     "system with identities",
			identity: vmIdentity(armcompute.ResourceIdentityTypeSystemAssigned, *uai.ID),
			wantCode: "InvalidParameter",
		},
		{
			name: "unknown identity", identity: vmIdentity(armcompute.ResourceIdentityTypeUserAssigned, missing),
			wantCode: "FailedIdentityOperation",
		},
		{
			name:     "bad type",
			identity: vmIdentity(armcompute.ResourceIdentityType("Sometimes")),
			wantCode: "InvalidParameter",
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vm := linuxVM(s.newNIC(t, "nic-"+slug(tt.name)))
			vm.Identity = tt.identity
			vm.Properties.OSProfile.ComputerName = new("h")

			got, err := s.putVM(t, netRG, "vm-"+slug(tt.name), vm)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err, i)

			if tt.wantType == "" {
				assert.True(t, got.Identity == nil || got.Identity.Type == nil ||
					*got.Identity.Type == armcompute.ResourceIdentityTypeNone)

				return
			}

			require.NotNil(t, got.Identity)
			assert.Equal(t, tt.wantType, string(*got.Identity.Type))

			if tt.wantAssig {
				assert.NotEmpty(t, *got.Identity.PrincipalID)
				assert.NotEmpty(t, *got.Identity.TenantID)
			} else {
				assert.Nil(t, got.Identity.PrincipalID)
			}

			require.Len(t, got.Identity.UserAssignedIdentities, len(tt.wantUAIs))

			for _, want := range tt.wantUAIs {
				v := got.Identity.UserAssignedIdentities[*want.ID]
				require.NotNil(t, v)
				assert.Equal(t, *want.Properties.PrincipalID, *v.PrincipalID)
				assert.Equal(t, *want.Properties.ClientID, *v.ClientID)
			}
		})
	}
}

func TestIdentitySDK_SystemAssignedLifecycle(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	vm := linuxVM(s.newNIC(t, "nic1"))
	vm.Identity = vmIdentity(armcompute.ResourceIdentityTypeSystemAssigned)

	first := s.mustVM(t, "vm1", vm)
	principal := *first.Identity.PrincipalID
	require.NotEmpty(t, principal)

	again := s.mustVM(t, "vm1", vm)
	assert.Equal(t, principal, *again.Identity.PrincipalID, "the system principal is generated once")
	assert.Equal(t, *first.Identity.TenantID, *again.Identity.TenantID)

	tagged := again
	tagged.Tags = map[string]*string{"a": new("b")}
	tagged.Identity = nil
	kept := s.mustVM(t, "vm1", tagged)
	assert.Equal(t, principal, *kept.Identity.PrincipalID, "an omitted identity block leaves the identity alone")

	vm.Identity = vmIdentity(armcompute.ResourceIdentityTypeNone)
	removed := s.mustVM(t, "vm1", vm)
	assert.True(t, removed.Identity == nil || *removed.Identity.Type == armcompute.ResourceIdentityTypeNone)

	vm.Identity = vmIdentity(armcompute.ResourceIdentityTypeSystemAssigned)
	renewed := s.mustVM(t, "vm1", vm)
	assert.NotEqual(t, principal, *renewed.Identity.PrincipalID, "re-enabling issues a new principal")

	pl, err := s.cf.NewVirtualMachinesClient().BeginUpdate(t.Context(), netRG, "vm1", armcompute.VirtualMachineUpdate{
		Identity: vmIdentity(armcompute.ResourceIdentityTypeNone),
	}, nil)
	require.NoError(t, err)

	patched, err := pl.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.True(t, patched.Identity == nil || *patched.Identity.Type == armcompute.ResourceIdentityTypeNone)
}

func TestIdentitySDK_ScaleSet(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	ss := s.scaleSet(t, 1)
	ss.Identity = &armcompute.VirtualMachineScaleSetIdentity{Type: new(armcompute.ResourceIdentityTypeSystemAssigned)}

	got, err := s.putScaleSet(t, "vmss1", ss)
	require.NoError(t, err)
	assert.NotEmpty(t, *got.Identity.PrincipalID)
	assert.NotEmpty(t, *got.Identity.TenantID)
}

func (s *cmpStack) appOpts() *arm.ClientOptions {
	return &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: s.url, Audience: "https://management.core.windows.net/"},
		}},
		Transport: s.hc,
		Retry:     policy.RetryOptions{MaxRetries: -1},
	}
}

func TestIdentitySDK_WebSite(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	s.handler.Registry.SetFunctionApps(fakeFunctionApps{})

	uai := s.userIdentity(t, "id-web")

	f, err := armappservice.NewClientFactory(aciSub, aciCred{}, s.appOpts())
	require.NoError(t, err)

	planPoller, err := f.NewPlansClient().BeginCreateOrUpdate(t.Context(), netRG, "plan1", armappservice.Plan{
		Location: new("westus"), Kind: new("linux"),
		SKU:        &armappservice.SKUDescription{Name: new("B1")},
		Properties: &armappservice.PlanProperties{Reserved: new(true)},
	}, nil)
	require.NoError(t, err)

	plan, err := planPoller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	site := func(identity *armappservice.ManagedServiceIdentity) armappservice.Site {
		return armappservice.Site{
			Location: new("westus"), Kind: new("app,linux"), Identity: identity,
			Properties: &armappservice.SiteProperties{
				ServerFarmID: plan.ID,
				SiteConfig:   &armappservice.SiteConfig{LinuxFxVersion: new("NODE|20-lts")},
			},
		}
	}
	put := func(identity *armappservice.ManagedServiceIdentity) (armappservice.Site, error) {
		pl, perr := f.NewWebAppsClient().BeginCreateOrUpdate(t.Context(), netRG, "site1", site(identity), nil)
		if perr != nil {
			return armappservice.Site{}, perr
		}

		res, rerr := pl.PollUntilDone(t.Context(), pollFast())

		return res.Site, rerr
	}

	sys, err := put(
		&armappservice.ManagedServiceIdentity{Type: new(armappservice.ManagedServiceIdentityTypeSystemAssigned)},
	)
	require.NoError(t, err)
	require.NotNil(t, sys.Identity)
	assert.NotEmpty(t, *sys.Identity.PrincipalID)
	assert.NotEmpty(t, *sys.Identity.TenantID)

	both, err := put(&armappservice.ManagedServiceIdentity{
		Type:                   new(armappservice.ManagedServiceIdentityTypeSystemAssignedUserAssigned),
		UserAssignedIdentities: map[string]*armappservice.UserAssignedIdentity{*uai.ID: {}},
	})
	require.NoError(t, err)
	assert.Equal(t, *sys.Identity.PrincipalID, *both.Identity.PrincipalID)
	assert.Equal(t, *uai.Properties.ClientID, *both.Identity.UserAssignedIdentities[*uai.ID].ClientID)
	assert.Equal(t, *uai.Properties.PrincipalID, *both.Identity.UserAssignedIdentities[*uai.ID].PrincipalID)

	_, err = put(&armappservice.ManagedServiceIdentity{
		Type:                   new(armappservice.ManagedServiceIdentityTypeUserAssigned),
		UserAssignedIdentities: map[string]*armappservice.UserAssignedIdentity{*uai.ID + "-ghost": {}},
	})
	require.Error(t, err)
	assert.Equal(t, "FailedIdentityOperation", sdkErrCode(err))

	none, err := put(&armappservice.ManagedServiceIdentity{Type: new(armappservice.ManagedServiceIdentityTypeNone)})
	require.NoError(t, err)
	assert.True(t, none.Identity == nil || *none.Identity.Type == armappservice.ManagedServiceIdentityTypeNone)
}

func TestIdentitySDK_ContainerGroup(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)

	mf, err := armmsi.NewClientFactory(aciSub, aciCred{}, &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: s.url, Audience: "https://management.core.windows.net/"},
		}},
		Transport: s.client,
		Retry:     policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	uai, err := mf.NewUserAssignedIdentitiesClient().CreateOrUpdate(t.Context(), "rg", "id-aci",
		armmsi.Identity{Location: new("westus")}, nil)
	require.NoError(t, err)

	put := func(identity *armcontainerinstance.ContainerGroupIdentity) (*armcontainerinstance.ContainerGroup, error) {
		g := aciOpts{}.group()
		g.Identity = identity
		pl, perr := s.factory.NewContainerGroupsClient().BeginCreateOrUpdate(t.Context(), "rg", "grp1", g, nil)
		if perr != nil {
			return nil, perr
		}

		res, rerr := pl.PollUntilDone(t.Context(), pollFast())

		return &res.ContainerGroup, rerr
	}

	sys, err := put(
		&armcontainerinstance.ContainerGroupIdentity{
			Type: new(armcontainerinstance.ResourceIdentityTypeSystemAssigned),
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, *sys.Identity.PrincipalID)
	assert.NotEmpty(t, *sys.Identity.TenantID)

	both, err := put(&armcontainerinstance.ContainerGroupIdentity{
		Type: new(armcontainerinstance.ResourceIdentityTypeSystemAssignedUserAssigned),
		UserAssignedIdentities: map[string]*armcontainerinstance.UserAssignedIdentities{
			*uai.ID: {},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, *sys.Identity.PrincipalID, *both.Identity.PrincipalID)
	assert.Equal(t, *uai.Properties.ClientID, *both.Identity.UserAssignedIdentities[*uai.ID].ClientID)

	got, err := s.factory.NewContainerGroupsClient().Get(t.Context(), "rg", "grp1", nil)
	require.NoError(t, err)
	assert.Equal(t, *sys.Identity.PrincipalID, *got.Identity.PrincipalID)

	_, err = put(&armcontainerinstance.ContainerGroupIdentity{
		Type:                   new(armcontainerinstance.ResourceIdentityTypeUserAssigned),
		UserAssignedIdentities: map[string]*armcontainerinstance.UserAssignedIdentities{*uai.ID + "-ghost": {}},
	})
	require.Error(t, err)
	assert.Equal(t, "FailedIdentityOperation", sdkErrCode(err))

	none, err := put(
		&armcontainerinstance.ContainerGroupIdentity{Type: new(armcontainerinstance.ResourceIdentityTypeNone)},
	)
	require.NoError(t, err)
	assert.True(t, none.Identity == nil || *none.Identity.Type == armcontainerinstance.ResourceIdentityTypeNone)
}

func TestIdentitySDK_WebSiteLegacySnapshot(t *testing.T) {
	t.Parallel()

	s := newCmpStack(t)
	s.handler.Registry.SetFunctionApps(fakeFunctionApps{})

	f, err := armappservice.NewClientFactory(aciSub, aciCred{}, s.appOpts())
	require.NoError(t, err)

	planPoller, err := f.NewPlansClient().BeginCreateOrUpdate(t.Context(), netRG, "plan1", armappservice.Plan{
		Location: new("westus"), Kind: new("linux"), SKU: &armappservice.SKUDescription{Name: new("B1")},
		Properties: &armappservice.PlanProperties{Reserved: new(true)},
	}, nil)
	require.NoError(t, err)

	plan, err := planPoller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	sitePoller, err := f.NewWebAppsClient().BeginCreateOrUpdate(t.Context(), netRG, "site1", armappservice.Site{
		Location: new("westus"), Kind: new("app,linux"),
		Properties: &armappservice.SiteProperties{
			ServerFarmID: plan.ID, SiteConfig: &armappservice.SiteConfig{LinuxFxVersion: new("NODE|20-lts")},
		},
	}, nil)
	require.NoError(t, err)

	_, err = sitePoller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	var snap map[string]any
	require.NoError(t, json.Unmarshal(s.handler.Snapshot(t.Context()), &snap))

	sites, _ := snap["web"].(map[string]any)["sites"].(map[string]any)
	require.Len(t, sites, 1)

	for _, site := range sites {
		m, _ := site.(map[string]any)
		delete(m, "managedIdentity")
		m["identity"] = map[string]any{"type": "SystemAssigned"}
	}

	legacy, err := json.Marshal(snap)
	require.NoError(t, err)

	dst := newCmpStack(t)
	dst.handler.Registry.SetFunctionApps(fakeFunctionApps{})
	require.NoError(t, dst.handler.Restore(t.Context(), legacy))

	df, err := armappservice.NewClientFactory(aciSub, aciCred{}, dst.appOpts())
	require.NoError(t, err)

	got, err := df.NewWebAppsClient().Get(t.Context(), netRG, "site1", nil)
	require.NoError(t, err)
	require.NotNil(t, got.Identity)
	assert.Equal(t, "SystemAssigned", string(*got.Identity.Type))
	assert.NotEmpty(t, *got.Identity.PrincipalID, "a restored system identity gets a principal")
}
