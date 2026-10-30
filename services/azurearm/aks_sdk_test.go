package azurearm_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice/v9"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const (
	aksRG  = "rg-aks"
	aksLoc = "westus2"
)

type aksStack struct {
	factory  *armcontainerservice.ClientFactory
	handler  *azurearm.Handler
	provider *azurearm.ContainerServiceProvider
	client   *http.Client
	url      string
}

func newAKSStack(t *testing.T) *aksStack {
	t.Helper()

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	return newAKSStackFor(t, h)
}

func newAKSStackFor(t *testing.T, h *azurearm.Handler) *aksStack {
	t.Helper()

	pp, ok := h.Registry.ProviderNamed("Microsoft.ContainerService")
	require.True(t, ok)

	prov, ok := pp.(*azurearm.ContainerServiceProvider)
	require.True(t, ok)

	t.Cleanup(func() { h.Shutdown(context.Background()) })

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	f, err := armcontainerservice.NewClientFactory(aciSub, aciCred{}, &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: srv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: srv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	s := &aksStack{factory: f, handler: h, provider: prov, client: srv.Client(), url: srv.URL}
	s.raw(t, http.MethodPut, "/subscriptions/"+aciSub+"/resourcegroups/"+aksRG, `{"location":"`+aksLoc+`"}`,
		"2021-04-01")

	return s
}

func (s *aksStack) raw(t *testing.T, method, path, body, apiVersion string) *http.Response {
	t.Helper()

	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}

	req, err := http.NewRequestWithContext(t.Context(), method, s.url+path+sep+"api-version="+apiVersion,
		strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	require.NoError(t, err)

	t.Cleanup(func() { _ = resp.Body.Close() })

	return resp
}

func aksPool(
	name string,
	mode armcontainerservice.AgentPoolMode,
	size string,
	count int32,
) *armcontainerservice.ManagedClusterAgentPoolProfile {
	return &armcontainerservice.ManagedClusterAgentPoolProfile{
		Name: new(name), Mode: &mode, VMSize: new(size), Count: new(count),
		OSType: new(armcontainerservice.OSTypeLinux),
	}
}

func aksCluster(name string) armcontainerservice.ManagedCluster {
	return armcontainerservice.ManagedCluster{
		Location: new(aksLoc),
		Tags:     map[string]*string{"env": new("test")},
		Identity: &armcontainerservice.ManagedClusterIdentity{
			Type: new(armcontainerservice.ResourceIdentityTypeSystemAssigned),
		},
		Properties: &armcontainerservice.ManagedClusterProperties{
			DNSPrefix:         new(name),
			KubernetesVersion: new("1.36"),
			AgentPoolProfiles: []*armcontainerservice.ManagedClusterAgentPoolProfile{
				aksPool("sys", armcontainerservice.AgentPoolModeSystem, "Standard_D4s_v5", 2),
			},
			NetworkProfile: &armcontainerservice.NetworkProfile{
				NetworkPlugin: new(armcontainerservice.NetworkPluginKubenet),
			},
		},
	}
}

func (s *aksStack) put(
	t *testing.T, name string, mod func(*armcontainerservice.ManagedCluster),
) (armcontainerservice.ManagedCluster, error) {
	t.Helper()

	mc := aksCluster(name)
	if mod != nil {
		mod(&mc)
	}

	poller, err := s.factory.NewManagedClustersClient().BeginCreateOrUpdate(t.Context(), aksRG, name, mc, nil)
	if err != nil {
		return armcontainerservice.ManagedCluster{}, err
	}

	res, err := poller.PollUntilDone(t.Context(), pollFast())
	if err != nil {
		return armcontainerservice.ManagedCluster{}, err
	}

	return res.ManagedCluster, nil
}

func (s *aksStack) mustPut(
	t *testing.T, name string, mod func(*armcontainerservice.ManagedCluster),
) armcontainerservice.ManagedCluster {
	t.Helper()

	mc, err := s.put(t, name, mod)
	require.NoError(t, err)

	return mc
}

func (s *aksStack) putPool(
	t *testing.T, cluster string, p *armcontainerservice.ManagedClusterAgentPoolProfile,
) (armcontainerservice.AgentPool, error) {
	t.Helper()

	poller, err := s.factory.NewAgentPoolsClient().BeginCreateOrUpdate(t.Context(), aksRG, cluster, *p.Name,
		armcontainerservice.AgentPool{Properties: &armcontainerservice.ManagedClusterAgentPoolProfileProperties{
			Mode: p.Mode, VMSize: p.VMSize, Count: p.Count, OSType: p.OSType, EnableAutoScaling: p.EnableAutoScaling,
			MinCount: p.MinCount, MaxCount: p.MaxCount, OrchestratorVersion: p.OrchestratorVersion,
		}}, nil)
	if err != nil {
		return armcontainerservice.AgentPool{}, err
	}

	res, err := poller.PollUntilDone(t.Context(), pollFast())
	if err != nil {
		return armcontainerservice.AgentPool{}, err
	}

	return res.AgentPool, nil
}

func (s *aksStack) deletePool(t *testing.T, pool string) error {
	t.Helper()

	poller, err := s.factory.NewAgentPoolsClient().BeginDelete(t.Context(), aksRG, "pools", pool, nil)
	if err != nil {
		return err
	}

	_, err = poller.PollUntilDone(t.Context(), pollFast())

	return err
}

func TestAKSClusterLifecycle(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	mc := s.mustPut(t, "life", nil)
	props := mc.Properties

	assert.Equal(t, "Succeeded", *props.ProvisioningState)
	assert.Equal(t, armcontainerservice.CodeRunning, *props.PowerState.Code)
	assert.Equal(t, "1.36", *props.KubernetesVersion)
	assert.Regexp(t, `^1\.36\.\d+$`, *props.CurrentKubernetesVersion)
	assert.Regexp(t, `^life-[0-9a-f]{8}\.hcp\.westus2\.azmk8s\.io$`, *props.Fqdn)
	assert.Equal(t, "MC_rg-aks_life_westus2", *props.NodeResourceGroup)
	assert.Equal(t, armcontainerservice.ResourceIdentityTypeSystemAssigned, *mc.Identity.Type)
	assert.NotEmpty(t, *mc.Identity.PrincipalID)
	assert.Equal(t, armcontainerservice.ManagedClusterSKUTierFree, *mc.SKU.Tier)
	assert.NotEmpty(t, *mc.ETag)
	require.Len(t, props.AgentPoolProfiles, 1)
	assert.Equal(t, props.CurrentKubernetesVersion, props.AgentPoolProfiles[0].CurrentOrchestratorVersion)
	assert.Equal(t, "10.244.0.0/16", *props.NetworkProfile.PodCidr)
	assert.Equal(t, "10.0.0.10", *props.NetworkProfile.DNSServiceIP)

	c := s.factory.NewManagedClustersClient()
	got, err := c.Get(t.Context(), aksRG, "life", nil)
	require.NoError(t, err)
	assert.Equal(t, *mc.ID, *got.ID)

	tagged, err := c.BeginUpdateTags(t.Context(), aksRG, "life",
		armcontainerservice.TagsObject{Tags: map[string]*string{"a": new("b")}}, nil)
	require.NoError(t, err)

	tres, err := tagged.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Equal(t, map[string]*string{"a": new("b")}, tres.Tags)

	s.mustPut(t, "life2", nil)

	var names []string

	pager := c.NewListByResourceGroupPager(aksRG, nil)
	for pager.More() {
		page, perr := pager.NextPage(t.Context())
		require.NoError(t, perr)

		for _, v := range page.Value {
			names = append(names, *v.Name)
		}
	}

	assert.Equal(t, []string{"life", "life2"}, names)

	all := c.NewListPager(nil)
	require.True(t, all.More())

	nodeRG := s.raw(
		t,
		http.MethodGet,
		"/subscriptions/"+aciSub+"/resourcegroups/MC_rg-aks_life_westus2",
		"",
		"2021-04-01",
	)
	assert.Equal(t, http.StatusOK, nodeRG.StatusCode)

	del, err := c.BeginDelete(t.Context(), aksRG, "life", nil)
	require.NoError(t, err)

	_, err = del.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	_, err = c.Get(t.Context(), aksRG, "life", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	nodeRG = s.raw(
		t,
		http.MethodGet,
		"/subscriptions/"+aciSub+"/resourcegroups/MC_rg-aks_life_westus2",
		"",
		"2021-04-01",
	)
	assert.Equal(t, http.StatusNotFound, nodeRG.StatusCode)
}

func TestAKSClusterValidation(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)

	tests := []struct {
		mod  func(*armcontainerservice.ManagedCluster)
		name string
		code string
	}{
		{name: "badname", code: "InvalidParameter", mod: nil},
		{name: "noprefix", code: "InvalidParameter", mod: func(m *armcontainerservice.ManagedCluster) {
			m.Properties.DNSPrefix = nil
		}},
		{name: "badprefix", code: "InvalidParameter", mod: func(m *armcontainerservice.ManagedCluster) {
			m.Properties.DNSPrefix = new("-bad-")
		}},
		{name: "noident", code: "InvalidParameter", mod: func(m *armcontainerservice.ManagedCluster) {
			m.Identity = nil
		}},
		{name: "nopools", code: "InvalidParameter", mod: func(m *armcontainerservice.ManagedCluster) {
			m.Properties.AgentPoolProfiles = nil
		}},
		{name: "nosystem", code: "OperationNotAllowed", mod: func(m *armcontainerservice.ManagedCluster) {
			m.Properties.AgentPoolProfiles[0].Mode = new(armcontainerservice.AgentPoolModeUser)
		}},
		{name: "badtier", code: "InvalidParameter", mod: func(m *armcontainerservice.ManagedCluster) {
			m.SKU = &armcontainerservice.ManagedClusterSKU{Tier: new(armcontainerservice.ManagedClusterSKUTier("Gold"))}
		}},
		{name: "unknownuai", code: "FailedIdentityOperation", mod: func(m *armcontainerservice.ManagedCluster) {
			m.Identity = &armcontainerservice.ManagedClusterIdentity{
				Type: new(armcontainerservice.ResourceIdentityTypeUserAssigned),
				UserAssignedIdentities: map[string]*armcontainerservice.ManagedServiceIdentityUserAssignedIdentitiesValue{
					"/subscriptions/" + aciSub + "/resourceGroups/rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/x": {},
				},
			}
		}},
		{name: "duppool", code: "InvalidParameter", mod: func(m *armcontainerservice.ManagedCluster) {
			m.Properties.AgentPoolProfiles = append(m.Properties.AgentPoolProfiles,
				aksPool("sys", armcontainerservice.AgentPoolModeUser, "Standard_D4s_v5", 1))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			name := tt.name
			if tt.name == "badname" {
				name = "bad name!"
			}

			_, err := s.put(t, name, tt.mod)
			require.Error(t, err)
			assert.Equal(t, tt.code, sdkErrCode(err))
		})
	}
}

func TestAKSKubernetesVersions(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)

	res, err := s.factory.NewManagedClustersClient().ListKubernetesVersions(t.Context(), aksLoc, nil)
	require.NoError(t, err)

	byVersion := map[string]*armcontainerservice.KubernetesVersion{}
	for _, v := range res.Values {
		byVersion[*v.Version] = v
	}

	require.Contains(t, byVersion, "1.36")
	assert.True(t, *byVersion["1.36"].IsDefault)
	assert.True(t, *byVersion["1.38"].IsPreview)
	assert.NotEmpty(t, byVersion["1.36"].PatchVersions)

	for patch, info := range byVersion["1.36"].PatchVersions {
		assert.True(t, strings.HasPrefix(patch, "1.36."))
		assert.NotEmpty(t, info.Upgrades, patch)
	}

	tests := []struct {
		name    string
		version string
		code    string
	}{
		{name: "minor", version: "1.35"},
		{name: "patch", version: "1.36.1"},
		{name: "unknownminor", version: "1.99", code: "K8sVersionNotSupported"},
		{name: "unknownpatch", version: "1.36.99", code: "K8sVersionNotSupported"},
		{name: "garbage", version: "latest", code: "K8sVersionNotSupported"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, perr := s.put(t, "ver-"+tt.name, func(m *armcontainerservice.ManagedCluster) {
				m.Properties.KubernetesVersion = new(tt.version)
			})

			if tt.code == "" {
				require.NoError(t, perr)

				return
			}

			assert.Equal(t, tt.code, sdkErrCode(perr))
		})
	}
}

func TestAKSUpgradeRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from string
		to   string
		code string
	}{
		{name: "nextminor", from: "1.35.2", to: "1.36"},
		{name: "patchup", from: "1.36.1", to: "1.36.3"},
		{name: "same", from: "1.36.3", to: "1.36.3"},
		{name: "skipminor", from: "1.34.4", to: "1.36", code: "InvalidUpgradeParameter"},
		{name: "downminor", from: "1.36.3", to: "1.35", code: "InvalidUpgradeParameter"},
		{name: "downpatch", from: "1.36.3", to: "1.36.1", code: "InvalidUpgradeParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newAKSStack(t)
			s.mustPut(
				t,
				"up",
				func(m *armcontainerservice.ManagedCluster) { m.Properties.KubernetesVersion = new(tt.from) },
			)

			next, err := s.put(t, "up", func(m *armcontainerservice.ManagedCluster) {
				m.Properties.KubernetesVersion = new(tt.to)
			})

			if tt.code != "" {
				assert.Equal(t, tt.code, sdkErrCode(err))

				return
			}

			require.NoError(t, err)
			assert.True(
				t,
				strings.HasPrefix(
					*next.Properties.CurrentKubernetesVersion,
					strings.Join(strings.Split(tt.to, ".")[:2], "."),
				),
			)
			assert.Equal(
				t,
				next.Properties.CurrentKubernetesVersion,
				next.Properties.AgentPoolProfiles[0].CurrentOrchestratorVersion,
			)
		})
	}
}

func TestAKSUpgradeProfiles(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	s.mustPut(t, "prof", func(m *armcontainerservice.ManagedCluster) { m.Properties.KubernetesVersion = new("1.36.1") })

	up, err := s.factory.NewManagedClustersClient().GetUpgradeProfile(t.Context(), aksRG, "prof", nil)
	require.NoError(t, err)

	targets := make([]string, 0, len(up.Properties.ControlPlaneProfile.Upgrades))
	for _, u := range up.Properties.ControlPlaneProfile.Upgrades {
		targets = append(targets, *u.KubernetesVersion)
	}

	assert.Equal(t, "1.36.1", *up.Properties.ControlPlaneProfile.KubernetesVersion)
	assert.Contains(t, targets, "1.36.3")
	assert.Contains(t, targets, "1.37.1")
	assert.NotContains(t, targets, "1.38.0")
	require.Len(t, up.Properties.AgentPoolProfiles, 1)

	pu, err := s.factory.NewAgentPoolsClient().GetUpgradeProfile(t.Context(), aksRG, "prof", "sys", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, *pu.Properties.LatestNodeImageVersion)

	av, err := s.factory.NewAgentPoolsClient().GetAvailableAgentPoolVersions(t.Context(), aksRG, "prof", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, av.Properties.AgentPoolVersions)
}

func TestAKSNetworkProfile(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)

	netp := func(mod func(*armcontainerservice.NetworkProfile)) func(*armcontainerservice.ManagedCluster) {
		return func(m *armcontainerservice.ManagedCluster) {
			np := &armcontainerservice.NetworkProfile{}
			mod(np)
			m.Properties.NetworkProfile = np
		}
	}

	azure := func(np *armcontainerservice.NetworkProfile) {
		np.NetworkPlugin = new(armcontainerservice.NetworkPluginAzure)
	}
	overlay := func(np *armcontainerservice.NetworkProfile) {
		azure(np)
		np.NetworkPluginMode = new(armcontainerservice.NetworkPluginModeOverlay)
	}

	tests := []struct {
		mod  func(*armcontainerservice.ManagedCluster)
		name string
		ok   bool
	}{
		{name: "kubenetdefaults", ok: true, mod: netp(func(np *armcontainerservice.NetworkProfile) {
			np.NetworkPlugin = new(armcontainerservice.NetworkPluginKubenet)
		})},
		{name: "overlay", ok: true, mod: netp(func(np *armcontainerservice.NetworkProfile) {
			overlay(np)
			np.PodCidr = new("192.168.0.0/16")
		})},
		{name: "overlaycustomsvc", ok: true, mod: netp(func(np *armcontainerservice.NetworkProfile) {
			overlay(np)
			np.ServiceCidr, np.DNSServiceIP = new("172.16.0.0/16"), new("172.16.0.53")
		})},
		{name: "azurenopod", ok: false, mod: netp(func(np *armcontainerservice.NetworkProfile) {
			azure(np)
			np.PodCidr = new("10.244.0.0/16")
		})},
		{name: "modewithkubenet", ok: false, mod: netp(func(np *armcontainerservice.NetworkProfile) {
			np.NetworkPlugin = new(armcontainerservice.NetworkPluginKubenet)
			np.NetworkPluginMode = new(armcontainerservice.NetworkPluginModeOverlay)
		})},
		{name: "badplugin", ok: false, mod: netp(func(np *armcontainerservice.NetworkProfile) {
			np.NetworkPlugin = new(armcontainerservice.NetworkPlugin("flannel"))
		})},
		{name: "badcidr", ok: false, mod: netp(func(np *armcontainerservice.NetworkProfile) {
			overlay(np)
			np.PodCidr = new("10.244.0.0/33")
		})},
		{name: "unmasked", ok: false, mod: netp(func(np *armcontainerservice.NetworkProfile) {
			overlay(np)
			np.ServiceCidr, np.DNSServiceIP = new("10.0.1.0/16"), new("10.0.0.10")
		})},
		{name: "svctoolarge", ok: false, mod: netp(func(np *armcontainerservice.NetworkProfile) {
			overlay(np)
			np.ServiceCidr, np.DNSServiceIP = new("10.0.0.0/8"), new("10.0.0.10")
		})},
		{name: "overlap", ok: false, mod: netp(func(np *armcontainerservice.NetworkProfile) {
			overlay(np)
			np.PodCidr, np.ServiceCidr, np.DNSServiceIP = new("10.0.0.0/16"), new("10.0.0.0/16"), new("10.0.0.10")
		})},
		{name: "dnsoutside", ok: false, mod: netp(func(np *armcontainerservice.NetworkProfile) {
			overlay(np)
			np.DNSServiceIP = new("10.1.0.10")
		})},
		{name: "dnsfirst", ok: false, mod: netp(func(np *armcontainerservice.NetworkProfile) {
			overlay(np)
			np.DNSServiceIP = new("10.0.0.1")
		})},
		{name: "ciliumpolicynodp", ok: false, mod: netp(func(np *armcontainerservice.NetworkProfile) {
			overlay(np)
			np.NetworkPolicy = new(armcontainerservice.NetworkPolicyCilium)
		})},
		{name: "azurepolicy", ok: true, mod: netp(func(np *armcontainerservice.NetworkProfile) {
			overlay(np)
			np.NetworkPolicy = new(armcontainerservice.NetworkPolicyAzure)
		})},
		{name: "kubenetazurepolicy", ok: false, mod: netp(func(np *armcontainerservice.NetworkProfile) {
			np.NetworkPlugin = new(armcontainerservice.NetworkPluginKubenet)
			np.NetworkPolicy = new(armcontainerservice.NetworkPolicyAzure)
		})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := s.put(t, "net-"+tt.name, tt.mod)
			if tt.ok {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Equal(t, "InvalidParameter", sdkErrCode(err))
		})
	}
}

func TestAKSNetworkImmutable(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	s.mustPut(t, "imm", nil)

	_, err := s.put(t, "imm", func(m *armcontainerservice.ManagedCluster) {
		m.Properties.NetworkProfile.PodCidr = new("172.20.0.0/16")
	})
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err))

	_, err = s.put(t, "imm", func(m *armcontainerservice.ManagedCluster) { m.Properties.DNSPrefix = new("other") })
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err))

	_, err = s.put(t, "imm", func(m *armcontainerservice.ManagedCluster) { m.Location = new("eastus") })
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err))

	_, err = s.put(t, "imm", func(m *armcontainerservice.ManagedCluster) { m.Tags = map[string]*string{"x": new("y")} })
	require.NoError(t, err)
}

func TestAKSAgentPools(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	s.mustPut(t, "pools", nil)

	user := aksPool("user1", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 1)
	got, err := s.putPool(t, "pools", user)
	require.NoError(t, err)
	assert.Equal(t, "user1", *got.Name)
	assert.Equal(t, armcontainerservice.AgentPoolModeUser, *got.Properties.Mode)
	assert.NotEmpty(t, *got.Properties.NodeImageVersion)
	assert.Equal(t, armcontainerservice.CodeRunning, *got.Properties.PowerState.Code)

	user.Count = new(int32(3))
	got, err = s.putPool(t, "pools", user)
	require.NoError(t, err)
	assert.EqualValues(t, 3, *got.Properties.Count)

	zero := aksPool("user1", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 0)
	got, err = s.putPool(t, "pools", zero)
	require.NoError(t, err)
	assert.EqualValues(t, 0, *got.Properties.Count)

	var names []string

	pager := s.factory.NewAgentPoolsClient().NewListPager(aksRG, "pools", nil)
	for pager.More() {
		page, perr := pager.NextPage(t.Context())
		require.NoError(t, perr)

		for _, v := range page.Value {
			names = append(names, *v.Name)
		}
	}

	assert.Equal(t, []string{"sys", "user1"}, names)

	mc, err := s.factory.NewManagedClustersClient().Get(t.Context(), aksRG, "pools", nil)
	require.NoError(t, err)
	assert.Len(t, mc.Properties.AgentPoolProfiles, 2)

	up, err := s.factory.NewAgentPoolsClient().BeginUpgradeNodeImageVersion(t.Context(), aksRG, "pools", "user1", nil)
	require.NoError(t, err)

	_, err = up.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	require.NoError(t, s.deletePool(t, "user1"))
	_, err = s.factory.NewAgentPoolsClient().Get(t.Context(), aksRG, "pools", "user1", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	assert.Equal(t, "OperationNotAllowed", sdkErrCode(s.deletePool(t, "sys")))

	_, err = s.putPool(t, "pools", aksPool("sys", armcontainerservice.AgentPoolModeUser, "Standard_D4s_v5", 2))
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err))

	second := aksPool("sys2", armcontainerservice.AgentPoolModeSystem, "Standard_D4s_v5", 2)
	_, err = s.putPool(t, "pools", second)
	require.NoError(t, err)
	require.NoError(t, s.deletePool(t, "sys"))
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(s.deletePool(t, "sys2")))

	_, err = s.putPool(t, "nocluster", user)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
}

func TestAKSPoolValidation(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	s.mustPut(t, "pv", nil)

	mk := aksPool

	type poolProfile = armcontainerservice.ManagedClusterAgentPoolProfile

	scale := func(p *poolProfile, minC, maxC int32) *poolProfile {
		p.EnableAutoScaling, p.MinCount, p.MaxCount = new(true), new(minC), new(maxC)

		return p
	}

	tests := []struct {
		pool *armcontainerservice.ManagedClusterAgentPoolProfile
		name string
		ok   bool
	}{
		{name: "validuser", ok: true, pool: mk("a1", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 1)},
		{
			name: "autoscale",
			ok:   true,
			pool: scale(mk("a2", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 2), 1, 5),
		},
		{
			name: "autoscalezero",
			ok:   true,
			pool: scale(mk("a3", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 0), 0, 5),
		},
		{name: "unknownsize", pool: mk("b1", armcontainerservice.AgentPoolModeUser, "Standard_Nope", 1)},
		{name: "nosize", pool: mk("b2", armcontainerservice.AgentPoolModeUser, "", 1)},
		{name: "bseriessystem", pool: mk("b3", armcontainerservice.AgentPoolModeSystem, "Standard_B4ms", 2)},
		{name: "smallsystem", pool: mk("b4", armcontainerservice.AgentPoolModeSystem, "Standard_D2s_v5", 2)},
		{name: "zerosystem", pool: mk("b5", armcontainerservice.AgentPoolModeSystem, "Standard_D4s_v5", 0)},
		{name: "toomany", pool: mk("b6", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 1001)},
		{name: "minmax", pool: scale(mk("b7", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 2), 5, 1)},
		{name: "outofrange", pool: scale(mk("b8", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 9), 1, 5)},
		{
			name: "systemmin0",
			pool: scale(mk("b9", armcontainerservice.AgentPoolModeSystem, "Standard_D4s_v5", 1), 0, 3),
		},
		{name: "minwithoutscale", pool: func() *armcontainerservice.ManagedClusterAgentPoolProfile {
			p := mk("c1", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 1)
			p.MinCount = new(int32(1))

			return p
		}()},
		{name: "uppercase", pool: mk("Bad", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 1)},
		{name: "toolong", pool: mk("abcdefghijklm", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 1)},
		{name: "windowslong", pool: func() *armcontainerservice.ManagedClusterAgentPoolProfile {
			p := mk("win1234", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 1)
			p.OSType = new(armcontainerservice.OSTypeWindows)

			return p
		}()},
		{name: "windowssystem", pool: func() *armcontainerservice.ManagedClusterAgentPoolProfile {
			p := mk("win1", armcontainerservice.AgentPoolModeSystem, "Standard_D4s_v5", 2)
			p.OSType = new(armcontainerservice.OSTypeWindows)

			return p
		}()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := s.putPool(t, "pv", tt.pool)
			if tt.ok {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Equal(t, "InvalidParameter", sdkErrCode(err))
		})
	}
}

func TestAKSPoolImmutableAndSkew(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	s.mustPut(t, "imm", nil)

	_, err := s.putPool(t, "imm", aksPool("z1", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 1))
	require.NoError(t, err)

	_, err = s.putPool(t, "imm", aksPool("z1", armcontainerservice.AgentPoolModeUser, "Standard_D4s_v5", 1))
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err))

	newer := aksPool("newer", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 1)
	newer.OrchestratorVersion = new("1.37")
	_, err = s.putPool(t, "imm", newer)
	assert.Equal(t, "InvalidUpgradeParameter", sdkErrCode(err))

	s.mustPut(t, "skew", func(m *armcontainerservice.ManagedCluster) { m.Properties.KubernetesVersion = new("1.38") })

	old := aksPool("old", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 1)
	old.OrchestratorVersion = new("1.34.4")
	_, err = s.putPool(t, "skew", old)
	assert.Equal(t, "InvalidUpgradeParameter", sdkErrCode(err))

	old.OrchestratorVersion = new("1.35.2")
	got, err := s.putPool(t, "skew", old)
	require.NoError(t, err)
	assert.Equal(t, "1.35.2", *got.Properties.CurrentOrchestratorVersion)

	old.OrchestratorVersion = new("1.35.2")
	next, err := s.put(t, "skew", func(m *armcontainerservice.ManagedCluster) {
		m.Properties.KubernetesVersion = new("1.38")
		m.Properties.AgentPoolProfiles = append(m.Properties.AgentPoolProfiles, old)
		m.Properties.AgentPoolProfiles[1].VMSize = new("Standard_D2s_v5")
	})
	require.NoError(t, err)
	assert.Equal(t, "1.35.2", *next.Properties.AgentPoolProfiles[1].CurrentOrchestratorVersion)
}

func TestAKSStartStop(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	s.mustPut(t, "power", nil)
	c := s.factory.NewManagedClustersClient()

	stop, err := c.BeginStop(t.Context(), aksRG, "power", nil)
	require.NoError(t, err)

	_, err = stop.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	got, err := c.Get(t.Context(), aksRG, "power", nil)
	require.NoError(t, err)
	assert.Equal(t, armcontainerservice.CodeStopped, *got.Properties.PowerState.Code)
	assert.Equal(t, armcontainerservice.CodeStopped, *got.Properties.AgentPoolProfiles[0].PowerState.Code)

	_, err = s.put(t, "power", nil)
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err))

	_, err = s.putPool(t, "power", aksPool("u", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 1))
	assert.Equal(t, "OperationNotAllowed", sdkErrCode(err))

	start, err := c.BeginStart(t.Context(), aksRG, "power", nil)
	require.NoError(t, err)

	_, err = start.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	got, err = c.Get(t.Context(), aksRG, "power", nil)
	require.NoError(t, err)
	assert.Equal(t, armcontainerservice.CodeRunning, *got.Properties.PowerState.Code)

	_, err = c.BeginStop(t.Context(), aksRG, "missing", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
}

type kubeconfigDoc struct {
	CurrentContext string `yaml:"current-context"`
	Clusters       []struct {
		Cluster struct {
			CA     string `yaml:"certificate-authority-data"`
			Server string `yaml:"server"`
		} `yaml:"cluster"`
		Name string `yaml:"name"`
	} `yaml:"clusters"`
	Users []struct {
		User struct {
			Token string `yaml:"token"`
		} `yaml:"user"`
		Name string `yaml:"name"`
	} `yaml:"users"`
}

func parseKubeconfig(t *testing.T, res armcontainerservice.CredentialResults) (kubeconfigDoc, []byte) {
	t.Helper()

	require.Len(t, res.Kubeconfigs, 1)

	var doc kubeconfigDoc

	require.NoError(t, yaml.Unmarshal(res.Kubeconfigs[0].Value, &doc))
	require.Len(t, doc.Clusters, 1)
	require.Len(t, doc.Users, 1)

	ca, err := base64.StdEncoding.DecodeString(doc.Clusters[0].Cluster.CA)
	require.NoError(t, err)

	return doc, ca
}

func TestAKSCredentials(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	mc := s.mustPut(t, "creds", nil)
	c := s.factory.NewManagedClustersClient()

	user, err := c.ListClusterUserCredentials(t.Context(), aksRG, "creds", nil)
	require.NoError(t, err)

	doc, ca := parseKubeconfig(t, user.CredentialResults)
	assert.Equal(t, "clusterUser", *user.Kubeconfigs[0].Name)
	assert.Equal(t, "https://"+*mc.Properties.Fqdn+":443", doc.Clusters[0].Cluster.Server)
	assert.Equal(t, "creds", doc.CurrentContext)
	assert.Equal(t, "clusterUser_rg-aks_creds", doc.Users[0].Name)
	assert.NotEmpty(t, doc.Users[0].User.Token)
	assert.Contains(t, string(ca), "BEGIN CERTIFICATE")

	admin, err := c.ListClusterAdminCredentials(t.Context(), aksRG, "creds", nil)
	require.NoError(t, err)

	adoc, aca := parseKubeconfig(t, admin.CredentialResults)
	assert.Equal(t, "clusterAdmin", *admin.Kubeconfigs[0].Name)
	assert.Equal(t, "clusterAdmin_rg-aks_creds", adoc.Users[0].Name)
	assert.Equal(t, ca, aca)

	mon, err := c.ListClusterMonitoringUserCredentials(t.Context(), aksRG, "creds", nil)
	require.NoError(t, err)
	assert.Equal(t, "clusterMonitoringUser", *mon.Kubeconfigs[0].Name)

	s.mustPut(t, "nolocal", func(m *armcontainerservice.ManagedCluster) {
		m.Properties.DisableLocalAccounts = new(true)
	})

	_, err = c.ListClusterAdminCredentials(t.Context(), aksRG, "nolocal", nil)
	assert.Equal(t, "BadRequest", sdkErrCode(err))

	_, err = c.ListClusterUserCredentials(t.Context(), aksRG, "nolocal", nil)
	require.NoError(t, err)

	_, err = c.ListClusterUserCredentials(t.Context(), aksRG, "absent", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
}

func TestAKSEtag(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	mc := s.mustPut(t, "tag", nil)

	c := s.factory.NewManagedClustersClient()

	_, err := c.BeginCreateOrUpdate(t.Context(), aksRG, "tag", aksCluster("tag"),
		&armcontainerservice.ManagedClustersClientBeginCreateOrUpdateOptions{IfMatch: new(`"stale"`)})

	var re *azcore.ResponseError

	require.ErrorAs(t, err, &re)
	assert.Equal(t, http.StatusPreconditionFailed, re.StatusCode)

	poller, err := c.BeginCreateOrUpdate(t.Context(), aksRG, "tag", aksCluster("tag"),
		&armcontainerservice.ManagedClustersClientBeginCreateOrUpdateOptions{IfMatch: mc.ETag})
	require.NoError(t, err)

	_, err = poller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	_, err = c.BeginCreateOrUpdate(t.Context(), aksRG, "tag", aksCluster("tag"),
		&armcontainerservice.ManagedClustersClientBeginCreateOrUpdateOptions{IfNoneMatch: new("*")})
	require.ErrorAs(t, err, &re)
	assert.Equal(t, http.StatusPreconditionFailed, re.StatusCode)

	resp := s.raw(t, http.MethodPut, "/subscriptions/"+aciSub+"/resourceGroups/"+aksRG+
		"/providers/Microsoft.ContainerService/managedClusters/tag2", mustJSON(t, aksCluster("tag2")), "2026-08-01")
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Azure-Asyncoperation"), "/operations/")
	assert.NotEmpty(t, resp.Header.Get("ETag"))
}

func TestAKSResourceGroupCascade(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	s.mustPut(t, "casc", nil)

	del := s.raw(t, http.MethodDelete, "/subscriptions/"+aciSub+"/resourcegroups/"+aksRG, "", "2021-04-01")
	require.Equal(t, http.StatusOK, del.StatusCode)

	nodeRG := s.raw(
		t,
		http.MethodGet,
		"/subscriptions/"+aciSub+"/resourcegroups/MC_rg-aks_casc_westus2",
		"",
		"2021-04-01",
	)
	assert.Equal(t, http.StatusNotFound, nodeRG.StatusCode)

	pager := s.factory.NewManagedClustersClient().NewListPager(nil)
	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)
		assert.Empty(t, page.Value)
	}
}

func TestAKSSnapshotRestore(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	s.mustPut(t, "snap", nil)

	_, err := s.putPool(t, "snap", aksPool("u1", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 2))
	require.NoError(t, err)

	creds, err := s.factory.NewManagedClustersClient().ListClusterUserCredentials(t.Context(), aksRG, "snap", nil)
	require.NoError(t, err)

	before, err := s.factory.NewManagedClustersClient().Get(t.Context(), aksRG, "snap", nil)
	require.NoError(t, err)

	data := s.handler.Snapshot(t.Context())
	require.NotEmpty(t, data)

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h2, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	s2 := newAKSStackFor(t, h2)
	require.NoError(t, h2.Restore(t.Context(), data))

	after, err := s2.factory.NewManagedClustersClient().Get(t.Context(), aksRG, "snap", nil)
	require.NoError(t, err)
	assert.Equal(t, *before.Properties.Fqdn, *after.Properties.Fqdn)
	assert.Equal(t, *before.Properties.NodeResourceGroup, *after.Properties.NodeResourceGroup)
	assert.Equal(t, *before.Properties.CurrentKubernetesVersion, *after.Properties.CurrentKubernetesVersion)
	assert.Len(t, after.Properties.AgentPoolProfiles, 2)
	assert.Equal(t, *before.Identity.PrincipalID, *after.Identity.PrincipalID)

	again, err := s2.factory.NewManagedClustersClient().ListClusterUserCredentials(t.Context(), aksRG, "snap", nil)
	require.NoError(t, err)
	assert.Equal(t, creds.Kubeconfigs[0].Value, again.Kubeconfigs[0].Value)

	pool, err := s2.factory.NewAgentPoolsClient().Get(t.Context(), aksRG, "snap", "u1", nil)
	require.NoError(t, err)
	assert.EqualValues(t, 2, *pool.Properties.Count)
}

func TestAKSSubnetReferences(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	vnet := `{"location":"westus2","properties":{"addressSpace":{"addressPrefixes":["10.1.0.0/16"]},` +
		`"subnets":[{"name":"nodes","properties":{"addressPrefix":"10.1.0.0/24"}}]}}`
	resp := s.raw(t, http.MethodPut, "/subscriptions/"+aciSub+"/resourceGroups/"+aksRG+
		"/providers/Microsoft.Network/virtualNetworks/vn", vnet, "2024-07-01")
	require.Less(t, resp.StatusCode, 300)

	subnet := func(name string) string {
		return "/subscriptions/" + aciSub + "/resourceGroups/" + aksRG +
			"/providers/Microsoft.Network/virtualNetworks/vn/subnets/" + name
	}

	tests := []struct {
		mod  func(*armcontainerservice.ManagedCluster)
		name string
		ok   bool
	}{
		{name: "known", ok: true, mod: func(m *armcontainerservice.ManagedCluster) {
			m.Properties.AgentPoolProfiles[0].VnetSubnetID = new(subnet("nodes"))
		}},
		{name: "missingsubnet", mod: func(m *armcontainerservice.ManagedCluster) {
			m.Properties.AgentPoolProfiles[0].VnetSubnetID = new(subnet("nope"))
		}},
		{name: "unknownvnet", ok: true, mod: func(m *armcontainerservice.ManagedCluster) {
			m.Properties.AgentPoolProfiles[0].VnetSubnetID = new(
				"/subscriptions/" + aciSub + "/resourceGroups/elsewhere/providers/Microsoft.Network/virtualNetworks/x/subnets/y",
			)
		}},
		{name: "notasubnet", mod: func(m *armcontainerservice.ManagedCluster) {
			m.Properties.AgentPoolProfiles[0].VnetSubnetID = new("subnet")
		}},
		{name: "podoverlap", mod: func(m *armcontainerservice.ManagedCluster) {
			m.Properties.AgentPoolProfiles[0].VnetSubnetID = new(subnet("nodes"))
			m.Properties.NetworkProfile.PodCidr = new("10.1.0.0/16")
		}},
		{name: "svcoverlap", mod: func(m *armcontainerservice.ManagedCluster) {
			m.Properties.AgentPoolProfiles[0].VnetSubnetID = new(subnet("nodes"))
			m.Properties.NetworkProfile.ServiceCidr = new("10.1.0.0/20")
			m.Properties.NetworkProfile.DNSServiceIP = new("10.1.0.10")
		}},
		{name: "twovnets", mod: func(m *armcontainerservice.ManagedCluster) {
			m.Properties.AgentPoolProfiles[0].VnetSubnetID = new(subnet("nodes"))
			other := aksPool("other", armcontainerservice.AgentPoolModeUser, "Standard_D2s_v5", 1)
			other.VnetSubnetID = new(
				"/subscriptions/" + aciSub + "/resourceGroups/elsewhere/providers/Microsoft.Network/virtualNetworks/x/subnets/y",
			)
			m.Properties.AgentPoolProfiles = append(m.Properties.AgentPoolProfiles, other)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := s.put(t, "sn-"+tt.name, tt.mod)
			if tt.ok {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, "InvalidParameter", sdkErrCode(err))
		})
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()

	b, err := json.Marshal(v)
	require.NoError(t, err)

	return string(b)
}
