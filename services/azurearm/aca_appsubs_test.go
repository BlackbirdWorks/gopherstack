package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appcontainers/armappcontainers/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestACA_AuthConfigs(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	env := s.mustEnv(t, "rg", "env1")
	s.mustApp(t, "rg", "app1", *env.ID, acaApp{image: "nginx"})

	auth := s.factory.NewContainerAppsAuthConfigsClient()

	tests := []struct {
		cfg      armappcontainers.AuthConfig
		name     string
		cfgName  string
		wantCode string
	}{
		{
			name:    "ok",
			cfgName: "current",
			cfg: armappcontainers.AuthConfig{Properties: &armappcontainers.AuthConfigProperties{
				Platform: &armappcontainers.AuthPlatform{Enabled: new(true)},
				GlobalValidation: &armappcontainers.GlobalValidation{
					UnauthenticatedClientAction: new(armappcontainers.UnauthenticatedClientActionV2Return401),
				},
				HTTPSettings: &armappcontainers.HTTPSettings{RequireHTTPS: new(true)},
			}},
		},
		{name: "wrong name", cfgName: "other", wantCode: "InvalidRequestParameterWithDetails",
			cfg: armappcontainers.AuthConfig{Properties: &armappcontainers.AuthConfigProperties{}}},
		{name: "bad action", cfgName: "current", wantCode: "InvalidRequestParameterWithDetails",
			cfg: armappcontainers.AuthConfig{Properties: &armappcontainers.AuthConfigProperties{
				GlobalValidation: &armappcontainers.GlobalValidation{
					UnauthenticatedClientAction: new(armappcontainers.UnauthenticatedClientActionV2("Explode")),
				},
			}}},
	}

	for _, tt := range tests {
		_, err := auth.CreateOrUpdate(t.Context(), "rg", "app1", tt.cfgName, tt.cfg, nil)
		if tt.wantCode != "" {
			require.Error(t, err, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(err), tt.name)

			continue
		}

		require.NoError(t, err, tt.name)
	}

	_, err := auth.CreateOrUpdate(t.Context(), "rg", "ghost", "current", armappcontainers.AuthConfig{
		Properties: &armappcontainers.AuthConfigProperties{},
	}, nil)
	require.Error(t, err)

	got, err := auth.Get(t.Context(), "rg", "app1", "current", nil)
	require.NoError(t, err)
	assert.Equal(t, "Microsoft.App/containerApps/authConfigs", *got.Type)
	assert.True(t, *got.Properties.Platform.Enabled)

	n := 0

	for pager := auth.NewListByContainerAppPager("rg", "app1", nil); pager.More(); {
		page, perr := pager.NextPage(t.Context())
		require.NoError(t, perr)

		n += len(page.Value)
	}

	assert.Equal(t, 1, n)

	other := newACAStack(t)
	require.NoError(t, other.handler.Restore(t.Context(), s.handler.Snapshot(t.Context())))

	back, err := other.factory.NewContainerAppsAuthConfigsClient().Get(t.Context(), "rg", "app1", "current", nil)
	require.NoError(t, err)
	assert.True(t, *back.Properties.HTTPSettings.RequireHTTPS)

	_, err = auth.Delete(t.Context(), "rg", "app1", "current", nil)
	require.NoError(t, err)

	_, err = auth.Get(t.Context(), "rg", "app1", "current", nil)
	require.Error(t, err)
}

func TestACA_SourceControls(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	env := s.mustEnv(t, "rg", "env1")
	s.mustApp(t, "rg", "app1", *env.ID, acaApp{image: "nginx"})

	sc := s.factory.NewContainerAppsSourceControlsClient()

	put := func(name, repo string) error {
		p, err := sc.BeginCreateOrUpdate(t.Context(), "rg", "app1", name, armappcontainers.SourceControl{
			Properties: &armappcontainers.SourceControlProperties{
				RepoURL: new(repo), Branch: new("main"),
				GithubActionConfiguration: &armappcontainers.GithubActionConfiguration{
					ContextPath: new("."), Image: new("app"),
					RegistryInfo: &armappcontainers.RegistryInfo{
						RegistryURL: new("r.azurecr.io"), RegistryUserName: new("u"), RegistryPassword: new("secret"),
					},
					AzureCredentials: &armappcontainers.AzureCredentials{
						ClientID: new("c"), ClientSecret: new("secret"), TenantID: new("t"),
					},
				},
			},
		}, nil)
		if err != nil {
			return err
		}

		_, err = p.PollUntilDone(t.Context(), pollFast())

		return err
	}

	require.Error(t, put("other", "https://github.com/o/r"))
	require.Error(t, put("current", ""))
	require.Error(t, put("current", "not a url"))
	require.NoError(t, put("current", "https://github.com/o/r"))

	got, err := sc.Get(t.Context(), "rg", "app1", "current", nil)
	require.NoError(t, err)
	assert.Equal(t, "https://github.com/o/r", *got.Properties.RepoURL)
	assert.Equal(t, armappcontainers.SourceControlOperationStateSucceeded, *got.Properties.OperationState)
	assert.Nil(t, got.Properties.GithubActionConfiguration.RegistryInfo.RegistryPassword)
	assert.Nil(t, got.Properties.GithubActionConfiguration.AzureCredentials.ClientSecret)
	assert.Equal(t, "r.azurecr.io", *got.Properties.GithubActionConfiguration.RegistryInfo.RegistryURL)

	dp, err := sc.BeginDelete(t.Context(), "rg", "app1", "current", nil)
	require.NoError(t, err)
	_, err = dp.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	_, err = sc.Get(t.Context(), "rg", "app1", "current", nil)
	require.Error(t, err)
}

func TestACA_SessionPools(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	env := s.mustEnv(t, "rg", "env1")
	pools := s.factory.NewContainerAppsSessionPoolsClient()

	pool := func(mod func(*armappcontainers.SessionPoolProperties)) armappcontainers.SessionPool {
		props := &armappcontainers.SessionPoolProperties{
			EnvironmentID: env.ID, ContainerType: new(armappcontainers.ContainerTypePythonLTS),
			PoolManagementType: new(armappcontainers.PoolManagementTypeDynamic),
			ScaleConfiguration: &armappcontainers.ScaleConfiguration{
				MaxConcurrentSessions: new(int32(5)),
			},
			SessionNetworkConfiguration: &armappcontainers.SessionNetworkConfiguration{
				Status: new(armappcontainers.SessionNetworkStatusEgressDisabled),
			},
			Secrets: []*armappcontainers.SessionPoolSecret{{Name: new("k"), Value: new("v")}},
		}
		if mod != nil {
			mod(props)
		}

		return armappcontainers.SessionPool{Location: new("local"), Properties: props}
	}

	tests := []struct {
		mod      func(*armappcontainers.SessionPoolProperties)
		name     string
		wantCode string
	}{
		{name: "ok"},
		{
			name:     "ghost env",
			wantCode: "ManagedEnvironmentNotFound",
			mod: func(p *armappcontainers.SessionPoolProperties) {
				p.EnvironmentID = new(*env.ID + "x")
			},
		},
		{
			name:     "no type",
			wantCode: "InvalidRequestParameterWithDetails",
			mod: func(p *armappcontainers.SessionPoolProperties) {
				p.ContainerType = nil
			},
		},
		{
			name:     "custom without template",
			wantCode: "InvalidRequestParameterWithDetails",
			mod: func(p *armappcontainers.SessionPoolProperties) {
				p.ContainerType = new(armappcontainers.ContainerTypeCustomContainer)
			},
		},
		{
			name:     "template on python",
			wantCode: "InvalidRequestParameterWithDetails",
			mod: func(p *armappcontainers.SessionPoolProperties) {
				p.CustomContainerTemplate = &armappcontainers.CustomContainerTemplate{}
			},
		},
		{
			name:     "dynamic config on manual",
			wantCode: "InvalidRequestParameterWithDetails",
			mod: func(p *armappcontainers.SessionPoolProperties) {
				p.PoolManagementType = new(armappcontainers.PoolManagementTypeManual)
				p.DynamicPoolConfiguration = &armappcontainers.DynamicPoolConfiguration{}
			},
		},
		{
			name:     "zero sessions",
			wantCode: "InvalidRequestParameterWithDetails",
			mod: func(p *armappcontainers.SessionPoolProperties) {
				p.ScaleConfiguration.MaxConcurrentSessions = new(int32(0))
			},
		},
	}

	for _, tt := range tests {
		p, err := pools.BeginCreateOrUpdate(t.Context(), "rg", "pool1", pool(tt.mod), nil)
		if err == nil {
			_, err = p.PollUntilDone(t.Context(), pollFast())
		}

		if tt.wantCode != "" {
			require.Error(t, err, tt.name)
			assert.Equal(t, tt.wantCode, sdkErrCode(err), tt.name)

			continue
		}

		require.NoError(t, err, tt.name)
	}

	got, err := pools.Get(t.Context(), "rg", "pool1", nil)
	require.NoError(t, err)
	assert.Equal(t, "https://local.dynamicsessions.io/subscriptions/"+aciSub+"/resourceGroups/rg/sessionPools/pool1",
		*got.Properties.PoolManagementEndpoint)
	require.Len(t, got.Properties.Secrets, 1)
	assert.Nil(t, got.Properties.Secrets[0].Value)
	assert.Equal(t, armappcontainers.SessionPoolProvisioningStateSucceeded, *got.Properties.ProvisioningState)

	up, err := pools.BeginUpdate(t.Context(), "rg", "pool1", armappcontainers.SessionPoolUpdatableProperties{
		Tags: map[string]*string{"k": new("v")},
	}, nil)
	require.NoError(t, err)
	upd, err := up.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Equal(t, "v", *upd.Tags["k"])
	assert.Equal(t, armappcontainers.ContainerTypePythonLTS, *upd.Properties.ContainerType)

	n := 0

	for pager := pools.NewListBySubscriptionPager(nil); pager.More(); {
		page, perr := pager.NextPage(t.Context())
		require.NoError(t, perr)

		n += len(page.Value)
	}

	assert.Equal(t, 1, n)

	other := newACAStack(t)
	require.NoError(t, other.handler.Restore(t.Context(), s.handler.Snapshot(t.Context())))

	back, err := other.factory.NewContainerAppsSessionPoolsClient().Get(t.Context(), "rg", "pool1", nil)
	require.NoError(t, err)
	assert.Equal(t, *env.ID, *back.Properties.EnvironmentID)

	dp, err := pools.BeginDelete(t.Context(), "rg", "pool1", nil)
	require.NoError(t, err)
	_, err = dp.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	_, err = pools.Get(t.Context(), "rg", "pool1", nil)
	require.Error(t, err)
}
