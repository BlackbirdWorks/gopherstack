package app

import (
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appservice/armappservice/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const webSDKRG = fnSDKRG

func (s *fnSDKStack) createLinuxPlan(t *testing.T, name, kind string, reserved bool) *armappservice.Plan {
	t.Helper()

	poller, err := s.factory.NewPlansClient().BeginCreateOrUpdate(t.Context(), webSDKRG, name, armappservice.Plan{
		Location: new("local"), Kind: new(kind),
		SKU:        &armappservice.SKUDescription{Name: new("B1"), Tier: new("Basic")},
		Properties: &armappservice.PlanProperties{Reserved: new(reserved)},
	}, nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollOpts())
	require.NoError(t, err)

	return &res.Plan
}

func (s *fnSDKStack) createWebApp(
	t *testing.T,
	name, planID string,
	cfg *armappservice.SiteConfig,
) (*armappservice.Site, error) {
	t.Helper()

	poller, err := s.factory.NewWebAppsClient().BeginCreateOrUpdate(t.Context(), webSDKRG, name, armappservice.Site{
		Location: new("local"), Kind: new("app,linux"),
		Properties: &armappservice.SiteProperties{ServerFarmID: new(planID), SiteConfig: cfg, HTTPSOnly: new(true)},
	}, nil)
	if err != nil {
		return nil, err
	}

	res, err := poller.PollUntilDone(t.Context(), pollOpts())
	if err != nil {
		return nil, err
	}

	return &res.Site, nil
}

func TestAzureAppServiceARMSDK_WebAppLifecycle(t *testing.T) {
	t.Parallel()

	s := newFnSDKStack(t)
	ctx := t.Context()
	apps := s.factory.NewWebAppsClient()
	plan := s.createLinuxPlan(t, "webplan", "linux", true)

	require.Equal(t, "Basic", *plan.SKU.Tier)

	site, err := s.createWebApp(t, "webapp1", *plan.ID, &armappservice.SiteConfig{
		LinuxFxVersion:  new("DOCKER|busybox:1.36"),
		AlwaysOn:        new(true),
		AppCommandLine:  new("httpd -f -p 8081"),
		HealthCheckPath: new("/healthz"),
		AppSettings: []*armappservice.NameValuePair{
			{Name: new("WEBSITES_PORT"), Value: new("8081")},
			{Name: new("GREETING"), Value: new("hi")},
		},
	})
	require.NoError(t, err)

	assert.Equal(t, "app,linux", *site.Kind)
	assert.Equal(t, "webapp1."+fnSDKDomain+":10012", *site.Properties.DefaultHostName)
	assert.Equal(t, "Running", *site.Properties.State)
	assert.True(t, *site.Properties.Reserved)
	assert.True(t, *site.Properties.HTTPSOnly)
	assert.Equal(t, armappservice.SiteAvailabilityStateNormal, *site.Properties.AvailabilityState)

	t.Run("config round trips", func(t *testing.T) {
		t.Parallel()

		cfg, cfgErr := apps.GetConfiguration(ctx, webSDKRG, "webapp1", nil)
		require.NoError(t, cfgErr)
		assert.Equal(t, "DOCKER|busybox:1.36", *cfg.Properties.LinuxFxVersion)
		assert.Equal(t, "httpd -f -p 8081", *cfg.Properties.AppCommandLine)
		assert.Equal(t, "/healthz", *cfg.Properties.HealthCheckPath)
		assert.True(t, *cfg.Properties.AlwaysOn)

		got, getErr := apps.Get(ctx, webSDKRG, "webapp1", nil)
		require.NoError(t, getErr)
		assert.True(t, *got.Properties.SiteConfig.AlwaysOn)
		assert.Equal(t, "/healthz", *got.Properties.SiteConfig.HealthCheckPath)
	})

	t.Run("settings are env of the container", func(t *testing.T) {
		t.Parallel()

		list, listErr := apps.ListApplicationSettings(ctx, webSDKRG, "webapp1", nil)
		require.NoError(t, listErr)
		assert.Equal(t, "hi", *list.Properties["GREETING"])

		code, body := s.call(t, http.MethodGet, "webapp1."+fnSDKDomain, "/index", nil, "", "")
		require.Equal(t, http.StatusOK, code, body)

		env := s.rt.lastEnv("webapp1")
		assert.Equal(t, "hi", env["GREETING"])
		assert.Equal(t, "8081", env["PORT"])
	})

	t.Run("stop start restart", func(t *testing.T) {
		t.Parallel()

		other, createErr := s.createWebApp(
			t,
			"webapp2",
			*plan.ID,
			&armappservice.SiteConfig{LinuxFxVersion: new("DOCKER|busybox")},
		)
		require.NoError(t, createErr)

		_, stopErr := apps.Stop(ctx, webSDKRG, *other.Name, nil)
		require.NoError(t, stopErr)

		got, getErr := apps.Get(ctx, webSDKRG, *other.Name, nil)
		require.NoError(t, getErr)
		assert.Equal(t, "Stopped", *got.Properties.State)

		code, _ := s.call(t, http.MethodGet, "webapp2."+fnSDKDomain, "/", nil, "", "")
		assert.Equal(t, http.StatusForbidden, code)

		_, startErr := apps.Start(ctx, webSDKRG, *other.Name, nil)
		require.NoError(t, startErr)

		_, restartErr := apps.Restart(ctx, webSDKRG, *other.Name, nil)
		require.NoError(t, restartErr)

		code, _ = s.call(t, http.MethodGet, "webapp2."+fnSDKDomain, "/", nil, "", "")
		assert.Equal(t, http.StatusOK, code)
	})

	t.Run("config update changes the stack", func(t *testing.T) {
		t.Parallel()

		app, createErr := s.createWebApp(
			t,
			"webapp3",
			*plan.ID,
			&armappservice.SiteConfig{LinuxFxVersion: new("NODE|20-lts")},
		)
		require.NoError(t, createErr)

		_, updErr := apps.UpdateConfiguration(ctx, webSDKRG, *app.Name, armappservice.SiteConfigResource{
			Properties: &armappservice.SiteConfig{
				LinuxFxVersion: new("PYTHON|3.12"),
				AppCommandLine: new("python srv.py"),
			},
		}, nil)
		require.NoError(t, updErr)

		cfg, cfgErr := apps.GetConfiguration(ctx, webSDKRG, *app.Name, nil)
		require.NoError(t, cfgErr)
		assert.Equal(t, "PYTHON|3.12", *cfg.Properties.LinuxFxVersion)

		_, badErr := apps.UpdateConfiguration(ctx, webSDKRG, *app.Name, armappservice.SiteConfigResource{
			Properties: &armappservice.SiteConfig{LinuxFxVersion: new("TOMCAT|10.0-java17")},
		}, nil)

		var respErr *azcore.ResponseError

		require.ErrorAs(t, badErr, &respErr)
		assert.Equal(t, http.StatusBadRequest, respErr.StatusCode)
	})

	t.Run("function-only operations are rejected", func(t *testing.T) {
		t.Parallel()

		_, keysErr := apps.ListHostKeys(ctx, webSDKRG, "webapp1", nil)

		var respErr *azcore.ResponseError

		require.ErrorAs(t, keysErr, &respErr)
		assert.Equal(t, http.StatusBadRequest, respErr.StatusCode)

		pager := apps.NewListFunctionsPager(webSDKRG, "webapp1", nil)
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)
		assert.Empty(t, page.Value)
	})
}

func TestAzureAppServiceARMSDK_Validation(t *testing.T) {
	t.Parallel()

	s := newFnSDKStack(t)
	linux := s.createLinuxPlan(t, "valLinux", "linux", true)
	windows := s.createLinuxPlan(t, "valWindows", "app", false)

	tests := []struct {
		cfg     *armappservice.SiteConfig
		name    string
		kind    string
		farm    string
		wantMsg string
	}{
		{
			&armappservice.SiteConfig{LinuxFxVersion: new("NODE|20-lts")},
			"linux app on non-reserved plan",
			"app,linux",
			*windows.ID,
			"Linux",
		},
		{
			&armappservice.SiteConfig{LinuxFxVersion: new("RUBY|3.2")},
			"unsupported stack",
			"app,linux",
			*linux.ID,
			"RUBY",
		},
		{nil, "stack required", "app,linux", *linux.ID, "linuxFxVersion"},
		{
			&armappservice.SiteConfig{LinuxFxVersion: new("NODE|20-lts"), HealthCheckPath: new("nope")},
			"health path",
			"app,linux",
			*linux.ID,
			"healthCheckPath",
		},
		{
			&armappservice.SiteConfig{LinuxFxVersion: new("NODE|20-lts")},
			"windows web app",
			"app",
			*linux.ID,
			"Linux web apps",
		},
		{
			&armappservice.SiteConfig{LinuxFxVersion: new("NODE|20-lts")},
			"plan required",
			"app,linux",
			"",
			"serverFarmId",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			poller, err := s.factory.NewWebAppsClient().
				BeginCreateOrUpdate(t.Context(), webSDKRG, "val-"+strings.ReplaceAll(tt.name, " ", "-"),
					armappservice.Site{
						Location: new("local"), Kind: new(tt.kind),
						Properties: &armappservice.SiteProperties{ServerFarmID: new(tt.farm), SiteConfig: tt.cfg},
					}, nil)
			if err == nil {
				_, err = poller.PollUntilDone(t.Context(), pollOpts())
			}

			var respErr *azcore.ResponseError

			require.ErrorAs(t, err, &respErr)
			assert.Equal(t, http.StatusBadRequest, respErr.StatusCode)
			assert.Contains(t, respErr.Error(), tt.wantMsg)
		})
	}
}
