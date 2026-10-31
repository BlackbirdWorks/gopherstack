package app

import (
	"archive/zip"
	"bytes"
	"errors"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appservice/armappservice/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const slotSDKApp = "swapapp"

func (s *fnSDKStack) createPlanSKU(t *testing.T, name, kind, sku, tier string) *armappservice.Plan {
	t.Helper()

	poller, err := s.factory.NewPlansClient().BeginCreateOrUpdate(t.Context(), webSDKRG, name, armappservice.Plan{
		Location: new("local"), Kind: new(kind),
		SKU:        &armappservice.SKUDescription{Name: new(sku), Tier: new(tier)},
		Properties: &armappservice.PlanProperties{Reserved: new(true)},
	}, nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollOpts())
	require.NoError(t, err)

	return &res.Plan
}

func (s *fnSDKStack) createSlot(t *testing.T, app, slot string, props *armappservice.SiteProperties) error {
	t.Helper()

	poller, err := s.factory.NewWebAppsClient().BeginCreateOrUpdateSlot(t.Context(), webSDKRG, app, slot,
		armappservice.Site{Location: new("local"), Kind: new("app,linux"), Properties: props}, nil)
	if err != nil {
		return err
	}

	_, err = poller.PollUntilDone(t.Context(), pollOpts())

	return err
}

// slotStack is a Standard-tier web app "swapapp" with a "staging" slot, both running the same stack.
type slotStack struct {
	*fnSDKStack
	planID string
}

func newSlotStack(t *testing.T) *slotStack {
	t.Helper()

	s := newFnSDKStack(t)
	plan := s.createPlanSKU(t, "slotplan", "linux", "S1", "Standard")
	_, err := s.createWebApp(t, slotSDKApp, *plan.ID, &armappservice.SiteConfig{LinuxFxVersion: new("NODE|20-lts")})
	require.NoError(t, err)

	err = s.createSlot(t, slotSDKApp, "staging", &armappservice.SiteProperties{
		SiteConfig: &armappservice.SiteConfig{LinuxFxVersion: new("NODE|20-lts")},
	})
	require.NoError(t, err)

	return &slotStack{fnSDKStack: s, planID: *plan.ID}
}

func (s *slotStack) host(slot string) string {
	if slot == "" {
		return slotSDKApp + "." + fnSDKDomain
	}

	return slotSDKApp + "-" + slot + "." + fnSDKDomain
}

func (s *slotStack) deploy(t *testing.T, slot, marker string) {
	t.Helper()

	name := slotSDKApp
	if slot != "" {
		name += "/" + slot
	}

	user, pass, _, err := s.backend.PublishingCredentials(name)
	require.NoError(t, err)

	zipped := zipOfFiles(t, map[string]string{"server.js": "1", "marker.txt": marker})
	scmHost := slotSDKApp + ".scm." + fnSDKDomain

	if slot != "" {
		scmHost = slotSDKApp + "-" + slot + ".scm." + fnSDKDomain
	}

	code, body := s.call(t, http.MethodPost, scmHost, "/api/zipdeploy", zipped, user, pass)
	require.Equal(t, http.StatusOK, code, body)
}

func (s *slotStack) get(t *testing.T, slot, path string) string {
	t.Helper()

	code, body := s.call(t, http.MethodGet, s.host(slot), path, nil, "", "")
	require.Equal(t, http.StatusOK, code, body)

	return body
}

func (s *slotStack) setStagingSettings(t *testing.T, kv map[string]string) {
	t.Helper()

	props := map[string]*string{}
	for k, v := range kv {
		props[k] = new(v)
	}

	_, err := s.factory.NewWebAppsClient().UpdateApplicationSettingsSlot(
		t.Context(), webSDKRG, slotSDKApp, "staging", armappservice.StringDictionary{Properties: props}, nil)
	require.NoError(t, err)
}

func (s *slotStack) setProdSettings(t *testing.T, kv map[string]string) {
	t.Helper()

	props := map[string]*string{}
	for k, v := range kv {
		props[k] = new(v)
	}

	_, err := s.factory.NewWebAppsClient().UpdateApplicationSettings(
		t.Context(), webSDKRG, slotSDKApp, armappservice.StringDictionary{Properties: props}, nil)
	require.NoError(t, err)
}

func (s *slotStack) setConnStrings(t *testing.T, slot string, kv map[string]string) {
	t.Helper()

	props := map[string]*armappservice.ConnStringValueTypePair{}
	for k, v := range kv {
		props[k] = &armappservice.ConnStringValueTypePair{
			Value: new(v),
			Type:  new(armappservice.ConnectionStringTypeSQLAzure),
		}
	}

	dict := armappservice.ConnectionStringDictionary{Properties: props}
	apps := s.factory.NewWebAppsClient()

	var err error
	if slot == "" {
		_, err = apps.UpdateConnectionStrings(t.Context(), webSDKRG, slotSDKApp, dict, nil)
	} else {
		_, err = apps.UpdateConnectionStringsSlot(t.Context(), webSDKRG, slotSDKApp, slot, dict, nil)
	}

	require.NoError(t, err)
}

func (s *slotStack) stickyNames(t *testing.T, appSettings, connStrings []string) {
	t.Helper()

	names := &armappservice.SlotConfigNames{}
	for _, n := range appSettings {
		names.AppSettingNames = append(names.AppSettingNames, new(n))
	}

	for _, n := range connStrings {
		names.ConnectionStringNames = append(names.ConnectionStringNames, new(n))
	}

	_, err := s.factory.NewWebAppsClient().UpdateSlotConfigurationNames(
		t.Context(), webSDKRG, slotSDKApp, armappservice.SlotConfigNamesResource{Properties: names}, nil)
	require.NoError(t, err)
}

func (s *slotStack) swap(t *testing.T, source, target string) error {
	t.Helper()

	entity := armappservice.CsmSlotEntity{TargetSlot: new(target), PreserveVnet: new(false)}
	apps := s.factory.NewWebAppsClient()

	if source == "" {
		poller, err := apps.BeginSwapSlotWithProduction(t.Context(), webSDKRG, slotSDKApp, entity, nil)
		if err != nil {
			return err
		}

		_, err = poller.PollUntilDone(t.Context(), pollOpts())

		return err
	}

	poller, err := apps.BeginSwapSlot(t.Context(), webSDKRG, slotSDKApp, source, entity, nil)
	if err != nil {
		return err
	}

	_, err = poller.PollUntilDone(t.Context(), pollOpts())

	return err
}

func responseCode(err error) int {
	if respErr, ok := errors.AsType[*azcore.ResponseError](err); ok {
		return respErr.StatusCode
	}

	return 0
}

// step runs one stage of a scenario whose stages build on each other, so they cannot run in parallel.
func step(t *testing.T, name string, fn func()) {
	t.Helper()
	t.Log("step:", name)
	fn()
}

func slotErr(t *testing.T, app, slot string, props *armappservice.SiteProperties, s *fnSDKStack) func() error {
	t.Helper()

	return func() error {
		err := s.createSlot(t, app, slot, props)

		return err
	}
}

func TestAzureAppServiceARMSDK_SlotCRUD(t *testing.T) {
	t.Parallel()

	s := newSlotStack(t)
	ctx := t.Context()
	apps := s.factory.NewWebAppsClient()

	got, err := apps.GetSlot(ctx, webSDKRG, slotSDKApp, "staging", nil)
	require.NoError(t, err)
	assert.Equal(t, slotSDKApp+"/staging", *got.Name)
	assert.Equal(t, "Microsoft.Web/sites/slots", *got.Type)
	assert.Equal(t, slotSDKApp+"-staging."+fnSDKDomain+":10012", *got.Properties.DefaultHostName)
	assert.Contains(t, got.Properties.EnabledHostNames, new(slotSDKApp+"-staging.scm."+fnSDKDomain+":10012"))
	assert.Equal(t, s.planID, *got.Properties.ServerFarmID, "a slot runs in its site's plan")
	assert.Equal(t, "Running", *got.Properties.State)

	var names []string

	pager := apps.NewListSlotsPager(webSDKRG, slotSDKApp, nil)
	for pager.More() {
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)

		for _, v := range page.Value {
			names = append(names, *v.Name)
		}
	}

	assert.Equal(t, []string{slotSDKApp + "/staging"}, names)

	prodSites := s.factory.NewWebAppsClient().NewListByResourceGroupPager(webSDKRG, nil)

	var siteNames []string

	for prodSites.More() {
		page, pageErr := prodSites.NextPage(ctx)
		require.NoError(t, pageErr)

		for _, v := range page.Value {
			siteNames = append(siteNames, *v.Name)
		}
	}

	assert.Equal(t, []string{slotSDKApp}, siteNames, "slots are not listed as sites")

	step(t, "settings are per slot", func() {
		s.setProdSettings(t, map[string]string{"WHO": "prod"})
		s.setStagingSettings(t, map[string]string{"WHO": "stg"})

		slotList, listErr := apps.ListApplicationSettingsSlot(ctx, webSDKRG, slotSDKApp, "staging", nil)
		require.NoError(t, listErr)
		assert.Equal(t, "stg", *slotList.Properties["WHO"])

		prodList, listErr := apps.ListApplicationSettings(ctx, webSDKRG, slotSDKApp, nil)
		require.NoError(t, listErr)
		assert.Equal(t, "prod", *prodList.Properties["WHO"])

		s.deploy(t, "", "prod")
		s.deploy(t, "staging", "stg")
		assert.Equal(t, "prod", s.get(t, "", "/env/WHO"))
		assert.Equal(t, "stg", s.get(t, "staging", "/env/WHO"))
		assert.Equal(t, "staging", s.get(t, "staging", "/env/WEBSITE_SLOT_NAME"))
		assert.Equal(t, "Production", s.get(t, "", "/env/WEBSITE_SLOT_NAME"))
	})

	step(t, "start stop restart", func() {
		_, stopErr := apps.StopSlot(ctx, webSDKRG, slotSDKApp, "staging", nil)
		require.NoError(t, stopErr)

		code, _ := s.call(t, http.MethodGet, s.host("staging"), "/", nil, "", "")
		assert.Equal(t, http.StatusForbidden, code)

		stopped, getErr := apps.GetSlot(ctx, webSDKRG, slotSDKApp, "staging", nil)
		require.NoError(t, getErr)
		assert.Equal(t, "Stopped", *stopped.Properties.State)
		assert.Equal(t, "Running", *s.mustGetProd(t).Properties.State, "production is not affected")

		_, startErr := apps.StartSlot(ctx, webSDKRG, slotSDKApp, "staging", nil)
		require.NoError(t, startErr)
		_, restartErr := apps.RestartSlot(ctx, webSDKRG, slotSDKApp, "staging", nil)
		require.NoError(t, restartErr)
		assert.Equal(t, "stg", s.get(t, "staging", "/marker"))
	})

	step(t, "web config", func() {
		_, cfgErr := apps.UpdateConfigurationSlot(
			ctx,
			webSDKRG,
			slotSDKApp,
			"staging",
			armappservice.SiteConfigResource{
				Properties: &armappservice.SiteConfig{WebSocketsEnabled: new(true)},
			},
			nil,
		)
		require.NoError(t, cfgErr)

		cfg, getErr := apps.GetConfigurationSlot(ctx, webSDKRG, slotSDKApp, "staging", nil)
		require.NoError(t, getErr)
		assert.True(t, *cfg.Properties.WebSocketsEnabled)
		assert.Equal(t, "$"+slotSDKApp+"__staging", *cfg.Properties.PublishingUsername)
	})

	step(t, "deleting a slot", func() {
		delErr := s.createSlot(t, slotSDKApp, "temp", nil)
		require.NoError(t, delErr)

		_, delErr = apps.DeleteSlot(ctx, webSDKRG, slotSDKApp, "temp", nil)
		require.NoError(t, delErr)

		_, getErr := apps.GetSlot(ctx, webSDKRG, slotSDKApp, "temp", nil)
		require.Error(t, getErr)
		assert.Equal(t, http.StatusNotFound, responseCode(getErr))

		_, ok := s.backend.App(slotSDKApp + "/temp")
		assert.False(t, ok)
	})

	step(t, "deleting the site deletes its slots", func() {
		_, ok := s.backend.App(slotSDKApp + "/staging")
		require.True(t, ok)

		_, delErr := apps.Delete(ctx, webSDKRG, slotSDKApp, nil)
		require.NoError(t, delErr)

		_, ok = s.backend.App(slotSDKApp + "/staging")
		assert.False(t, ok)

		_, getErr := apps.GetSlot(ctx, webSDKRG, slotSDKApp, "staging", nil)
		assert.Equal(t, http.StatusNotFound, responseCode(getErr))
	})
}

func (s *fnSDKStack) mustGetProd(t *testing.T) *armappservice.Site {
	t.Helper()

	got, err := s.factory.NewWebAppsClient().Get(t.Context(), webSDKRG, slotSDKApp, nil)
	require.NoError(t, err)

	return &got.Site
}

func TestAzureAppServiceARMSDK_SlotSwapStickySettings(t *testing.T) {
	t.Parallel()

	s := newSlotStack(t)
	ctx := t.Context()
	apps := s.factory.NewWebAppsClient()

	s.stickyNames(t, []string{"STICKY"}, []string{"StickyDB"})

	names, err := apps.ListSlotConfigurationNames(ctx, webSDKRG, slotSDKApp, nil)
	require.NoError(t, err)
	require.Len(t, names.Properties.AppSettingNames, 1)
	assert.Equal(t, "STICKY", *names.Properties.AppSettingNames[0])

	s.setProdSettings(t, map[string]string{
		"SWAPPED": "from-prod", "STICKY": "prod-only", "FUNCTIONS_EXTENSION_VERSION": "~4",
	})
	s.setStagingSettings(t, map[string]string{
		"SWAPPED": "from-staging", "STICKY": "staging-only", "FUNCTIONS_EXTENSION_VERSION": "~3",
	})
	s.setConnStrings(t, "", map[string]string{"DB": "prod-db", "StickyDB": "prod-sticky-db"})
	s.setConnStrings(t, "staging", map[string]string{"DB": "stg-db", "StickyDB": "stg-sticky-db"})
	s.deploy(t, "", "prod-content")
	s.deploy(t, "staging", "staging-content")

	require.Equal(t, "prod-content", s.get(t, "", "/marker"))
	startedBefore := s.rt.started()

	require.NoError(t, s.swap(t, "staging", "production"))

	assert.Equal(t, "staging-content", s.get(t, "", "/marker"), "production serves the swapped-in content")
	assert.Equal(t, "prod-content", s.get(t, "staging", "/marker"), "staging holds the previous production")
	assert.Greater(t, s.rt.started(), startedBefore, "both apps restarted")

	assert.Equal(t, "from-staging", s.get(t, "", "/env/SWAPPED"))
	assert.Equal(t, "from-prod", s.get(t, "staging", "/env/SWAPPED"))
	assert.Equal(t, "prod-only", s.get(t, "", "/env/STICKY"), "slot settings stay with the slot")
	assert.Equal(t, "staging-only", s.get(t, "staging", "/env/STICKY"))
	assert.Equal(t, "~4", s.get(t, "", "/env/FUNCTIONS_EXTENSION_VERSION"), "_EXTENSION_VERSION settings never swap")
	assert.Equal(t, "~3", s.get(t, "staging", "/env/FUNCTIONS_EXTENSION_VERSION"))

	prodConn, err := apps.ListConnectionStrings(ctx, webSDKRG, slotSDKApp, nil)
	require.NoError(t, err)
	assert.Equal(t, "stg-db", *prodConn.Properties["DB"].Value)
	assert.Equal(t, "prod-sticky-db", *prodConn.Properties["StickyDB"].Value)

	slotConn, err := apps.ListConnectionStringsSlot(ctx, webSDKRG, slotSDKApp, "staging", nil)
	require.NoError(t, err)
	assert.Equal(t, "prod-db", *slotConn.Properties["DB"].Value)
	assert.Equal(t, "stg-sticky-db", *slotConn.Properties["StickyDB"].Value)

	prod := s.mustGetProd(t)
	require.NotNil(t, prod.Properties.SlotSwapStatus)
	assert.Equal(t, "staging", *prod.Properties.SlotSwapStatus.SourceSlotName)
	assert.Equal(t, "production", *prod.Properties.SlotSwapStatus.DestinationSlotName)

	step(t, "swapping back restores the previous production", func() {
		require.NoError(t, s.swap(t, "staging", "production"))
		assert.Equal(t, "prod-content", s.get(t, "", "/marker"))
		assert.Equal(t, "from-prod", s.get(t, "", "/env/SWAPPED"))
		assert.Equal(t, "prod-only", s.get(t, "", "/env/STICKY"))
	})

	step(t, "the production slot may be the swap source", func() {
		require.NoError(t, s.swap(t, "", "staging"))
		assert.Equal(t, "staging-content", s.get(t, "", "/marker"))
		assert.Equal(t, "prod-only", s.get(t, "", "/env/STICKY"))
	})

	step(t, "web config follows the content", func() {
		_, cfgErr := apps.UpdateConfigurationSlot(
			ctx,
			webSDKRG,
			slotSDKApp,
			"staging",
			armappservice.SiteConfigResource{
				Properties: &armappservice.SiteConfig{LinuxFxVersion: new("NODE|22-lts"), AlwaysOn: new(true)},
			},
			nil,
		)
		require.NoError(t, cfgErr)
		require.NoError(t, s.swap(t, "staging", "production"))

		prodCfg, getErr := apps.GetConfiguration(ctx, webSDKRG, slotSDKApp, nil)
		require.NoError(t, getErr)
		assert.Equal(t, "NODE|22-lts", *prodCfg.Properties.LinuxFxVersion)
		assert.False(t, *prodCfg.Properties.AlwaysOn, "alwaysOn is not swapped")

		slotCfg, getErr := apps.GetConfigurationSlot(ctx, webSDKRG, slotSDKApp, "staging", nil)
		require.NoError(t, getErr)
		assert.Equal(t, "NODE|20-lts", *slotCfg.Properties.LinuxFxVersion)
		assert.True(t, *slotCfg.Properties.AlwaysOn)
	})
}

func TestAzureAppServiceARMSDK_SlotSwapWithPreview(t *testing.T) {
	t.Parallel()

	s := newSlotStack(t)
	ctx := t.Context()
	apps := s.factory.NewWebAppsClient()
	entity := armappservice.CsmSlotEntity{TargetSlot: new("production"), PreserveVnet: new(false)}

	s.stickyNames(t, []string{"STICKY"}, nil)
	s.setProdSettings(t, map[string]string{"SWAPPED": "from-prod", "STICKY": "prod-only"})
	s.setStagingSettings(t, map[string]string{"SWAPPED": "from-staging", "STICKY": "staging-only"})
	s.deploy(t, "", "prod-content")
	s.deploy(t, "staging", "staging-content")

	_, err := apps.ApplySlotConfigurationSlot(ctx, webSDKRG, slotSDKApp, "staging", entity, nil)
	require.NoError(t, err)

	step(t, "first phase runs the source with the target's slot settings", func() {
		assert.Equal(t, "prod-only", s.get(t, "staging", "/env/STICKY"))
		assert.Equal(t, "from-staging", s.get(t, "staging", "/env/SWAPPED"))
		assert.Equal(t, "staging-content", s.get(t, "staging", "/marker"), "content has not moved yet")
		assert.Equal(t, "prod-content", s.get(t, "", "/marker"))
	})

	step(t, "a prepared slot cannot be edited", func() {
		_, setErr := apps.UpdateApplicationSettingsSlot(ctx, webSDKRG, slotSDKApp, "staging",
			armappservice.StringDictionary{Properties: map[string]*string{"X": new("1")}}, nil)
		require.Error(t, setErr)
		assert.Equal(t, http.StatusConflict, responseCode(setErr))
		assert.Contains(t, setErr.Error(), "prepared for swap")

		_, applyErr := apps.ApplySlotConfigToProduction(ctx, webSDKRG, slotSDKApp,
			armappservice.CsmSlotEntity{TargetSlot: new("staging"), PreserveVnet: new(false)}, nil)
		require.Error(t, applyErr, "the target cannot start another preview")
	})

	step(t, "reset cancels", func() {
		_, resetErr := apps.ResetSlotConfigurationSlot(ctx, webSDKRG, slotSDKApp, "staging", nil)
		require.NoError(t, resetErr)

		assert.Equal(t, "staging-only", s.get(t, "staging", "/env/STICKY"))
		assert.Equal(t, "prod-content", s.get(t, "", "/marker"))

		s.setStagingSettings(t, map[string]string{"SWAPPED": "from-staging", "STICKY": "staging-only", "X": "1"})
	})

	step(t, "complete after the preview", func() {
		_, applyErr := apps.ApplySlotConfigurationSlot(ctx, webSDKRG, slotSDKApp, "staging", entity, nil)
		require.NoError(t, applyErr)
		require.NoError(t, s.swap(t, "staging", "production"))

		assert.Equal(t, "staging-content", s.get(t, "", "/marker"))
		assert.Equal(t, "prod-content", s.get(t, "staging", "/marker"))
		assert.Equal(t, "prod-only", s.get(t, "", "/env/STICKY"))
		assert.Equal(t, "staging-only", s.get(t, "staging", "/env/STICKY"), "the slot's own setting is back")
		assert.Equal(t, "1", s.get(t, "", "/env/X"))

		s.setStagingSettings(t, map[string]string{"STICKY": "staging-only", "X": "2"})
	})

	step(t, "production as the preview source", func() {
		_, prodApplyErr := apps.ApplySlotConfigToProduction(ctx, webSDKRG, slotSDKApp,
			armappservice.CsmSlotEntity{TargetSlot: new("staging"), PreserveVnet: new(false)}, nil)
		require.NoError(t, prodApplyErr)
		assert.Equal(t, "staging-only", s.get(t, "", "/env/STICKY"))

		_, resetErr := apps.ResetProductionSlotConfig(ctx, webSDKRG, slotSDKApp, nil)
		require.NoError(t, resetErr)
		assert.Equal(t, "prod-only", s.get(t, "", "/env/STICKY"))
	})
}

func TestAzureAppServiceARMSDK_SlotDifferences(t *testing.T) {
	t.Parallel()

	s := newSlotStack(t)
	apps := s.factory.NewWebAppsClient()

	s.stickyNames(t, []string{"STICKY"}, nil)
	s.setProdSettings(t, map[string]string{"A": "prod", "STICKY": "p", "SAME": "x"})
	s.setStagingSettings(t, map[string]string{"A": "stg", "STICKY": "s", "SAME": "x", "ONLYSTG": "1"})

	pager := apps.NewListSlotDifferencesSlotPager(webSDKRG, slotSDKApp, "staging",
		armappservice.CsmSlotEntity{TargetSlot: new("production"), PreserveVnet: new(false)}, nil)

	got := map[string][2]string{}

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		for _, d := range page.Value {
			assert.Equal(t, "AppSetting", *d.Properties.SettingType)

			var target string
			if d.Properties.ValueInTargetSlot != nil {
				target = *d.Properties.ValueInTargetSlot
			}

			got[*d.Properties.SettingName] = [2]string{*d.Properties.ValueInCurrentSlot, target}
		}
	}

	assert.Equal(t, map[string][2]string{"A": {"stg", "prod"}, "ONLYSTG": {"1", ""}}, got,
		"only swapped settings that differ are listed")
}

func TestAzureAppServiceARMSDK_SlotLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		kind      string
		siteKind  string
		sku, tier string
		want      int
	}{
		{"free", "linux", "app,linux", "F1", "Free", 0},
		{"basic", "linux", "app,linux", "B1", "Basic", 0},
		{"standard", "linux", "app,linux", "S1", "Standard", 5},
		{"premium v3", "linux", "app,linux", "P1v3", "PremiumV3", 20},
		{"isolated", "linux", "app,linux", "I1v2", "IsolatedV2", 20},
		{"function consumption", "functionapp", "functionapp,linux", "Y1", "Dynamic", 1},
		{"function premium", "functionapp", "functionapp,linux", "EP1", "ElasticPremium", 2},
		{"function flex", "functionapp", "functionapp,linux", "FC1", "FlexConsumption", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newFnSDKStack(t)
			plan := s.createPlanSKU(t, "limitplan", tt.kind, tt.sku, tt.tier)

			poller, err := s.factory.NewWebAppsClient().BeginCreateOrUpdate(t.Context(), webSDKRG, "limited",
				armappservice.Site{
					Location: new("local"), Kind: new(tt.siteKind),
					Properties: &armappservice.SiteProperties{
						ServerFarmID: new(*plan.ID),
						SiteConfig:   &armappservice.SiteConfig{LinuxFxVersion: new(limitFx(tt.siteKind))},
					},
				}, nil)
			require.NoError(t, err)
			_, err = poller.PollUntilDone(t.Context(), pollOpts())
			require.NoError(t, err)

			for i := range tt.want {
				slotErr := s.createSlot(t, "limited", "s"+string(rune('a'+i)), nil)
				require.NoError(t, slotErr, "slot %d is within the limit", i+1)
			}

			overErr := s.createSlot(t, "limited", "over", nil)
			require.Error(t, overErr)
			assert.Equal(t, http.StatusConflict, responseCode(overErr))

			if tt.want > 0 {
				updErr := s.createSlot(t, "limited", "sa", nil)
				require.NoError(t, updErr, "updating an existing slot is not a new slot")
			}
		})
	}
}

func limitFx(kind string) string {
	if kind == "app,linux" {
		return "NODE|20-lts"
	}

	return "NODE|20"
}

func TestAzureAppServiceARMSDK_SlotErrors(t *testing.T) {
	t.Parallel()

	s := newSlotStack(t)
	apps := s.factory.NewWebAppsClient()

	err := s.createSlot(t, slotSDKApp, "second", &armappservice.SiteProperties{
		SiteConfig: &armappservice.SiteConfig{LinuxFxVersion: new("NODE|20-lts")},
	})
	require.NoError(t, err)

	tests := []struct {
		fn   func() error
		name string
		want int
	}{
		{func() error { return s.swap(t, "staging", "staging") }, "swap with itself", http.StatusBadRequest},
		{func() error { return s.swap(t, "staging", "nope") }, "unknown target", http.StatusNotFound},
		{func() error { return s.swap(t, "nope", "production") }, "unknown source", http.StatusNotFound},
		{func() error { return s.swap(t, "", "production") }, "production with production", http.StatusBadRequest},
		{slotErr(t, slotSDKApp, "production", nil, s.fnSDKStack), "reserved name", http.StatusBadRequest},
		{slotErr(t, slotSDKApp, "bad_name", nil, s.fnSDKStack), "bad characters", http.StatusBadRequest},
		{
			slotErr(t, slotSDKApp, "averyveryverylongslotname", nil, s.fnSDKStack),
			"name too long",
			http.StatusBadRequest,
		},
		{slotErr(t, "ghost", "x", nil, s.fnSDKStack), "unknown site", http.StatusNotFound},
		{
			slotErr(
				t,
				slotSDKApp,
				"other",
				&armappservice.SiteProperties{ServerFarmID: new(s.planID + "x")},
				s.fnSDKStack,
			),
			"different plan",
			http.StatusBadRequest,
		},
		{
			func() error {
				_, e := apps.GetSlot(t.Context(), webSDKRG, slotSDKApp, "nope", nil)

				return e
			},
			"get unknown",
			http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fnErr := tt.fn()
			require.Error(t, fnErr)
			assert.Equal(t, tt.want, responseCode(fnErr), fnErr.Error())
		})
	}

	step(t, "a swap between two slots", func() {
		s.deploy(t, "staging", "one")
		s.deploy(t, "second", "two")
		require.NoError(t, s.swap(t, "staging", "second"))
		assert.Equal(t, "two", s.get(t, "staging", "/marker"))
		assert.Equal(t, "one", s.get(t, "second", "/marker"))
	})

	step(t, "a slot may not take a site's hostname", func() {
		plan := s.planID
		_, siteErr := s.createWebApp(
			t,
			slotSDKApp+"-staging",
			plan,
			&armappservice.SiteConfig{LinuxFxVersion: new("NODE|20-lts")},
		)
		require.Error(t, siteErr)
		assert.Equal(t, http.StatusConflict, responseCode(siteErr))
	})

	step(t, "cloning copies settings, not content", func() {
		s.setStagingSettings(t, map[string]string{"CLONED": "yes", "OVERRIDE": "src"})
		s.deploy(t, "staging", "src-content")

		src := armappservice.CloningInfo{
			SourceWebAppID:       new(*s.mustGetSlot(t, "staging").ID),
			AppSettingsOverrides: map[string]*string{"OVERRIDE": new("dst")},
		}
		cloneErr := s.createSlot(t, slotSDKApp, "clone", &armappservice.SiteProperties{CloningInfo: &src})
		require.NoError(t, cloneErr)

		list, listErr := apps.ListApplicationSettingsSlot(t.Context(), webSDKRG, slotSDKApp, "clone", nil)
		require.NoError(t, listErr)
		assert.Equal(t, "yes", *list.Properties["CLONED"])
		assert.Equal(t, "dst", *list.Properties["OVERRIDE"])

		code, body := s.call(t, http.MethodGet, s.host("clone"), "/", nil, "", "")
		assert.Equal(t, http.StatusOK, code)
		assert.Contains(t, body, "waiting for your content")
	})
}

func (s *fnSDKStack) mustGetSlot(t *testing.T, slot string) *armappservice.Site {
	t.Helper()

	got, err := s.factory.NewWebAppsClient().GetSlot(t.Context(), webSDKRG, slotSDKApp, slot, nil)
	require.NoError(t, err)

	return &got.Site
}

func TestAzureAppServiceARMSDK_SlotPersistence(t *testing.T) {
	t.Parallel()

	src := newSlotStack(t)
	src.stickyNames(t, []string{"STICKY"}, nil)
	src.setStagingSettings(t, map[string]string{"A": "slot"})
	src.deploy(t, "staging", "staged")
	require.NoError(t, src.arm.Restore(t.Context(), src.arm.Snapshot(t.Context())), "own snapshot restores")

	dst := newFnSDKStack(t)
	require.NoError(t, dst.arm.Restore(t.Context(), src.arm.Snapshot(t.Context())))
	require.NoError(t, dst.backend.Restore(t.Context(), src.backend.Snapshot(t.Context())))

	got, err := dst.factory.NewWebAppsClient().GetSlot(t.Context(), webSDKRG, slotSDKApp, "staging", nil)
	require.NoError(t, err)
	assert.Equal(t, slotSDKApp+"/staging", *got.Name)

	names, err := dst.factory.NewWebAppsClient().ListSlotConfigurationNames(t.Context(), webSDKRG, slotSDKApp, nil)
	require.NoError(t, err)
	assert.Equal(t, "STICKY", *names.Properties.AppSettingNames[0])

	list, err := dst.factory.NewWebAppsClient().
		ListApplicationSettingsSlot(t.Context(), webSDKRG, slotSDKApp, "staging", nil)
	require.NoError(t, err)
	assert.Equal(t, "slot", *list.Properties["A"])

	d := &slotStack{fnSDKStack: dst}
	assert.Equal(t, "staged", d.get(t, "staging", "/marker"), "the restored slot serves its restored package")
}

func zipOfFiles(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for name, content := range files {
		w, err := zw.Create(name)
		require.NoError(t, err)

		_, err = w.Write([]byte(content))
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func (f *fnFakeRuntime) started() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.specs)
}
