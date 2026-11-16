package app

import (
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appservice/armappservice/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fnSlotStack is a Y1 function app "slotfn" with a "staging" slot.
func fnSlotStack(t *testing.T) *fnSDKStack {
	t.Helper()

	s := newFnSDKStack(t)
	plan := s.createPlan(t, "slotfnplan")
	s.createSite(t, "slotfn", *plan.ID, map[string]string{"FUNCTIONS_WORKER_RUNTIME": "node"})

	poller, err := s.factory.NewWebAppsClient().BeginCreateOrUpdateSlot(t.Context(), fnSDKRG, "slotfn", "staging",
		armappservice.Site{
			Location: new("local"), Kind: new("functionapp,linux"),
			Properties: &armappservice.SiteProperties{
				SiteConfig: &armappservice.SiteConfig{LinuxFxVersion: new("NODE|20")},
			},
		}, nil)
	require.NoError(t, err)

	_, err = poller.PollUntilDone(t.Context(), pollOpts())
	require.NoError(t, err)

	return s
}

func TestAzureFunctionsARMSDK_AutoSwap(t *testing.T) {
	t.Parallel()

	s := fnSlotStack(t)
	apps := s.factory.NewWebAppsClient()

	_, err := apps.UpdateConfigurationSlot(t.Context(), fnSDKRG, "slotfn", "staging", armappservice.SiteConfigResource{
		Properties: &armappservice.SiteConfig{AutoSwapSlotName: new("production")},
	}, nil)
	require.NoError(t, err)

	_, err = s.backend.Deploy("slotfn", fnZip(t), "t", "prod")
	require.NoError(t, err)

	before := s.backend.Package("slotfn")

	user, pass, _, err := s.backend.PublishingCredentials("slotfn/staging")
	require.NoError(t, err)

	staged := zipOfFiles(t, map[string]string{"host.json": `{"version":"2.0"}`, "marker.txt": "from-staging"})
	code, body := s.call(t, http.MethodPost, "slotfn-staging.scm."+fnSDKDomain, "/api/zipdeploy", staged, user, pass)
	require.Equal(t, http.StatusOK, code, body)

	require.Eventually(t, func() bool {
		return string(s.backend.Package("slotfn")) == string(staged)
	}, 20*time.Second, 50*time.Millisecond, "the deployed slot swapped into production")
	assert.Equal(
		t,
		string(before),
		string(s.backend.Package("slotfn/staging")),
		"production's old package is now staged",
	)

	_, err = apps.UpdateConfiguration(t.Context(), fnSDKRG, "slotfn", armappservice.SiteConfigResource{
		Properties: &armappservice.SiteConfig{AutoSwapSlotName: new("staging")},
	}, nil)
	require.Error(t, err, "auto swap is set on a slot, not on production")

	_, err = apps.UpdateConfigurationSlot(t.Context(), fnSDKRG, "slotfn", "staging", armappservice.SiteConfigResource{
		Properties: &armappservice.SiteConfig{AutoSwapSlotName: new("nope")},
	}, nil)
	require.Error(t, err, "the target must be a slot of the app")
}

func TestAzureFunctionsARMSDK_AutoSwapRejectedForLinuxWebApp(t *testing.T) {
	t.Parallel()

	s := newSlotStack(t)

	_, err := s.factory.NewWebAppsClient().UpdateConfigurationSlot(t.Context(), fnSDKRG, slotSDKApp, "staging",
		armappservice.SiteConfigResource{
			Properties: &armappservice.SiteConfig{AutoSwapSlotName: new("production")},
		}, nil)
	require.Error(t, err)
}

func TestAzureFunctionsARMSDK_TrafficRouting(t *testing.T) {
	t.Parallel()

	s := fnSlotStack(t)
	apps := s.factory.NewWebAppsClient()

	for _, sl := range []string{"slotfn", "slotfn/staging"} {
		_, err := s.backend.Deploy(sl, fnZip(t), "t", "m")
		require.NoError(t, err)
	}

	slotName := func(path string) string {
		code, body := s.call(t, http.MethodGet, "slotfn."+fnSDKDomain, path, nil, "", "")
		require.Equal(t, http.StatusOK, code)

		return body
	}

	assert.Equal(t, "Production", slotName("/env/WEBSITE_SLOT_NAME"))
	assert.Equal(t, "staging", slotName("/env/WEBSITE_SLOT_NAME?x-ms-routing-name=staging"))

	_, err := apps.UpdateConfiguration(t.Context(), fnSDKRG, "slotfn", armappservice.SiteConfigResource{
		Properties: &armappservice.SiteConfig{Experiments: &armappservice.Experiments{
			RampUpRules: []*armappservice.RampUpRule{
				{
					Name: new(
						"staging",
					),
					ActionHostName:    new("slotfn-staging." + fnSDKDomain),
					ReroutePercentage: new(100.0),
				},
			},
		}},
	}, nil)
	require.NoError(t, err)

	assert.Equal(t, "staging", slotName("/env/WEBSITE_SLOT_NAME"), "a 100 percent rule routes to the slot")
	assert.Equal(t, "Production", slotName("/env/WEBSITE_SLOT_NAME?x-ms-routing-name=self"))
}
