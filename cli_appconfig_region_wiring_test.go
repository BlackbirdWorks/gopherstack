package main

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	appconfigbackend "github.com/blackbirdworks/gopherstack/services/appconfig"
	appconfigdatabackend "github.com/blackbirdworks/gopherstack/services/appconfigdata"
)

func TestWireAppConfigDeployments_PublishesToDeploymentRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "home-region", region: regionA},
		{name: "other-region", region: regionB},
		{name: "third-region", region: "ap-south-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ac := appconfigbackend.NewHandler(appconfigbackend.NewInMemoryBackend(regionAccount, regionA))
			ac.EnableRegions()

			acd := appconfigdatabackend.NewHandler(appconfigdatabackend.NewInMemoryBackend())
			acd.EnableRegions(regionA)

			wireAppConfigDeployments(ac, acd)

			bk, ok := ac.BackendFor(tc.region).(*appconfigbackend.InMemoryBackend)
			require.True(t, ok)

			app, err := bk.CreateApplication("wired", "", nil)
			require.NoError(t, err)

			env, err := bk.CreateEnvironment(app.ID, "env", "", nil, nil)
			require.NoError(t, err)

			profile, err := bk.CreateConfigurationProfile(
				app.ID,
				"profile",
				"",
				"hosted",
				"AWS.Freeform",
				"",
				"",
				nil,
				nil,
			)
			require.NoError(t, err)

			hcv, err := bk.CreateHostedConfigurationVersion(
				app.ID,
				profile.ID,
				"application/json",
				"",
				"",
				[]byte(`{}`),
				nil,
			)
			require.NoError(t, err)

			strategy, err := bk.CreateDeploymentStrategy("now", "", 0, 0, 100, "LINEAR", "NONE", nil)
			require.NoError(t, err)

			_, err = bk.StartDeployment(
				app.ID,
				env.ID,
				profile.ID,
				strategy.ID,
				strconv.FormatInt(int64(hcv.VersionNumber), 10),
				"",
				nil,
				nil,
				nil,
			)
			require.NoError(t, err)

			for _, r := range []string{regionA, regionB, "ap-south-1"} {
				want := 0
				if r == tc.region {
					want = 1
				}

				assert.Len(t, acd.BackendFor(r).ListProfiles(), want, r)
			}
		})
	}
}
