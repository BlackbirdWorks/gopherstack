package app

import (
	"archive/zip"
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appservice/armappservice/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func flexSDKConfig() *armappservice.FunctionAppConfig {
	return &armappservice.FunctionAppConfig{
		Deployment: &armappservice.FunctionsDeployment{Storage: &armappservice.FunctionsDeploymentStorage{
			Type:  new(armappservice.FunctionsDeploymentStorageTypeBlobContainer),
			Value: new("https://acct.blob.core.windows.net/deploy"),
			Authentication: &armappservice.FunctionsDeploymentStorageAuthentication{
				Type: new(armappservice.AuthenticationTypeSystemAssignedIdentity),
			},
		}},
		Runtime: &armappservice.FunctionsRuntime{
			Name:    new(armappservice.RuntimeNameNode),
			Version: new("22"),
		},
		ScaleAndConcurrency: &armappservice.FunctionsScaleAndConcurrency{InstanceMemoryMB: new(int32(2048))},
	}
}

func (s *fnSDKStack) createFlexPlan(t *testing.T, name string) *armappservice.Plan {
	t.Helper()

	poller, err := s.factory.NewPlansClient().BeginCreateOrUpdate(t.Context(), fnSDKRG, name, armappservice.Plan{
		Location: new("local"), Kind: new("functionapp"),
		SKU:        &armappservice.SKUDescription{Name: new("FC1"), Tier: new("FlexConsumption")},
		Properties: &armappservice.PlanProperties{Reserved: new(true)},
	}, nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollOpts())
	require.NoError(t, err)

	return &res.Plan
}

func (s *fnSDKStack) putSiteWith(
	t *testing.T, name, planID string, mutate func(*armappservice.SiteProperties),
) error {
	t.Helper()

	props := &armappservice.SiteProperties{ServerFarmID: new(planID), SiteConfig: &armappservice.SiteConfig{}}
	mutate(props)

	poller, err := s.factory.NewWebAppsClient().BeginCreateOrUpdate(t.Context(), fnSDKRG, name, armappservice.Site{
		Location: new("local"), Kind: new("functionapp,linux"), Properties: props,
	}, nil)
	if err != nil {
		return err
	}

	_, err = poller.PollUntilDone(t.Context(), pollOpts())

	return err
}

func TestAzureFunctionsARMSDK_Flex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(*armappservice.FunctionAppConfig)
		name     string
		wantCode int
	}{
		{func(*armappservice.FunctionAppConfig) {}, "valid", 0},
		{func(c *armappservice.FunctionAppConfig) { c.Runtime.Name = new(armappservice.RuntimeName("cobol")) },
			"unknown runtime", http.StatusBadRequest},
		{func(c *armappservice.FunctionAppConfig) { c.Runtime.Version = new("14") },
			"unsupported node version", http.StatusBadRequest},
		{func(c *armappservice.FunctionAppConfig) { c.ScaleAndConcurrency.InstanceMemoryMB = new(int32(1024)) },
			"bad memory size", http.StatusBadRequest},
		{func(c *armappservice.FunctionAppConfig) {
			c.ScaleAndConcurrency.MaximumInstanceCount = new(int32(1001))
		},
			"too many instances", http.StatusBadRequest},
		{func(c *armappservice.FunctionAppConfig) { c.Deployment.Storage.Value = new("not a url") },
			"bad storage url", http.StatusBadRequest},
		{func(c *armappservice.FunctionAppConfig) {
			c.Deployment.Storage.Authentication.Type = new(armappservice.AuthenticationTypeUserAssignedIdentity)
		}, "identity auth without identity", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newFnSDKStack(t)
			plan := s.createFlexPlan(t, "flexplan")
			cfg := flexSDKConfig()
			tt.mutate(cfg)

			err := s.putSiteWith(t, "flexapp", *plan.ID, func(p *armappservice.SiteProperties) {
				p.FunctionAppConfig = cfg
			})

			if tt.wantCode != 0 {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, responseCode(err))

				return
			}

			require.NoError(t, err)

			got, getErr := s.factory.NewWebAppsClient().Get(t.Context(), fnSDKRG, "flexapp", nil)
			require.NoError(t, getErr)

			fc := got.Properties.FunctionAppConfig
			require.NotNil(t, fc)
			assert.Equal(t, "node", string(*fc.Runtime.Name))
			assert.Equal(t, int32(100), *fc.ScaleAndConcurrency.MaximumInstanceCount, "default instance count")
			assert.Equal(t, int32(2048), *fc.ScaleAndConcurrency.InstanceMemoryMB)
			assert.Equal(t, "https://acct.blob.core.windows.net/deploy", *fc.Deployment.Storage.Value)

			info, ok := s.backend.App("flexapp")
			require.True(t, ok)
			assert.Equal(
				t,
				"mcr.microsoft.com/azure-functions/node:4-node22",
				info.Image,
				"runtime.version picks the image",
			)
		})
	}

	t.Run("config needs a flex plan", func(t *testing.T) {
		t.Parallel()

		s := newFnSDKStack(t)
		plan := s.createPlan(t, "dynplan")

		err := s.putSiteWith(t, "dynapp", *plan.ID, func(p *armappservice.SiteProperties) {
			p.FunctionAppConfig = flexSDKConfig()
		})
		require.Error(t, err)
		assert.Equal(t, http.StatusBadRequest, responseCode(err))
	})

	t.Run("flex plan needs config", func(t *testing.T) {
		t.Parallel()

		s := newFnSDKStack(t)
		plan := s.createFlexPlan(t, "flexplan")

		err := s.putSiteWith(t, "flexapp", *plan.ID, func(*armappservice.SiteProperties) {})
		require.Error(t, err)
		assert.Equal(t, http.StatusBadRequest, responseCode(err))
	})

	t.Run("publish without a type", func(t *testing.T) {
		t.Parallel()

		s := newFnSDKStack(t)
		plan := s.createFlexPlan(t, "flexplan")
		require.NoError(t, s.putSiteWith(t, "flexapp", *plan.ID, func(p *armappservice.SiteProperties) {
			p.FunctionAppConfig = flexSDKConfig()
		}))

		poller, err := s.factory.NewWebAppsClient().BeginListPublishingCredentials(t.Context(), fnSDKRG, "flexapp", nil)
		require.NoError(t, err)

		cred, err := poller.PollUntilDone(t.Context(), pollOpts())
		require.NoError(t, err)

		user, pass := *cred.Properties.PublishingUserName, *cred.Properties.PublishingPassword
		host := "flexapp.scm." + fnSDKDomain

		code, _ := s.call(t, http.MethodPost, host, "/api/publish?RemoteBuild=false", fnZip(t), user, pass)
		assert.Equal(t, http.StatusOK, code)

		code, _ = s.call(t, http.MethodPost, host, "/api/publish?RemoteBuild=true", fnZip(t), user, pass)
		assert.Equal(t, http.StatusBadRequest, code)
	})
}

func TestAzureFunctionsARMSDK_Management(t *testing.T) {
	t.Parallel()

	s := newFnSDKStack(t)
	plan := s.createPlan(t, "mgmtplan")
	apps := s.factory.NewWebAppsClient()

	deploy := func(t *testing.T, name string) {
		t.Helper()

		s.createSite(t, name, *plan.ID, map[string]string{"FUNCTIONS_WORKER_RUNTIME": "node"})
		_, err := s.backend.Deploy(name, fnZip(t), "t", "m")
		require.NoError(t, err)
	}

	t.Run("delete function", func(t *testing.T) {
		t.Parallel()

		deploy(t, "delapp")

		_, err := apps.DeleteFunction(t.Context(), fnSDKRG, "delapp", "hello", nil)
		require.NoError(t, err)

		zr, err := zip.NewReader(bytes.NewReader(s.backend.Package("delapp")), int64(len(s.backend.Package("delapp"))))
		require.NoError(t, err)

		for _, f := range zr.File {
			assert.False(t, strings.HasPrefix(f.Name, "hello/"), f.Name)
		}

		assert.NotEmpty(t, zr.File, "host.json stays")

		_, err = apps.DeleteFunction(t.Context(), fnSDKRG, "delapp", "ghost", nil)
		require.Error(t, err)
		assert.Equal(t, http.StatusNotFound, responseCode(err))

		_, err = apps.DeleteFunction(t.Context(), fnSDKRG, "delapp", "tick", nil)
		require.Error(t, err)
		assert.Equal(t, http.StatusBadRequest, responseCode(err), "tick has no function.json")
	})

	t.Run("admin token", func(t *testing.T) {
		t.Parallel()

		deploy(t, "tokapp")

		res, err := apps.GetFunctionsAdminToken(t.Context(), fnSDKRG, "tokapp", nil)
		require.NoError(t, err)
		require.NotNil(t, res.Value)

		parts := strings.Split(*res.Value, ".")
		require.Len(t, parts, 3)

		claims, err := base64.RawURLEncoding.DecodeString(parts[1])
		require.NoError(t, err)

		var c map[string]any
		require.NoError(t, json.Unmarshal(claims, &c))
		assert.Equal(t, "https://tokapp.azurewebsites.net/azurefunctions", c["aud"])
		assert.Greater(t, c["exp"], c["iat"])

		sig, err := base64.RawURLEncoding.DecodeString(parts[2])
		require.NoError(t, err)

		key := s.rt.lastEnv("tokapp")["WEBSITE_AUTH_ENCRYPTION_KEY"]
		assert.Empty(t, key, "no host started yet")

		_, err = s.backend.Functions(t.Context(), "tokapp")
		require.NoError(t, err)

		raw := s.rt.lastEnv("tokapp")["WEBSITE_AUTH_ENCRYPTION_KEY"]
		require.NotEmpty(t, raw)

		keyBytes, err := hex.DecodeString(raw)
		require.NoError(t, err)

		mac := hmac.New(sha256.New, keyBytes)
		_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
		assert.True(t, hmac.Equal(sig, mac.Sum(nil)), "the host's signing key validates the token")
	})

	t.Run("sync triggers", func(t *testing.T) {
		t.Parallel()

		deploy(t, "syncapp")

		_, err := apps.SyncFunctionTriggers(t.Context(), fnSDKRG, "syncapp", nil)
		require.NoError(t, err)

		_, err = apps.SyncFunctionTriggers(t.Context(), fnSDKRG, "ghostapp", nil)
		require.Error(t, err)
		assert.Equal(t, http.StatusNotFound, responseCode(err))
	})
}
