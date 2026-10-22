package app

import (
	"context"
	"encoding/base64"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerregistry/armcontainerregistry"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	acr "github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
	gcpartifactregistry "github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
)

type staticCredential struct{}

func (staticCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "emulator", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

type acrStack struct {
	arm  *httptest.Server
	acr  *httptest.Server
	acrH *acr.Handler
}

func newACRStack(t *testing.T) *acrStack {
	t.Helper()

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	backend := acr.NewInMemoryBackend()
	backend.SetContext(t.Context())

	acrH := acr.NewHandler(backend, acr.DefaultSettings())
	t.Cleanup(func() { acrH.Shutdown(t.Context()) })

	wireAzureContainerRegistry(map[string]service.Registerable{"AzureARM": armH, "AzureContainerRegistry": acrH})

	e := echo.New()
	e.Any("/*", armH.Handler())

	armSrv := httptest.NewTLSServer(e)
	t.Cleanup(armSrv.Close)

	acrSrv := httptest.NewServer(acrH)
	t.Cleanup(acrSrv.Close)

	return &acrStack{arm: armSrv, acr: acrSrv, acrH: acrH}
}

func (s *acrStack) clients(t *testing.T) *armcontainerregistry.ClientFactory {
	t.Helper()

	f, err := armcontainerregistry.NewClientFactory("00000000-0000-0000-0000-000000000000", staticCredential{},
		&arm.ClientOptions{
			Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
				cloud.ResourceManager: {Endpoint: s.arm.URL, Audience: "https://management.core.windows.net/"},
			}},
			Transport: s.arm.Client(),
			Retry:     policy.RetryOptions{MaxRetries: -1}})
	require.NoError(t, err)

	return f
}

type acrFixture struct {
	*acrStack

	f       *armcontainerregistry.ClientFactory
	regs    *armcontainerregistry.RegistriesClient
	created armcontainerregistry.RegistriesClientCreateResponse
}

func newACRFixture(t *testing.T) *acrFixture {
	t.Helper()

	s := newACRStack(t)
	f := s.clients(t)
	regs := f.NewRegistriesClient()

	create, err := regs.BeginCreate(t.Context(), "sdk-rg", "sdkregistry", armcontainerregistry.Registry{
		Location: new("local"),
		SKU:      &armcontainerregistry.SKU{Name: new(armcontainerregistry.SKUNamePremium)},
		Tags:     map[string]*string{"env": new("test")},
		Properties: &armcontainerregistry.RegistryProperties{
			AdminUserEnabled: new(true),
		},
	}, nil)
	require.NoError(t, err)

	created, err := create.PollUntilDone(t.Context(), nil)
	require.NoError(t, err)

	return &acrFixture{acrStack: s, f: f, regs: regs, created: created}
}

func TestAzureContainerRegistryARMSDK(t *testing.T) {
	t.Parallel()

	t.Run("create", func(t *testing.T) {
		t.Parallel()

		created := newACRFixture(t).created
		assert.Equal(t, "sdkregistry", *created.Name)
		assert.Equal(t, armcontainerregistry.ProvisioningStateSucceeded, *created.Properties.ProvisioningState)
		assert.Equal(t, "sdkregistry.azurecr.127.0.0.1:10011", *created.Properties.LoginServer)
		assert.True(t, *created.Properties.AdminUserEnabled)
	})

	t.Run("credentials", func(t *testing.T) {
		t.Parallel()

		fx := newACRFixture(t)
		regs, ctx := fx.regs, t.Context()

		creds, credErr := regs.ListCredentials(ctx, "sdk-rg", "sdkregistry", nil)
		require.NoError(t, credErr)
		assert.Equal(t, "sdkregistry", *creds.Username)
		require.Len(t, creds.Passwords, 2)
		assert.Equal(t, armcontainerregistry.PasswordNamePassword, *creds.Passwords[0].Name)

		next, regErr := regs.RegenerateCredential(
			ctx,
			"sdk-rg",
			"sdkregistry",
			armcontainerregistry.RegenerateCredentialParameters{
				Name: new(armcontainerregistry.PasswordNamePassword),
			},
			nil,
		)
		require.NoError(t, regErr)
		assert.NotEqual(t, *creds.Passwords[0].Value, *next.Passwords[0].Value)
		assert.Equal(t, *creds.Passwords[1].Value, *next.Passwords[1].Value)
	})

	t.Run("data plane push and pull with the admin credentials", func(t *testing.T) {
		t.Parallel()

		fx := newACRFixture(t)
		s, regs, ctx := fx.acrStack, fx.regs, t.Context()

		creds, credErr := regs.ListCredentials(ctx, "sdk-rg", "sdkregistry", nil)
		require.NoError(t, credErr)

		c := ocitest.Client{
			T: t, Base: s.acr.URL, Host: "sdkregistry.azurecr.localhost",
			Prepare: func(r *http.Request) {
				r.Header.Set("Authorization", "Basic "+
					base64.StdEncoding.EncodeToString([]byte(*creds.Username+":"+*creds.Passwords[0].Value)))
			},
		}

		img := c.PushImage("hello/world", "1.0")

		got := c.Do(http.MethodGet, "/v2/hello/world/manifests/1.0", nil, nil)
		require.Equal(t, http.StatusOK, got.Status)
		assert.Equal(t, img.Body, got.Body)

		tags := c.Do(http.MethodGet, "/acr/v1/hello/world/_tags", nil, nil)
		require.Equal(t, http.StatusOK, tags.Status)
		assert.Contains(t, string(tags.Body), `"name":"1.0"`)
	})

	t.Run("update", func(t *testing.T) {
		t.Parallel()

		fx := newACRFixture(t)
		s, regs, ctx := fx.acrStack, fx.regs, t.Context()

		upd, updErr := regs.BeginUpdate(ctx, "sdk-rg", "sdkregistry", armcontainerregistry.RegistryUpdateParameters{
			Tags:       map[string]*string{"env": new("prod")},
			Properties: &armcontainerregistry.RegistryPropertiesUpdateParameters{AdminUserEnabled: new(false)},
		}, nil)
		require.NoError(t, updErr)

		got, pollErr := upd.PollUntilDone(ctx, nil)
		require.NoError(t, pollErr)
		assert.Equal(t, "prod", *got.Tags["env"])
		assert.False(t, *got.Properties.AdminUserEnabled)

		_, credErr := regs.ListCredentials(ctx, "sdk-rg", "sdkregistry", nil)
		require.Error(t, credErr)

		c := ocitest.Client{T: t, Base: s.acr.URL, Host: "sdkregistry.azurecr.localhost"}
		assert.Equal(t, http.StatusUnauthorized, c.Do(http.MethodGet, "/v2/", nil, nil).Status)
	})

	t.Run("name availability", func(t *testing.T) {
		t.Parallel()

		fx := newACRFixture(t)
		regs, ctx := fx.regs, t.Context()

		taken, nameErr := regs.CheckNameAvailability(ctx, armcontainerregistry.RegistryNameCheckRequest{
			Name: new("sdkregistry"), Type: new("Microsoft.ContainerRegistry/registries"),
		}, nil)
		require.NoError(t, nameErr)
		assert.False(t, *taken.NameAvailable)

		free, nameErr := regs.CheckNameAvailability(ctx, armcontainerregistry.RegistryNameCheckRequest{
			Name: new("freeregistry"), Type: new("Microsoft.ContainerRegistry/registries"),
		}, nil)
		require.NoError(t, nameErr)
		assert.True(t, *free.NameAvailable)
	})

	t.Run("replications and webhooks", func(t *testing.T) {
		t.Parallel()

		fx := newACRFixture(t)
		f, ctx := fx.f, t.Context()

		rep, repErr := f.NewReplicationsClient().BeginCreate(ctx, "sdk-rg", "sdkregistry", "westus",
			armcontainerregistry.Replication{Location: new("westus")}, nil)
		require.NoError(t, repErr)

		_, repErr = rep.PollUntilDone(ctx, nil)
		require.NoError(t, repErr)

		hook, hookErr := f.NewWebhooksClient().BeginCreate(ctx, "sdk-rg", "sdkregistry", "deployhook",
			armcontainerregistry.WebhookCreateParameters{
				Location: new("local"),
				Properties: &armcontainerregistry.WebhookPropertiesCreateParameters{
					ServiceURI: new("https://example.com/hook"),
					Actions:    []*armcontainerregistry.WebhookAction{new(armcontainerregistry.WebhookActionPush)},
				},
			}, nil)
		require.NoError(t, hookErr)

		made, hookErr := hook.PollUntilDone(ctx, nil)
		require.NoError(t, hookErr)
		assert.Equal(t, "deployhook", *made.Name)

		cfg, cfgErr := f.NewWebhooksClient().GetCallbackConfig(ctx, "sdk-rg", "sdkregistry", "deployhook", nil)
		require.NoError(t, cfgErr)
		assert.Equal(t, "https://example.com/hook", *cfg.ServiceURI)

		pager := f.NewReplicationsClient().NewListPager("sdk-rg", "sdkregistry", nil)
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)
		assert.Len(t, page.Value, 1)
	})

	t.Run("list and delete", func(t *testing.T) {
		t.Parallel()

		fx := newACRFixture(t)
		s, regs, ctx := fx.acrStack, fx.regs, t.Context()

		pager := regs.NewListByResourceGroupPager("sdk-rg", nil)
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)
		assert.Len(t, page.Value, 1)

		del, delErr := regs.BeginDelete(ctx, "sdk-rg", "sdkregistry", nil)
		require.NoError(t, delErr)

		_, delErr = del.PollUntilDone(ctx, nil)
		require.NoError(t, delErr)

		_, getErr := regs.Get(ctx, "sdk-rg", "sdkregistry", nil)
		require.Error(t, getErr)

		_, found := s.acrH.Backend.Registry("sdkregistry")
		assert.False(t, found, "deleting through ARM removes the data plane registry")
	})
}

func TestReserveFixedServicePorts_AzureContainerRegistry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rangeStart int
		rangeEnd   int
		wantTaken  bool
	}{
		{"port inside pool range", 10000, 10100, true},
		{"port outside pool range", 20000, 20100, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(tt.rangeStart, tt.rangeEnd)
			require.NoError(t, err)

			cli := CLI{AzureACR: acr.Settings{Port: acr.DefaultPort}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(acr.DefaultPort))
		})
	}
}

func TestRegistryDataDir(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    string
		persist bool
	}{
		{"off", "", false},
		{"on", "/data/registries", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, (&CLI{DataDir: "/data", Persist: tt.persist}).GetRegistryDataDir())
		})
	}
}

func TestReserveFixedServicePorts_GCPArtifactRegistry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rangeStart int
		rangeEnd   int
		wantTaken  bool
	}{
		{"port inside pool range", 8100, 8200, true},
		{"port outside pool range", 10000, 10100, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(tt.rangeStart, tt.rangeEnd)
			require.NoError(t, err)

			cli := CLI{GCPArtifactRegistry: gcpartifactregistry.Settings{Port: gcpartifactregistry.DefaultPort}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantTaken, alloc.IsAllocated(gcpartifactregistry.DefaultPort))
		})
	}
}
