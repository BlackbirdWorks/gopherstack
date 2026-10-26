package app

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/keyvault/armkeyvault"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azurekeyvault"
)

const (
	kvSDKSub    = "00000000-0000-0000-0000-000000000000"
	kvSDKRG     = "kv-rg"
	kvSDKTenant = "11111111-1111-1111-1111-111111111111"
)

type kvSDKCredential struct{}

func (kvSDKCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "emulator", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func newKVSDKClient(t *testing.T) *armkeyvault.VaultsClient {
	t.Helper()

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	kv := azurekeyvault.NewInMemoryBackend(false)
	armH.Registry.SetKeyVaults(&azureKeyVaultsAdapter{backend: kv})

	e := echo.New()
	e.Any("/*", armH.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	opts := &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: srv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: srv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	}

	c, err := armkeyvault.NewVaultsClient(kvSDKSub, kvSDKCredential{}, opts)
	require.NoError(t, err)

	return c
}

func createSDKVault(
	t *testing.T, c *armkeyvault.VaultsClient, name string, protect bool, mode *armkeyvault.CreateMode,
) {
	t.Helper()

	sku := armkeyvault.SKUNameStandard
	fam := armkeyvault.SKUFamilyA
	props := &armkeyvault.VaultProperties{
		TenantID:       new(kvSDKTenant),
		SKU:            &armkeyvault.SKU{Name: &sku, Family: &fam},
		AccessPolicies: []*armkeyvault.AccessPolicyEntry{},
		CreateMode:     mode,
	}

	if protect {
		props.EnablePurgeProtection = new(true)
	}

	poller, err := c.BeginCreateOrUpdate(t.Context(), kvSDKRG, name, armkeyvault.VaultCreateOrUpdateParameters{
		Location: new("westus"), Properties: props,
	}, nil)
	require.NoError(t, err)

	_, err = poller.PollUntilDone(t.Context(), &runtime.PollUntilDoneOptions{Frequency: time.Millisecond})
	require.NoError(t, err)
}

func TestAzureKeyVaultARM_DeletedVaultsSDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		protect   bool
		wantPurge bool
	}{
		{name: "purge allowed", wantPurge: true},
		{name: "purge protection blocks purge", protect: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newKVSDKClient(t)
			createSDKVault(t, c, "sdkvault", tt.protect, nil)

			_, err := c.Delete(t.Context(), kvSDKRG, "sdkvault", nil)
			require.NoError(t, err)

			got, err := c.GetDeleted(t.Context(), "sdkvault", "westus", nil)
			require.NoError(t, err)
			require.NotNil(t, got.Properties)
			assert.Contains(t, *got.Properties.VaultID, "/vaults/sdkvault")
			assert.NotNil(t, got.Properties.ScheduledPurgeDate)
			assert.NotNil(t, got.Properties.DeletionDate)

			pager := c.NewListDeletedPager(nil)
			page, err := pager.NextPage(t.Context())
			require.NoError(t, err)
			require.Len(t, page.Value, 1)
			assert.Equal(t, "sdkvault", *page.Value[0].Name)

			poller, err := c.BeginPurgeDeleted(t.Context(), "sdkvault", "westus", nil)
			if !tt.wantPurge {
				require.Error(t, err)

				_, err = c.GetDeleted(t.Context(), "sdkvault", "westus", nil)
				require.NoError(t, err, "a protected vault stays soft-deleted")

				return
			}

			require.NoError(t, err)

			_, err = poller.PollUntilDone(t.Context(), &runtime.PollUntilDoneOptions{Frequency: time.Millisecond})
			require.NoError(t, err)

			_, err = c.GetDeleted(t.Context(), "sdkvault", "westus", nil)
			require.Error(t, err)
		})
	}
}

func TestAzureKeyVaultARM_RecoverSDK(t *testing.T) {
	t.Parallel()

	c := newKVSDKClient(t)
	createSDKVault(t, c, "recoverme", false, nil)

	_, err := c.Delete(t.Context(), kvSDKRG, "recoverme", nil)
	require.NoError(t, err)

	mode := armkeyvault.CreateModeRecover
	createSDKVault(t, c, "recoverme", false, &mode)

	got, err := c.Get(t.Context(), kvSDKRG, "recoverme", nil)
	require.NoError(t, err)
	assert.Equal(t, "recoverme", *got.Name)

	_, err = c.GetDeleted(t.Context(), "recoverme", "westus", nil)
	require.Error(t, err)
}

func TestAzureKeyVaultARM_CheckNameAvailabilitySDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, c *armkeyvault.VaultsClient)
		name       string
		vault      string
		wantReason armkeyvault.Reason
		wantFree   bool
	}{
		{name: "free", vault: "freename", wantFree: true},
		{name: "invalid", vault: "1bad", wantReason: armkeyvault.ReasonAccountNameInvalid},
		{
			name: "taken", vault: "taken", wantReason: armkeyvault.ReasonAlreadyExists,
			setup: func(t *testing.T, c *armkeyvault.VaultsClient) {
				t.Helper()
				createSDKVault(t, c, "taken", false, nil)
			},
		},
		{
			name: "soft deleted still reserved", vault: "reserved", wantReason: armkeyvault.ReasonAlreadyExists,
			setup: func(t *testing.T, c *armkeyvault.VaultsClient) {
				t.Helper()
				createSDKVault(t, c, "reserved", false, nil)
				_, err := c.Delete(t.Context(), kvSDKRG, "reserved", nil)
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newKVSDKClient(t)
			if tt.setup != nil {
				tt.setup(t, c)
			}

			vt := armkeyvault.VaultCheckNameAvailabilityParameters{
				Name: new(tt.vault), Type: new("Microsoft.KeyVault/vaults"),
			}
			res, err := c.CheckNameAvailability(t.Context(), vt, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantFree, *res.NameAvailable)

			if !tt.wantFree {
				require.NotNil(t, res.Reason)
				assert.Equal(t, tt.wantReason, *res.Reason)
			}
		})
	}
}
