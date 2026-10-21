package azure_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azappconfig/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ac "github.com/blackbirdworks/gopherstack/services/azureappconfig"
)

const (
	azureAppConfigStoreName = "gopherstack-appcfg"
	azureAppConfigStorePath = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/" +
		"gopherstack-ac-test-rg/providers/Microsoft.AppConfiguration/configurationStores/" + azureAppConfigStoreName
)

const azureAppConfigFixture = `
resource "azurerm_resource_group" "ac" {
  name     = "gopherstack-ac-test-rg"
  location = "local"
}

resource "azurerm_app_configuration" "ac" {
  name                = "gopherstack-appcfg"
  resource_group_name = azurerm_resource_group.ac.name
  location            = azurerm_resource_group.ac.location
  sku                 = "standard"
}
`

// TestTerraform_Azure_AppConfiguration applies a store, requires an empty second plan, then
// writes a plain key, a Key Vault reference and a feature flag through the official SDK with
// the connection string ARM hands out and reads them back through both planes, then destroys.
//
// azurerm_app_configuration_key and _feature cannot run against a custom metadata_host:
// go-azure-sdk's environments.FromEndpoint has no App Configuration domain suffix in the
// metadata document, so the provider fails with "could not determine AppConfiguration domain
// suffix". The key-value resources are exercised through the data plane instead.
func TestTerraform_Azure_AppConfiguration(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	dir := t.TempDir()
	hcl := azurermProviderBlock(endpoint) + azureAppConfigFixture

	applyAzureTofuNoDrift(t, dir, hcl)

	t.Run("keys readable through the data plane", func(t *testing.T) {
		t.Parallel()
		verifyAppConfigRoundTrip(t)
	})
}

func readWriteConnectionString(ctx context.Context, t *testing.T, c *http.Client) string {
	t.Helper()

	keys := armCallAPI(ctx, t, c, http.MethodPost, azureAppConfigStorePath+"/listKeys", "2024-05-01")
	list, _ := keys["value"].([]any)
	require.NotEmpty(t, list)

	for _, k := range list {
		key, _ := k.(map[string]any)
		if ro, _ := key["readOnly"].(bool); !ro {
			conn, _ := key["connectionString"].(string)
			require.NotEmpty(t, conn)

			return conn
		}
	}

	require.Fail(t, "no read-write access key")

	return ""
}

func verifyAppConfigRoundTrip(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	c := armHTTPClient(t)

	store := armCallAPI(ctx, t, c, http.MethodGet, azureAppConfigStorePath, "2024-05-01")
	props, _ := store["properties"].(map[string]any)
	assert.Equal(t, "https://"+azureAppConfigStoreName+".azconfig.localhost:"+hostPortAppConfig, props["endpoint"])

	opts := &azappconfig.ClientOptions{}
	opts.Transport, opts.Retry = c, policy.RetryOptions{MaxRetries: -1}

	client, err := azappconfig.NewClientFromConnectionString(readWriteConnectionString(ctx, t, c), opts)
	require.NoError(t, err)

	put := func(key, label, value, contentType string) {
		opts := &azappconfig.SetSettingOptions{ContentType: &contentType}
		if label != "" {
			opts.Label = &label
		}

		_, setErr := client.SetSetting(ctx, key, &value, opts)
		require.NoError(t, setErr)
	}

	put("app/color", "prod", "red", "text/plain")
	put("secrets/db", "", `{"uri":"https://example.vault.azure.net/secrets/db"}`, ac.ContentTypeKeyVaultRef)
	put(ac.FeatureFlagPrefix+"beta", "", `{"id":"beta","enabled":true}`, ac.ContentTypeFeatureFlag)

	label := "prod"
	got, err := client.GetSetting(ctx, "app/color", &azappconfig.GetSettingOptions{Label: &label})
	require.NoError(t, err)
	assert.Equal(t, "red", *got.Value)

	ref, err := client.GetSetting(ctx, "secrets/db", nil)
	require.NoError(t, err)
	assert.Equal(t, ac.ContentTypeKeyVaultRef, *ref.ContentType)

	flag, err := client.GetSetting(ctx, ac.FeatureFlagPrefix+"beta", nil)
	require.NoError(t, err)
	assert.Equal(t, ac.ContentTypeFeatureFlag, *flag.ContentType)

	arm := armCallAPI(ctx, t, c, http.MethodGet, azureAppConfigStorePath+"/keyValues/app~2Fcolor$prod", "2024-05-01")
	assert.Equal(t, "red", arm["properties"].(map[string]any)["value"], "ARM and the data plane share key-values")
}
