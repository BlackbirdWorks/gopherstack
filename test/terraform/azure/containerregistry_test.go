package azure_test

import (
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
)

const azureACRFixture = `
resource "azurerm_resource_group" "acr" {
  name     = "gopherstack-acr-test-rg"
  location = "local"
}

resource "azurerm_container_registry" "acr" {
  name                = "gopherstackacr"
  resource_group_name = azurerm_resource_group.acr.name
  location            = azurerm_resource_group.acr.location
  sku                 = "Basic"
  admin_enabled       = true
}

output "login_server" {
  value = azurerm_container_registry.acr.login_server
}

output "admin_username" {
  value = azurerm_container_registry.acr.admin_username
}

output "admin_password" {
  value     = azurerm_container_registry.acr.admin_password
  sensitive = true
}
`

// TestTerraform_Azure_ContainerRegistry applies a registry with the admin user, requires an empty second plan,
// then pushes and pulls an image over the Docker Registry v2 API with the admin credentials, and destroys it.
func TestTerraform_Azure_ContainerRegistry(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	dir := t.TempDir()
	hcl := azurermProviderBlock(endpoint) + azureACRFixture

	applyAzureTofuNoDrift(t, dir, hcl)

	loginServer := tofuOutput(t, dir, "login_server")
	user := tofuOutput(t, dir, "admin_username")
	pass := tofuOutput(t, dir, "admin_password")

	assert.Equal(t, "gopherstackacr.azurecr.localhost:"+hostPortACR, loginServer)
	assert.Equal(t, "gopherstackacr", user)
	require.NotEmpty(t, pass)

	c := ocitest.Client{
		T: t, Base: "http://127.0.0.1:" + hostPortACR, Host: loginServer,
		Prepare: func(r *http.Request) {
			r.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(user+":"+pass)))
		},
	}

	img := c.PushImage("tf/app", "v1")

	got := c.Do(http.MethodGet, "/v2/tf/app/manifests/v1", nil, map[string]string{"Accept": ocitest.ManifestMediaType})
	require.Equal(t, http.StatusOK, got.Status)
	assert.Equal(t, img.Body, got.Body)

	denied := ocitest.Client{T: t, Base: c.Base, Host: loginServer}.Do(http.MethodGet, "/v2/", nil, nil)
	assert.Equal(t, http.StatusUnauthorized, denied.Status)
}
