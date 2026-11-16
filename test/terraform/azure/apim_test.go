package azure_test

import "testing"

const azureAPIMFixture = `
resource "azurerm_resource_group" "apim" {
  name     = "gopherstack-apim-test-rg"
  location = "local"
}

resource "azurerm_api_management" "apim" {
  name                = "gopherstack-apim"
  location            = azurerm_resource_group.apim.location
  resource_group_name = azurerm_resource_group.apim.name
  publisher_name      = "Gopherstack"
  publisher_email     = "ops@example.com"
  sku_name            = "Developer_1"
}

resource "azurerm_api_management_named_value" "nv" {
  name                = "region"
  resource_group_name = azurerm_resource_group.apim.name
  api_management_name = azurerm_api_management.apim.name
  display_name        = "region"
  value               = "local"
}

resource "azurerm_api_management_backend" "be" {
  name                = "orders"
  resource_group_name = azurerm_resource_group.apim.name
  api_management_name = azurerm_api_management.apim.name
  protocol            = "http"
  url                 = "https://orders.example.com"
}

resource "azurerm_api_management_group" "grp" {
  name                = "partners"
  resource_group_name = azurerm_resource_group.apim.name
  api_management_name = azurerm_api_management.apim.name
  display_name        = "Partners"
}

resource "azurerm_api_management_user" "usr" {
  user_id             = "partner1"
  resource_group_name = azurerm_resource_group.apim.name
  api_management_name = azurerm_api_management.apim.name
  first_name          = "Pat"
  last_name           = "Partner"
  email               = "pat@example.com"
}
`

// TestTerraform_Azure_APIManagement applies an API Management service with a named value, a backend, a group and a
// user through the azurerm provider, then requires an empty second plan and a clean destroy.
func TestTerraform_Azure_APIManagement(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	applyAzureTofuNoDrift(t, t.TempDir(), azurermProviderBlock(endpoint)+azureAPIMFixture)
}
