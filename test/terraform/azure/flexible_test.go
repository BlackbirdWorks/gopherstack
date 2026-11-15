package azure_test

import "testing"

const azureFlexibleFixture = `
resource "azurerm_resource_group" "flex" {
  name     = "gopherstack-flex-test-rg"
  location = "local"
}

resource "azurerm_postgresql_flexible_server" "pg" {
  name                          = "gopherstack-pg-flex"
  resource_group_name           = azurerm_resource_group.flex.name
  location                      = azurerm_resource_group.flex.location
  version                       = "16"
  administrator_login           = "pgadmin"
  administrator_password        = "Sup3rS3cret!pw"
  sku_name                      = "B_Standard_B1ms"
  storage_mb                    = 32768
  public_network_access_enabled = true
  zone                          = "1"
}

resource "azurerm_postgresql_flexible_server_firewall_rule" "pg" {
  name             = "office"
  server_id        = azurerm_postgresql_flexible_server.pg.id
  start_ip_address = "10.0.0.1"
  end_ip_address   = "10.0.0.9"
}

resource "azurerm_postgresql_flexible_server_database" "pg" {
  name      = "appdb"
  server_id = azurerm_postgresql_flexible_server.pg.id
  collation = "en_US.utf8"
  charset   = "UTF8"
}

resource "azurerm_mysql_flexible_server" "my" {
  name                   = "gopherstack-my-flex"
  resource_group_name    = azurerm_resource_group.flex.name
  location               = azurerm_resource_group.flex.location
  version                = "8.0.21"
  administrator_login    = "myadmin"
  administrator_password = "Sup3rS3cret!pw"
  sku_name               = "B_Standard_B1ms"
  zone                   = "1"
}

resource "azurerm_mysql_flexible_database" "my" {
  name                = "appdb"
  resource_group_name = azurerm_resource_group.flex.name
  server_name         = azurerm_mysql_flexible_server.my.name
  charset             = "utf8mb4"
  collation           = "utf8mb4_unicode_ci"
}
`

// TestTerraform_Azure_FlexibleServers applies a PostgreSQL and a MySQL flexible server with a firewall rule and
// databases through the azurerm provider, then requires an empty second plan and a clean destroy.
func TestTerraform_Azure_FlexibleServers(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	applyAzureTofuNoDrift(t, t.TempDir(), azurermProviderBlock(endpoint)+azureFlexibleFixture)
}
