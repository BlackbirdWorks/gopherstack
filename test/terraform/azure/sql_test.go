package azure_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The gopherstack container has no container runtime socket, so Azure SQL runs state-only here: ARM state is
// real, there is no SQL Server behind the server's port.
const azureSQLFixture = `
resource "azurerm_resource_group" "sql" {
  name     = "gopherstack-sql-test-rg"
  location = "local"
}

resource "azurerm_mssql_server" "sql" {
  name                         = "gopherstack-sql-server"
  resource_group_name          = azurerm_resource_group.sql.name
  location                     = azurerm_resource_group.sql.location
  version                      = "12.0"
  administrator_login          = "sqladmin"
  administrator_login_password = "Gopher-Stack-1!"
  minimum_tls_version          = "1.2"

  tags = {
    env = "test"
  }
}

resource "azurerm_mssql_database" "app" {
  name         = "appdb"
  server_id    = azurerm_mssql_server.sql.id
  collation    = "SQL_Latin1_General_CP1_CI_AS"
  max_size_gb  = 2
  sku_name     = "S0"
  license_type = "LicenseIncluded"
}

resource "azurerm_mssql_elasticpool" "pool" {
  name                = "gopherstack-sql-pool"
  resource_group_name = azurerm_resource_group.sql.name
  location            = azurerm_resource_group.sql.location
  server_name         = azurerm_mssql_server.sql.name
  max_size_gb         = 50

  sku {
    name     = "GP_Gen5"
    tier     = "GeneralPurpose"
    family   = "Gen5"
    capacity = 2
  }

  per_database_settings {
    min_capacity = 0
    max_capacity = 2
  }
}

resource "azurerm_mssql_database" "pooled" {
  name            = "pooleddb"
  server_id       = azurerm_mssql_server.sql.id
  elastic_pool_id = azurerm_mssql_elasticpool.pool.id
  sku_name        = "ElasticPool"
}

resource "azurerm_mssql_firewall_rule" "office" {
  name             = "office"
  server_id        = azurerm_mssql_server.sql.id
  start_ip_address = "10.0.0.1"
  end_ip_address   = "10.0.0.9"
}

resource "azurerm_mssql_firewall_rule" "azure" {
  name             = "AllowAllWindowsAzureIps"
  server_id        = azurerm_mssql_server.sql.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

output "fqdn" {
  value = azurerm_mssql_server.sql.fully_qualified_domain_name
}

output "database_id" {
  value = azurerm_mssql_database.app.id
}
`

// TestTerraform_Azure_MSSQL applies a logical server with a database and firewall rules, requires an empty
// second plan, and destroys it.
func TestTerraform_Azure_MSSQL(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	dir := t.TempDir()
	applyAzureTofuNoDrift(t, dir, azurermProviderBlock(endpoint)+azureSQLFixture)

	assert.Equal(t, "gopherstack-sql-server.database.windows.net", tofuOutput(t, dir, "fqdn"))
	assert.Contains(t, tofuOutput(t, dir, "database_id"), "/servers/gopherstack-sql-server/databases/appdb")
}
