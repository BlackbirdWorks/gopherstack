package azure_test

import "testing"

const azureKeyVaultFixture = `
resource "azurerm_resource_group" "kv" {
  name     = "gopherstack-kv-test-rg"
  location = "local"
}

resource "azurerm_key_vault" "kv" {
  name                       = "gopherstackkvtest"
  location                   = azurerm_resource_group.kv.location
  resource_group_name        = azurerm_resource_group.kv.name
  tenant_id                  = "00000000-0000-0000-0000-000000000000"
  sku_name                   = "standard"
  soft_delete_retention_days = 7
  purge_protection_enabled   = false
}

resource "azurerm_key_vault_access_policy" "kv" {
  key_vault_id = azurerm_key_vault.kv.id
  tenant_id    = "00000000-0000-0000-0000-000000000000"
  object_id    = "11111111-1111-1111-1111-111111111111"

  secret_permissions = ["Get", "List", "Set", "Delete"]
  key_permissions    = ["Get", "List", "Create", "Delete"]
}

resource "azurerm_key_vault_secret" "kv" {
  name         = "tf-secret"
  value        = "tf-secret-value"
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_key_vault_access_policy.kv]
}

resource "azurerm_key_vault_key" "kv" {
  name         = "tf-key"
  key_vault_id = azurerm_key_vault.kv.id
  key_type     = "RSA"
  key_size     = 2048
  key_opts     = ["decrypt", "encrypt", "sign", "unwrapKey", "verify", "wrapKey"]
  depends_on   = [azurerm_key_vault_access_policy.kv]
}
`

// TestTerraform_Azure_KeyVault applies a vault, access policy, secret and key
// through the azurerm provider (ARM control plane plus the Key Vault data
// plane over HTTPS), then requires an empty second plan and a clean destroy.
func TestTerraform_Azure_KeyVault(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	applyAzureTofuNoDrift(t, t.TempDir(), azurermProviderBlock(endpoint)+azureKeyVaultFixture)
}
