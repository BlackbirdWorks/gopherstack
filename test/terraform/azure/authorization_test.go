package azure_test

import "testing"

const azureAuthorizationFixture = `
resource "azurerm_resource_group" "auth" {
  name     = "gopherstack-auth-test-rg"
  location = "local"
}

resource "azurerm_storage_account" "auth" {
  name                     = "gopherstackauthtest"
  resource_group_name      = azurerm_resource_group.auth.name
  location                 = azurerm_resource_group.auth.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_role_assignment" "reader" {
  scope                            = azurerm_resource_group.auth.id
  role_definition_name             = "Reader"
  principal_id                     = "11111111-1111-1111-1111-111111111111"
  principal_type                   = "ServicePrincipal"
  skip_service_principal_aad_check = true
}

resource "azurerm_role_assignment" "blob" {
  scope                            = azurerm_storage_account.auth.id
  role_definition_id = format(
    "%s/providers/Microsoft.Authorization/roleDefinitions/%s",
    "/subscriptions/00000000-0000-0000-0000-000000000000",
    "ba92f5b4-2d11-453d-a403-e96b0029c9fe",
  )
  principal_id                     = "11111111-1111-1111-1111-111111111111"
  principal_type                   = "ServicePrincipal"
  skip_service_principal_aad_check = true
}

resource "azurerm_management_lock" "account" {
  name       = "keep-account"
  scope      = azurerm_storage_account.auth.id
  lock_level = "CanNotDelete"
  notes      = "managed by terraform"
}

resource "azurerm_policy_definition" "audit" {
  name         = "gopherstack-audit-storage"
  policy_type  = "Custom"
  mode         = "All"
  display_name = "Audit storage accounts"

  policy_rule = <<RULE
{
  "if": {"field": "type", "equals": "Microsoft.Storage/storageAccounts"},
  "then": {"effect": "audit"}
}
RULE
}

resource "azurerm_resource_group_policy_assignment" "audit" {
  name                 = "gopherstack-audit"
  resource_group_id    = azurerm_resource_group.auth.id
  policy_definition_id = azurerm_policy_definition.audit.id
}
`

// TestTerraform_Azure_Authorization applies role assignments (by role name and
// by role definition id), a management lock, a custom policy definition and a
// policy assignment through the azurerm provider, then requires an empty second
// plan and a clean destroy (the lock goes before the account it protects).
func TestTerraform_Azure_Authorization(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	applyAzureTofuNoDrift(t, t.TempDir(), azurermProviderBlock(endpoint)+azureAuthorizationFixture)
}

const azureAuthorizationScopedFixture = `
data "azurerm_subscription" "current" {}

resource "azurerm_role_definition" "custom" {
  name        = "gopherstack-custom-reader"
  scope       = data.azurerm_subscription.current.id
  description = "reads resource groups"

  permissions {
    actions     = ["Microsoft.Resources/subscriptions/resourceGroups/read"]
    not_actions = []
  }

  assignable_scopes = [data.azurerm_subscription.current.id]
}

resource "azurerm_policy_definition" "tagged" {
  name         = "gopherstack-require-env-tag"
  policy_type  = "Custom"
  mode         = "All"
  display_name = "Require the env tag"

  policy_rule = <<RULE
{
  "if": {"field": "tags['env']", "exists": "false"},
  "then": {"effect": "audit"}
}
RULE
}

resource "azurerm_policy_set_definition" "set" {
  name         = "gopherstack-initiative"
  policy_type  = "Custom"
  display_name = "Initiative"

  policy_definition_reference {
    policy_definition_id = azurerm_policy_definition.tagged.id
  }
}

resource "azurerm_subscription_policy_assignment" "set" {
  name                 = "gopherstack-sub-assignment"
  subscription_id      = data.azurerm_subscription.current.id
  policy_definition_id = azurerm_policy_set_definition.set.id
  enforce              = false
}
`

// TestTerraform_Azure_AuthorizationScoped applies a custom role definition, a policy set definition and a
// subscription-scope policy assignment, then requires an empty second plan and a clean destroy.
func TestTerraform_Azure_AuthorizationScoped(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	applyAzureTofuNoDrift(t, t.TempDir(), azurermProviderBlock(endpoint)+azureAuthorizationScopedFixture)
}
