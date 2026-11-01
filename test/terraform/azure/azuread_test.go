package azure_test

import (
	"fmt"
	"testing"
)

// azureadProviderBlock points the azuread provider at the gopherstack ARM metadata document, which advertises
// the Microsoft Graph listener as the environment's graph endpoint.
func azureadProviderBlock(metadataHost string) string {
	return fmt.Sprintf(`terraform {
  required_providers {
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 3.0"
    }
  }
  required_version = ">= 1.0"
}

provider "azuread" {
  metadata_host = %[1]q
  environment   = "gopherstack"
  tenant_id     = "00000000-0000-0000-0000-000000000000"
  client_id     = "00000000-0000-0000-0000-000000000000"
  client_secret = "gopherstack"
}
`, metadataHost)
}

const azureadFixture = `
resource "azuread_application" "app" {
  display_name     = "gopherstack-tf-app"
  sign_in_audience = "AzureADMyOrg"
  identifier_uris  = ["api://gopherstack-tf-app"]

  web {
    redirect_uris = ["https://localhost/callback"]
  }

  required_resource_access {
    resource_app_id = "00000003-0000-0000-c000-000000000000"

    resource_access {
      id   = "e1fe6dd8-ba31-4d61-89e7-88639da4683d"
      type = "Scope"
    }
  }
}

resource "azuread_service_principal" "sp" {
  client_id = azuread_application.app.client_id
}

resource "azuread_application_password" "secret" {
  application_id = azuread_application.app.id
  display_name   = "terraform"
}
`

// TestTerraform_Azure_AzureAD applies an application, its service principal and a client secret through the
// azuread provider against the Microsoft Graph listener, then requires an empty second plan and a clean destroy.
func TestTerraform_Azure_AzureAD(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	applyAzureTofuNoDrift(t, t.TempDir(), azureadProviderBlock(endpoint)+azureadFixture)
}
