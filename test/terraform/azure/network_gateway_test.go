package azure_test

import (
	"testing"
)

const azureGatewayFixture = `
resource "azurerm_resource_group" "gw" {
  name     = "gopherstack-gw-test-rg"
  location = "westus"
}

resource "azurerm_virtual_network" "gw" {
  name                = "gopherstack-gw-vnet"
  location            = azurerm_resource_group.gw.location
  resource_group_name = azurerm_resource_group.gw.name
  address_space       = ["10.30.0.0/16"]
}

resource "azurerm_subnet" "appgw" {
  name                 = "appgw"
  resource_group_name  = azurerm_resource_group.gw.name
  virtual_network_name = azurerm_virtual_network.gw.name
  address_prefixes     = ["10.30.1.0/24"]
}

resource "azurerm_subnet" "firewall" {
  name                 = "AzureFirewallSubnet"
  resource_group_name  = azurerm_resource_group.gw.name
  virtual_network_name = azurerm_virtual_network.gw.name
  address_prefixes     = ["10.30.2.0/26"]
}

resource "azurerm_public_ip" "appgw" {
  name                = "gopherstack-gw-appgw-pip"
  location            = azurerm_resource_group.gw.location
  resource_group_name = azurerm_resource_group.gw.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_public_ip" "firewall" {
  name                = "gopherstack-gw-fw-pip"
  location            = azurerm_resource_group.gw.location
  resource_group_name = azurerm_resource_group.gw.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_application_gateway" "gw" {
  name                = "gopherstack-gw-appgw"
  location            = azurerm_resource_group.gw.location
  resource_group_name = azurerm_resource_group.gw.name

  sku {
    name     = "Standard_v2"
    tier     = "Standard_v2"
    capacity = 2
  }

  gateway_ip_configuration {
    name      = "gateway-ip"
    subnet_id = azurerm_subnet.appgw.id
  }

  frontend_port {
    name = "http"
    port = 80
  }

  frontend_ip_configuration {
    name                 = "public"
    public_ip_address_id = azurerm_public_ip.appgw.id
  }

  backend_address_pool {
    name         = "pool"
    ip_addresses = ["10.30.3.4"]
  }

  backend_http_settings {
    name                  = "settings"
    cookie_based_affinity = "Disabled"
    port                  = 80
    protocol              = "Http"
    request_timeout       = 30
  }

  http_listener {
    name                           = "listener"
    frontend_ip_configuration_name = "public"
    frontend_port_name             = "http"
    protocol                       = "Http"
  }

  request_routing_rule {
    name                       = "rule"
    rule_type                  = "Basic"
    priority                   = 10
    http_listener_name         = "listener"
    backend_address_pool_name  = "pool"
    backend_http_settings_name = "settings"
  }
}

resource "azurerm_firewall_policy" "gw" {
  name                = "gopherstack-gw-policy"
  location            = azurerm_resource_group.gw.location
  resource_group_name = azurerm_resource_group.gw.name
  sku                 = "Standard"
}

resource "azurerm_firewall_policy_rule_collection_group" "gw" {
  name               = "gopherstack-gw-rcg"
  firewall_policy_id = azurerm_firewall_policy.gw.id
  priority           = 500

  network_rule_collection {
    name     = "allow-dns"
    priority = 400
    action   = "Allow"

    rule {
      name                  = "dns"
      protocols             = ["UDP"]
      source_addresses      = ["10.30.0.0/16"]
      destination_addresses = ["168.63.129.16"]
      destination_ports     = ["53"]
    }
  }
}

resource "azurerm_firewall" "gw" {
  name                = "gopherstack-gw-fw"
  location            = azurerm_resource_group.gw.location
  resource_group_name = azurerm_resource_group.gw.name
  sku_name            = "AZFW_VNet"
  sku_tier            = "Standard"
  firewall_policy_id  = azurerm_firewall_policy.gw.id

  ip_configuration {
    name                 = "configuration"
    subnet_id            = azurerm_subnet.firewall.id
    public_ip_address_id = azurerm_public_ip.firewall.id
  }

  depends_on = [azurerm_firewall_policy_rule_collection_group.gw]
}

output "appgw_public_ip" {
  value = azurerm_public_ip.appgw.ip_address
}
`

// TestTerraform_Azure_NetworkGateways applies a v2 application gateway with one listener, rule and pool plus an Azure
// Firewall with a firewall policy and rule collection group, requires an empty second plan, then destroys everything.
func TestTerraform_Azure_NetworkGateways(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	dir := t.TempDir()
	applyAzureTofuNoDrift(t, dir, azurermProviderBlock(endpoint)+azureGatewayFixture)

	if got := tofuOutput(t, dir, "appgw_public_ip"); got == "" {
		t.Fatal("application gateway public IP output is empty")
	}
}
