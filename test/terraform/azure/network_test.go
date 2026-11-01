package azure_test

import (
	"testing"
)

const azureNetworkFixture = `
resource "azurerm_resource_group" "net" {
  name     = "gopherstack-net-test-rg"
  location = "westus"
}

resource "azurerm_virtual_network" "net" {
  name                = "gopherstack-net-vnet"
  location            = azurerm_resource_group.net.location
  resource_group_name = azurerm_resource_group.net.name
  address_space       = ["10.20.0.0/16"]
}

resource "azurerm_subnet" "workload" {
  name                 = "workload"
  resource_group_name  = azurerm_resource_group.net.name
  virtual_network_name = azurerm_virtual_network.net.name
  address_prefixes     = ["10.20.1.0/24"]
}

resource "azurerm_subnet" "endpoints" {
  name                 = "endpoints"
  resource_group_name  = azurerm_resource_group.net.name
  virtual_network_name = azurerm_virtual_network.net.name
  address_prefixes     = ["10.20.2.0/24"]
}

resource "azurerm_public_ip" "lb" {
  name                = "gopherstack-net-lb-pip"
  location            = azurerm_resource_group.net.location
  resource_group_name = azurerm_resource_group.net.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_lb" "net" {
  name                = "gopherstack-net-lb"
  location            = azurerm_resource_group.net.location
  resource_group_name = azurerm_resource_group.net.name
  sku                 = "Standard"

  frontend_ip_configuration {
    name                 = "public"
    public_ip_address_id = azurerm_public_ip.lb.id
  }
}

resource "azurerm_lb_backend_address_pool" "net" {
  name            = "backend"
  loadbalancer_id = azurerm_lb.net.id
}

resource "azurerm_lb_probe" "net" {
  name            = "http"
  loadbalancer_id = azurerm_lb.net.id
  protocol        = "Http"
  port            = 80
  request_path    = "/healthz"
}

resource "azurerm_lb_rule" "net" {
  name                           = "web"
  loadbalancer_id                = azurerm_lb.net.id
  protocol                       = "Tcp"
  frontend_port                  = 80
  backend_port                   = 8080
  frontend_ip_configuration_name = "public"
  backend_address_pool_ids       = [azurerm_lb_backend_address_pool.net.id]
  probe_id                       = azurerm_lb_probe.net.id
}

resource "azurerm_network_interface" "member" {
  name                = "gopherstack-net-nic"
  location            = azurerm_resource_group.net.location
  resource_group_name = azurerm_resource_group.net.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.workload.id
    private_ip_address_allocation = "Dynamic"
  }
}

resource "azurerm_network_interface_backend_address_pool_association" "net" {
  network_interface_id    = azurerm_network_interface.member.id
  ip_configuration_name   = "internal"
  backend_address_pool_id = azurerm_lb_backend_address_pool.net.id
}

resource "azurerm_public_ip" "nat" {
  name                = "gopherstack-net-nat-pip"
  location            = azurerm_resource_group.net.location
  resource_group_name = azurerm_resource_group.net.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_nat_gateway" "net" {
  name                    = "gopherstack-net-nat"
  location                = azurerm_resource_group.net.location
  resource_group_name     = azurerm_resource_group.net.name
  sku_name                = "Standard"
  idle_timeout_in_minutes = 10
}

resource "azurerm_nat_gateway_public_ip_association" "net" {
  nat_gateway_id       = azurerm_nat_gateway.net.id
  public_ip_address_id = azurerm_public_ip.nat.id
}

resource "azurerm_subnet_nat_gateway_association" "net" {
  subnet_id      = azurerm_subnet.workload.id
  nat_gateway_id = azurerm_nat_gateway.net.id
}

resource "azurerm_storage_account" "net" {
  name                     = "gopherstacknetsa"
  location                 = azurerm_resource_group.net.location
  resource_group_name      = azurerm_resource_group.net.name
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_private_dns_zone" "blob" {
  name                = "privatelink.blob.core.windows.net"
  resource_group_name = azurerm_resource_group.net.name
}

resource "azurerm_private_endpoint" "blob" {
  name                = "gopherstack-net-pe"
  location            = azurerm_resource_group.net.location
  resource_group_name = azurerm_resource_group.net.name
  subnet_id           = azurerm_subnet.endpoints.id

  private_service_connection {
    name                           = "blob"
    private_connection_resource_id = azurerm_storage_account.net.id
    subresource_names              = ["blob"]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name                 = "default"
    private_dns_zone_ids = [azurerm_private_dns_zone.blob.id]
  }
}

output "lb_frontend_ip" {
  value = azurerm_public_ip.lb.ip_address
}

output "pe_private_ip" {
  value = azurerm_private_endpoint.blob.private_service_connection[0].private_ip_address
}
`

// TestTerraform_Azure_Network applies a load balancer with a pool, probe and rule, a NIC in the pool, a NAT gateway on
// a subnet and a private endpoint with a DNS zone group to a storage account, requires an empty second plan, then
// destroys everything.
func TestTerraform_Azure_Network(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	dir := t.TempDir()
	applyAzureTofuNoDrift(t, dir, azurermProviderBlock(endpoint)+azureNetworkFixture)

	if got := tofuOutput(t, dir, "pe_private_ip"); got != "10.20.2.4" {
		t.Fatalf("private endpoint address = %q, want 10.20.2.4", got)
	}
}
