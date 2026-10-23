package azure_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const azureCosmosFixture = `
resource "azurerm_resource_group" "cosmos" {
  name     = "gopherstack-cosmos-test-rg"
  location = "local"
}

resource "azurerm_cosmosdb_account" "cosmos" {
  name                = "gopherstack-cosmos-test"
  location            = azurerm_resource_group.cosmos.location
  resource_group_name = azurerm_resource_group.cosmos.name
  offer_type          = "Standard"
  kind                = "GlobalDocumentDB"

  tags                       = { env = "test" }
  ip_range_filter            = ["10.0.0.1", "10.0.0.2"]
  automatic_failover_enabled = true
  free_tier_enabled          = false

  capabilities {
    name = "EnableAggregationPipeline"
  }

  consistency_policy {
    consistency_level       = "BoundedStaleness"
    max_interval_in_seconds = 300
    max_staleness_prefix    = 100000
  }

  geo_location {
    location          = azurerm_resource_group.cosmos.location
    failover_priority = 0
  }
}

resource "azurerm_cosmosdb_sql_database" "cosmos" {
  name                = "tfdb"
  resource_group_name = azurerm_cosmosdb_account.cosmos.resource_group_name
  account_name        = azurerm_cosmosdb_account.cosmos.name
}

resource "azurerm_cosmosdb_sql_container" "cosmos" {
  name                  = "tfcontainer"
  resource_group_name   = azurerm_cosmosdb_account.cosmos.resource_group_name
  account_name          = azurerm_cosmosdb_account.cosmos.name
  database_name         = azurerm_cosmosdb_sql_database.cosmos.name
  partition_key_paths   = ["/pk"]
  partition_key_version = 2
  default_ttl           = -1

  unique_key {
    paths = ["/email"]
  }
}

resource "azurerm_cosmosdb_sql_database" "shared" {
  name                = "tfdb-shared"
  resource_group_name = azurerm_cosmosdb_account.cosmos.resource_group_name
  account_name        = azurerm_cosmosdb_account.cosmos.name
  throughput          = 400
}

resource "azurerm_cosmosdb_sql_container" "autoscale" {
  name                  = "tfcontainer-auto"
  resource_group_name   = azurerm_cosmosdb_account.cosmos.resource_group_name
  account_name          = azurerm_cosmosdb_account.cosmos.name
  database_name         = azurerm_cosmosdb_sql_database.cosmos.name
  partition_key_paths   = ["/tenant", "/user"]
  partition_key_kind    = "MultiHash"
  partition_key_version = 2

  autoscale_settings {
    max_throughput = 4000
  }

  indexing_policy {
    indexing_mode = "consistent"

    included_path {
      path = "/*"
    }

    excluded_path {
      path = "/secret/*"
    }
  }
}

output "primary_key" {
  value     = azurerm_cosmosdb_account.cosmos.primary_key
  sensitive = true
}

output "endpoint" {
  value = azurerm_cosmosdb_account.cosmos.endpoint
}
`

// tofuOutput reads a root output from the state applyAzureTofuNoDrift left in dir.
func tofuOutput(t *testing.T, dir, name string) string {
	t.Helper()

	cmd := exec.Command(tofuBinaryPath, "output", "-raw", "-no-color", name)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "TF_IN_AUTOMATION=1", "SSL_CERT_FILE="+certPEMPath)

	out, err := cmd.Output()
	require.NoError(t, err, "tofu output %s", name)

	return strings.TrimSpace(string(out))
}

// TestTerraform_Azure_CosmosDB applies a Cosmos DB account, SQL database and container, requires an empty second
// plan, and then proves the resources are live on the data plane by writing and reading an item with the account's
// own primary key (the data plane validates signatures in this suite).
func TestTerraform_Azure_CosmosDB(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	dir := t.TempDir()
	applyAzureTofuNoDrift(t, dir, azurermProviderBlock(endpoint)+azureCosmosFixture)

	key := tofuOutput(t, dir, "primary_key")
	docEndpoint := tofuOutput(t, dir, "endpoint")
	assert.Contains(t, docEndpoint, "localhost:"+hostPortCosmos)

	cred, err := azcosmos.NewKeyCredential(key)
	require.NoError(t, err)

	client, err := azcosmos.NewClientWithKey(docEndpoint, cred, nil)
	require.NoError(t, err)

	container, err := client.NewContainer("tfdb", "tfcontainer")
	require.NoError(t, err)

	pk := azcosmos.NewPartitionKeyString("p1")
	item, err := json.Marshal(map[string]any{"id": "item1", "pk": "p1", "email": "a@example.com"})
	require.NoError(t, err)

	ctx := t.Context()
	_, err = container.CreateItem(ctx, pk, item, nil)
	require.NoError(t, err)

	read, err := container.ReadItem(ctx, pk, "item1", nil)
	require.NoError(t, err)
	assert.JSONEq(t, string(item), stripSystemFields(t, read.Value))

	_, err = container.CreateItem(ctx, azcosmos.NewPartitionKeyString("p1"), mustJSON(t, map[string]any{
		"id": "item2", "pk": "p1", "email": "a@example.com",
	}), nil)
	require.Error(t, err, "the unique key policy set through ARM is enforced by the data plane")

	badCred, err := azcosmos.NewKeyCredential("YmFkLWtleS1ub3QtdGhlLXJlYWwtb25l")
	require.NoError(t, err)

	badClient, err := azcosmos.NewClientWithKey(docEndpoint, badCred, nil)
	require.NoError(t, err)

	badContainer, err := badClient.NewContainer("tfdb", "tfcontainer")
	require.NoError(t, err)

	_, err = badContainer.ReadItem(ctx, pk, "item1", nil)
	require.Error(t, err, "a key the account did not hand out is rejected")
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()

	b, err := json.Marshal(v)
	require.NoError(t, err)

	return b
}

func stripSystemFields(t *testing.T, raw []byte) string {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m))

	for k := range m {
		if strings.HasPrefix(k, "_") {
			delete(m, k)
		}
	}

	return string(mustJSON(t, m))
}
