package azure_test

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"runtime"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const azureRedisFixture = `
resource "azurerm_resource_group" "redis" {
  name     = "gopherstack-redis-test-rg"
  location = "local"
}

resource "azurerm_redis_cache" "basic" {
  name                 = "gopherstack-redis-basic"
  location             = azurerm_resource_group.redis.location
  resource_group_name  = azurerm_resource_group.redis.name
  capacity             = 0
  family               = "C"
  sku_name             = "Basic"
  non_ssl_port_enabled = false
  minimum_tls_version  = "1.2"
}

resource "azurerm_redis_cache" "standard" {
  name                = "gopherstack-redis-std"
  location            = azurerm_resource_group.redis.location
  resource_group_name = azurerm_resource_group.redis.name
  capacity            = 1
  family              = "C"
  sku_name            = "Standard"

  redis_configuration {
    maxmemory_policy = "allkeys-lru"
  }

  tags = {
    env = "test"
  }
}

output "basic_ssl_port" {
  value = azurerm_redis_cache.basic.ssl_port
}

output "basic_key" {
  value     = azurerm_redis_cache.basic.primary_access_key
  sensitive = true
}

output "basic_hostname" {
  value = azurerm_redis_cache.basic.hostname
}

output "std_ssl_port" {
  value = azurerm_redis_cache.standard.ssl_port
}

output "std_key" {
  value     = azurerm_redis_cache.standard.primary_access_key
  sensitive = true
}

output "std_secondary_key" {
  value     = azurerm_redis_cache.standard.secondary_access_key
  sensitive = true
}
`

// redisTLSClient dials a cache's TLS port, which listens inside the container, via the container's bridge IP.
func redisTLSClient(t *testing.T, port, password string) *goredis.Client {
	t.Helper()

	ip, err := sharedContainer.ContainerIP(t.Context())
	require.NoError(t, err)

	pem, err := os.ReadFile(certPEMPath)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(pem))

	c := goredis.NewClient(&goredis.Options{
		Addr: net.JoinHostPort(ip, port), Password: password,
		TLSConfig: &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12},
	})
	t.Cleanup(func() { _ = c.Close() })

	return c
}

// TestTerraform_Azure_RedisCache applies a Basic and a Standard cache, requires an empty second plan,
// connects to each over TLS with the access keys ARM hands out, then destroys.
func TestTerraform_Azure_RedisCache(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	dir := t.TempDir()
	applyAzureTofuNoDrift(t, dir, azurermProviderBlock(endpoint)+azureRedisFixture)

	if runtime.GOOS != "linux" {
		t.Skip("the container bridge IP is only routable on Linux")
	}

	assert.Equal(t, "gopherstack-redis-basic.redis.cache.windows.net", tofuOutput(t, dir, "basic_hostname"))

	basic := redisTLSClient(t, tofuOutput(t, dir, "basic_ssl_port"), tofuOutput(t, dir, "basic_key"))
	require.NoError(t, basic.Set(t.Context(), "k", "v", 0).Err())

	got, err := basic.Get(t.Context(), "k").Result()
	require.NoError(t, err)
	assert.Equal(t, "v", got)

	require.Error(t, redisTLSClient(t, tofuOutput(t, dir, "basic_ssl_port"), "wrong").Ping(t.Context()).Err())

	std := redisTLSClient(t, tofuOutput(t, dir, "std_ssl_port"), tofuOutput(t, dir, "std_secondary_key"))
	require.NoError(t, std.Ping(t.Context()).Err(), "the secondary key authenticates too")
}
