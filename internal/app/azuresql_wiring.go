package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azuresql"
)

// wireAzureData wires the Azure data services (Redis and SQL) into ARM.
func wireAzureData(byName map[string]service.Registerable) {
	wireAzureRedis(byName)
	wireAzureSQL(byName)
}

// wireAzureSQL gives ARM's Microsoft.Sql provider the SQL Server engines.
func wireAzureSQL(byName map[string]service.Registerable) {
	sqlH, ok := byName["AzureSQL"].(*azuresql.Handler)
	if !ok {
		return
	}

	if armH, armOk := byName["AzureARM"].(*azurearm.Handler); armOk {
		armH.Registry.SetSQL(&azureSQLAdapter{b: sqlH.Backend})
	}
}

// azureSQLAdapter adapts services/azuresql's backend to services/azurearm.SQL.
type azureSQLAdapter struct {
	b *azuresql.InMemoryBackend
}

func sqlEndpointOf(ep azuresql.Endpoint) azurearm.SQLEndpoint {
	return azurearm.SQLEndpoint{Host: ep.Host, Port: ep.Port}
}

func (a *azureSQLAdapter) EnsureServer(id string, spec azurearm.SQLSpec) (azurearm.SQLEndpoint, error) {
	ep, err := a.b.Ensure(id, azuresql.Spec{AdminLogin: spec.AdminLogin, AdminPassword: spec.AdminPassword})

	return sqlEndpointOf(ep), err
}

func (a *azureSQLAdapter) Endpoint(id string) (azurearm.SQLEndpoint, bool) {
	ep, ok := a.b.Endpoint(id)

	return sqlEndpointOf(ep), ok
}

func (a *azureSQLAdapter) DeleteServer(id string) error {
	return ignoreMissing(a.b.Delete(id), azuresql.ErrNotFound)
}

func (a *azureSQLAdapter) EnsureDatabase(serverID, name, collation string) error {
	return a.b.EnsureDatabase(serverID, name, collation)
}

func (a *azureSQLAdapter) DropDatabase(serverID, name string) error {
	return ignoreMissing(a.b.DropDatabase(serverID, name), azuresql.ErrNotFound)
}
