package ocimysql_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) mkConfig(t *testing.T, name string, vars *my.ConfigurationVariables) *string {
	t.Helper()

	resp, err := e.aas.CreateConfiguration(t.Context(), my.CreateConfigurationRequest{
		CompartmentId: new(testCompartment), ShapeName: new(shapeSmall), DisplayName: new(name), Variables: vars,
	})
	require.NoError(t, err)

	return resp.Id
}

func (l *fakeLauncher) queriesOf(id string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.Clone(l.insts[id].queries)
}

func (l *fakeLauncher) startsOf(id string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.insts[id].starts
}

func TestConfigurationVariablesApplied(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	ctx := t.Context()

	cfg := e.mkConfig(t, "tuned", &my.ConfigurationVariables{
		MaxConnections: new(77), SkipNameResolve: new(true), SqlMode: new("STRICT_ALL_TABLES"),
		ThreadPoolSize: new(8),
	})

	db := e.mkEngineDB(t, "tuned", func(d *my.CreateDbSystemDetails) { d.ConfigurationId = cfg })
	got := l.queriesOf(*db.Id)

	assert.Contains(t, got, "SET PERSIST max_connections = 77")
	assert.Contains(t, got, "SET PERSIST_ONLY skip_name_resolve = ON", "a variable read at start is persisted for it")
	assert.Contains(t, got, "SET PERSIST sql_mode = 'STRICT_ALL_TABLES'")
	assert.NotContains(t, got, "RESET PERSIST")
	assert.Equal(t, 1, l.startsOf(*db.Id), "the server restarts so the persisted variable takes effect")

	plain := e.mkEngineDB(t, "plain", nil)
	assert.Empty(t, l.queriesOf(*plain.Id), "shape defaults are not applied to the container")
	assert.Zero(t, l.startsOf(*plain.Id))

	next := e.mkConfig(t, "next", &my.ConfigurationVariables{MaxConnections: new(55)})

	_, err := e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
		DbSystemId: db.Id, ConfigurationId: next,
	})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateActive)

	got = l.queriesOf(*db.Id)
	assert.Contains(t, got, "RESET PERSIST", "the previous configuration is dropped")
	assert.Equal(t, "SET PERSIST max_connections = 55", got[len(got)-1])
	assert.Equal(t, 2, l.startsOf(*db.Id))
}

func TestConfigurationChangeWhileStopped(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	ctx := t.Context()
	db := e.mkEngineDB(t, "stopped", nil)

	_, err := e.db.StopDbSystem(ctx, my.StopDbSystemRequest{DbSystemId: db.Id, ShutdownType: my.InnoDbShutdownModeFast})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateInactive)

	cfg := e.mkConfig(t, "tuned", &my.ConfigurationVariables{MaxConnections: new(99)})

	_, err = e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
		DbSystemId: db.Id, ConfigurationId: cfg,
	})
	require.NoError(t, err)
	assert.Empty(t, l.queriesOf(*db.Id), "a stopped server cannot take variables")
	e.settle()

	_, err = e.db.StartDbSystem(ctx, my.StartDbSystemRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	e.awaitState(t, db.Id, my.DbSystemLifecycleStateActive)

	assert.Contains(t, l.queriesOf(*db.Id), "SET PERSIST max_connections = 99", "the next start applies it")
}

func TestConfigurationAppliedToReplicas(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	ctx := t.Context()
	small := e.mkConfigFor(t, shapeReplica, "tuned8", &my.ConfigurationVariables{MaxConnections: new(77)})
	db := e.mkEngineDB(t, "primary", func(d *my.CreateDbSystemDetails) {
		d.ShapeName, d.ConfigurationId = new(shapeReplica), small
	})
	rep := e.mkReplica(t, db.Id, "r1")

	require.Eventually(t, func() bool {
		r, err := e.rp.GetReplica(ctx, my.GetReplicaRequest{ReplicaId: rep.Id})

		return err == nil && r.LifecycleState == my.ReplicaLifecycleStateActive
	}, e.waitFor(), 20*time.Millisecond)

	assert.True(t, slices.ContainsFunc(l.queriesOf(*rep.Id), func(q string) bool {
		return strings.Contains(q, "max_connections = 77")
	}), "a replica inherits the configuration of its DB system")
}

func (e *env) mkConfigFor(t *testing.T, shape, name string, vars *my.ConfigurationVariables) *string {
	t.Helper()

	resp, err := e.aas.CreateConfiguration(t.Context(), my.CreateConfigurationRequest{
		CompartmentId: new(testCompartment), ShapeName: new(shape), DisplayName: new(name), Variables: vars,
	})
	require.NoError(t, err)

	return resp.Id
}
