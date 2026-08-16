package mwaa_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

func TestBackendReset(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	seedEnv(t, b, "env1")
	seedEnv(t, b, "env2")

	require.Equal(t, 2, mwaa.EnvironmentCount(b))

	b.Reset()

	assert.Equal(t, 0, mwaa.EnvironmentCount(b))
	assert.Equal(t, 0, mwaa.ARNIndexSize(b))
}

func TestMultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)

	for range 3 {
		seedEnv(t, b, "env1")
		b.Reset()
		assert.Equal(t, 0, mwaa.EnvironmentCount(b))
	}
}

func TestEnvironmentCount_CreateDelete(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	assert.Equal(t, 0, mwaa.EnvironmentCount(b))

	_, err := b.CreateEnvironment(context.Background(), "count-env-1", newCreateReq())
	require.NoError(t, err)
	assert.Equal(t, 1, mwaa.EnvironmentCount(b))

	_, err = b.CreateEnvironment(context.Background(), "count-env-2", newCreateReq())
	require.NoError(t, err)
	assert.Equal(t, 2, mwaa.EnvironmentCount(b))

	_, err = b.DeleteEnvironment(context.Background(), "count-env-1")
	require.NoError(t, err)
	assert.Equal(t, 1, mwaa.EnvironmentCount(b))
}

// ─────────────────────────────────────────────────────────────
// 15. HTTP snapshot: defaults visible via GetEnvironment HTTP
// ─────────────────────────────────────────────────────────────

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	seedEnv(t, b, "persist-env")

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	err := b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	assert.Equal(t, 1, mwaa.EnvironmentCount(b2))
	assert.Equal(t, 1, mwaa.ARNIndexSize(b2))

	env, err := b2.GetEnvironment(context.Background(), "persist-env")
	require.NoError(t, err)
	assert.Equal(t, "persist-env", env.Name)
}

func TestSnapshot_WithLoggingConfig(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.LoggingConfiguration = &mwaa.LoggingConfiguration{
		SchedulerLogs: &mwaa.ModuleLoggingConfiguration{LogLevel: "WARNING"},
	}

	_, err := b.CreateEnvironment(context.Background(), "snap-log-env", req)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	require.NoError(t, b2.Restore(t.Context(), snap))

	b2.GetEnvironment(context.Background(), "snap-log-env")
	env, err := b2.GetEnvironment(context.Background(), "snap-log-env")
	require.NoError(t, err)
	require.NotNil(t, env.LoggingConfiguration)
	require.NotNil(t, env.LoggingConfiguration.SchedulerLogs)
	assert.Equal(t, "WARNING", env.LoggingConfiguration.SchedulerLogs.LogLevel)
}

func TestSnapshot_WithNetworkConfig(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	req := newCreateReq()
	req.NetworkConfiguration = &mwaa.NetworkConfig{
		SubnetIDs:        []string{"subnet-snap1", "subnet-snap2"},
		SecurityGroupIDs: []string{"sg-snap1"},
	}

	_, err := b.CreateEnvironment(context.Background(), "snap-nc-env", req)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	b2 := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	require.NoError(t, b2.Restore(t.Context(), snap))

	b2.GetEnvironment(context.Background(), "snap-nc-env")
	env, err := b2.GetEnvironment(context.Background(), "snap-nc-env")
	require.NoError(t, err)
	require.NotNil(t, env.NetworkConfiguration)
	assert.Equal(t, []string{"subnet-snap1", "subnet-snap2"}, env.NetworkConfiguration.SubnetIDs)
}
