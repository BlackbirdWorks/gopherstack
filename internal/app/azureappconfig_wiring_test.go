package app

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/services/azureappconfig"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

func TestReserveFixedServicePorts_AzureAppConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		port int
		want bool
	}{
		{name: "default port inside pool", port: azureappconfig.DefaultPort, want: true},
		{name: "port outside pool", port: 9995, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(10000, 10100)
			require.NoError(t, err)

			cli := CLI{AzureAppConfig: azureappconfig.Settings{Port: tt.port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)
			assert.Equal(t, tt.want, alloc.IsAllocated(tt.port))
		})
	}
}

func TestAzureAppConfigDefaultPortsAgree(t *testing.T) {
	t.Parallel()

	assert.Equal(t, azureappconfig.DefaultPort, azurearm.DefaultAppConfigPort)
}

func TestAzureAppConfigAdapterSharesStateWithDataPlane(t *testing.T) {
	t.Parallel()

	backend := azureappconfig.NewInMemoryBackend()
	a := &azureAppConfigAdapter{backend: backend}
	val := "red"

	created, err := a.PutStore("adapterstore")
	require.NoError(t, err)

	got, ok := a.StoreCreated("adapterstore")
	require.True(t, ok)
	assert.Equal(t, created, got)

	keys, err := a.StoreKeys("adapterstore")
	require.NoError(t, err)
	require.Len(t, keys, 4)

	fresh, err := a.RegenerateStoreKey("adapterstore", keys[0].ID)
	require.NoError(t, err)
	assert.NotEqual(t, keys[0].Value, fresh.Value)

	_, err = a.PutKeyValue("adapterstore", azurearm.AppConfigKV{Key: "k", Label: "l", Value: &val})
	require.NoError(t, err)

	kv, err := backend.GetKeyValue("adapterstore", "k", "l", time.Time{})
	require.NoError(t, err, "a key-value written through ARM is visible to the data plane")
	assert.Equal(t, val, *kv.Value)

	_, err = backend.SetLock("adapterstore", "k", "l", true, azureappconfig.Precondition{})
	require.NoError(t, err)

	_, err = a.PutKeyValue("adapterstore", azurearm.AppConfigKV{Key: "k", Label: "l", Value: &val})
	require.ErrorIs(t, err, azurearm.ErrAppConfigKeyLocked)
	require.ErrorIs(t, a.DeleteKeyValue("adapterstore", "k", "l"), azurearm.ErrAppConfigKeyLocked)

	listed, err := a.ListKeyValues("adapterstore")
	require.NoError(t, err)
	require.Len(t, listed, 1)
	assert.True(t, listed[0].Locked)

	_, ok = a.GetKeyValue("adapterstore", "missing", "")
	assert.False(t, ok)
	require.NoError(t, a.DeleteKeyValue("adapterstore", "missing", ""))

	require.NoError(t, a.DeleteStore("adapterstore"))
	require.NoError(t, a.DeleteStore("adapterstore"), "deleting a missing store is idempotent")
}
