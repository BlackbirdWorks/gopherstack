package app

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azureeventhubs"
)

func TestReserveFixedServicePorts_AzureEventHubs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		port int
		want bool
	}{
		{name: "default http port inside pool", port: azureeventhubs.DefaultPort, want: true},
		{name: "port outside pool", port: 9996, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(10000, 10100)
			require.NoError(t, err)

			cli := CLI{AzureEventHubs: azureeventhubs.Settings{Port: tt.port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)
			assert.Equal(t, tt.want, alloc.IsAllocated(tt.port))
		})
	}
}

func TestAzureEventHubsAdapter(t *testing.T) {
	t.Parallel()

	backend := azureeventhubs.NewInMemoryBackend()
	a := &azureEventHubsAdapter{backend: backend}

	require.NoError(t, a.EnsureNamespace("ns"))

	info, err := a.CreateHub("ns", "hub", 3, 48*time.Hour)
	require.NoError(t, err)
	assert.Equal(t, []string{"0", "1", "2"}, info.PartitionIDs)
	assert.Equal(t, 48*time.Hour, info.Retention)

	_, ok := a.HubInfo("ns", "hub")
	assert.True(t, ok)

	require.NoError(t, a.CreateConsumerGroup("ns", "hub", "cg"))

	rule, err := a.PutRule("ns", "hub", "r", []string{"Send"})
	require.NoError(t, err)
	assert.NotEmpty(t, rule.PrimaryKey)

	got, ok := a.GetRule("ns", "hub", "r")
	require.True(t, ok)
	assert.Equal(t, rule.PrimaryKey, got.PrimaryKey)

	_, err = a.PutRule("ns", "hub", "bad", nil)
	require.Error(t, err)

	require.NoError(t, a.DeleteRule("ns", "hub", "r"))
	require.NoError(t, a.DeleteRule("ns", "hub", "r"), "missing rule is not an error")
	require.NoError(t, a.DeleteConsumerGroup("ns", "hub", "cg"))
	require.NoError(t, a.DeleteHub("ns", "hub"))
	require.NoError(t, a.DeleteHub("ns", "hub"))
	require.NoError(t, a.DeleteNamespace("ns"))
	require.NoError(t, a.DeleteNamespace("ns"))

	wireAzureARMEventHubs(nil, nil)

	_ = azurearm.EventHubInfo{}
}
