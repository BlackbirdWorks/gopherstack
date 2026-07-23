package iotwireless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

func TestInMemoryBackend_NetworkAnalyzerConfig_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		configName       string
		description      string
		wirelessDevices  []string
		wirelessGateways []string
	}{
		{
			name:             "full_config",
			configName:       "nc-full",
			description:      "Full network analyzer config",
			wirelessDevices:  []string{"dev-1", "dev-2"},
			wirelessGateways: []string{"gw-1"},
		},
		{
			name:             "minimal_config",
			configName:       "nc-minimal",
			description:      "",
			wirelessDevices:  nil,
			wirelessGateways: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotwireless.NewInMemoryBackend()

			nc, err := b.CreateNetworkAnalyzerConfig(
				testAccountID, testRegion, tt.configName, tt.description,
				tt.wirelessDevices, tt.wirelessGateways, nil, nil, nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.configName, nc.Name)
			assert.NotEmpty(t, nc.ARN)

			got, err := b.GetNetworkAnalyzerConfig(testAccountID, testRegion, tt.configName)
			require.NoError(t, err)
			assert.Equal(t, tt.description, got.Description)

			if tt.wirelessDevices != nil {
				assert.Equal(t, tt.wirelessDevices, got.WirelessDevices)
			}

			// Update.
			err = b.UpdateNetworkAnalyzerConfig(
				testAccountID, testRegion, tt.configName, "updated desc",
				[]string{"new-dev"}, []string{"new-gw"}, nil,
			)
			require.NoError(t, err)

			updated, err := b.GetNetworkAnalyzerConfig(testAccountID, testRegion, tt.configName)
			require.NoError(t, err)
			assert.Equal(t, "updated desc", updated.Description)
			assert.Equal(t, []string{"new-dev"}, updated.WirelessDevices)

			// Delete.
			err = b.DeleteNetworkAnalyzerConfig(testAccountID, testRegion, tt.configName)
			require.NoError(t, err)

			_, err = b.GetNetworkAnalyzerConfig(testAccountID, testRegion, tt.configName)
			require.Error(t, err)
			assert.ErrorIs(t, err, iotwireless.ErrNetworkAnalyzerConfigNotFound)
		})
	}
}

func TestInMemoryBackend_Reset_ClearsNetworkAnalyzerConfigs(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	_, err := b.CreateNetworkAnalyzerConfig(testAccountID, testRegion, "nc1", "", nil, nil, nil, nil, nil)
	require.NoError(t, err)

	b.Reset()

	configs := b.ListNetworkAnalyzerConfigs(testAccountID, testRegion)
	assert.Empty(t, configs)
}
