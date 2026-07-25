package iotwireless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

func TestInMemoryBackend_WirelessGatewayCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		gwName      string
		description string
		wantErr     bool
	}{
		{
			name:        "create_and_get",
			gwName:      "gw-1",
			description: "test gateway",
		},
		{
			name:    "get_nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			if tt.wantErr {
				_, err := bk.GetWirelessGateway(testAccountID, testRegion, "no-such-id")
				require.Error(t, err)

				return
			}

			gw, err := bk.CreateWirelessGateway(
				testAccountID,
				testRegion,
				tt.gwName,
				tt.description,
				nil,
				map[string]string{"team": "infra"},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.gwName, gw.Name)
			assert.NotEmpty(t, gw.ID)
			assert.NotEmpty(t, gw.ARN)
			assert.Equal(t, "infra", gw.Tags["team"])

			got, err := bk.GetWirelessGateway(testAccountID, testRegion, gw.ID)
			require.NoError(t, err)
			assert.Equal(t, gw.ID, got.ID)

			err = bk.DeleteWirelessGateway(testAccountID, testRegion, gw.ID)
			require.NoError(t, err)

			_, err = bk.GetWirelessGateway(testAccountID, testRegion, gw.ID)
			require.Error(t, err)
		})
	}
}

func TestInMemoryBackend_WirelessGateway_DeleteNotFound(t *testing.T) {
	t.Parallel()

	bk := iotwireless.NewInMemoryBackend()
	err := bk.DeleteWirelessGateway(testAccountID, testRegion, "no-such-id")
	require.Error(t, err)
	assert.ErrorIs(t, err, iotwireless.ErrGatewayNotFound)
}

func TestInMemoryBackend_ListWirelessGateways(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		gwNames   []string
		wantCount int
	}{
		{name: "empty", wantCount: 0},
		{name: "two", gwNames: []string{"gw-a", "gw-b"}, wantCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := iotwireless.NewInMemoryBackend()

			for _, name := range tt.gwNames {
				_, err := bk.CreateWirelessGateway(testAccountID, testRegion, name, "", nil, nil)
				require.NoError(t, err)
			}

			gws := bk.ListWirelessGateways(testAccountID, testRegion)
			assert.Len(t, gws, tt.wantCount)
		})
	}
}

// TestInMemoryBackend_SortedListWirelessGateways verifies deterministic sort order.
func TestInMemoryBackend_SortedListWirelessGateways(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	for _, name := range []string{"z-gw", "a-gw", "m-gw"} {
		_, err := b.CreateWirelessGateway(testAccountID, testRegion, name, "", nil, nil)
		require.NoError(t, err)
	}

	gws := b.ListWirelessGateways(testAccountID, testRegion)
	require.Len(t, gws, 3)
	assert.Equal(t, "a-gw", gws[0].Name)
	assert.Equal(t, "m-gw", gws[1].Name)
	assert.Equal(t, "z-gw", gws[2].Name)
}
