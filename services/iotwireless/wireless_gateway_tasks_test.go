package iotwireless_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

func TestCreateWirelessGatewayTaskDefinition_ARNUsesRegionAndAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
		wantARN   string
	}{
		{
			name:      "default region and account",
			accountID: "000000000000",
			region:    "us-east-1",
			wantARN:   "arn:aws:iotwireless:us-east-1:000000000000:WirelessGatewayTaskDefinition/",
		},
		{
			name:      "eu-west-1 cross-region",
			accountID: "111122223333",
			region:    "eu-west-1",
			wantARN:   "arn:aws:iotwireless:eu-west-1:111122223333:WirelessGatewayTaskDefinition/",
		},
		{
			name:      "ap-southeast-2 cross-region",
			accountID: "999988887777",
			region:    "ap-southeast-2",
			wantARN:   "arn:aws:iotwireless:ap-southeast-2:999988887777:WirelessGatewayTaskDefinition/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotwireless.NewInMemoryBackend()
			def, err := b.CreateWirelessGatewayTaskDefinition(tt.accountID, tt.region, "taskdef", false, nil)
			require.NoError(t, err)
			assert.Greater(t, len(def.ARN), len(tt.wantARN), "ARN too short")
			assert.Equal(t, tt.wantARN, def.ARN[:len(tt.wantARN)],
				"ARN %q does not start with %q", def.ARN, tt.wantARN)
		})
	}
}

// TestInMemoryBackend_GatewayTaskDefinition_UpdateFieldRoundTrips locks in
// that CreateWirelessGatewayTaskDefinition's Update object (LoRaWAN
// firmware version, UpdateDataRole, UpdateDataSource) is stored and echoed
// back on Get, instead of being silently accepted and dropped.
func TestInMemoryBackend_GatewayTaskDefinition_UpdateFieldRoundTrips(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	update := map[string]any{
		"UpdateDataRole":   "role-arn",
		"UpdateDataSource": "s3://bucket/fw",
	}

	def, err := b.CreateWirelessGatewayTaskDefinition(testAccountID, testRegion, "taskdef", true, update)
	require.NoError(t, err)

	got, err := b.GetWirelessGatewayTaskDefinition(def.ID)
	require.NoError(t, err)
	assert.Equal(t, "role-arn", got.Update["UpdateDataRole"])
	assert.True(t, got.AutoCreateTasks)
}

// TestInMemoryBackend_GatewayTask_TracksCreatedAt locks in that
// CreateWirelessGatewayTask records a creation timestamp, so
// GetWirelessGatewayTask's TaskCreatedAt field reflects real state instead
// of always being empty.
func TestInMemoryBackend_GatewayTask_TracksCreatedAt(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	before := time.Now()
	task, err := b.CreateWirelessGatewayTask("gw-1", "taskdef-1")
	require.NoError(t, err)
	assert.False(t, task.CreatedAt.Before(before.Add(-time.Second)))

	got, err := b.GetWirelessGatewayTask("gw-1")
	require.NoError(t, err)
	assert.False(t, got.CreatedAt.IsZero())
}

// TestHandler_ListWirelessGatewayTaskDefinitions_EntryShape locks in that
// list entries carry Arn/Id/LoRaWAN only, matching
// types.UpdateWirelessGatewayTaskEntry -- not the fuller Name/AutoCreateTasks
// shape Get returns.
func TestHandler_ListWirelessGatewayTaskDefinitions_EntryShape(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, "POST", "/wireless-gateway-task-definitions",
		`{"Name":"def1","AutoCreateTasks":true,"Update":{"LoRaWAN":{"UpdateSignature":"sig"}}}`)
	require.Equal(t, 201, rec.Code)

	rec = doIoTWRequest(t, h, "GET", "/wireless-gateway-task-definitions", "")
	require.Equal(t, 200, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	entries, ok := resp["TaskDefinitions"].([]any)
	require.True(t, ok)
	require.Len(t, entries, 1)

	entry, ok := entries[0].(map[string]any)
	require.True(t, ok)
	assert.NotContains(t, entry, "Name", "list entries must not carry Name")
	assert.NotContains(t, entry, "AutoCreateTasks", "list entries must not carry AutoCreateTasks")
	assert.Contains(t, entry, "Id")
	assert.Contains(t, entry, "Arn")
}
