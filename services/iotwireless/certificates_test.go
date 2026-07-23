package iotwireless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

// TestInMemoryBackend_CertificateARN_UsesRegion verifies that certificate ARN uses the gateway region.
func TestInMemoryBackend_CertificateARN_UsesRegion(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	gw, err := b.CreateWirelessGateway(testAccountID, testRegion, "gw-cert", "", nil, nil)
	require.NoError(t, err)

	certARN, err := b.AssociateWirelessGatewayWithCertificate(testAccountID, testRegion, gw.ID, "cert-abc")
	require.NoError(t, err)
	assert.Contains(t, certARN, testRegion, "Certificate ARN should include the region")
	assert.Contains(t, certARN, "cert-abc")
}

func TestInMemoryBackend_ImportTask_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		runTest func(*testing.T, *iotwireless.InMemoryBackend)
		name    string
	}{
		{
			name: "get_not_found",
			runTest: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				_, err := b.GetWirelessDeviceImportTask("no-such-id")
				require.Error(t, err)
				assert.ErrorIs(t, err, iotwireless.ErrImportTaskNotFound)
			},
		},
		{
			name: "delete_not_found",
			runTest: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				err := b.DeleteWirelessDeviceImportTask("no-such-id")
				require.Error(t, err)
				assert.ErrorIs(t, err, iotwireless.ErrImportTaskNotFound)
			},
		},
		{
			name: "update_not_found",
			runTest: func(t *testing.T, b *iotwireless.InMemoryBackend) {
				t.Helper()
				err := b.UpdateWirelessDeviceImportTask("no-such-id", "dest")
				require.Error(t, err)
				assert.ErrorIs(t, err, iotwireless.ErrImportTaskNotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotwireless.NewInMemoryBackend()
			tt.runTest(t, b)
		})
	}
}

func TestInMemoryBackend_ImportTask_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		destination string
	}{
		{name: "lorawan_dest", destination: "lorawan-destination"},
		{name: "sidewalk_dest", destination: "sidewalk-destination"},
		{name: "empty_dest", destination: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotwireless.NewInMemoryBackend()

			// Start.
			task, err := b.StartWirelessDeviceImportTask(testAccountID, testRegion, tt.destination)
			require.NoError(t, err)
			assert.NotEmpty(t, task.ID)
			assert.NotEmpty(t, task.ARN)
			assert.Equal(t, tt.destination, task.DestinationName)
			assert.Equal(t, "Initialized", task.Status)

			// Get.
			got, err := b.GetWirelessDeviceImportTask(task.ID)
			require.NoError(t, err)
			assert.Equal(t, task.ID, got.ID)
			assert.Equal(t, tt.destination, got.DestinationName)

			// List.
			tasks := b.ListWirelessDeviceImportTasks()
			assert.Len(t, tasks, 1)

			// Update.
			err = b.UpdateWirelessDeviceImportTask(task.ID, "updated-dest")
			require.NoError(t, err)

			got, err = b.GetWirelessDeviceImportTask(task.ID)
			require.NoError(t, err)
			assert.Equal(t, "updated-dest", got.DestinationName)

			// Delete.
			err = b.DeleteWirelessDeviceImportTask(task.ID)
			require.NoError(t, err)

			// Verify deleted.
			_, err = b.GetWirelessDeviceImportTask(task.ID)
			require.Error(t, err)
			require.ErrorIs(t, err, iotwireless.ErrImportTaskNotFound)

			// List should be empty.
			tasks = b.ListWirelessDeviceImportTasks()
			assert.Empty(t, tasks)
		})
	}
}

func TestInMemoryBackend_ImportTaskARN_ContainsRegionAndAccount(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	task, err := b.StartWirelessDeviceImportTask(testAccountID, testRegion, "d")
	require.NoError(t, err)

	assert.Contains(t, task.ARN, testRegion)
	assert.Contains(t, task.ARN, testAccountID)
	assert.Contains(t, task.ARN, "ImportTask")
}

func TestInMemoryBackend_SingleImportTaskARN_ContainsRegionAndAccount(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	task, err := b.StartSingleWirelessDeviceImportTask(testAccountID, testRegion, "d")
	require.NoError(t, err)

	assert.Contains(t, task.ARN, testRegion)
	assert.Contains(t, task.ARN, testAccountID)
	assert.NotEmpty(t, task.WirelessDeviceID)
}

func TestInMemoryBackend_Reset_ClearsImportTasks(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	_, err := b.StartWirelessDeviceImportTask(testAccountID, testRegion, "d1")
	require.NoError(t, err)

	_, err = b.StartWirelessDeviceImportTask(testAccountID, testRegion, "d2")
	require.NoError(t, err)

	assert.Len(t, b.ListWirelessDeviceImportTasks(), 2)

	b.Reset()

	assert.Empty(t, b.ListWirelessDeviceImportTasks())
}
