package iotwireless_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

// TestInMemoryBackend_SortedListFuotaTasks verifies deterministic sort order for FUOTA tasks.
func TestInMemoryBackend_SortedListFuotaTasks(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	for _, name := range []string{"ft-z", "ft-a", "ft-m"} {
		_, err := b.CreateFuotaTask(testAccountID, testRegion, name, "", "", "", "", 0, 0, 0, nil, nil)
		require.NoError(t, err)
	}

	tasks := b.ListFuotaTasks(testAccountID, testRegion)
	require.Len(t, tasks, 3)
	assert.Equal(t, "ft-a", tasks[0].Name)
	assert.Equal(t, "ft-m", tasks[1].Name)
	assert.Equal(t, "ft-z", tasks[2].Name)
}

// TestInMemoryBackend_FuotaTaskAssociations_TrackMultiple locks in that a
// FUOTA task can have several associated wireless devices and multicast
// groups at once (ListWirelessDevices' FuotaTaskId filter and
// ListMulticastGroupsByFuotaTask's list return type both imply more than
// one), and that per-item disassociation and cascade cleanup on delete work
// correctly. A prior single-slot map[string]string implementation silently
// dropped every association but the most recent for a given FUOTA task.
func TestInMemoryBackend_FuotaTaskAssociations_TrackMultiple(t *testing.T) {
	t.Parallel()

	b := iotwireless.NewInMemoryBackend()

	ft, err := b.CreateFuotaTask(testAccountID, testRegion, "ft-1", "", "", "", "", 0, 0, 0, nil, nil)
	require.NoError(t, err)

	d1, err := b.CreateWirelessDevice(testAccountID, testRegion, "d1", "LoRaWAN", "", "", "", nil, nil, nil)
	require.NoError(t, err)
	d2, err := b.CreateWirelessDevice(testAccountID, testRegion, "d2", "LoRaWAN", "", "", "", nil, nil, nil)
	require.NoError(t, err)

	mg1, err := b.CreateMulticastGroup(testAccountID, testRegion, "mg1", "", nil, nil)
	require.NoError(t, err)
	mg2, err := b.CreateMulticastGroup(testAccountID, testRegion, "mg2", "", nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.AssociateWirelessDeviceWithFuotaTask(ft.ID, d1.ID))
	require.NoError(t, b.AssociateWirelessDeviceWithFuotaTask(ft.ID, d2.ID))
	require.NoError(t, b.AssociateMulticastGroupWithFuotaTask(ft.ID, mg1.ID))
	require.NoError(t, b.AssociateMulticastGroupWithFuotaTask(ft.ID, mg2.ID))

	assert.ElementsMatch(t, []string{d1.ID, d2.ID}, b.ListFuotaTaskDeviceIDs(ft.ID))

	groups := b.ListMulticastGroupsByFuotaTask(testAccountID, testRegion, ft.ID)
	require.Len(t, groups, 2)

	// Disassociating one multicast group must leave the other intact.
	require.NoError(t, b.DisassociateMulticastGroupFromFuotaTask(ft.ID, mg1.ID))
	groups = b.ListMulticastGroupsByFuotaTask(testAccountID, testRegion, ft.ID)
	require.Len(t, groups, 1)
	assert.Equal(t, mg2.ID, groups[0].ID)

	// Disassociating one device must leave the other intact.
	require.NoError(t, b.DisassociateWirelessDeviceFromFuotaTask(ft.ID, d1.ID))
	assert.Equal(t, []string{d2.ID}, b.ListFuotaTaskDeviceIDs(ft.ID))

	// Deleting the FUOTA task must cascade-clean both association sets.
	require.NoError(t, b.DeleteFuotaTask(testAccountID, testRegion, ft.ID))
	assert.Empty(t, b.ListFuotaTaskDeviceIDs(ft.ID))
	assert.Empty(t, b.ListMulticastGroupsByFuotaTask(testAccountID, testRegion, ft.ID))
}
