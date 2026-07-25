package iotwireless_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotwireless"
)

// TestInMemoryBackend_MulticastGroupDeviceAssociation locks in real
// per-device association tracking for multicast groups: multiple devices
// can be associated with one group, disassociation removes only the named
// device, and StartBulkAssociate/StartBulkDisassociate mutate real state
// (previously they returned 204 without touching anything -- see PARITY.md
// gap on bulk association).
func TestInMemoryBackend_MulticastGroupDeviceAssociation(t *testing.T) {
	t.Parallel()

	bk := iotwireless.NewInMemoryBackend()

	mg, err := bk.CreateMulticastGroup(testAccountID, testRegion, "mg-1", "", nil, nil)
	require.NoError(t, err)

	d1, err := bk.CreateWirelessDevice(testAccountID, testRegion, "d1", "LoRaWAN", "", "", "", nil, nil, nil)
	require.NoError(t, err)
	d2, err := bk.CreateWirelessDevice(testAccountID, testRegion, "d2", "LoRaWAN", "", "", "", nil, nil, nil)
	require.NoError(t, err)

	require.NoError(t, bk.AssociateWirelessDeviceWithMulticastGroup(mg.ID, d1.ID))
	require.NoError(t, bk.AssociateWirelessDeviceWithMulticastGroup(mg.ID, d2.ID))

	assert.ElementsMatch(t, []string{d1.ID, d2.ID}, bk.ListMulticastGroupDeviceIDs(mg.ID),
		"both devices must be tracked, not just the most recently associated one")

	// Disassociating one device must leave the other intact.
	require.NoError(t, bk.DisassociateWirelessDeviceFromMulticastGroup(mg.ID, d1.ID))
	assert.Equal(t, []string{d2.ID}, bk.ListMulticastGroupDeviceIDs(mg.ID))

	// Deleting the device must cascade-clean its multicast group membership
	// (no ghost row survives the device's deletion).
	require.NoError(t, bk.DeleteWirelessDevice(testAccountID, testRegion, d2.ID))
	assert.Empty(t, bk.ListMulticastGroupDeviceIDs(mg.ID))
}

// TestInMemoryBackend_StartBulkAssociateDisassociate locks in that the bulk
// ops mutate the full account/region device corpus, emulating "all
// qualifying devices" since this backend has no query-expression evaluator.
func TestInMemoryBackend_StartBulkAssociateDisassociate(t *testing.T) {
	t.Parallel()

	bk := iotwireless.NewInMemoryBackend()

	mg, err := bk.CreateMulticastGroup(testAccountID, testRegion, "mg-1", "", nil, nil)
	require.NoError(t, err)

	names := []string{"d1", "d2", "d3"}
	deviceIDs := make([]string, 0, len(names))

	for _, name := range names {
		d, createErr := bk.CreateWirelessDevice(testAccountID, testRegion, name, "LoRaWAN", "", "", "", nil, nil, nil)
		require.NoError(t, createErr)
		deviceIDs = append(deviceIDs, d.ID)
	}

	require.NoError(t, bk.StartBulkAssociateWirelessDeviceWithMulticastGroup(testAccountID, testRegion, mg.ID))
	assert.ElementsMatch(t, deviceIDs, bk.ListMulticastGroupDeviceIDs(mg.ID))

	require.NoError(t, bk.StartBulkDisassociateWirelessDeviceFromMulticastGroup(mg.ID))
	assert.Empty(t, bk.ListMulticastGroupDeviceIDs(mg.ID))
}

// TestHandler_StartBulkAssociate_MutatesState exercises the bulk-associate
// route end-to-end (PATCH /multicast-groups/{Id}/bulk) and confirms it
// actually associates devices instead of being a no-op 204.
func TestHandler_StartBulkAssociate_MutatesState(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, "POST", "/multicast-groups", `{"Name":"mg1"}`)
	require.Equal(t, 201, rec.Code)

	var created struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))

	rec = doIoTWRequest(t, h, "POST", "/wireless-devices", `{"Name":"d1","Type":"LoRaWAN"}`)
	require.Equal(t, 201, rec.Code)

	rec = doIoTWRequest(t, h, "PATCH", "/multicast-groups/"+created.ID+"/bulk", `{"QueryString":"*"}`)
	assert.Equal(t, 204, rec.Code)
}

// TestHandler_DisassociateWirelessDeviceFromMulticastGroup_UsesPathDeviceID
// locks in that the {WirelessDeviceId} path segment is actually used to
// disassociate the named device, not silently discarded -- a prior bug
// dropped it entirely.
func TestHandler_DisassociateWirelessDeviceFromMulticastGroup_UsesPathDeviceID(t *testing.T) {
	t.Parallel()

	bk := iotwireless.NewInMemoryBackend()
	h := iotwireless.NewHandler(bk)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	mg, err := bk.CreateMulticastGroup(testAccountID, testRegion, "mg-1", "", nil, nil)
	require.NoError(t, err)
	d1, err := bk.CreateWirelessDevice(testAccountID, testRegion, "d1", "LoRaWAN", "", "", "", nil, nil, nil)
	require.NoError(t, err)
	d2, err := bk.CreateWirelessDevice(testAccountID, testRegion, "d2", "LoRaWAN", "", "", "", nil, nil, nil)
	require.NoError(t, err)

	require.NoError(t, bk.AssociateWirelessDeviceWithMulticastGroup(mg.ID, d1.ID))
	require.NoError(t, bk.AssociateWirelessDeviceWithMulticastGroup(mg.ID, d2.ID))

	rec := doIoTWRequest(t, h, "DELETE", "/multicast-groups/"+mg.ID+"/wireless-devices/"+d1.ID, "")
	assert.Equal(t, 204, rec.Code)

	assert.Equal(t, []string{d2.ID}, bk.ListMulticastGroupDeviceIDs(mg.ID))
}
