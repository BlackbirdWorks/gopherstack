package medialive

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- InputDevice operations ---

// ClaimDevice registers a device (by ID) into this account.
func (b *InMemoryBackend) ClaimDevice(id string) (*InputDevice, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: id required", ErrInvalidParameter)
	}

	b.mu.Lock("ClaimDevice")
	defer b.mu.Unlock()

	if b.inputDevices.Has(id) {
		return nil, fmt.Errorf("%w: device %s already claimed", ErrConflict, id)
	}

	d := &storedInputDevice{
		ARN:                     b.inputDeviceARN(id),
		ID:                      id,
		Name:                    id,
		SerialNumber:            id,
		MacAddress:              "00:00:00:00:00:00",
		DeviceType:              deviceTypeHD,
		ConnectionState:         deviceConnectionConnected,
		DeviceSettingsSyncState: deviceSettingsSynced,
		DeviceUpdateStatus:      deviceUpdateUpToDate,
		Tags:                    make(map[string]string),
	}
	b.inputDevices.Put(d)

	return d.toDevice(), nil
}

// ListInputDevices returns a paginated list of input devices.
func (b *InMemoryBackend) ListInputDevices(
	maxResults int,
	nextToken string,
) ([]*InputDevice, string, error) {
	b.mu.RLock("ListInputDevices")
	defer b.mu.RUnlock()

	all := b.inputDevices.All()

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	devices := make([]*InputDevice, 0, len(pg.Data))
	for _, d := range pg.Data {
		devices = append(devices, d.toDevice())
	}

	return devices, pg.Next, nil
}

// DescribeInputDevice returns an input device by ID.
func (b *InMemoryBackend) DescribeInputDevice(deviceID string) (*InputDevice, error) {
	b.mu.RLock("DescribeInputDevice")
	defer b.mu.RUnlock()

	d, ok := b.inputDevices.Get(deviceID)
	if !ok {
		return nil, fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	return d.toDevice(), nil
}

// UpdateInputDevice updates the name of an input device.
func (b *InMemoryBackend) UpdateInputDevice(deviceID, name string) (*InputDevice, error) {
	b.mu.Lock("UpdateInputDevice")
	defer b.mu.Unlock()

	d, ok := b.inputDevices.Get(deviceID)
	if !ok {
		return nil, fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	if name != "" {
		d.Name = name
	}

	return d.toDevice(), nil
}

// RebootInputDevice initiates a reboot of the device (no-op in emulation).
func (b *InMemoryBackend) RebootInputDevice(deviceID string) error {
	b.mu.RLock("RebootInputDevice")
	defer b.mu.RUnlock()

	if !b.inputDevices.Has(deviceID) {
		return fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	return nil
}

// TransferInputDevice initiates a transfer of the device to another account.
func (b *InMemoryBackend) TransferInputDevice(
	deviceID, targetCustomerID, targetRegion, message string,
) error {
	b.mu.Lock("TransferInputDevice")
	defer b.mu.Unlock()

	d, ok := b.inputDevices.Get(deviceID)
	if !ok {
		return fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	if d.PendingTransfer != nil {
		return fmt.Errorf("%w: device %s already has a pending transfer", ErrConflict, deviceID)
	}

	d.PendingTransfer = &storedInputDeviceTransfer{
		TargetCustomerID: targetCustomerID,
		TargetRegion:     targetRegion,
		Message:          message,
	}
	b.pendingTransferDeviceIDs[deviceID] = struct{}{}

	return nil
}

// AcceptInputDeviceTransfer accepts an incoming transfer and completes it.
func (b *InMemoryBackend) AcceptInputDeviceTransfer(deviceID string) error {
	b.mu.Lock("AcceptInputDeviceTransfer")
	defer b.mu.Unlock()

	d, ok := b.inputDevices.Get(deviceID)
	if !ok {
		return fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	if d.PendingTransfer == nil {
		return fmt.Errorf("%w: device %s has no pending transfer", ErrConflict, deviceID)
	}

	d.PendingTransfer = nil
	delete(b.pendingTransferDeviceIDs, deviceID)

	return nil
}

// CancelInputDeviceTransfer cancels an outgoing transfer.
func (b *InMemoryBackend) CancelInputDeviceTransfer(deviceID string) error {
	b.mu.Lock("CancelInputDeviceTransfer")
	defer b.mu.Unlock()

	d, ok := b.inputDevices.Get(deviceID)
	if !ok {
		return fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	if d.PendingTransfer == nil {
		return fmt.Errorf("%w: device %s has no pending transfer", ErrConflict, deviceID)
	}

	d.PendingTransfer = nil
	delete(b.pendingTransferDeviceIDs, deviceID)

	return nil
}

// RejectInputDeviceTransfer rejects an incoming transfer.
func (b *InMemoryBackend) RejectInputDeviceTransfer(deviceID string) error {
	b.mu.Lock("RejectInputDeviceTransfer")
	defer b.mu.Unlock()

	d, ok := b.inputDevices.Get(deviceID)
	if !ok {
		return fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	if d.PendingTransfer == nil {
		return fmt.Errorf("%w: device %s has no pending transfer", ErrConflict, deviceID)
	}

	d.PendingTransfer = nil
	delete(b.pendingTransferDeviceIDs, deviceID)

	return nil
}

// ListInputDeviceTransfers lists devices with pending transfers.
// transferType must be "OUTGOING" or "INCOMING"; in this mock both resolve
// against the same pending-transfer store (we don't track the recipient side
// separately).
func (b *InMemoryBackend) ListInputDeviceTransfers(
	transferType string,
	maxResults int,
	nextToken string,
) ([]*InputDeviceTransfer, string, error) {
	if transferType != transferTypeOutgoing && transferType != transferTypeIncoming {
		return nil, "", fmt.Errorf(
			"%w: transferType must be OUTGOING or INCOMING",
			ErrInvalidParameter,
		)
	}

	b.mu.RLock("ListInputDeviceTransfers")
	defer b.mu.RUnlock()

	all := make([]*storedInputDevice, 0, len(b.pendingTransferDeviceIDs))
	for deviceID := range b.pendingTransferDeviceIDs {
		if d, ok := b.inputDevices.Get(deviceID); ok {
			all = append(all, d)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	pg := page.New(all, nextToken, maxResults, defaultMaxResults)

	transfers := make([]*InputDeviceTransfer, 0, len(pg.Data))
	for _, d := range pg.Data {
		transfers = append(transfers, d.toPendingTransfer(transferType))
	}

	return transfers, pg.Next, nil
}

// --- InputDevice lifecycle extras ---

// StartInputDevice starts an input device (no-op aside from existence check).
func (b *InMemoryBackend) StartInputDevice(deviceID string) error {
	b.mu.RLock("StartInputDevice")
	defer b.mu.RUnlock()

	if !b.inputDevices.Has(deviceID) {
		return fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	return nil
}

// StopInputDevice stops an input device (no-op aside from existence check).
func (b *InMemoryBackend) StopInputDevice(deviceID string) error {
	b.mu.RLock("StopInputDevice")
	defer b.mu.RUnlock()

	if !b.inputDevices.Has(deviceID) {
		return fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	return nil
}

// StartInputDeviceMaintenanceWindow opens a maintenance window for the
// device (no-op aside from existence check). StartInputDeviceMaintenanceWindowOutput
// carries no fields on the real wire, and the device shapes have no
// maintenanceWindowActive member to flip -- see gopherstack-7ux2.
func (b *InMemoryBackend) StartInputDeviceMaintenanceWindow(deviceID string) error {
	b.mu.RLock("StartInputDeviceMaintenanceWindow")
	defer b.mu.RUnlock()

	if !b.inputDevices.Has(deviceID) {
		return fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	return nil
}

// DescribeInputDeviceThumbnail returns the device (thumbnail image data is not emulated).
func (b *InMemoryBackend) DescribeInputDeviceThumbnail(deviceID string) (*InputDevice, error) {
	b.mu.RLock("DescribeInputDeviceThumbnail")
	defer b.mu.RUnlock()

	d, ok := b.inputDevices.Get(deviceID)
	if !ok {
		return nil, fmt.Errorf("%w: inputDevice %s not found", ErrNotFound, deviceID)
	}

	return d.toDevice(), nil
}
