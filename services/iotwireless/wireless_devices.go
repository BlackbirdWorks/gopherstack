package iotwireless

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func wirelessDeviceARN(region, accountID, id string) string {
	return arn.Build("iotwireless", region, accountID, fmt.Sprintf("WirelessDevice/%s", id))
}

// copyWirelessDevice returns a shallow copy of d with an independent Tags map.
func copyWirelessDevice(d *WirelessDevice) *WirelessDevice {
	cp := *d
	cp.Tags = make(map[string]string, len(d.Tags))
	maps.Copy(cp.Tags, d.Tags)

	return &cp
}

// CreateWirelessDevice creates a new wireless device.
func (b *InMemoryBackend) CreateWirelessDevice(
	accountID, region, name, devType, destinationName, description string,
	tags map[string]string,
) (*WirelessDevice, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	arn := wirelessDeviceARN(region, accountID, id)

	d := &WirelessDevice{
		ID:              id,
		ARN:             arn,
		Name:            name,
		Type:            devType,
		DestinationName: destinationName,
		Description:     description,
		Tags:            newTagsCopy(tags),
		CreatedAt:       time.Now(),
		AccountID:       accountID,
		Region:          region,
	}

	b.devices.Put(d)
	b.storeResourceTagsLocked(arn, tags)

	return copyWirelessDevice(d), nil
}

// GetWirelessDevice returns a wireless device by ID.
func (b *InMemoryBackend) GetWirelessDevice(accountID, region, id string) (*WirelessDevice, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	d, ok := b.devices.Get(compositeKey(accountID, region, id))
	if !ok {
		return nil, ErrDeviceNotFound
	}

	return copyWirelessDevice(d), nil
}

// ListWirelessDevices returns all wireless devices for the given account and region,
// sorted by name for deterministic output.
func (b *InMemoryBackend) ListWirelessDevices(accountID, region string) []*WirelessDevice {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := b.devices.All()
	result := make([]*WirelessDevice, 0, len(all))

	for _, d := range all {
		if d.AccountID == accountID && d.Region == region {
			result = append(result, copyWirelessDevice(d))
		}
	}

	slices.SortFunc(result, func(a, b *WirelessDevice) int {
		return cmp.Compare(a.Name, b.Name)
	})

	return result
}

// DeleteWirelessDevice deletes a wireless device.
func (b *InMemoryBackend) DeleteWirelessDevice(accountID, region, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := compositeKey(accountID, region, id)

	d, ok := b.devices.Get(key)
	if !ok {
		return ErrDeviceNotFound
	}

	delete(b.resourceTags, d.ARN)
	b.devices.Delete(key)

	return nil
}

// AssociateWirelessDeviceWithThing associates a wireless device with an IoT Thing.
// Returns ErrDeviceNotFound when the device does not exist.
func (b *InMemoryBackend) AssociateWirelessDeviceWithThing(
	accountID, region, wirelessDeviceID, thingArn string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.devices.Has(compositeKey(accountID, region, wirelessDeviceID)) {
		return ErrDeviceNotFound
	}

	b.wirelessDeviceThings[wirelessDeviceID] = thingArn

	return nil
}

// GetWirelessDeviceThingArn returns the ARN of the IoT Thing associated with
// a wireless device, or "" if AssociateWirelessDeviceWithThing was never
// called (or the association was since cleared).
func (b *InMemoryBackend) GetWirelessDeviceThingArn(wirelessDeviceID string) string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.wirelessDeviceThings[wirelessDeviceID]
}

// AddWirelessDeviceInternal inserts a WirelessDevice directly into the backend, bypassing ID generation.
// Intended for test setup only.
func (b *InMemoryBackend) AddWirelessDeviceInternal(accountID, region string, d *WirelessDevice) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := copyWirelessDevice(d)
	cp.AccountID = accountID
	cp.Region = region
	b.devices.Put(cp)
	b.storeResourceTagsLocked(d.ARN, d.Tags)
}

// UpdateWirelessDevice updates mutable fields on an existing wireless device.
func (b *InMemoryBackend) UpdateWirelessDevice(
	accountID, region, id, name, description, destinationName string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.devices.Get(compositeKey(accountID, region, id))
	if !ok {
		return ErrDeviceNotFound
	}

	if name != "" {
		d.Name = name
	}

	d.Description = description

	if destinationName != "" {
		d.DestinationName = destinationName
	}

	return nil
}

// DisassociateWirelessDeviceFromThing clears the thing association for a wireless device.
func (b *InMemoryBackend) DisassociateWirelessDeviceFromThing(
	accountID, region, wirelessDeviceID string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.devices.Has(compositeKey(accountID, region, wirelessDeviceID)) {
		return ErrDeviceNotFound
	}

	delete(b.wirelessDeviceThings, wirelessDeviceID)

	return nil
}

// --- Queued messages operations ---

// ListQueuedMessages returns queued messages for a wireless device.
func (b *InMemoryBackend) ListQueuedMessages(wirelessDeviceID string) []QueuedMessage {
	b.mu.RLock()
	defer b.mu.RUnlock()

	msgs, ok := b.queuedMessages[wirelessDeviceID]
	if !ok {
		return []QueuedMessage{}
	}

	result := make([]QueuedMessage, len(msgs))
	copy(result, msgs)

	return result
}

// DeleteQueuedMessages clears the message queue for a wireless device.
func (b *InMemoryBackend) DeleteQueuedMessages(wirelessDeviceID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.queuedMessages, wirelessDeviceID)

	return nil
}

// EnqueueMessage appends a downlink message to a wireless device's message
// queue, so that a subsequent ListQueuedMessages reflects messages sent via
// SendDataToWirelessDevice.
func (b *InMemoryBackend) EnqueueMessage(wirelessDeviceID string, msg QueuedMessage) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.queuedMessages[wirelessDeviceID] = append(b.queuedMessages[wirelessDeviceID], msg)
}

// QueuedMessage represents a downlink message queued for a wireless device.
type QueuedMessage struct {
	ReceivedAt    time.Time
	MessageID     string
	PayloadBase64 string
	TransmitMode  int32
}
