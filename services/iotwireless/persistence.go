package iotwireless

import (
	"encoding/json"
	"maps"
)

// Snapshottable is an optional interface that a StorageBackend may implement to
// support state serialisation and restoration (e.g. for --persist mode).
// Backends that do not implement it are silently skipped during snapshot/restore.
type Snapshottable interface {
	Snapshot() []byte
	Restore(data []byte) error
}

// Snapshot implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Returns nil for non-snapshottable backends.
func (h *Handler) Snapshot() []byte {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Non-snapshottable backends are skipped.
func (h *Handler) Restore(data []byte) error {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Restore(data)
	}

	return nil
}

// deviceRecord serialises a WirelessDevice together with its resource key.
type deviceRecord struct {
	Device    *WirelessDevice `json:"device"`
	AccountID string          `json:"accountID"`
	Region    string          `json:"region"`
}

// gatewayRecord serialises a WirelessGateway together with its resource key.
type gatewayRecord struct {
	Gateway   *WirelessGateway `json:"gateway"`
	AccountID string           `json:"accountID"`
	Region    string           `json:"region"`
}

// serviceProfileRecord serialises a ServiceProfile together with its resource key.
type serviceProfileRecord struct {
	Profile   *ServiceProfile `json:"profile"`
	AccountID string          `json:"accountID"`
	Region    string          `json:"region"`
}

// destinationRecord serialises a Destination together with its resource key.
type destinationRecord struct {
	Destination *Destination `json:"destination"`
	AccountID   string       `json:"accountID"`
	Region      string       `json:"region"`
}

// backendSnapshot is the serialisable form of InMemoryBackend state.
type backendSnapshot struct {
	ResourceTags    map[string]map[string]string `json:"resourceTags,omitempty"`
	Devices         []deviceRecord               `json:"devices,omitempty"`
	Gateways        []gatewayRecord              `json:"gateways,omitempty"`
	ServiceProfiles []serviceProfileRecord       `json:"serviceProfiles,omitempty"`
	Destinations    []destinationRecord          `json:"destinations,omitempty"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		ResourceTags: make(map[string]map[string]string, len(b.resourceTags)),
	}

	for k, d := range b.devices {
		snap.Devices = append(snap.Devices, deviceRecord{
			AccountID: k.AccountID,
			Region:    k.Region,
			Device:    d,
		})
	}

	for k, gw := range b.gateways {
		snap.Gateways = append(snap.Gateways, gatewayRecord{
			AccountID: k.AccountID,
			Region:    k.Region,
			Gateway:   gw,
		})
	}

	for k, sp := range b.serviceProfiles {
		snap.ServiceProfiles = append(snap.ServiceProfiles, serviceProfileRecord{
			AccountID: k.AccountID,
			Region:    k.Region,
			Profile:   sp,
		})
	}

	for k, dest := range b.destinations {
		snap.Destinations = append(snap.Destinations, destinationRecord{
			AccountID:   k.AccountID,
			Region:      k.Region,
			Destination: dest,
		})
	}

	for arn, tags := range b.resourceTags {
		tagsCopy := make(map[string]string, len(tags))
		maps.Copy(tagsCopy, tags)

		snap.ResourceTags[arn] = tagsCopy
	}

	data, err := json.Marshal(snap)
	if err != nil {
		// Marshal of plain Go structs with JSON tags should never fail.
		// Returning nil signals to the persistence layer that nothing was captured.
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.devices = make(map[resourceKey]*WirelessDevice, len(snap.Devices))
	b.gateways = make(map[resourceKey]*WirelessGateway, len(snap.Gateways))
	b.serviceProfiles = make(map[resourceKey]*ServiceProfile, len(snap.ServiceProfiles))
	b.destinations = make(map[resourceKey]*Destination, len(snap.Destinations))
	b.resourceTags = make(map[string]map[string]string, len(snap.ResourceTags))

	for _, rec := range snap.Devices {
		if rec.Device == nil {
			continue
		}

		key := resourceKey{AccountID: rec.AccountID, Region: rec.Region, ID: rec.Device.ID}
		b.devices[key] = rec.Device
	}

	for _, rec := range snap.Gateways {
		if rec.Gateway == nil {
			continue
		}

		key := resourceKey{AccountID: rec.AccountID, Region: rec.Region, ID: rec.Gateway.ID}
		b.gateways[key] = rec.Gateway
	}

	for _, rec := range snap.ServiceProfiles {
		if rec.Profile == nil {
			continue
		}

		key := resourceKey{AccountID: rec.AccountID, Region: rec.Region, ID: rec.Profile.ID}
		b.serviceProfiles[key] = rec.Profile
	}

	for _, rec := range snap.Destinations {
		if rec.Destination == nil {
			continue
		}

		key := resourceKey{AccountID: rec.AccountID, Region: rec.Region, ID: rec.Destination.Name}
		b.destinations[key] = rec.Destination
	}

	for arn, tags := range snap.ResourceTags {
		tagsCopy := make(map[string]string, len(tags))
		maps.Copy(tagsCopy, tags)

		b.resourceTags[arn] = tagsCopy
	}

	return nil
}
