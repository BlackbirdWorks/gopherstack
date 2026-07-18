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

func multicastGroupARN(region, accountID, id string) string {
	return arn.Build("iotwireless", region, accountID, fmt.Sprintf("MulticastGroup/%s", id))
}

func copyMulticastGroup(mg *MulticastGroup) *MulticastGroup {
	cp := *mg
	cp.Tags = make(map[string]string, len(mg.Tags))
	maps.Copy(cp.Tags, mg.Tags)

	return &cp
}

// CreateMulticastGroup creates a new multicast group.
func (b *InMemoryBackend) CreateMulticastGroup(
	accountID, region, name, description string,
	tags map[string]string,
) (*MulticastGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	arn := multicastGroupARN(region, accountID, id)

	mg := &MulticastGroup{
		ID:          id,
		ARN:         arn,
		Name:        name,
		Description: description,
		Status:      "Pending",
		Tags:        newTagsCopy(tags),
		CreatedAt:   time.Now(),
		AccountID:   accountID,
		Region:      region,
	}

	b.multicastGroups.Put(mg)
	b.storeResourceTagsLocked(arn, tags)

	return copyMulticastGroup(mg), nil
}

// GetMulticastGroup returns a multicast group by ID.
func (b *InMemoryBackend) GetMulticastGroup(accountID, region, id string) (*MulticastGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	mg, ok := b.multicastGroups.Get(compositeKey(accountID, region, id))
	if !ok {
		return nil, ErrMulticastGroupNotFound
	}

	return copyMulticastGroup(mg), nil
}

// ListMulticastGroups returns all multicast groups for the given account and region,
// sorted by name for deterministic output.
func (b *InMemoryBackend) ListMulticastGroups(accountID, region string) []*MulticastGroup {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := b.multicastGroups.All()
	result := make([]*MulticastGroup, 0, len(all))

	for _, mg := range all {
		if mg.AccountID == accountID && mg.Region == region {
			result = append(result, copyMulticastGroup(mg))
		}
	}

	slices.SortFunc(result, func(a, b *MulticastGroup) int {
		return cmp.Compare(a.Name, b.Name)
	})

	return result
}

// DeleteMulticastGroup deletes a multicast group by ID.
func (b *InMemoryBackend) DeleteMulticastGroup(accountID, region, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := compositeKey(accountID, region, id)

	mg, ok := b.multicastGroups.Get(key)
	if !ok {
		return ErrMulticastGroupNotFound
	}

	delete(b.resourceTags, mg.ARN)
	b.multicastGroups.Delete(key)

	return nil
}

// UpdateMulticastGroup updates mutable fields on an existing multicast group.
func (b *InMemoryBackend) UpdateMulticastGroup(accountID, region, id, name, description string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	mg, ok := b.multicastGroups.Get(compositeKey(accountID, region, id))
	if !ok {
		return ErrMulticastGroupNotFound
	}

	if name != "" {
		mg.Name = name
	}

	mg.Description = description

	return nil
}

// AssociateWirelessDeviceWithMulticastGroup records the association of a wireless device with a multicast group.
func (b *InMemoryBackend) AssociateWirelessDeviceWithMulticastGroup(multicastGroupID, wirelessDeviceID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.multicastGroupDevices[multicastGroupID] = wirelessDeviceID

	return nil
}

// CancelMulticastGroupSession marks the multicast group session as cancelled.
// If no session is active, the call is a no-op (idempotent).
func (b *InMemoryBackend) CancelMulticastGroupSession(multicastGroupID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.multicastGroupSessions, multicastGroupID)
	delete(b.multicastGroupSessionStart, multicastGroupID)

	return nil
}

// DisassociateWirelessDeviceFromMulticastGroup removes a device from a multicast group.
func (b *InMemoryBackend) DisassociateWirelessDeviceFromMulticastGroup(
	multicastGroupID string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.multicastGroupDevices, multicastGroupID)

	return nil
}

// StartMulticastGroupSession marks a multicast group session as active,
// recording its start time so GetMulticastGroupSession can report it back.
func (b *InMemoryBackend) StartMulticastGroupSession(multicastGroupID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.multicastGroupSessions[multicastGroupID] = true
	b.multicastGroupSessionStart[multicastGroupID] = time.Now().UTC()

	return nil
}

// GetMulticastGroupSession returns the start time of a multicast group's active
// session. Returns ErrMulticastGroupSessionNotFound if no session has been
// started (or it has since been cancelled), matching real AWS's
// ResourceNotFoundException for a group with no active session.
func (b *InMemoryBackend) GetMulticastGroupSession(multicastGroupID string) (time.Time, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.multicastGroupSessions[multicastGroupID] {
		return time.Time{}, ErrMulticastGroupSessionNotFound
	}

	return b.multicastGroupSessionStart[multicastGroupID], nil
}
