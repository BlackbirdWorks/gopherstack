package ec2

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// DescribeNetworkInterfaceAttribute returns a requested attribute for a network interface.
func (b *InMemoryBackend) DescribeNetworkInterfaceAttribute(
	niID string, _ string,
) (*NIAttributeResult, error) {
	if niID == "" {
		return nil, fmt.Errorf("%w: NetworkInterfaceId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeNetworkInterfaceAttribute")
	defer b.mu.RUnlock()

	ni, ok := b.networkInterfaces.Get(niID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, niID)
	}

	return &NIAttributeResult{
		NetworkInterfaceID: niID,
		Description:        ni.Description,
		SourceDestCheck:    ni.SourceDestCheck,
	}, nil
}

// ResetNetworkInterfaceAttribute resets sourceDestCheck to true for a network interface.
func (b *InMemoryBackend) ResetNetworkInterfaceAttribute(niID string) error {
	if niID == "" {
		return fmt.Errorf("%w: NetworkInterfaceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ResetNetworkInterfaceAttribute")
	defer b.mu.Unlock()

	ni, ok := b.networkInterfaces.Get(niID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, niID)
	}
	ni.SourceDestCheck = true

	return nil
}

// ---- Network Interface permissions ----

// DescribeNetworkInterfacePermissions returns permissions for the given NIDs (or all).
func (b *InMemoryBackend) DescribeNetworkInterfacePermissions(
	niIDs []string,
) []*NetworkInterfacePermission {
	b.mu.RLock("DescribeNetworkInterfacePermissions")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(niIDs))
	for _, id := range niIDs {
		filter[id] = true
	}

	var out []*NetworkInterfacePermission
	for _, perm := range b.niPermissions.All() {
		if len(filter) > 0 && !filter[perm.NetworkInterfaceID] {
			continue
		}
		cp := *perm
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PermissionID < out[j].PermissionID })

	return out
}

// CreateNetworkInterfacePermission creates a new permission on a network interface.
func (b *InMemoryBackend) CreateNetworkInterfacePermission(
	niID, awsAccountID, awsService, permission string,
) (*NetworkInterfacePermission, error) {
	if niID == "" {
		return nil, fmt.Errorf("%w: NetworkInterfaceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateNetworkInterfacePermission")
	defer b.mu.Unlock()

	if _, ok := b.networkInterfaces.Get(niID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, niID)
	}

	perm := &NetworkInterfacePermission{
		PermissionID:       "eni-perm-" + uuid.New().String()[:8],
		NetworkInterfaceID: niID,
		AwsAccountID:       awsAccountID,
		AwsService:         awsService,
		Permission:         permission,
		State:              "granted",
	}
	b.niPermissions.Put(perm)

	return perm, nil
}

// DeleteNetworkInterfacePermission removes a network interface permission.
func (b *InMemoryBackend) DeleteNetworkInterfacePermission(permissionID string) error {
	if permissionID == "" {
		return fmt.Errorf("%w: NetworkInterfacePermissionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteNetworkInterfacePermission")
	defer b.mu.Unlock()

	if _, ok := b.niPermissions.Get(permissionID); !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInterfacePermissionNotFound, permissionID)
	}
	b.niPermissions.Delete(permissionID)

	return nil
}

// ---- IPv6 address assignment ----

// AssignIpv6Addresses assigns IPv6 addresses to a network interface.
func (b *InMemoryBackend) AssignIpv6Addresses(niID string, count int) ([]string, error) {
	if niID == "" {
		return nil, fmt.Errorf("%w: NetworkInterfaceId is required", ErrInvalidParameter)
	}
	if count < 1 {
		count = 1
	}

	b.mu.Lock("AssignIpv6Addresses")
	defer b.mu.Unlock()

	if _, ok := b.networkInterfaces.Get(niID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, niID)
	}

	assigned := make([]string, 0, count)
	for range count {
		addr := fmt.Sprintf("2001:db8::%s", uuid.New().String()[:4])
		b.niIPv6Addresses[niID] = append(b.niIPv6Addresses[niID], addr)
		assigned = append(assigned, addr)
	}

	return assigned, nil
}

// UnassignIpv6Addresses removes IPv6 addresses from a network interface.
func (b *InMemoryBackend) UnassignIpv6Addresses(niID string, addresses []string) error {
	if niID == "" {
		return fmt.Errorf("%w: NetworkInterfaceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("UnassignIpv6Addresses")
	defer b.mu.Unlock()

	if _, ok := b.networkInterfaces.Get(niID); !ok {
		return fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, niID)
	}

	toRemove := make(map[string]bool, len(addresses))
	for _, a := range addresses {
		toRemove[a] = true
	}

	filtered := b.niIPv6Addresses[niID][:0]
	for _, a := range b.niIPv6Addresses[niID] {
		if !toRemove[a] {
			filtered = append(filtered, a)
		}
	}
	b.niIPv6Addresses[niID] = filtered

	return nil
}

// ---- DescribeAccountAttributes ----

// AccountAttribute represents a single EC2 account attribute.
type AccountAttribute struct {
	Name   string   `json:"name,omitempty"`
	Values []string `json:"values,omitempty"`
}
