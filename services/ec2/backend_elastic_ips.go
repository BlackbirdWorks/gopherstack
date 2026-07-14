package ec2

import (
	"fmt"
	"sort"
	"time"
)

// DescribeAddressesAttribute returns domain-name attributes for Elastic IPs.
func (b *InMemoryBackend) DescribeAddressesAttribute(allocationIDs []string) []AddressAttribute {
	b.mu.RLock("DescribeAddressesAttribute")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(allocationIDs))
	for _, id := range allocationIDs {
		filter[id] = true
	}

	var out []AddressAttribute
	for _, addr := range b.addresses.All() {
		if len(filter) > 0 && !filter[addr.AllocationID] {
			continue
		}
		attr := AddressAttribute{
			AllocationID: addr.AllocationID,
			PublicIP:     addr.PublicIP,
		}
		if stored, ok := b.addressAttributes.Get(addr.AllocationID); ok {
			attr.DomainName = stored.DomainName
		}
		out = append(out, attr)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AllocationID < out[j].AllocationID })

	return out
}

// ModifyAddressAttribute sets the domain name for an Elastic IP.
func (b *InMemoryBackend) ModifyAddressAttribute(allocationID, domainName string) error {
	if allocationID == "" {
		return fmt.Errorf("%w: AllocationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyAddressAttribute")
	defer b.mu.Unlock()

	addr, ok := b.addresses.Get(allocationID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, allocationID)
	}
	b.addressAttributes.Put(&AddressAttribute{
		AllocationID: allocationID,
		PublicIP:     addr.PublicIP,
		DomainName:   domainName,
	})

	return nil
}

// ResetAddressAttribute clears the domain name for an Elastic IP.
func (b *InMemoryBackend) ResetAddressAttribute(allocationID string) error {
	if allocationID == "" {
		return fmt.Errorf("%w: AllocationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ResetAddressAttribute")
	defer b.mu.Unlock()

	if _, ok := b.addresses.Get(allocationID); !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, allocationID)
	}
	b.addressAttributes.Delete(allocationID)

	return nil
}

// ---- Instance console output ----

// EnableAddressTransfer enables EIP transfer to another account.
func (b *InMemoryBackend) EnableAddressTransfer(
	allocationID, transferAccountID string,
) (*AddressTransfer, error) {
	if allocationID == "" {
		return nil, fmt.Errorf("%w: AllocationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableAddressTransfer")
	defer b.mu.Unlock()

	addr, ok := b.addresses.Get(allocationID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, allocationID)
	}

	transfer := &AddressTransfer{
		AllocationID:        allocationID,
		PublicIP:            addr.PublicIP,
		TransferAccountID:   transferAccountID,
		TransferOfferStatus: "pending",
		TransferOfferExpiry: time.Now().UTC().AddDate(0, 0, addressTransferOfferDays),
	}
	b.addressTransfers[allocationID] = transfer

	return transfer, nil
}

// DisableAddressTransfer cancels an EIP transfer.
func (b *InMemoryBackend) DisableAddressTransfer(allocationID string) error {
	if allocationID == "" {
		return fmt.Errorf("%w: AllocationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableAddressTransfer")
	defer b.mu.Unlock()

	if _, ok := b.addresses.Get(allocationID); !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, allocationID)
	}
	delete(b.addressTransfers, allocationID)

	return nil
}

// DescribeAddressTransfers returns address transfers, optionally filtered by allocation ID.
func (b *InMemoryBackend) DescribeAddressTransfers(allocationIDs []string) []*AddressTransfer {
	b.mu.RLock("DescribeAddressTransfers")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(allocationIDs))
	for _, id := range allocationIDs {
		filter[id] = true
	}

	var out []*AddressTransfer
	for _, t := range b.addressTransfers {
		if len(filter) > 0 && !filter[t.AllocationID] {
			continue
		}
		cp := *t
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AllocationID < out[j].AllocationID })

	return out
}

// ---- Subnet CIDR reservations ----

// RestoreAddressToClassic moves an EIP from VPC to EC2-Classic (no-op in mock).
func (b *InMemoryBackend) RestoreAddressToClassic(publicIP string) error {
	if publicIP == "" {
		return fmt.Errorf("%w: PublicIp is required", ErrInvalidParameter)
	}

	return nil
}

// ---- ReportInstanceStatus ----

// findAddressByPublicIPLocked scans the addresses map for an EIP by public IP
// address. Must be called with b.mu held.
func (b *InMemoryBackend) findAddressByPublicIPLocked(publicIP string) *Address {
	for _, addr := range b.addresses.All() {
		if addr.PublicIP == publicIP {
			return addr
		}
	}

	return nil
}

// MoveAddressToVpc marks an existing Elastic IP as moving into the VPC
// platform, overlaying EC2-Classic move status on top of the existing
// address state.
func (b *InMemoryBackend) MoveAddressToVpc(publicIP string) (*Address, error) {
	if publicIP == "" {
		return nil, fmt.Errorf("%w: PublicIp is required", ErrInvalidParameter)
	}

	b.mu.Lock("MoveAddressToVpc")
	defer b.mu.Unlock()

	addr := b.findAddressByPublicIPLocked(publicIP)
	if addr == nil {
		return nil, fmt.Errorf("%w: %s", ErrPublicIPNotFound, publicIP)
	}
	b.movingAddresses.Put(&MovingAddressStatus{
		PublicIP:   publicIP,
		MoveStatus: moveStatusMovingToVpc,
	})

	cp := *addr

	return &cp, nil
}

// DescribeMovingAddresses returns the moving-address status entries created
// by MoveAddressToVpc, optionally filtered by public IP.
func (b *InMemoryBackend) DescribeMovingAddresses(publicIPs []string) []*MovingAddressStatus {
	b.mu.RLock("DescribeMovingAddresses")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(publicIPs))
	for _, ip := range publicIPs {
		filter[ip] = true
	}

	out := make([]*MovingAddressStatus, 0, b.movingAddresses.Len())

	for _, st := range b.movingAddresses.All() {
		if len(filter) > 0 && !filter[st.PublicIP] {
			continue
		}

		cp := *st
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].PublicIP < out[j].PublicIP })

	return out
}

const moveStatusMovingToVpc = "movingToVpc"

// MovingAddressStatus represents the EC2-Classic move state of an Elastic IP,
// as tracked by MoveAddressToVpc/DescribeMovingAddresses.
type MovingAddressStatus struct {
	PublicIP   string `json:"publicIP,omitempty"`
	MoveStatus string `json:"moveStatus,omitempty"`
}
