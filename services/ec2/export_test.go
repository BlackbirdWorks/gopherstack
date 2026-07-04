package ec2

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"time"
)

// DefaultJanitorInterval exposes the package default janitor interval for testing.
const DefaultJanitorInterval = defaultJanitorInterval

// DefaultTerminatedTTL exposes the package default terminated instance TTL for testing.
const DefaultTerminatedTTL = defaultTerminatedTTL

// DefaultCancelledSpotTTL exposes the package default cancelled spot request TTL for testing.
const DefaultCancelledSpotTTL = defaultCancelledSpotTTL

// TickLifecycleForTest synchronously runs one pass of the lifecycle reconciler,
// advancing any transitional instance states to their next stable state.
// Used in tests to avoid waiting for the background goroutine.
func (b *InMemoryBackend) TickLifecycleForTest() {
	b.reconcileInstanceLifecycle()
}

// SweepTerminatedInstancesForTest exposes sweepTerminatedInstances for unit tests.
func (j *Janitor) SweepTerminatedInstancesForTest(ctx context.Context) {
	j.sweepTerminatedInstances(ctx)
}

// SweepCancelledSpotRequestsForTest exposes sweepCancelledSpotRequests for unit tests.
func (j *Janitor) SweepCancelledSpotRequestsForTest(ctx context.Context) {
	j.sweepCancelledSpotRequests(ctx)
}

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	return h.janitor.TaskTimeout
}

// GetJanitorInterval returns the Interval configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the interval.
func (h *Handler) GetJanitorInterval() time.Duration {
	return h.janitor.Interval
}

// GetJanitorTerminatedTTL returns the TerminatedTTL configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the TTL.
func (h *Handler) GetJanitorTerminatedTTL() time.Duration {
	return h.janitor.TerminatedTTL
}

// GetJanitorCancelledSpotTTL returns the CancelledSpotTTL on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the TTL.
func (h *Handler) GetJanitorCancelledSpotTTL() time.Duration {
	return h.janitor.CancelledSpotTTL
}

// SetInstanceTerminatedAtForTest sets the TerminatedAt field on an instance for testing.
// This allows tests to back-date the termination time to trigger immediate sweeping.
func (b *InMemoryBackend) SetInstanceTerminatedAtForTest(id string, t time.Time) {
	b.mu.Lock("SetInstanceTerminatedAtForTest")
	defer b.mu.Unlock()

	if inst, ok := b.instances[id]; ok {
		inst.TerminatedAt = t
	}
}

// SetSpotRequestCancelledAtForTest sets the CancelledAt field on a spot request for testing.
func (b *InMemoryBackend) SetSpotRequestCancelledAtForTest(id string, t time.Time) {
	b.mu.Lock("SetSpotRequestCancelledAtForTest")
	defer b.mu.Unlock()

	if req, ok := b.spotRequests[id]; ok {
		req.CancelledAt = t
	}
}

// InjectOrphanedENIForTest injects a NetworkInterface directly into the backend
// map without going through TerminateInstances, simulating state restored from
// a snapshot that predates the ENI-cleanup fix. Used to test the janitor's
// defensive ENI sweep.
func (b *InMemoryBackend) InjectOrphanedENIForTest(eni *NetworkInterface) {
	b.mu.Lock("InjectOrphanedENIForTest")
	defer b.mu.Unlock()

	b.networkInterfaces[eni.ID] = eni
}

// ---- count helpers for new resource types ----

// AddressTransferCount returns the number of address transfers in the backend.
func (b *InMemoryBackend) AddressTransferCount() int {
	b.mu.RLock("AddressTransferCount")
	defer b.mu.RUnlock()

	return len(b.addressTransfers)
}

// CapacityReservationCount returns the number of capacity reservations in the backend.
func (b *InMemoryBackend) CapacityReservationCount() int {
	b.mu.RLock("CapacityReservationCount")
	defer b.mu.RUnlock()

	return len(b.capacityReservations)
}

// ReservedInstancesExchangeCount returns the number of reserved instances exchanges.
func (b *InMemoryBackend) ReservedInstancesExchangeCount() int {
	b.mu.RLock("ReservedInstancesExchangeCount")
	defer b.mu.RUnlock()

	return len(b.reservedInstancesExchanges)
}

// TGWMulticastDomainAssociationCount returns the number of TGW multicast domain associations.
func (b *InMemoryBackend) TGWMulticastDomainAssociationCount() int {
	b.mu.RLock("TGWMulticastDomainAssociationCount")
	defer b.mu.RUnlock()

	return len(b.tgwMulticastDomainAssociations)
}

// TGWPeeringAttachmentCount returns the number of TGW peering attachments.
func (b *InMemoryBackend) TGWPeeringAttachmentCount() int {
	b.mu.RLock("TGWPeeringAttachmentCount")
	defer b.mu.RUnlock()

	return len(b.tgwPeeringAttachments)
}

// TGWVpcAttachmentCount returns the number of TGW VPC attachments.
func (b *InMemoryBackend) TGWVpcAttachmentCount() int {
	b.mu.RLock("TGWVpcAttachmentCount")
	defer b.mu.RUnlock()

	return len(b.tgwVpcAttachments)
}

// VpcEndpointConnectionCount returns the number of VPC endpoint connections.
func (b *InMemoryBackend) VpcEndpointConnectionCount() int {
	b.mu.RLock("VpcEndpointConnectionCount")
	defer b.mu.RUnlock()

	return len(b.vpcEndpointConnections)
}

// VpcPeeringConnectionCount returns the number of VPC peering connections.
func (b *InMemoryBackend) VpcPeeringConnectionCount() int {
	b.mu.RLock("VpcPeeringConnectionCount")
	defer b.mu.RUnlock()

	return len(b.vpcPeeringConnections)
}

// ByoipCidrCount returns the number of BYOIP CIDRs in the backend.
func (b *InMemoryBackend) ByoipCidrCount() int {
	b.mu.RLock("ByoipCidrCount")
	defer b.mu.RUnlock()

	return len(b.byoipCidrs)
}

// SeedReservedInstancesOffering inserts a reserved instances offering directly (for tests).
func (b *InMemoryBackend) SeedReservedInstancesOffering(
	offeringID, instanceType, az, productDesc, offeringType string,
	duration int64,
	fixedPrice, usagePrice float64,
) {
	b.mu.Lock("SeedReservedInstancesOffering")
	defer b.mu.Unlock()

	b.reservedInstancesOfferings[offeringID] = &ReservedInstancesOffering{
		ReservedInstancesOfferingID: offeringID,
		InstanceType:                instanceType,
		AvailabilityZone:            az,
		ProductDescription:          productDesc,
		OfferingType:                offeringType,
		Duration:                    duration,
		FixedPrice:                  fixedPrice,
		UsagePrice:                  usagePrice,
	}
}

// DedicatedHostCount returns the number of dedicated hosts in the backend.
func (b *InMemoryBackend) DedicatedHostCount() int {
	b.mu.RLock("DedicatedHostCount")
	defer b.mu.RUnlock()

	return len(b.dedicatedHosts)
}

// HandlerOpsLen returns the number of operations registered in the handler's dispatch table.
func (h *Handler) HandlerOpsLen() int {
	return len(h.ops)
}

// AddImageForTest injects an AMI stub directly into the backend image map.
// Used by tests to set up pagination scenarios without going through CreateImage.
func (b *InMemoryBackend) AddImageForTest(img AMIStub) {
	b.mu.Lock("AddImageForTest")
	defer b.mu.Unlock()

	b.images[img.ImageID] = &img
}

// ExportDispatch calls the handler's dispatch method and returns the XML response as a string.
// Used by accuracy tests to call handlers without a full HTTP round-trip.
func ExportDispatch(h *Handler, vals url.Values) (string, error) {
	action := vals.Get("Action")

	resp, err := h.dispatch(action, vals, fmt.Sprintf("test-req-%s", action))
	if err != nil {
		return "", err
	}

	xmlBytes, marshalErr := xml.Marshal(resp)
	if marshalErr != nil {
		return "", marshalErr
	}

	return string(xmlBytes), nil
}
