package ec2

import (
	"fmt"
	"sort"
)

// EnableVgwRoutePropagation enables route propagation for a VGW in a route table.
func (b *InMemoryBackend) EnableVgwRoutePropagation(routeTableID, gatewayID string) error {
	if routeTableID == "" || gatewayID == "" {
		return fmt.Errorf("%w: RouteTableId and GatewayId are required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableVgwRoutePropagation")
	defer b.mu.Unlock()

	key := routeTableID + ":" + gatewayID
	b.vgwRoutePropagation[key] = true

	return nil
}

// DisableVgwRoutePropagation disables route propagation for a VGW in a route table.
func (b *InMemoryBackend) DisableVgwRoutePropagation(routeTableID, gatewayID string) error {
	if routeTableID == "" || gatewayID == "" {
		return fmt.Errorf("%w: RouteTableId and GatewayId are required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableVgwRoutePropagation")
	defer b.mu.Unlock()

	key := routeTableID + ":" + gatewayID
	delete(b.vgwRoutePropagation, key)

	return nil
}

// ---- Default credit specification ----

// ModifyTransitGateway updates properties of a transit gateway.
func (b *InMemoryBackend) ModifyTransitGateway(tgwID, description string) error {
	if tgwID == "" {
		return fmt.Errorf("%w: TransitGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyTransitGateway")
	defer b.mu.Unlock()

	tgw, ok := b.transitGateways.Get(tgwID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, tgwID)
	}
	if description != "" {
		tgw.Description = description
	}

	return nil
}

// DescribeTransitGateways returns transit gateways, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeTransitGateways(ids []string) []*TransitGateway {
	b.mu.RLock("DescribeTransitGateways")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*TransitGateway, 0, b.transitGateways.Len())

	for _, tgw := range b.transitGateways.All() {
		if len(idSet) > 0 && !idSet[tgw.ID] {
			continue
		}

		cp := *tgw
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})

	return out
}

// CreateTransitGateway creates a new transit gateway stub.
func (b *InMemoryBackend) CreateTransitGateway(description string) (*TransitGateway, error) {
	b.mu.Lock("CreateTransitGateway")
	defer b.mu.Unlock()

	tgw := &TransitGateway{
		ID:          "tgw-" + b.AccountID[:8],
		Description: description,
		State:       stateAvailable,
		OwnerID:     b.AccountID,
	}
	b.transitGateways.Put(tgw)

	cp := *tgw

	return &cp, nil
}

// DeleteTransitGateway removes a transit gateway stub.
func (b *InMemoryBackend) DeleteTransitGateway(id string) error {
	if id == "" {
		return fmt.Errorf("%w: TransitGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGateway")
	defer b.mu.Unlock()

	if _, ok := b.transitGateways.Get(id); !ok {
		return fmt.Errorf("%w: transit gateway %s not found", ErrInvalidParameter, id)
	}
	b.transitGateways.Delete(id)
	delete(b.tags, id)

	return nil
}

// initParityFinalMaps initialises the state added for the final EC2 parity
// sweep (gopherstack-5o9): TGW route table propagations, interruptible
// Capacity Reservation allocations, and EC2-Classic moving-address status
// (split out to keep newInMemoryBackendMaps under the funlen limit).
func initParityFinalMaps(b *InMemoryBackend) {
	b.tgwRTPropagations = make(map[string]map[string]*TransitGatewayRouteTablePropagation)
	b.reachabilityAnalyzerOrgSharing = false
}

// ---- VerifiedAccess modify ----

// tgwAttachmentResourceLocked returns the resource ID/type backing a TGW
// attachment, scanning the existing per-type attachment maps. Must be called
// with b.mu held.
func (b *InMemoryBackend) tgwAttachmentResourceLocked(attachmentID string) (string, string) {
	if att, ok := b.tgwVpcAttachments.Get(attachmentID); ok {
		return att.VpcID, tgwResourceTypeVPC
	}

	if att, ok := b.tgwPeeringAttachments.Get(attachmentID); ok {
		return att.AccepterTransitGatewayID, "peering"
	}

	if att, ok := b.tgwConnects.Get(attachmentID); ok {
		return att.TransportTransitGatewayAttachmentID, "connect"
	}

	return "", ""
}

// EnableTransitGatewayRouteTablePropagation enables route propagation from an
// attachment into a TGW route table.
func (b *InMemoryBackend) EnableTransitGatewayRouteTablePropagation(
	routeTableID, attachmentID string,
) (*TransitGatewayRouteTablePropagation, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("EnableTransitGatewayRouteTablePropagation")
	defer b.mu.Unlock()

	if _, ok := b.tgwRouteTables.Get(routeTableID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWRouteTableNotFound, routeTableID)
	}

	if !b.transitGatewayAttachmentExistsLocked(attachmentID) {
		return nil, fmt.Errorf("%w: %s", ErrTGWAttachmentNotFound, attachmentID)
	}

	resourceID, resourceType := b.tgwAttachmentResourceLocked(attachmentID)

	if b.tgwRTPropagations[routeTableID] == nil {
		b.tgwRTPropagations[routeTableID] = make(map[string]*TransitGatewayRouteTablePropagation)
	}

	prop := &TransitGatewayRouteTablePropagation{
		ResourceID:                 resourceID,
		ResourceType:               resourceType,
		State:                      tgwPropagationStateEnabled,
		TransitGatewayAttachmentID: attachmentID,
	}
	b.tgwRTPropagations[routeTableID][attachmentID] = prop

	cp := *prop

	return &cp, nil
}

// DisableTransitGatewayRouteTablePropagation disables a previously enabled
// route propagation.
func (b *InMemoryBackend) DisableTransitGatewayRouteTablePropagation(
	routeTableID, attachmentID string,
) (*TransitGatewayRouteTablePropagation, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisableTransitGatewayRouteTablePropagation")
	defer b.mu.Unlock()

	inner, ok := b.tgwRTPropagations[routeTableID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWPropagationNotFound, attachmentID)
	}

	prop, ok := inner[attachmentID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTGWPropagationNotFound, attachmentID)
	}

	delete(inner, attachmentID)

	cp := *prop
	cp.State = tgwPropagationStateDisabled

	return &cp, nil
}

// DescribeTransitGatewayAttachments returns a unified view over the existing
// VPC, peering, and Connect attachment maps.
func (b *InMemoryBackend) DescribeTransitGatewayAttachments(ids []string) []*TransitGatewayAttachmentSummary {
	b.mu.RLock("DescribeTransitGatewayAttachments")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	out := make([]*TransitGatewayAttachmentSummary, 0)

	for _, att := range b.tgwVpcAttachments.All() {
		if len(filter) > 0 && !filter[att.TransitGatewayAttachmentID] {
			continue
		}

		out = append(out, &TransitGatewayAttachmentSummary{
			TransitGatewayAttachmentID: att.TransitGatewayAttachmentID,
			TransitGatewayID:           att.TransitGatewayID,
			ResourceID:                 att.VpcID,
			ResourceType:               tgwResourceTypeVPC,
			State:                      att.State,
		})
	}

	for _, att := range b.tgwPeeringAttachments.All() {
		if len(filter) > 0 && !filter[att.TransitGatewayAttachmentID] {
			continue
		}

		out = append(out, &TransitGatewayAttachmentSummary{
			TransitGatewayAttachmentID: att.TransitGatewayAttachmentID,
			TransitGatewayID:           att.RequesterTransitGatewayID,
			ResourceID:                 att.AccepterTransitGatewayID,
			ResourceType:               "peering",
			State:                      att.State,
		})
	}

	for _, att := range b.tgwConnects.All() {
		if len(filter) > 0 && !filter[att.TransitGatewayAttachmentID] {
			continue
		}

		out = append(out, &TransitGatewayAttachmentSummary{
			TransitGatewayAttachmentID: att.TransitGatewayAttachmentID,
			TransitGatewayID:           att.TransitGatewayID,
			ResourceID:                 att.TransportTransitGatewayAttachmentID,
			ResourceType:               "connect",
			State:                      att.State,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].TransitGatewayAttachmentID < out[j].TransitGatewayAttachmentID
	})

	return out
}

// ---- Capacity Reservation extras ----

const (
	tgwPropagationStateEnabled  = "enabled"
	tgwPropagationStateDisabled = "disabled"
)

// TransitGatewayAttachmentSummary is the unified view of a TGW attachment
// returned by DescribeTransitGatewayAttachments. It aggregates rows sourced
// from the existing per-type attachment maps (VPC, peering, Connect) rather
// than a dedicated map, since each attachment already has a single owning map
// elsewhere in the backend.
type TransitGatewayAttachmentSummary struct {
	TransitGatewayAttachmentID string
	TransitGatewayID           string
	ResourceID                 string
	ResourceType               string
	State                      string
}
