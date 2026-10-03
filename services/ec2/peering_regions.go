package ec2

// peerRegionBackend returns the backend serving region when it is not b's own.
func (b *InMemoryBackend) peerRegionBackend(region string) *InMemoryBackend {
	if region == "" || region == b.Region || b.regionBackend == nil {
		return nil
	}

	return b.regionBackend(region)
}

// counterpartRegion is the region holding the other side of a peering seen from b.
func (b *InMemoryBackend) counterpartRegion(requesterRegion, accepterRegion string) string {
	if requesterRegion == "" || requesterRegion == b.Region {
		return accepterRegion
	}

	return requesterRegion
}

// CreateTransitGatewayPeeringAttachment creates the attachment here and mirrors it into the accepter region.
func (b *InMemoryBackend) CreateTransitGatewayPeeringAttachment(
	transitGatewayID, peerTransitGatewayID, peerAccountID, peerRegion string,
) (*TransitGatewayPeeringAttachment, error) {
	att, err := b.createTransitGatewayPeeringAttachmentLocal(
		transitGatewayID, peerTransitGatewayID, peerAccountID, peerRegion,
	)
	if err != nil {
		return nil, err
	}

	if peer := b.peerRegionBackend(peerRegion); peer != nil {
		peer.putTGWPeeringAttachmentMirror(att)
	}

	return att, nil
}

// AcceptTransitGatewayPeeringAttachment accepts the attachment and updates the other region's copy.
func (b *InMemoryBackend) AcceptTransitGatewayPeeringAttachment(
	id string,
) (*TransitGatewayPeeringAttachment, error) {
	att, err := b.acceptTransitGatewayPeeringAttachmentLocal(id)
	if err != nil {
		return nil, err
	}

	if peer := b.peerRegionBackend(b.counterpartRegion(att.RequesterRegion, att.AccepterRegion)); peer != nil {
		peer.setTGWPeeringAttachmentState(id, att.State)
	}

	return att, nil
}

// RejectTransitGatewayPeeringAttachment rejects the attachment and updates the other region's copy.
func (b *InMemoryBackend) RejectTransitGatewayPeeringAttachment(
	id string,
) (*TransitGatewayPeeringAttachment, error) {
	att, err := b.rejectTransitGatewayPeeringAttachmentLocal(id)
	if err != nil {
		return nil, err
	}

	if peer := b.peerRegionBackend(b.counterpartRegion(att.RequesterRegion, att.AccepterRegion)); peer != nil {
		peer.setTGWPeeringAttachmentState(id, att.State)
	}

	return att, nil
}

// DeleteTransitGatewayPeeringAttachment deletes the attachment in both regions.
func (b *InMemoryBackend) DeleteTransitGatewayPeeringAttachment(id string) (*TransitGatewayPeeringAttachment, error) {
	att, err := b.deleteTransitGatewayPeeringAttachmentLocal(id)
	if err != nil {
		return nil, err
	}

	if peer := b.peerRegionBackend(b.counterpartRegion(att.RequesterRegion, att.AccepterRegion)); peer != nil {
		_, _ = peer.deleteTransitGatewayPeeringAttachmentLocal(id)
	}

	return att, nil
}

func (b *InMemoryBackend) putTGWPeeringAttachmentMirror(att *TransitGatewayPeeringAttachment) {
	b.mu.Lock("putTGWPeeringAttachmentMirror")
	defer b.mu.Unlock()

	cp := *att
	b.tgwPeeringAttachments.Put(&cp)
}

func (b *InMemoryBackend) setTGWPeeringAttachmentState(id, state string) {
	b.mu.Lock("setTGWPeeringAttachmentState")
	defer b.mu.Unlock()

	if att, ok := b.tgwPeeringAttachments.Get(id); ok {
		att.State = state
	}
}

// CreateVpcPeeringConnection creates the connection here and mirrors it into the accepter region.
func (b *InMemoryBackend) CreateVpcPeeringConnection(
	requesterVPCID, accepterVPCID, peerOwnerID, peerRegion string,
) (*VpcPeeringConnection, error) {
	pc, err := b.createVpcPeeringConnectionLocal(requesterVPCID, accepterVPCID, peerOwnerID, peerRegion)
	if err != nil {
		return nil, err
	}

	if peer := b.peerRegionBackend(pc.AccepterRegion); peer != nil {
		peer.putVpcPeeringConnectionMirror(pc)
	}

	return pc, nil
}

// AcceptVpcPeeringConnection accepts the connection and updates the other region's copy.
func (b *InMemoryBackend) AcceptVpcPeeringConnection(id string) (*VpcPeeringConnection, error) {
	pc, err := b.acceptVpcPeeringConnectionLocal(id)
	if err != nil {
		return nil, err
	}

	if peer := b.peerRegionBackend(b.counterpartRegion(pc.RequesterRegion, pc.AccepterRegion)); peer != nil {
		peer.setVpcPeeringConnectionState(id, pc.State)
	}

	return pc, nil
}

// RejectVpcPeeringConnection rejects the connection and updates the other region's copy.
func (b *InMemoryBackend) RejectVpcPeeringConnection(id string) error {
	if err := b.rejectVpcPeeringConnectionLocal(id); err != nil {
		return err
	}

	if peer := b.peerRegionBackend(b.vpcPeeringCounterpart(id)); peer != nil {
		peer.setVpcPeeringConnectionState(id, "rejected")
	}

	return nil
}

// DeleteVpcPeeringConnection deletes the connection in both regions.
func (b *InMemoryBackend) DeleteVpcPeeringConnection(id string) error {
	region := b.vpcPeeringCounterpart(id)

	if err := b.deleteVpcPeeringConnectionLocal(id); err != nil {
		return err
	}

	if peer := b.peerRegionBackend(region); peer != nil {
		_ = peer.deleteVpcPeeringConnectionLocal(id)
	}

	return nil
}

func (b *InMemoryBackend) vpcPeeringCounterpart(id string) string {
	b.mu.RLock("vpcPeeringCounterpart")
	defer b.mu.RUnlock()

	pc, ok := b.vpcPeeringConnections.Get(id)
	if !ok {
		return ""
	}

	return b.counterpartRegion(pc.RequesterRegion, pc.AccepterRegion)
}

func (b *InMemoryBackend) putVpcPeeringConnectionMirror(pc *VpcPeeringConnection) {
	b.mu.Lock("putVpcPeeringConnectionMirror")
	defer b.mu.Unlock()

	cp := *pc
	b.vpcPeeringConnections.Put(&cp)
}

func (b *InMemoryBackend) setVpcPeeringConnectionState(id, state string) {
	b.mu.Lock("setVpcPeeringConnectionState")
	defer b.mu.Unlock()

	if pc, ok := b.vpcPeeringConnections.Get(id); ok {
		pc.State = state
	}
}

// otherBackends returns every other region's backend.
func (b *InMemoryBackend) otherBackends() []*InMemoryBackend {
	if b.allBackends == nil {
		return nil
	}

	var out []*InMemoryBackend

	for _, bk := range b.allBackends() {
		if bk != b {
			out = append(out, bk)
		}
	}

	return out
}

// peerSnapshot returns a copy of snapshot id from another region when b does not hold it.
func (b *InMemoryBackend) peerSnapshot(id string) *Snapshot {
	b.mu.RLock("peerSnapshot")
	_, local := b.snapshots.Get(id)
	b.mu.RUnlock()

	if local {
		return nil
	}

	for _, bk := range b.otherBackends() {
		if snap := bk.snapshotCopy(id); snap != nil {
			return snap
		}
	}

	return nil
}

func (b *InMemoryBackend) snapshotCopy(id string) *Snapshot {
	b.mu.RLock("snapshotCopy")
	defer b.mu.RUnlock()

	snap, ok := b.snapshots.Get(id)
	if !ok {
		return nil
	}

	cp := *snap

	return &cp
}

// peerImage returns a copy of AMI id and its snapshots from another region when b does not hold it.
func (b *InMemoryBackend) peerImage(id string) (*AMIStub, map[string]*Snapshot) {
	b.mu.RLock("peerImage")
	local := b.lookupImageLocked(id) != nil
	b.mu.RUnlock()

	if local {
		return nil, nil
	}

	for _, bk := range b.otherBackends() {
		if img, snaps := bk.imageCopy(id); img != nil {
			return img, snaps
		}
	}

	return nil, nil
}

func (b *InMemoryBackend) imageCopy(id string) (*AMIStub, map[string]*Snapshot) {
	b.mu.RLock("imageCopy")
	defer b.mu.RUnlock()

	img, ok := b.images.Get(id)
	if !ok {
		return nil, nil
	}

	cp := *img
	snaps := make(map[string]*Snapshot, len(img.BlockDeviceMappings))

	for _, m := range img.BlockDeviceMappings {
		if snap, found := b.snapshots.Get(m.SnapshotID); found {
			sc := *snap
			snaps[m.SnapshotID] = &sc
		}
	}

	return &cp, snaps
}
