package ec2

func (b *InMemoryBackend) indexInstanceLocked(inst *Instance) {
	if inst == nil || inst.VPCID == "" {
		return
	}

	ids, ok := b.instanceIDsByVPC[inst.VPCID]
	if !ok {
		ids = make(map[string]struct{})
		b.instanceIDsByVPC[inst.VPCID] = ids
	}

	ids[inst.ID] = struct{}{}
}

func (b *InMemoryBackend) deindexInstanceLocked(inst *Instance) {
	if inst == nil || inst.VPCID == "" {
		return
	}

	ids, ok := b.instanceIDsByVPC[inst.VPCID]
	if !ok {
		return
	}

	delete(ids, inst.ID)
	if len(ids) == 0 {
		delete(b.instanceIDsByVPC, inst.VPCID)
	}
}

func (b *InMemoryBackend) indexENILocked(eniID string, eni *NetworkInterface) {
	if eni == nil || eni.InstanceID == "" {
		return
	}

	eniIDs, ok := b.eniIDsByInstance[eni.InstanceID]
	if !ok {
		eniIDs = make(map[string]struct{})
		b.eniIDsByInstance[eni.InstanceID] = eniIDs
	}

	eniIDs[eniID] = struct{}{}
	if eni.AttachmentID != "" {
		b.eniIDByAttachment[eni.AttachmentID] = eniID
	}
}

func (b *InMemoryBackend) deindexENILocked(eniID string, eni *NetworkInterface) {
	if eni == nil {
		return
	}

	if eni.InstanceID != "" {
		eniIDs, ok := b.eniIDsByInstance[eni.InstanceID]
		if ok {
			delete(eniIDs, eniID)
			if len(eniIDs) == 0 {
				delete(b.eniIDsByInstance, eni.InstanceID)
			}
		}
	}

	if eni.AttachmentID != "" {
		delete(b.eniIDByAttachment, eni.AttachmentID)
	}
}

// primaryNetworkInterfaceLocked returns the primary (deviceIndex 0) network
// interface attached to instanceID, falling back to any attached interface if
// none is explicitly device-index 0. Returns nil if the instance has no ENIs.
// Must be called with b.mu held.
func (b *InMemoryBackend) primaryNetworkInterfaceLocked(instanceID string) *NetworkInterface {
	var fallback *NetworkInterface

	for eniID := range b.eniIDsByInstance[instanceID] {
		eni, ok := b.networkInterfaces[eniID]
		if !ok {
			continue
		}

		if eni.DeviceIndex == 0 {
			return eni
		}

		if fallback == nil {
			fallback = eni
		}
	}

	return fallback
}

func (b *InMemoryBackend) indexSubnetLocked(subnetID, vpcID string) {
	if subnetID == "" || vpcID == "" {
		return
	}

	ids, ok := b.subnetIDsByVPC[vpcID]
	if !ok {
		ids = make(map[string]struct{})
		b.subnetIDsByVPC[vpcID] = ids
	}

	ids[subnetID] = struct{}{}
}

func (b *InMemoryBackend) deindexSubnetLocked(subnetID, vpcID string) {
	ids, ok := b.subnetIDsByVPC[vpcID]
	if !ok {
		return
	}

	delete(ids, subnetID)
	if len(ids) == 0 {
		delete(b.subnetIDsByVPC, vpcID)
	}
}

func (b *InMemoryBackend) indexRouteTableLocked(rtID, vpcID string) {
	if rtID == "" || vpcID == "" {
		return
	}

	ids, ok := b.routeTableIDsByVPC[vpcID]
	if !ok {
		ids = make(map[string]struct{})
		b.routeTableIDsByVPC[vpcID] = ids
	}

	ids[rtID] = struct{}{}
}

func (b *InMemoryBackend) deindexRouteTableLocked(rtID, vpcID string) {
	ids, ok := b.routeTableIDsByVPC[vpcID]
	if !ok {
		return
	}

	delete(ids, rtID)
	if len(ids) == 0 {
		delete(b.routeTableIDsByVPC, vpcID)
	}
}

func (b *InMemoryBackend) indexSGLocked(sgID, vpcID string) {
	if sgID == "" || vpcID == "" {
		return
	}

	ids, ok := b.sgIDsByVPC[vpcID]
	if !ok {
		ids = make(map[string]struct{})
		b.sgIDsByVPC[vpcID] = ids
	}

	ids[sgID] = struct{}{}
}

func (b *InMemoryBackend) deindexSGLocked(sgID, vpcID string) {
	ids, ok := b.sgIDsByVPC[vpcID]
	if !ok {
		return
	}

	delete(ids, sgID)
	if len(ids) == 0 {
		delete(b.sgIDsByVPC, vpcID)
	}
}

// indexENIByVPCLocked records eniID under its VPC. Unlike indexENILocked (keyed
// by the ENI's instance, which changes on attach/detach), the ENI's VPC is
// immutable, so this is maintained only at ENI create/delete sites — never on
// attach/detach — to keep the DeleteVpc cascade correct.
func (b *InMemoryBackend) indexENIByVPCLocked(eniID string, eni *NetworkInterface) {
	if eni == nil || eni.VPCID == "" {
		return
	}

	ids, ok := b.eniIDsByVPC[eni.VPCID]
	if !ok {
		ids = make(map[string]struct{})
		b.eniIDsByVPC[eni.VPCID] = ids
	}

	ids[eniID] = struct{}{}
}

func (b *InMemoryBackend) deindexENIByVPCLocked(eniID string, eni *NetworkInterface) {
	if eni == nil || eni.VPCID == "" {
		return
	}

	ids, ok := b.eniIDsByVPC[eni.VPCID]
	if !ok {
		return
	}

	delete(ids, eniID)
	if len(ids) == 0 {
		delete(b.eniIDsByVPC, eni.VPCID)
	}
}

// indexNatGatewayLocked records a NAT gateway under its VPC so DeleteVpc can
// find it without scanning the whole natGateways map.
func (b *InMemoryBackend) indexNatGatewayLocked(ngw *NatGateway) {
	if ngw == nil || ngw.VPCID == "" {
		return
	}

	ids, ok := b.natGatewayIDsByVPC[ngw.VPCID]
	if !ok {
		ids = make(map[string]struct{})
		b.natGatewayIDsByVPC[ngw.VPCID] = ids
	}

	ids[ngw.ID] = struct{}{}
}

func (b *InMemoryBackend) deindexNatGatewayLocked(ngw *NatGateway) {
	if ngw == nil || ngw.VPCID == "" {
		return
	}

	ids, ok := b.natGatewayIDsByVPC[ngw.VPCID]
	if !ok {
		return
	}

	delete(ids, ngw.ID)
	if len(ids) == 0 {
		delete(b.natGatewayIDsByVPC, ngw.VPCID)
	}
}

func initSecondaryIndexMaps(b *InMemoryBackend) {
	b.instanceIDsByVPC = make(map[string]map[string]struct{})
	b.eniIDsByInstance = make(map[string]map[string]struct{})
	b.eniIDByAttachment = make(map[string]string)
	b.subnetIDsByVPC = make(map[string]map[string]struct{})
	b.routeTableIDsByVPC = make(map[string]map[string]struct{})
	b.sgIDsByVPC = make(map[string]map[string]struct{})
	b.natGatewayIDsByVPC = make(map[string]map[string]struct{})
	b.eniIDsByVPC = make(map[string]map[string]struct{})
}

func (b *InMemoryBackend) rebuildSecondaryIndexesLocked() {
	initSecondaryIndexMaps(b)

	for _, inst := range b.instances {
		b.indexInstanceLocked(inst)
	}

	for eniID, eni := range b.networkInterfaces {
		b.indexENILocked(eniID, eni)
		b.indexENIByVPCLocked(eniID, eni)
	}

	for _, ngw := range b.natGateways {
		b.indexNatGatewayLocked(ngw)
	}

	for id, subnet := range b.subnets {
		b.indexSubnetLocked(id, subnet.VPCID)
	}

	for id, rt := range b.routeTables {
		b.indexRouteTableLocked(id, rt.VPCID)
	}

	for id, sg := range b.securityGroups {
		b.indexSGLocked(id, sg.VPCID)
	}
}
