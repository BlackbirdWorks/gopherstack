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

func (b *InMemoryBackend) rebuildSecondaryIndexesLocked() {
	b.instanceIDsByVPC = make(map[string]map[string]struct{})
	b.eniIDsByInstance = make(map[string]map[string]struct{})
	b.eniIDByAttachment = make(map[string]string)

	for _, inst := range b.instances {
		b.indexInstanceLocked(inst)
	}

	for eniID, eni := range b.networkInterfaces {
		b.indexENILocked(eniID, eni)
	}
}
