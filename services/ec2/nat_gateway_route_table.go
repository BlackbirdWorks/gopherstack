package ec2

// createRegionalNatRouteTableLocked gives a regional NAT gateway the route table AWS
// creates for it, with a default route to the VPC's internet gateway when one is attached.
func (b *InMemoryBackend) createRegionalNatRouteTableLocked(ngw *NatGateway) {
	id := newRouteTableID()
	rt := &RouteTable{ID: id, VPCID: ngw.VPCID, Routes: []Route{}, Associations: []RouteAssociation{}}

	if igwID := b.attachedInternetGatewayLocked(ngw.VPCID); igwID != "" {
		rt.Routes = append(rt.Routes, Route{DestinationCIDR: cidrAllIPv4, GatewayID: igwID, State: stateActive})
	}

	b.routeTables.Put(rt)
	b.indexRouteTableLocked(id, ngw.VPCID)
	ngw.RouteTableID = id
}

func (b *InMemoryBackend) deleteRegionalNatRouteTableLocked(ngw *NatGateway) {
	if ngw.RouteTableID == "" {
		return
	}

	if rt, ok := b.routeTables.Get(ngw.RouteTableID); ok {
		b.deindexRouteTableLocked(rt.ID, rt.VPCID)
		b.routeTables.Delete(rt.ID)
		delete(b.tags, rt.ID)
	}
}

func (b *InMemoryBackend) attachedInternetGatewayLocked(vpcID string) string {
	for _, igw := range b.internetGateways.All() {
		for _, att := range igw.Attachments {
			if att.VPCID == vpcID {
				return igw.ID
			}
		}
	}

	return ""
}

// syncRegionalNatRoutesLocked adds (igwID != "") or removes the default internet route on the
// route tables of the VPC's regional NAT gateways.
func (b *InMemoryBackend) syncRegionalNatRoutesLocked(vpcID, igwID string, attach bool) {
	for id := range b.natGatewayIDsByVPC[vpcID] {
		ngw, ok := b.natGateways.Get(id)
		if !ok || ngw.RouteTableID == "" {
			continue
		}

		rt, ok := b.routeTables.Get(ngw.RouteTableID)
		if !ok {
			continue
		}

		kept := rt.Routes[:0]
		for _, r := range rt.Routes {
			if r.GatewayID != igwID {
				kept = append(kept, r)
			}
		}

		rt.Routes = kept
		if attach {
			rt.Routes = append(rt.Routes, Route{DestinationCIDR: cidrAllIPv4, GatewayID: igwID, State: stateActive})
		}
	}
}
