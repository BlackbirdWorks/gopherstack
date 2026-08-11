package networkmanager

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// epochPtr returns a pointer to t's epoch-seconds value, or nil if t is
// zero -- matches every other service in this campaign's identical helper.
func epochPtr(t time.Time) *float64 {
	if t.IsZero() {
		return nil
	}

	e := awstime.Epoch(t)

	return &e
}

func tagsKV(t *tags.Tags) []tags.KV {
	if t == nil {
		return nil
	}

	return tags.MapToKV(t.Clone())
}

// ---- Global Network ----

func toGlobalNetworkWire(g *GlobalNetwork) *globalNetworkWire {
	if g == nil {
		return nil
	}

	return &globalNetworkWire{
		GlobalNetworkArn: g.GlobalNetworkArn,
		GlobalNetworkID:  g.GlobalNetworkID,
		CreatedAt:        epochPtr(g.CreatedAt),
		Description:      g.Description,
		State:            g.State,
		Tags:             tagsKV(g.Tags),
	}
}

// ---- Site ----

func toLocationWire(l *Location) *locationWire {
	if l == nil {
		return nil
	}

	return &locationWire{Address: l.Address, Latitude: l.Latitude, Longitude: l.Longitude}
}

func fromLocationWire(l *locationWire) *Location {
	if l == nil {
		return nil
	}

	return &Location{Address: l.Address, Latitude: l.Latitude, Longitude: l.Longitude}
}

func toSiteWire(s *Site) *siteWire {
	if s == nil {
		return nil
	}

	return &siteWire{
		SiteArn:         s.SiteArn,
		SiteID:          s.SiteID,
		GlobalNetworkID: s.GlobalNetworkID,
		CreatedAt:       epochPtr(s.CreatedAt),
		Description:     s.Description,
		Location:        toLocationWire(s.Location),
		State:           s.State,
		Tags:            tagsKV(s.Tags),
	}
}

// ---- Device ----

func toAWSLocationWire(l *AWSLocation) *awsLocationWire {
	if l == nil {
		return nil
	}

	return &awsLocationWire{SubnetArn: l.SubnetArn, Zone: l.Zone}
}

func fromAWSLocationWire(l *awsLocationWire) *AWSLocation {
	if l == nil {
		return nil
	}

	return &AWSLocation{SubnetArn: l.SubnetArn, Zone: l.Zone}
}

func toDeviceWire(d *Device) *deviceWire {
	if d == nil {
		return nil
	}

	return &deviceWire{
		DeviceArn:       d.DeviceArn,
		DeviceID:        d.DeviceID,
		GlobalNetworkID: d.GlobalNetworkID,
		CreatedAt:       epochPtr(d.CreatedAt),
		AWSLocation:     toAWSLocationWire(d.AWSLocation),
		Location:        toLocationWire(d.Location),
		Description:     d.Description,
		Model:           d.Model,
		SerialNumber:    d.SerialNumber,
		SiteID:          d.SiteID,
		Type:            d.Type,
		Vendor:          d.Vendor,
		State:           d.State,
		Tags:            tagsKV(d.Tags),
	}
}

// ---- Link ----

func toBandwidthWire(b *Bandwidth) *bandwidthWire {
	if b == nil {
		return nil
	}

	return &bandwidthWire{DownloadSpeed: b.DownloadSpeed, UploadSpeed: b.UploadSpeed}
}

func fromBandwidthWire(b *bandwidthWire) *Bandwidth {
	if b == nil {
		return nil
	}

	return &Bandwidth{DownloadSpeed: b.DownloadSpeed, UploadSpeed: b.UploadSpeed}
}

func toLinkWire(l *Link) *linkWire {
	if l == nil {
		return nil
	}

	return &linkWire{
		LinkArn:         l.LinkArn,
		LinkID:          l.LinkID,
		GlobalNetworkID: l.GlobalNetworkID,
		SiteID:          l.SiteID,
		CreatedAt:       epochPtr(l.CreatedAt),
		Bandwidth:       toBandwidthWire(l.Bandwidth),
		Description:     l.Description,
		Provider:        l.Provider,
		Type:            l.Type,
		State:           l.State,
		Tags:            tagsKV(l.Tags),
	}
}

// ---- Link Association ----

func toLinkAssociationWire(a *LinkAssociation) *linkAssociationWire {
	if a == nil {
		return nil
	}

	return &linkAssociationWire{
		DeviceID:             a.DeviceID,
		GlobalNetworkID:      a.GlobalNetworkID,
		LinkID:               a.LinkID,
		LinkAssociationState: a.LinkAssociationState,
	}
}

// ---- Connection ----

func toConnectionWire(c *Connection) *connectionWire {
	if c == nil {
		return nil
	}

	return &connectionWire{
		ConnectionArn:     c.ConnectionArn,
		ConnectionID:      c.ConnectionID,
		ConnectedDeviceID: c.ConnectedDeviceID,
		ConnectedLinkID:   c.ConnectedLinkID,
		DeviceID:          c.DeviceID,
		GlobalNetworkID:   c.GlobalNetworkID,
		LinkID:            c.LinkID,
		CreatedAt:         epochPtr(c.CreatedAt),
		Description:       c.Description,
		State:             c.State,
		Tags:              tagsKV(c.Tags),
	}
}

// ---- Association families ----

func toCustomerGatewayAssociationWire(a *CustomerGatewayAssociation) *customerGatewayAssociationWire {
	if a == nil {
		return nil
	}

	return &customerGatewayAssociationWire{
		CustomerGatewayArn: a.CustomerGatewayArn,
		DeviceID:           a.DeviceID,
		GlobalNetworkID:    a.GlobalNetworkID,
		LinkID:             a.LinkID,
		State:              a.State,
	}
}

func toTransitGatewayRegistrationWire(r *TransitGatewayRegistration) *transitGatewayRegistrationWire {
	if r == nil {
		return nil
	}

	w := &transitGatewayRegistrationWire{
		GlobalNetworkID:   r.GlobalNetworkID,
		TransitGatewayArn: r.TransitGatewayArn,
	}

	if r.State != nil {
		w.State = &transitGatewayRegistrationStateWire{Code: r.State.Code, Message: r.State.Message}
	}

	return w
}

func toTransitGatewayConnectPeerAssociationWire(
	a *TransitGatewayConnectPeerAssociation,
) *transitGatewayConnectPeerAssociationWire {
	if a == nil {
		return nil
	}

	return &transitGatewayConnectPeerAssociationWire{
		DeviceID:                     a.DeviceID,
		GlobalNetworkID:              a.GlobalNetworkID,
		LinkID:                       a.LinkID,
		State:                        a.State,
		TransitGatewayConnectPeerArn: a.TransitGatewayConnectPeerArn,
	}
}

func toConnectPeerAssociationWire(a *ConnectPeerAssociation) *connectPeerAssociationWire {
	if a == nil {
		return nil
	}

	return &connectPeerAssociationWire{
		ConnectPeerID:   a.ConnectPeerID,
		DeviceID:        a.DeviceID,
		GlobalNetworkID: a.GlobalNetworkID,
		LinkID:          a.LinkID,
		State:           a.State,
	}
}

// ---- Connect Peer (Cloud WAN) ----

func toConnectPeerConfigurationWire(c *ConnectPeerConfiguration) *connectPeerConfigurationWire {
	if c == nil {
		return nil
	}

	bgps := make([]connectPeerBgpConfigurationWire, len(c.BgpConfigurations))
	for i, bc := range c.BgpConfigurations {
		bgps[i] = connectPeerBgpConfigurationWire(bc)
	}

	return &connectPeerConfigurationWire{
		BgpConfigurations:  bgps,
		CoreNetworkAddress: c.CoreNetworkAddress,
		InsideCidrBlocks:   append([]string(nil), c.InsideCidrBlocks...),
		PeerAddress:        c.PeerAddress,
		Protocol:           c.Protocol,
	}
}

func toConnectPeerWire(c *ConnectPeer) *connectPeerWire {
	if c == nil {
		return nil
	}

	return &connectPeerWire{
		ConnectPeerID:       c.ConnectPeerID,
		ConnectAttachmentID: c.ConnectAttachmentID,
		CoreNetworkID:       c.CoreNetworkID,
		CreatedAt:           epochPtr(c.CreatedAt),
		Configuration:       toConnectPeerConfigurationWire(c.Configuration),
		EdgeLocation:        c.EdgeLocation,
		State:               c.State,
		SubnetArn:           c.SubnetArn,
		Tags:                tagsKV(c.Tags),
	}
}

func toConnectPeerSummaryWire(c *ConnectPeer) connectPeerSummaryWire {
	return connectPeerSummaryWire{
		ConnectPeerID:       c.ConnectPeerID,
		ConnectAttachmentID: c.ConnectAttachmentID,
		ConnectPeerState:    c.State,
		CoreNetworkID:       c.CoreNetworkID,
		CreatedAt:           epochPtr(c.CreatedAt),
		EdgeLocation:        c.EdgeLocation,
		SubnetArn:           c.SubnetArn,
		Tags:                tagsKV(c.Tags),
	}
}

// ---- Core Network ----

func toCoreNetworkWire(c *CoreNetwork) *coreNetworkWire {
	if c == nil {
		return nil
	}

	return &coreNetworkWire{
		CoreNetworkArn:  c.CoreNetworkArn,
		CoreNetworkID:   c.CoreNetworkID,
		GlobalNetworkID: c.GlobalNetworkID,
		CreatedAt:       epochPtr(c.CreatedAt),
		Description:     c.Description,
		State:           c.State,
		Tags:            tagsKV(c.Tags),
	}
}

func toCoreNetworkSummaryWire(c *CoreNetwork) coreNetworkSummaryWire {
	return coreNetworkSummaryWire{
		CoreNetworkArn:  c.CoreNetworkArn,
		CoreNetworkID:   c.CoreNetworkID,
		Description:     c.Description,
		GlobalNetworkID: c.GlobalNetworkID,
		OwnerAccountID:  "",
		State:           c.State,
	}
}

// ---- Core Network Policy lifecycle ----

func toCoreNetworkPolicyErrorsWire(errs []CoreNetworkPolicyError) []coreNetworkPolicyErrorWire {
	if errs == nil {
		return nil
	}

	out := make([]coreNetworkPolicyErrorWire, len(errs))
	for i, e := range errs {
		out[i] = coreNetworkPolicyErrorWire(e)
	}

	return out
}

func toCoreNetworkPolicyWire(v *CoreNetworkPolicyVersion, alias string) *coreNetworkPolicyWire {
	if v == nil {
		return nil
	}

	return &coreNetworkPolicyWire{
		Alias:           alias,
		ChangeSetState:  v.ChangeSetState,
		CoreNetworkID:   v.CoreNetworkID,
		CreatedAt:       epochPtr(v.CreatedAt),
		Description:     v.Description,
		PolicyDocument:  v.PolicyDocument,
		PolicyVersionID: v.PolicyVersionID,
	}
}

func toCoreNetworkPolicyVersionWire(v *CoreNetworkPolicyVersion, alias string) coreNetworkPolicyVersionWire {
	return coreNetworkPolicyVersionWire{
		Alias:           alias,
		ChangeSetState:  v.ChangeSetState,
		CoreNetworkID:   v.CoreNetworkID,
		CreatedAt:       epochPtr(v.CreatedAt),
		Description:     v.Description,
		PolicyVersionID: v.PolicyVersionID,
	}
}

// ---- Core Network Prefix List Association ----

func toPrefixListAssociationWire(a *CoreNetworkPrefixListAssociation) prefixListAssociationWire {
	return prefixListAssociationWire{
		CoreNetworkID:   a.CoreNetworkID,
		PrefixListAlias: a.PrefixListAlias,
		PrefixListArn:   a.PrefixListArn,
	}
}

// ---- Attachments ----

func toAttachmentErrorsWire(errs []AttachmentError) []attachmentErrorWire {
	if errs == nil {
		return nil
	}

	out := make([]attachmentErrorWire, len(errs))
	for i, e := range errs {
		out[i] = attachmentErrorWire(e)
	}

	return out
}

func toAttachmentWire(a *Attachment) *attachmentWire {
	if a == nil {
		return nil
	}

	return &attachmentWire{
		AttachmentID:               a.AttachmentID,
		AttachmentType:             a.AttachmentType,
		CoreNetworkArn:             a.CoreNetworkArn,
		CoreNetworkID:              a.CoreNetworkID,
		CreatedAt:                  epochPtr(a.CreatedAt),
		UpdatedAt:                  epochPtr(a.UpdatedAt),
		EdgeLocation:               a.EdgeLocation,
		EdgeLocations:              append([]string(nil), a.EdgeLocations...),
		NetworkFunctionGroupName:   a.NetworkFunctionGroupName,
		OwnerAccountID:             a.OwnerAccountID,
		ResourceArn:                a.ResourceArn,
		SegmentName:                a.SegmentName,
		State:                      a.State,
		AttachmentPolicyRuleNumber: a.AttachmentPolicyRuleNumber,
		LastModificationErrors:     toAttachmentErrorsWire(a.LastModificationErrors),
		Tags:                       tagsKV(a.Tags),
	}
}

func toVpcOptionsWire(o *VpcOptions) *vpcOptionsWire {
	if o == nil {
		return nil
	}

	return &vpcOptionsWire{
		ApplianceModeSupport:            o.ApplianceModeSupport,
		DNSSupport:                      o.DNSSupport,
		Ipv6Support:                     o.Ipv6Support,
		SecurityGroupReferencingSupport: o.SecurityGroupReferencingSupport,
	}
}

func fromVpcOptionsWire(o *vpcOptionsWire) *VpcOptions {
	if o == nil {
		return nil
	}

	return &VpcOptions{
		ApplianceModeSupport:            o.ApplianceModeSupport,
		DNSSupport:                      o.DNSSupport,
		Ipv6Support:                     o.Ipv6Support,
		SecurityGroupReferencingSupport: o.SecurityGroupReferencingSupport,
	}
}

func toVpcAttachmentWire(a *Attachment) *vpcAttachmentWire {
	if a == nil {
		return nil
	}

	return &vpcAttachmentWire{
		Attachment: toAttachmentWire(a),
		Options:    toVpcOptionsWire(a.VpcOptions),
		SubnetArns: append([]string(nil), a.SubnetArns...),
	}
}

func toConnectAttachmentWire(a *Attachment) *connectAttachmentWire {
	if a == nil {
		return nil
	}

	var opts *connectAttachmentOptionsWire
	if a.ConnectOptions != nil {
		opts = &connectAttachmentOptionsWire{Protocol: a.ConnectOptions.Protocol}
	}

	return &connectAttachmentWire{
		Attachment:            toAttachmentWire(a),
		Options:               opts,
		TransportAttachmentID: a.TransportAttachmentID,
	}
}

func toSiteToSiteVpnAttachmentWire(a *Attachment) *siteToSiteVpnAttachmentWire {
	if a == nil {
		return nil
	}

	return &siteToSiteVpnAttachmentWire{
		Attachment:       toAttachmentWire(a),
		VpnConnectionArn: a.VpnConnectionArn,
	}
}

func toDirectConnectGatewayAttachmentWire(a *Attachment) *directConnectGatewayAttachmentWire {
	if a == nil {
		return nil
	}

	return &directConnectGatewayAttachmentWire{
		Attachment:              toAttachmentWire(a),
		DirectConnectGatewayArn: a.DirectConnectGatewayArn,
	}
}

func toTransitGatewayRouteTableAttachmentWire(a *Attachment) *transitGatewayRouteTableAttachmentWire {
	if a == nil {
		return nil
	}

	return &transitGatewayRouteTableAttachmentWire{
		Attachment:                  toAttachmentWire(a),
		PeeringID:                   a.PeeringID,
		TransitGatewayRouteTableArn: a.TransitGatewayRouteTableArn,
	}
}

// ---- Peerings ----

func toPeeringErrorsWire(errs []PeeringError) []peeringErrorWire {
	if errs == nil {
		return nil
	}

	out := make([]peeringErrorWire, len(errs))
	for i, e := range errs {
		out[i] = peeringErrorWire(e)
	}

	return out
}

func toPeeringWire(p *Peering) *peeringWire {
	if p == nil {
		return nil
	}

	return &peeringWire{
		CoreNetworkArn:         p.CoreNetworkArn,
		CoreNetworkID:          p.CoreNetworkID,
		CreatedAt:              epochPtr(p.CreatedAt),
		EdgeLocation:           p.EdgeLocation,
		LastModificationErrors: toPeeringErrorsWire(p.LastModificationErrors),
		OwnerAccountID:         p.OwnerAccountID,
		PeeringID:              p.PeeringID,
		PeeringType:            p.PeeringType,
		ResourceArn:            p.ResourceArn,
		State:                  p.State,
		Tags:                   tagsKV(p.Tags),
	}
}

func toTransitGatewayPeeringWire(p *Peering) *transitGatewayPeeringWire {
	if p == nil {
		return nil
	}

	return &transitGatewayPeeringWire{
		Peering:                           toPeeringWire(p),
		TransitGatewayArn:                 p.TransitGatewayArn,
		TransitGatewayPeeringAttachmentID: p.TransitGatewayPeeringAttachmentID,
	}
}

// ---- Route Analysis ----

func toRouteAnalysisEndpointWire(e *RouteAnalysisEndpoint) *routeAnalysisEndpointWire {
	if e == nil {
		return nil
	}

	return &routeAnalysisEndpointWire{
		IPAddress:                   e.IPAddress,
		TransitGatewayArn:           e.TransitGatewayArn,
		TransitGatewayAttachmentArn: e.TransitGatewayAttachmentArn,
	}
}

func toRouteAnalysisPathWire(p *RouteAnalysisPath) *routeAnalysisPathWire {
	if p == nil {
		return nil
	}

	w := &routeAnalysisPathWire{}
	if p.CompletionStatus != nil {
		w.CompletionStatus = &routeAnalysisCompletionWire{
			ReasonCode: p.CompletionStatus.ReasonCode,
			ResultCode: p.CompletionStatus.ResultCode,
		}
	}

	for _, c := range p.Path {
		pc := pathComponentWire{DestinationCidrBlock: c.DestinationCidrBlock, Sequence: c.Sequence}
		if c.Resource != nil {
			pc.Resource = &networkResourceSummaryWire{
				Definition:           c.Resource.Definition,
				IsMiddlebox:          c.Resource.IsMiddlebox,
				NameTag:              c.Resource.NameTag,
				RegisteredGatewayArn: c.Resource.RegisteredGatewayArn,
				ResourceArn:          c.Resource.ResourceArn,
				ResourceType:         c.Resource.ResourceType,
			}
		}

		w.Path = append(w.Path, pc)
	}

	return w
}

func toRouteAnalysisWire(r *RouteAnalysis) *routeAnalysisWire {
	if r == nil {
		return nil
	}

	return &routeAnalysisWire{
		Destination:       toRouteAnalysisEndpointWire(r.Destination),
		Source:            toRouteAnalysisEndpointWire(r.Source),
		ForwardPath:       toRouteAnalysisPathWire(r.ForwardPath),
		ReturnPath:        toRouteAnalysisPathWire(r.ReturnPath),
		GlobalNetworkID:   r.GlobalNetworkID,
		OwnerAccountID:    r.OwnerAccountID,
		RouteAnalysisID:   r.RouteAnalysisID,
		Status:            r.Status,
		IncludeReturnPath: r.IncludeReturnPath,
	}
}

// ---- Core Network Policy change set / change events ----

func toCoreNetworkChangeValuesWire(v *CoreNetworkChangeValues) *coreNetworkChangeValuesWire {
	if v == nil {
		return nil
	}

	return &coreNetworkChangeValuesWire{
		SegmentName: v.SegmentName, NetworkFunctionGroupName: v.NetworkFunctionGroupName,
	}
}

func toCoreNetworkChangesWire(changes []CoreNetworkChange) []coreNetworkChangeWire {
	out := make([]coreNetworkChangeWire, 0, len(changes))

	for _, c := range changes {
		out = append(out, coreNetworkChangeWire{
			Action: c.Action, Identifier: c.Identifier, IdentifierPath: c.IdentifierPath, Type: c.Type,
			NewValues:      toCoreNetworkChangeValuesWire(c.NewValues),
			PreviousValues: toCoreNetworkChangeValuesWire(c.PreviousValues),
		})
	}

	return out
}

func toCoreNetworkChangeEventsWire(events []CoreNetworkChangeEvent) []coreNetworkChangeEventWire {
	out := make([]coreNetworkChangeEventWire, 0, len(events))

	for _, e := range events {
		out = append(out, coreNetworkChangeEventWire{
			Action: e.Action, IdentifierPath: e.IdentifierPath, Status: e.Status, Type: e.Type,
			Values: toCoreNetworkChangeValuesWire(e.Values), EventTime: epochPtr(e.EventTime),
		})
	}

	return out
}

// ---- Organizations integration ----

func toOrganizationStatusWire(o *organizationStatus) *organizationStatusWire {
	if o == nil {
		return nil
	}

	accts := make([]accountStatusWire, len(o.AccountStatusList))
	for i, a := range o.AccountStatusList {
		accts[i] = accountStatusWire(a)
	}

	return &organizationStatusWire{
		AccountStatusList:                  accts,
		OrganizationAwsServiceAccessStatus: o.OrganizationAwsServiceAccessStatus,
		OrganizationID:                     o.OrganizationID,
		SLRDeploymentStatus:                o.SLRDeploymentStatus,
	}
}
