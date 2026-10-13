package cloudformation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	networkmanagerbackend "github.com/blackbirdworks/gopherstack/services/networkmanager"
)

const (
	resTypeNMGlobalNetwork                  = "AWS::NetworkManager::GlobalNetwork"
	resTypeNMSite                           = "AWS::NetworkManager::Site"
	resTypeNMDevice                         = "AWS::NetworkManager::Device"
	resTypeNMLink                           = "AWS::NetworkManager::Link"
	resTypeNMLinkAssociation                = "AWS::NetworkManager::LinkAssociation"
	resTypeNMCustomerGatewayAssociation     = "AWS::NetworkManager::CustomerGatewayAssociation"
	resTypeNMTransitGatewayRegistration     = "AWS::NetworkManager::TransitGatewayRegistration"
	resTypeNMCoreNetwork                    = "AWS::NetworkManager::CoreNetwork"
	resTypeNMVpcAttachment                  = "AWS::NetworkManager::VpcAttachment"
	resTypeNMSiteToSiteVpnAttachment        = "AWS::NetworkManager::SiteToSiteVpnAttachment"
	resTypeNMConnectAttachment              = "AWS::NetworkManager::ConnectAttachment"
	resTypeNMConnectPeer                    = "AWS::NetworkManager::ConnectPeer"
	resTypeNMTransitGatewayPeering          = "AWS::NetworkManager::TransitGatewayPeering"
	resTypeNMTransitGatewayRouteTableAttach = "AWS::NetworkManager::TransitGatewayRouteTableAttachment"
	resTypeNMDirectConnectGatewayAttachment = "AWS::NetworkManager::DirectConnectGatewayAttachment"
	nmStateAvailable                        = "AVAILABLE"
	nmStateDeleted                          = "DELETED"
	nmCompositeSep                          = "|"
	nmAttrCreatedAt                         = "CreatedAt"
	nmAttrCoreNetworkArn                    = "CoreNetworkArn"
	nmAttrEdgeLocation                      = "EdgeLocation"
	nmAttrState                             = "State"
	nmCompositeParts                        = 3
	propGlobalNetworkID                     = "GlobalNetworkId"
	propCoreNetworkID                       = "CoreNetworkId"
	propDeviceID                            = "DeviceId"
	propLinkID                              = "LinkId"
)

// nmResource provisions one AWS::NetworkManager::* type against the Network Manager backend.
type nmResource struct {
	create func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (string, error)
	// poll reports the resource's state and whether it still exists.
	poll   func(bk *networkmanagerbackend.InMemoryBackend, id string) (string, bool)
	remove func(bk *networkmanagerbackend.InMemoryBackend, id string) error
	// attrs returns Fn::GetAtt values once the resource is available.
	attrs func(bk *networkmanagerbackend.InMemoryBackend, id string) map[string]string
}

type nmCreateCtx struct {
	props       map[string]any
	params      map[string]string
	physicalIDs map[string]string
}

func (c nmCreateCtx) str(key string) string { return strProp(c.props, key, c.params, c.physicalIDs) }

func (c nmCreateCtx) id(key string) string { return nmIDOf(c.str(key)) }

func (c nmCreateCtx) tags() map[string]string { return tagListProp(c.props, c.params, c.physicalIDs) }

func (c nmCreateCtx) list(key string) []string {
	return strSliceProp(c.props[key], c.params, c.physicalIDs)
}

func (c nmCreateCtx) sub(key string) nmCreateCtx {
	m, _ := c.props[key].(map[string]any)

	return nmCreateCtx{props: m, params: c.params, physicalIDs: c.physicalIDs}
}

func (c nmCreateCtx) flag(key string) bool {
	switch v := c.props[key].(type) {
	case bool:
		return v
	case string:
		return v == boolTrue
	}

	return false
}

// nmIDOf reduces an ID, ARN or composite physical ID to its final identifier segment.
func nmIDOf(s string) string {
	if i := strings.LastIndexAny(s, "/|"); i >= 0 {
		return s[i+1:]
	}

	return s
}

func nmGlobalNetworkOf(arn string) string {
	parts := strings.Split(arn, "/")
	if len(parts) >= nmCompositeParts {
		return parts[len(parts)-2]
	}

	return ""
}

func nmLocation(c nmCreateCtx) *networkmanagerbackend.Location {
	if _, ok := c.props["Location"].(map[string]any); !ok {
		return nil
	}

	l := c.sub("Location")

	return &networkmanagerbackend.Location{
		Address: l.str("Address"), Latitude: l.str("Latitude"), Longitude: l.str("Longitude"),
	}
}

func nmFirst[T any](page []T) (T, bool) {
	var zero T
	if len(page) == 0 {
		return zero, false
	}

	return page[0], true
}

func nmResourceFor(resourceType string) (nmResource, bool) {
	if res, ok := nmCoreResourceFor(resourceType); ok {
		return res, true
	}

	switch resourceType {
	case resTypeNMCoreNetwork:
		return nmCoreNetworkResource(), true
	case resTypeNMVpcAttachment:
		return nmVpcAttachmentResource(), true
	case resTypeNMSiteToSiteVpnAttachment:
		return nmVpnAttachmentResource(), true
	case resTypeNMConnectAttachment:
		return nmConnectAttachmentResource(), true
	case resTypeNMConnectPeer:
		return nmConnectPeerResource(), true
	case resTypeNMTransitGatewayPeering:
		return nmTransitGatewayPeeringResource(), true
	case resTypeNMTransitGatewayRouteTableAttach:
		return nmTransitGatewayRouteTableAttachmentResource(), true
	case resTypeNMDirectConnectGatewayAttachment:
		return nmDirectConnectGatewayAttachmentResource(), true
	default:
		return nmResource{}, false
	}
}

func nmCoreResourceFor(resourceType string) (nmResource, bool) {
	switch resourceType {
	case resTypeNMGlobalNetwork:
		return nmGlobalNetworkResource(), true
	case resTypeNMSite:
		return nmSiteResource(), true
	case resTypeNMDevice:
		return nmDeviceResource(), true
	case resTypeNMLink:
		return nmLinkResource(), true
	case resTypeNMLinkAssociation:
		return nmLinkAssociationResource(), true
	case resTypeNMCustomerGatewayAssociation:
		return nmCustomerGatewayAssociationResource(), true
	case resTypeNMTransitGatewayRegistration:
		return nmTransitGatewayRegistrationResource(), true
	default:
		return nmResource{}, false
	}
}

func (rc *ResourceCreator) createNetworkManagerResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	res, ok := nmResourceFor(resourceType)
	if !ok {
		return "", false, nil
	}

	if rc.backends.NetworkManager == nil {
		return logicalID + "-stub", true, nil
	}

	bk := rc.backends.NetworkManager.Backend

	id, err := res.create(bk, nmCreateCtx{props: props, params: params, physicalIDs: physicalIDs})
	if err != nil {
		return "", true, fmt.Errorf("create %s %s: %w", resourceType, logicalID, err)
	}

	err = awaitResource(resourceType+" "+id, func() (bool, error) {
		state, found := res.poll(bk, id)
		if !found {
			return false, fmt.Errorf("%w: %s", errResourceCreateFailed, id)
		}

		return state == nmStateAvailable, nil
	})
	if err != nil {
		return "", true, err
	}

	for k, v := range res.attrs(bk, id) {
		if v != "" {
			physicalIDs[logicalID+"/"+k] = v
		}
	}

	return id, true, nil
}

func (rc *ResourceCreator) deleteNetworkManagerResource(resourceType, physicalID string) (bool, error) {
	res, ok := nmResourceFor(resourceType)
	if !ok {
		return false, nil
	}

	if rc.backends.NetworkManager == nil {
		return true, nil
	}

	bk := rc.backends.NetworkManager.Backend

	if _, found := res.poll(bk, physicalID); !found {
		return true, nil
	}

	if err := res.remove(bk, physicalID); err != nil {
		return true, err
	}

	return true, awaitResource(resourceType+" "+physicalID, func() (bool, error) {
		state, found := res.poll(bk, physicalID)

		return !found || state == nmStateDeleted, nil
	})
}

func nmGlobalNetworkResource() nmResource {
	return nmResource{
		create: func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (string, error) {
			return bk.CreateGlobalNetwork(c.str("Description"), c.tags()).GlobalNetworkID, nil
		},
		poll: func(bk *networkmanagerbackend.InMemoryBackend, id string) (string, bool) {
			p, err := bk.DescribeGlobalNetworks([]string{id}, "", 0)
			g, ok := nmFirst(p.Data)
			if err != nil || !ok {
				return "", false
			}

			return g.State, true
		},
		remove: func(bk *networkmanagerbackend.InMemoryBackend, id string) error {
			_, err := bk.DeleteGlobalNetwork(id)

			return err
		},
		attrs: func(bk *networkmanagerbackend.InMemoryBackend, id string) map[string]string {
			p, _ := bk.DescribeGlobalNetworks([]string{id}, "", 0)
			g, ok := nmFirst(p.Data)
			if !ok {
				return nil
			}

			return map[string]string{"Id": g.GlobalNetworkID, "Arn": g.GlobalNetworkArn}
		},
	}
}

func nmSiteResource() nmResource {
	get := func(bk *networkmanagerbackend.InMemoryBackend, arn string) (*networkmanagerbackend.Site, bool) {
		p, err := bk.GetSites(nmGlobalNetworkOf(arn), []string{nmIDOf(arn)}, "", 0)
		s, ok := nmFirst(p.Data)

		return s, err == nil && ok
	}

	return nmResource{
		create: func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (string, error) {
			s, err := bk.CreateSite(c.id(propGlobalNetworkID), c.str("Description"), nmLocation(c), c.tags())
			if err != nil {
				return "", err
			}

			return s.SiteArn, nil
		},
		poll: func(bk *networkmanagerbackend.InMemoryBackend, arn string) (string, bool) {
			s, ok := get(bk, arn)
			if !ok {
				return "", false
			}

			return s.State, true
		},
		remove: func(bk *networkmanagerbackend.InMemoryBackend, arn string) error {
			_, err := bk.DeleteSite(nmGlobalNetworkOf(arn), nmIDOf(arn))

			return err
		},
		attrs: func(bk *networkmanagerbackend.InMemoryBackend, arn string) map[string]string {
			s, ok := get(bk, arn)
			if !ok {
				return nil
			}

			return map[string]string{
				"SiteArn": s.SiteArn, "SiteId": s.SiteID, nmAttrState: s.State,
				nmAttrCreatedAt: s.CreatedAt.Format(time.RFC3339),
			}
		},
	}
}

func nmDeviceResource() nmResource {
	get := func(bk *networkmanagerbackend.InMemoryBackend, arn string) (*networkmanagerbackend.Device, bool) {
		p, err := bk.GetDevices(nmGlobalNetworkOf(arn), []string{nmIDOf(arn)}, "", "", 0)
		d, ok := nmFirst(p.Data)

		return d, err == nil && ok
	}

	return nmResource{
		create: func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (string, error) {
			var awsLoc *networkmanagerbackend.AWSLocation
			if _, ok := c.props["AWSLocation"].(map[string]any); ok {
				l := c.sub("AWSLocation")
				awsLoc = &networkmanagerbackend.AWSLocation{SubnetArn: l.str("SubnetArn"), Zone: l.str("Zone")}
			}

			d, err := bk.CreateDevice(
				c.id(propGlobalNetworkID), awsLoc, nmLocation(c), c.str("Description"), c.str("Model"),
				c.str("SerialNumber"), c.id("SiteId"), c.str("Type"), c.str("Vendor"), c.tags(),
			)
			if err != nil {
				return "", err
			}

			return d.DeviceArn, nil
		},
		poll: func(bk *networkmanagerbackend.InMemoryBackend, arn string) (string, bool) {
			d, ok := get(bk, arn)
			if !ok {
				return "", false
			}

			return d.State, true
		},
		remove: func(bk *networkmanagerbackend.InMemoryBackend, arn string) error {
			_, err := bk.DeleteDevice(nmGlobalNetworkOf(arn), nmIDOf(arn))

			return err
		},
		attrs: func(bk *networkmanagerbackend.InMemoryBackend, arn string) map[string]string {
			d, ok := get(bk, arn)
			if !ok {
				return nil
			}

			return map[string]string{
				"DeviceArn": d.DeviceArn, "DeviceId": d.DeviceID, nmAttrState: d.State,
				nmAttrCreatedAt: d.CreatedAt.Format(time.RFC3339),
			}
		},
	}
}

func nmLinkResource() nmResource {
	get := func(bk *networkmanagerbackend.InMemoryBackend, arn string) (*networkmanagerbackend.Link, bool) {
		p, err := bk.GetLinks(nmGlobalNetworkOf(arn), []string{nmIDOf(arn)}, "", "", "", "", 0)
		l, ok := nmFirst(p.Data)

		return l, err == nil && ok
	}

	return nmResource{
		create: func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (string, error) {
			bw := c.sub("Bandwidth")
			bandwidth := &networkmanagerbackend.Bandwidth{
				DownloadSpeed: int32Prop(bw.props, "DownloadSpeed", c.params, c.physicalIDs),
				UploadSpeed:   int32Prop(bw.props, "UploadSpeed", c.params, c.physicalIDs),
			}

			l, err := bk.CreateLink(
				c.id(propGlobalNetworkID), c.id("SiteId"), bandwidth,
				c.str("Description"), c.str("Provider"), c.str("Type"), c.tags(),
			)
			if err != nil {
				return "", err
			}

			return l.LinkArn, nil
		},
		poll: func(bk *networkmanagerbackend.InMemoryBackend, arn string) (string, bool) {
			l, ok := get(bk, arn)
			if !ok {
				return "", false
			}

			return l.State, true
		},
		remove: func(bk *networkmanagerbackend.InMemoryBackend, arn string) error {
			_, err := bk.DeleteLink(nmGlobalNetworkOf(arn), nmIDOf(arn))

			return err
		},
		attrs: func(bk *networkmanagerbackend.InMemoryBackend, arn string) map[string]string {
			l, ok := get(bk, arn)
			if !ok {
				return nil
			}

			return map[string]string{
				"LinkArn": l.LinkArn, "LinkId": l.LinkID, nmAttrState: l.State,
				nmAttrCreatedAt: l.CreatedAt.Format(time.RFC3339),
			}
		},
	}
}

func nmNoAttrs(*networkmanagerbackend.InMemoryBackend, string) map[string]string { return nil }

func nmLinkAssociationResource() nmResource {
	parts := func(id string) (string, string, string) {
		p := strings.SplitN(id, nmCompositeSep, nmCompositeParts)
		for len(p) < nmCompositeParts {
			p = append(p, "")
		}

		return p[0], p[1], p[2]
	}

	return nmResource{
		create: func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (string, error) {
			gn, dev, link := c.id(propGlobalNetworkID), c.id(propDeviceID), c.id(propLinkID)
			if _, err := bk.AssociateLink(gn, dev, link); err != nil {
				return "", err
			}

			return strings.Join([]string{gn, dev, link}, nmCompositeSep), nil
		},
		poll: func(bk *networkmanagerbackend.InMemoryBackend, id string) (string, bool) {
			gn, dev, link := parts(id)
			p, err := bk.GetLinkAssociations(gn, dev, link, "", 0)
			a, ok := nmFirst(p.Data)
			if err != nil || !ok {
				return "", false
			}

			return a.LinkAssociationState, true
		},
		remove: func(bk *networkmanagerbackend.InMemoryBackend, id string) error {
			gn, dev, link := parts(id)
			_, err := bk.DisassociateLink(gn, dev, link)

			return err
		},
		attrs: nmNoAttrs,
	}
}

func nmCustomerGatewayAssociationResource() nmResource {
	parts := func(id string) (string, string) {
		gn, arn, _ := strings.Cut(id, nmCompositeSep)

		return gn, arn
	}

	return nmResource{
		create: func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (string, error) {
			gn, arn := c.id(propGlobalNetworkID), c.str("CustomerGatewayArn")
			if _, err := bk.AssociateCustomerGateway(gn, arn, c.id(propDeviceID), c.id(propLinkID)); err != nil {
				return "", err
			}

			return gn + nmCompositeSep + arn, nil
		},
		poll: func(bk *networkmanagerbackend.InMemoryBackend, id string) (string, bool) {
			gn, arn := parts(id)
			p, err := bk.GetCustomerGatewayAssociations(gn, []string{arn}, "", 0)
			a, ok := nmFirst(p.Data)
			if err != nil || !ok {
				return "", false
			}

			return a.State, true
		},
		remove: func(bk *networkmanagerbackend.InMemoryBackend, id string) error {
			gn, arn := parts(id)
			_, err := bk.DisassociateCustomerGateway(gn, arn)

			return err
		},
		attrs: nmNoAttrs,
	}
}

func nmTransitGatewayRegistrationResource() nmResource {
	parts := func(id string) (string, string) {
		gn, arn, _ := strings.Cut(id, nmCompositeSep)

		return gn, arn
	}

	return nmResource{
		create: func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (string, error) {
			gn, arn := c.id(propGlobalNetworkID), c.str("TransitGatewayArn")
			if _, err := bk.RegisterTransitGateway(gn, arn); err != nil {
				return "", err
			}

			return gn + nmCompositeSep + arn, nil
		},
		poll: func(bk *networkmanagerbackend.InMemoryBackend, id string) (string, bool) {
			gn, arn := parts(id)
			p, err := bk.GetTransitGatewayRegistrations(gn, []string{arn}, "", 0)
			r, ok := nmFirst(p.Data)
			if err != nil || !ok || r.State == nil {
				return "", false
			}

			return r.State.Code, true
		},
		remove: func(bk *networkmanagerbackend.InMemoryBackend, id string) error {
			gn, arn := parts(id)
			_, err := bk.DeregisterTransitGateway(gn, arn)

			return err
		},
		attrs: nmNoAttrs,
	}
}

func nmCoreNetworkResource() nmResource {
	return nmResource{
		create: func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (string, error) {
			policy := c.str("PolicyDocument")
			if doc, ok := c.props["PolicyDocument"].(map[string]any); ok {
				raw, err := json.Marshal(resolveDeep(doc, c.params, c.physicalIDs))
				if err != nil {
					return "", err
				}

				policy = string(raw)
			}

			cn, err := bk.CreateCoreNetwork(c.id(propGlobalNetworkID), c.str("Description"), policy, c.tags())
			if err != nil {
				return "", err
			}

			return cn.CoreNetworkID, nil
		},
		poll: func(bk *networkmanagerbackend.InMemoryBackend, id string) (string, bool) {
			cn, err := bk.GetCoreNetwork(id)
			if err != nil {
				return "", false
			}

			return cn.State, true
		},
		remove: func(bk *networkmanagerbackend.InMemoryBackend, id string) error {
			_, err := bk.DeleteCoreNetwork(id)

			return err
		},
		attrs: func(bk *networkmanagerbackend.InMemoryBackend, id string) map[string]string {
			cn, err := bk.GetCoreNetwork(id)
			if err != nil {
				return nil
			}

			return map[string]string{
				propCoreNetworkID: cn.CoreNetworkID, nmAttrCoreNetworkArn: cn.CoreNetworkArn, nmAttrState: cn.State,
				nmAttrCreatedAt: cn.CreatedAt.Format(time.RFC3339),
			}
		},
	}
}

// nmAttachmentResource builds an attachment type; the attachment policy is assumed to auto-accept.
func nmAttachmentResource(
	create func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (*networkmanagerbackend.Attachment, error),
	get func(bk *networkmanagerbackend.InMemoryBackend, id string) (*networkmanagerbackend.Attachment, error),
) nmResource {
	return nmResource{
		create: func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (string, error) {
			a, err := create(bk, c)
			if err != nil {
				return "", err
			}

			if _, err = bk.AcceptAttachment(a.AttachmentID); err != nil {
				return "", err
			}

			return a.AttachmentID, nil
		},
		poll: func(bk *networkmanagerbackend.InMemoryBackend, id string) (string, bool) {
			a, err := get(bk, id)
			if err != nil {
				return "", false
			}

			return a.State, true
		},
		remove: func(bk *networkmanagerbackend.InMemoryBackend, id string) error {
			_, err := bk.DeleteAttachment(id)

			return err
		},
		attrs: func(bk *networkmanagerbackend.InMemoryBackend, id string) map[string]string {
			a, err := get(bk, id)
			if err != nil {
				return nil
			}

			return map[string]string{
				"AttachmentId": a.AttachmentID, "AttachmentType": a.AttachmentType,
				nmAttrCoreNetworkArn: a.CoreNetworkArn, propCoreNetworkID: a.CoreNetworkID,
				nmAttrEdgeLocation: a.EdgeLocation, "OwnerAccountId": a.OwnerAccountID,
				"ResourceArn": a.ResourceArn, "SegmentName": a.SegmentName, nmAttrState: a.State,
				nmAttrCreatedAt: a.CreatedAt.Format(time.RFC3339), "UpdatedAt": a.UpdatedAt.Format(time.RFC3339),
			}
		},
	}
}

func nmVpcAttachmentResource() nmResource {
	return nmAttachmentResource(
		func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (*networkmanagerbackend.Attachment, error) {
			o := c.sub("Options")
			opts := &networkmanagerbackend.VpcOptions{
				ApplianceModeSupport:            o.flag("ApplianceModeSupport"),
				DNSSupport:                      o.flag("DnsSupport"),
				Ipv6Support:                     o.flag("Ipv6Support"),
				SecurityGroupReferencingSupport: o.flag("SecurityGroupReferencingSupport"),
			}

			return bk.CreateVpcAttachment(
				c.id(
					propCoreNetworkID,
				),
				c.str("VpcArn"),
				c.list("SubnetArns"),
				opts,
				c.str("RoutingPolicyLabel"),
				c.tags(),
			)
		},
		(*networkmanagerbackend.InMemoryBackend).GetVpcAttachment,
	)
}

func nmVpnAttachmentResource() nmResource {
	return nmAttachmentResource(
		func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (*networkmanagerbackend.Attachment, error) {
			return bk.CreateSiteToSiteVpnAttachment(
				c.id(propCoreNetworkID), c.str("VpnConnectionArn"), c.str("RoutingPolicyLabel"), c.tags(),
			)
		},
		(*networkmanagerbackend.InMemoryBackend).GetSiteToSiteVpnAttachment,
	)
}

func nmConnectAttachmentResource() nmResource {
	return nmAttachmentResource(
		func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (*networkmanagerbackend.Attachment, error) {
			return bk.CreateConnectAttachment(
				c.id(propCoreNetworkID), c.str("EdgeLocation"), c.id("TransportAttachmentId"),
				c.sub("Options").str("Protocol"), c.str("RoutingPolicyLabel"), c.tags(),
			)
		},
		(*networkmanagerbackend.InMemoryBackend).GetConnectAttachment,
	)
}

func nmDirectConnectGatewayAttachmentResource() nmResource {
	return nmAttachmentResource(
		func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (*networkmanagerbackend.Attachment, error) {
			return bk.CreateDirectConnectGatewayAttachment(
				c.id(propCoreNetworkID), c.str("DirectConnectGatewayArn"), c.list("EdgeLocations"),
				c.str("RoutingPolicyLabel"), c.tags(),
			)
		},
		(*networkmanagerbackend.InMemoryBackend).GetDirectConnectGatewayAttachment,
	)
}

func nmTransitGatewayRouteTableAttachmentResource() nmResource {
	return nmAttachmentResource(
		func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (*networkmanagerbackend.Attachment, error) {
			return bk.CreateTransitGatewayRouteTableAttachment(
				c.id("PeeringId"), c.str("TransitGatewayRouteTableArn"), c.str("RoutingPolicyLabel"), c.tags(),
			)
		},
		(*networkmanagerbackend.InMemoryBackend).GetTransitGatewayRouteTableAttachment,
	)
}

func nmConnectPeerResource() nmResource {
	return nmResource{
		create: func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (string, error) {
			var bgp *networkmanagerbackend.BgpOptions
			if _, ok := c.props["BgpOptions"].(map[string]any); ok {
				bgp = &networkmanagerbackend.BgpOptions{
					PeerAsn: int64Prop(c.sub("BgpOptions").props, "PeerAsn", c.params, c.physicalIDs),
				}
			}

			p, err := bk.CreateConnectPeer(
				c.id("ConnectAttachmentId"), c.str("PeerAddress"), bgp, c.str("CoreNetworkAddress"),
				c.str("SubnetArn"), c.list("InsideCidrBlocks"), c.tags(),
			)
			if err != nil {
				return "", err
			}

			return p.ConnectPeerID, nil
		},
		poll: func(bk *networkmanagerbackend.InMemoryBackend, id string) (string, bool) {
			p, err := bk.GetConnectPeer(id)
			if err != nil {
				return "", false
			}

			return p.State, true
		},
		remove: func(bk *networkmanagerbackend.InMemoryBackend, id string) error {
			_, err := bk.DeleteConnectPeer(id)

			return err
		},
		attrs: func(bk *networkmanagerbackend.InMemoryBackend, id string) map[string]string {
			p, err := bk.GetConnectPeer(id)
			if err != nil {
				return nil
			}

			out := map[string]string{
				"ConnectPeerId":    p.ConnectPeerID,
				propCoreNetworkID:  p.CoreNetworkID,
				nmAttrEdgeLocation: p.EdgeLocation,
				nmAttrState:        p.State,
				nmAttrCreatedAt:    p.CreatedAt.Format(time.RFC3339),
			}
			if p.Configuration != nil {
				out["Configuration.CoreNetworkAddress"] = p.Configuration.CoreNetworkAddress
				out["Configuration.PeerAddress"] = p.Configuration.PeerAddress
				out["Configuration.Protocol"] = p.Configuration.Protocol
			}

			return out
		},
	}
}

func nmTransitGatewayPeeringResource() nmResource {
	return nmResource{
		create: func(bk *networkmanagerbackend.InMemoryBackend, c nmCreateCtx) (string, error) {
			p, err := bk.CreateTransitGatewayPeering(c.id(propCoreNetworkID), c.str("TransitGatewayArn"), c.tags())
			if err != nil {
				return "", err
			}

			return p.PeeringID, nil
		},
		poll: func(bk *networkmanagerbackend.InMemoryBackend, id string) (string, bool) {
			p, err := bk.GetTransitGatewayPeering(id)
			if err != nil {
				return "", false
			}

			return p.State, true
		},
		remove: func(bk *networkmanagerbackend.InMemoryBackend, id string) error {
			_, err := bk.DeletePeering(id)

			return err
		},
		attrs: func(bk *networkmanagerbackend.InMemoryBackend, id string) map[string]string {
			p, err := bk.GetTransitGatewayPeering(id)
			if err != nil {
				return nil
			}

			return map[string]string{
				"PeeringId": p.PeeringID, "PeeringType": p.PeeringType, nmAttrCoreNetworkArn: p.CoreNetworkArn,
				nmAttrEdgeLocation: p.EdgeLocation, "OwnerAccountId": p.OwnerAccountID, "ResourceArn": p.ResourceArn,
				nmAttrState: p.State, nmAttrCreatedAt: p.CreatedAt.Format(time.RFC3339),
				"TransitGatewayPeeringAttachmentId": p.TransitGatewayPeeringAttachmentID,
			}
		},
	}
}

// createNetworkManagerThenDirectConnect tries the Network Manager types before the Direct Connect chain.
func (rc *ResourceCreator) createNetworkManagerThenDirectConnect(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if id, ok, err := rc.createNetworkManagerResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	return rc.createDirectConnectThenAdvanced(ctx, logicalID, resourceType, props, params, physicalIDs)
}

func (rc *ResourceCreator) deleteNetworkManagerThenDirectConnect(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if handled, err := rc.deleteNetworkManagerResource(resourceType, physicalID); handled {
		return true, err
	}

	return rc.deleteDirectConnectThenAdvanced(ctx, resourceType, physicalID)
}
