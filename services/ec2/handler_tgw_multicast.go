package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

// ---- Handler registration ----

func registerTGWMulticastOps(h *Handler, ops map[string]ec2ActionFn) {
	// Transit Gateway Multicast Domains
	ops["CreateTransitGatewayMulticastDomain"] = h.handleCreateTransitGatewayMulticastDomain
	ops["DescribeTransitGatewayMulticastDomains"] = h.handleDescribeTransitGatewayMulticastDomains
	ops["DeleteTransitGatewayMulticastDomain"] = h.handleDeleteTransitGatewayMulticastDomain

	// Transit Gateway Multicast Domain associations
	ops["AssociateTransitGatewayMulticastDomain"] = h.handleAssociateTransitGatewayMulticastDomain
	ops["DisassociateTransitGatewayMulticastDomain"] = h.handleDisassociateTransitGatewayMulticastDomain
	ops["GetTransitGatewayMulticastDomainAssociations"] = h.handleGetTransitGatewayMulticastDomainAssociations

	// Transit Gateway Multicast Group members / sources
	ops["RegisterTransitGatewayMulticastGroupMembers"] = h.handleRegisterTransitGatewayMulticastGroupMembers
	ops["DeregisterTransitGatewayMulticastGroupMembers"] = h.handleDeregisterTransitGatewayMulticastGroupMembers
	ops["RegisterTransitGatewayMulticastGroupSources"] = h.handleRegisterTransitGatewayMulticastGroupSources
	ops["DeregisterTransitGatewayMulticastGroupSources"] = h.handleDeregisterTransitGatewayMulticastGroupSources
	ops["SearchTransitGatewayMulticastGroups"] = h.handleSearchTransitGatewayMulticastGroups

	// Transit Gateway Metering Policies
	ops["CreateTransitGatewayMeteringPolicy"] = h.handleCreateTransitGatewayMeteringPolicy
	ops["DescribeTransitGatewayMeteringPolicies"] = h.handleDescribeTransitGatewayMeteringPolicies
	ops["DeleteTransitGatewayMeteringPolicy"] = h.handleDeleteTransitGatewayMeteringPolicy

	// Transit Gateway Metering Policy entries
	ops["CreateTransitGatewayMeteringPolicyEntry"] = h.handleCreateTransitGatewayMeteringPolicyEntry
	ops["DeleteTransitGatewayMeteringPolicyEntry"] = h.handleDeleteTransitGatewayMeteringPolicyEntry
}

func tgwMulticastSupportedOperations() []string {
	return []string{
		"CreateTransitGatewayMulticastDomain",
		"DescribeTransitGatewayMulticastDomains",
		"DeleteTransitGatewayMulticastDomain",
		"AssociateTransitGatewayMulticastDomain",
		"DisassociateTransitGatewayMulticastDomain",
		"GetTransitGatewayMulticastDomainAssociations",
		"RegisterTransitGatewayMulticastGroupMembers",
		"DeregisterTransitGatewayMulticastGroupMembers",
		"RegisterTransitGatewayMulticastGroupSources",
		"DeregisterTransitGatewayMulticastGroupSources",
		"SearchTransitGatewayMulticastGroups",
		"CreateTransitGatewayMeteringPolicy",
		"DescribeTransitGatewayMeteringPolicies",
		"DeleteTransitGatewayMeteringPolicy",
		"CreateTransitGatewayMeteringPolicyEntry",
		"DeleteTransitGatewayMeteringPolicyEntry",
	}
}

// ---- XML types: multicast domains ----

type tgwMulticastDomainOptionsItem struct {
	AutoAcceptSharedAssociations string `xml:"autoAcceptSharedAssociations,omitempty"`
	Igmpv2Support                string `xml:"igmpv2Support,omitempty"`
	StaticSourcesSupport         string `xml:"staticSourcesSupport,omitempty"`
}

type tgwMulticastDomainItem struct {
	CreationTime                    string                        `xml:"creationTime,omitempty"`
	TransitGatewayID                string                        `xml:"transitGatewayId,omitempty"`
	TransitGatewayMulticastDomainID string                        `xml:"transitGatewayMulticastDomainId,omitempty"`
	OwnerID                         string                        `xml:"ownerId,omitempty"`
	State                           string                        `xml:"state,omitempty"`
	Options                         tgwMulticastDomainOptionsItem `xml:"options"`
	TagSet                          []simpleTagItem               `xml:"tagSet>item"`
}

type createTransitGatewayMulticastDomainResponse struct {
	XMLName   xml.Name               `xml:"CreateTransitGatewayMulticastDomainResponse"`
	Xmlns     string                 `xml:"xmlns,attr"`
	RequestID string                 `xml:"requestId"`
	Domain    tgwMulticastDomainItem `xml:"transitGatewayMulticastDomain"`
}

type describeTransitGatewayMulticastDomainsResponse struct {
	XMLName   xml.Name `xml:"DescribeTransitGatewayMulticastDomainsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	NextToken string   `xml:"nextToken,omitempty"`
	Domains   struct {
		Items []tgwMulticastDomainItem `xml:"item"`
	} `xml:"transitGatewayMulticastDomains"`
}

type deleteTransitGatewayMulticastDomainResponse struct {
	XMLName   xml.Name               `xml:"DeleteTransitGatewayMulticastDomainResponse"`
	Xmlns     string                 `xml:"xmlns,attr"`
	RequestID string                 `xml:"requestId"`
	Domain    tgwMulticastDomainItem `xml:"transitGatewayMulticastDomain"`
}

// ---- XML types: multicast domain associations ----

type subnetAssociationItem struct {
	State    string `xml:"state,omitempty"`
	SubnetID string `xml:"subnetId,omitempty"`
}

// tgwMulticastDomainAssociationsAggregate is the shape returned by Associate and
// Disassociate: one record per attachment, with all affected subnets nested.
type tgwMulticastDomainAssociationsAggregate struct {
	ResourceID                      string `xml:"resourceId,omitempty"`
	ResourceOwnerID                 string `xml:"resourceOwnerId,omitempty"`
	ResourceType                    string `xml:"resourceType,omitempty"`
	TransitGatewayAttachmentID      string `xml:"transitGatewayAttachmentId,omitempty"`
	TransitGatewayMulticastDomainID string `xml:"transitGatewayMulticastDomainId,omitempty"`
	Subnets                         struct {
		Items []subnetAssociationItem `xml:"item"`
	} `xml:"subnets"`
}

type associateTransitGatewayMulticastDomainResponse struct {
	XMLName      xml.Name                                `xml:"AssociateTransitGatewayMulticastDomainResponse"`
	Xmlns        string                                  `xml:"xmlns,attr"`
	RequestID    string                                  `xml:"requestId"`
	Associations tgwMulticastDomainAssociationsAggregate `xml:"associations"`
}

type disassociateTransitGatewayMulticastDomainResponse struct {
	XMLName      xml.Name                                `xml:"DisassociateTransitGatewayMulticastDomainResponse"`
	Xmlns        string                                  `xml:"xmlns,attr"`
	RequestID    string                                  `xml:"requestId"`
	Associations tgwMulticastDomainAssociationsAggregate `xml:"associations"`
}

// tgwMulticastGetAssociationItem is the shape returned by
// GetTransitGatewayMulticastDomainAssociations: one record per subnet.
type tgwMulticastGetAssociationItem struct {
	ResourceID                 string                `xml:"resourceId,omitempty"`
	ResourceOwnerID            string                `xml:"resourceOwnerId,omitempty"`
	ResourceType               string                `xml:"resourceType,omitempty"`
	Subnet                     subnetAssociationItem `xml:"subnet"`
	TransitGatewayAttachmentID string                `xml:"transitGatewayAttachmentId,omitempty"`
}

type getTransitGatewayMulticastDomainAssociationsResponse struct {
	XMLName                     xml.Name `xml:"GetTransitGatewayMulticastDomainAssociationsResponse"`
	Xmlns                       string   `xml:"xmlns,attr"`
	RequestID                   string   `xml:"requestId"`
	NextToken                   string   `xml:"nextToken,omitempty"`
	MulticastDomainAssociations struct {
		Items []tgwMulticastGetAssociationItem `xml:"item"`
	} `xml:"multicastDomainAssociations"`
}

// ---- XML types: multicast group members / sources ----

type tgwMulticastGroupRegisteredItem struct {
	GroupIPAddress                  string   `xml:"groupIpAddress,omitempty"`
	TransitGatewayMulticastDomainID string   `xml:"transitGatewayMulticastDomainId,omitempty"`
	RegisteredNetworkInterfaceIDs   []string `xml:"registeredNetworkInterfaceIds>item"`
}

type tgwMulticastGroupDeregisteredItem struct {
	GroupIPAddress                  string   `xml:"groupIpAddress,omitempty"`
	TransitGatewayMulticastDomainID string   `xml:"transitGatewayMulticastDomainId,omitempty"`
	DeregisteredNetworkInterfaceIDs []string `xml:"deregisteredNetworkInterfaceIds>item"`
}

type registerTransitGatewayMulticastGroupMembersResponse struct {
	XMLName    xml.Name                        `xml:"RegisterTransitGatewayMulticastGroupMembersResponse"`
	Xmlns      string                          `xml:"xmlns,attr"`
	RequestID  string                          `xml:"requestId"`
	Registered tgwMulticastGroupRegisteredItem `xml:"registeredMulticastGroupMembers"`
}

type deregisterTransitGatewayMulticastGroupMembersResponse struct {
	XMLName      xml.Name                          `xml:"DeregisterTransitGatewayMulticastGroupMembersResponse"`
	Xmlns        string                            `xml:"xmlns,attr"`
	RequestID    string                            `xml:"requestId"`
	Deregistered tgwMulticastGroupDeregisteredItem `xml:"deregisteredMulticastGroupMembers"`
}

type registerTransitGatewayMulticastGroupSourcesResponse struct {
	XMLName    xml.Name                        `xml:"RegisterTransitGatewayMulticastGroupSourcesResponse"`
	Xmlns      string                          `xml:"xmlns,attr"`
	RequestID  string                          `xml:"requestId"`
	Registered tgwMulticastGroupRegisteredItem `xml:"registeredMulticastGroupSources"`
}

type deregisterTransitGatewayMulticastGroupSourcesResponse struct {
	XMLName      xml.Name                          `xml:"DeregisterTransitGatewayMulticastGroupSourcesResponse"`
	Xmlns        string                            `xml:"xmlns,attr"`
	RequestID    string                            `xml:"requestId"`
	Deregistered tgwMulticastGroupDeregisteredItem `xml:"deregisteredMulticastGroupSources"`
}

type tgwMulticastGroupItem struct {
	GroupIPAddress             string `xml:"groupIpAddress,omitempty"`
	MemberType                 string `xml:"memberType,omitempty"`
	NetworkInterfaceID         string `xml:"networkInterfaceId,omitempty"`
	ResourceID                 string `xml:"resourceId,omitempty"`
	ResourceType               string `xml:"resourceType,omitempty"`
	SourceType                 string `xml:"sourceType,omitempty"`
	TransitGatewayAttachmentID string `xml:"transitGatewayAttachmentId,omitempty"`
	GroupMember                bool   `xml:"groupMember"`
	GroupSource                bool   `xml:"groupSource"`
}

type searchTransitGatewayMulticastGroupsResponse struct {
	XMLName         xml.Name `xml:"SearchTransitGatewayMulticastGroupsResponse"`
	Xmlns           string   `xml:"xmlns,attr"`
	RequestID       string   `xml:"requestId"`
	NextToken       string   `xml:"nextToken,omitempty"`
	MulticastGroups struct {
		Items []tgwMulticastGroupItem `xml:"item"`
	} `xml:"multicastGroups"`
}

// ---- XML types: metering policies ----

type tgwMeteringPolicyItem struct {
	UpdateEffectiveAt              string          `xml:"updateEffectiveAt,omitempty"`
	TransitGatewayID               string          `xml:"transitGatewayId,omitempty"`
	TransitGatewayMeteringPolicyID string          `xml:"transitGatewayMeteringPolicyId,omitempty"`
	State                          string          `xml:"state,omitempty"`
	MiddleboxAttachmentIDs         []string        `xml:"middleboxAttachmentIdSet>item"`
	TagSet                         []simpleTagItem `xml:"tagSet>item"`
}

type createTransitGatewayMeteringPolicyResponse struct {
	XMLName   xml.Name              `xml:"CreateTransitGatewayMeteringPolicyResponse"`
	Xmlns     string                `xml:"xmlns,attr"`
	RequestID string                `xml:"requestId"`
	Policy    tgwMeteringPolicyItem `xml:"transitGatewayMeteringPolicy"`
}

type describeTransitGatewayMeteringPoliciesResponse struct {
	XMLName   xml.Name `xml:"DescribeTransitGatewayMeteringPoliciesResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	NextToken string   `xml:"nextToken,omitempty"`
	Policies  struct {
		Items []tgwMeteringPolicyItem `xml:"item"`
	} `xml:"transitGatewayMeteringPolicies"`
}

type deleteTransitGatewayMeteringPolicyResponse struct {
	XMLName   xml.Name              `xml:"DeleteTransitGatewayMeteringPolicyResponse"`
	Xmlns     string                `xml:"xmlns,attr"`
	RequestID string                `xml:"requestId"`
	Policy    tgwMeteringPolicyItem `xml:"transitGatewayMeteringPolicy"`
}

// ---- XML types: metering policy entries ----

type tgwMeteringPolicyRuleItem struct {
	DestinationCidrBlock                    string `xml:"destinationCidrBlock,omitempty"`
	DestinationPortRange                    string `xml:"destinationPortRange,omitempty"`
	DestinationTransitGatewayAttachmentID   string `xml:"destinationTransitGatewayAttachmentId,omitempty"`
	DestinationTransitGatewayAttachmentType string `xml:"destinationTransitGatewayAttachmentType,omitempty"`
	Protocol                                string `xml:"protocol,omitempty"`
	SourceCidrBlock                         string `xml:"sourceCidrBlock,omitempty"`
	SourcePortRange                         string `xml:"sourcePortRange,omitempty"`
	SourceTransitGatewayAttachmentID        string `xml:"sourceTransitGatewayAttachmentId,omitempty"`
	SourceTransitGatewayAttachmentType      string `xml:"sourceTransitGatewayAttachmentType,omitempty"`
}

type tgwMeteringPolicyEntryItem struct {
	MeteringPolicyRule tgwMeteringPolicyRuleItem `xml:"meteringPolicyRule"`
	UpdateEffectiveAt  string                    `xml:"updateEffectiveAt,omitempty"`
	UpdatedAt          string                    `xml:"updatedAt,omitempty"`
	MeteredAccount     string                    `xml:"meteredAccount,omitempty"`
	State              string                    `xml:"state,omitempty"`
	PolicyRuleNumber   int                       `xml:"policyRuleNumber,omitempty"`
}

type createTransitGatewayMeteringPolicyEntryResponse struct {
	XMLName   xml.Name                   `xml:"CreateTransitGatewayMeteringPolicyEntryResponse"`
	Xmlns     string                     `xml:"xmlns,attr"`
	RequestID string                     `xml:"requestId"`
	Entry     tgwMeteringPolicyEntryItem `xml:"transitGatewayMeteringPolicyEntry"`
}

type deleteTransitGatewayMeteringPolicyEntryResponse struct {
	XMLName   xml.Name                   `xml:"DeleteTransitGatewayMeteringPolicyEntryResponse"`
	Xmlns     string                     `xml:"xmlns,attr"`
	RequestID string                     `xml:"requestId"`
	Entry     tgwMeteringPolicyEntryItem `xml:"transitGatewayMeteringPolicyEntry"`
}

// ---- conversion helpers ----

const timeLayoutISO = "2006-01-02T15:04:05.000Z"

// parseTagSpecificationPlural is parseTagSpecification for the handful of EC2
// ops whose request field is TagSpecifications.N (plural), not the near-universal
// TagSpecification.N. CreateTransitGatewayMeteringPolicyInput is one of five
// such ops in the pinned SDK (ec2@v1.319.1 serializers.go:72985, FlatKey
// "TagSpecifications"); the other four (CreateCapacityReservation,
// CreateTransitGatewayPolicyTable, CreateTransitGatewayRouteTable,
// CreateTransitGatewayVpcAttachment) belong to other families.
func parseTagSpecificationPlural(vals url.Values, resourceType string) map[string]string {
	tags := make(map[string]string)

	for i := 1; i <= maxTagsPerRequest; i++ {
		rt := vals.Get(fmt.Sprintf("TagSpecifications.%d.ResourceType", i))
		if rt == "" {
			break
		}

		if rt != resourceType {
			continue
		}

		for j := 1; j <= maxTagsPerRequest; j++ {
			key := vals.Get(fmt.Sprintf("TagSpecifications.%d.Tag.%d.Key", i, j))
			if key == "" {
				break
			}

			tags[key] = vals.Get(fmt.Sprintf("TagSpecifications.%d.Tag.%d.Value", i, j))
		}
	}

	return tags
}

func tgwMulticastDomainToItem(d *TransitGatewayMulticastDomain, tags map[string]string) tgwMulticastDomainItem {
	return tgwMulticastDomainItem{
		CreationTime:                    d.CreationTime.UTC().Format(timeLayoutISO),
		TransitGatewayID:                d.TransitGatewayID,
		TransitGatewayMulticastDomainID: d.ID,
		OwnerID:                         d.OwnerID,
		State:                           d.State,
		Options: tgwMulticastDomainOptionsItem{
			AutoAcceptSharedAssociations: d.AutoAcceptSharedAssociations,
			Igmpv2Support:                d.Igmpv2Support,
			StaticSourcesSupport:         d.StaticSourcesSupport,
		},
		TagSet: tagItemsFromMap(tags),
	}
}

func tgwMeteringPolicyToItem(p *TransitGatewayMeteringPolicy, tags map[string]string) tgwMeteringPolicyItem {
	return tgwMeteringPolicyItem{
		UpdateEffectiveAt:              p.UpdateEffectiveAt.UTC().Format(timeLayoutISO),
		TransitGatewayID:               p.TransitGatewayID,
		TransitGatewayMeteringPolicyID: p.ID,
		State:                          p.State,
		MiddleboxAttachmentIDs:         p.MiddleboxAttachmentIDs,
		TagSet:                         tagItemsFromMap(tags),
	}
}

func tgwMeteringPolicyEntryToItem(e *TransitGatewayMeteringPolicyEntry) tgwMeteringPolicyEntryItem {
	return tgwMeteringPolicyEntryItem{
		UpdateEffectiveAt: e.UpdateEffectiveAt.UTC().Format(timeLayoutISO),
		UpdatedAt:         e.UpdatedAt.UTC().Format(timeLayoutISO),
		MeteredAccount:    e.MeteredAccount,
		PolicyRuleNumber:  e.PolicyRuleNumber,
		State:             e.State,
		MeteringPolicyRule: tgwMeteringPolicyRuleItem{
			DestinationCidrBlock:                    e.DestinationCidrBlock,
			DestinationPortRange:                    e.DestinationPortRange,
			DestinationTransitGatewayAttachmentID:   e.DestinationTransitGatewayAttachmentID,
			DestinationTransitGatewayAttachmentType: e.DestinationTransitGatewayAttachmentType,
			Protocol:                                e.Protocol,
			SourceCidrBlock:                         e.SourceCidrBlock,
			SourcePortRange:                         e.SourcePortRange,
			SourceTransitGatewayAttachmentID:        e.SourceTransitGatewayAttachmentID,
			SourceTransitGatewayAttachmentType:      e.SourceTransitGatewayAttachmentType,
		},
	}
}

// ---- Handlers: multicast domains ----

func (h *Handler) handleCreateTransitGatewayMulticastDomain(vals url.Values, reqID string) (any, error) {
	tgwID := vals.Get("TransitGatewayId")
	autoAccept := vals.Get("Options.AutoAcceptSharedAssociations")
	igmpv2 := vals.Get("Options.Igmpv2Support")
	staticSources := vals.Get("Options.StaticSourcesSupport")
	tags := parseTagSpecification(vals, "transit-gateway-multicast-domain")

	domain, err := h.Backend.CreateTransitGatewayMulticastDomain(tgwID, autoAccept, igmpv2, staticSources, tags)
	if err != nil {
		return nil, err
	}

	return &createTransitGatewayMulticastDomainResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Domain:    tgwMulticastDomainToItem(domain, tags),
	}, nil
}

func (h *Handler) handleDescribeTransitGatewayMulticastDomains(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "TransitGatewayMulticastDomainIds")
	domains := h.Backend.DescribeTransitGatewayMulticastDomains(ids)

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	domains, nextToken = pageSlice(domains, offset, maxResults)

	resp := &describeTransitGatewayMulticastDomainsResponse{Xmlns: ec2XMLNS, RequestID: reqID, NextToken: nextToken}

	for _, d := range domains {
		resp.Domains.Items = append(resp.Domains.Items, tgwMulticastDomainToItem(d, h.Backend.TagsForResource(d.ID)))
	}

	return resp, nil
}

func (h *Handler) handleDeleteTransitGatewayMulticastDomain(vals url.Values, reqID string) (any, error) {
	id := vals.Get("TransitGatewayMulticastDomainId")

	existing := h.Backend.DescribeTransitGatewayMulticastDomains([]string{id})
	tags := h.Backend.TagsForResource(id)

	if err := h.Backend.DeleteTransitGatewayMulticastDomain(id); err != nil {
		return nil, err
	}

	var item tgwMulticastDomainItem

	if len(existing) > 0 {
		item = tgwMulticastDomainToItem(existing[0], tags)
		item.State = tgwRouteStateDeleted
	}

	return &deleteTransitGatewayMulticastDomainResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Domain:    item,
	}, nil
}

// ---- Handlers: multicast domain associations ----

func assocsToAggregate(
	assocs []*TransitGatewayMulticastDomainAssociation,
) tgwMulticastDomainAssociationsAggregate {
	agg := tgwMulticastDomainAssociationsAggregate{ResourceType: tgwResourceTypeVPC}

	for _, a := range assocs {
		agg.TransitGatewayAttachmentID = a.TransitGatewayAttachmentID
		agg.TransitGatewayMulticastDomainID = a.TransitGatewayMulticastDomainID
		agg.Subnets.Items = append(agg.Subnets.Items, subnetAssociationItem{
			State:    a.State,
			SubnetID: a.SubnetID,
		})
	}

	return agg
}

func (h *Handler) handleAssociateTransitGatewayMulticastDomain(
	vals url.Values,
	reqID string,
) (any, error) {
	domainID := vals.Get("TransitGatewayMulticastDomainId")
	attachmentID := vals.Get("TransitGatewayAttachmentId")
	subnetIDs := parseMemberList(vals, "SubnetIds")

	assocs, err := h.Backend.AssociateTransitGatewayMulticastDomain(domainID, attachmentID, subnetIDs)
	if err != nil {
		return nil, err
	}

	return &associateTransitGatewayMulticastDomainResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		Associations: assocsToAggregate(assocs),
	}, nil
}

func (h *Handler) handleDisassociateTransitGatewayMulticastDomain(
	vals url.Values,
	reqID string,
) (any, error) {
	domainID := vals.Get("TransitGatewayMulticastDomainId")
	attachmentID := vals.Get("TransitGatewayAttachmentId")
	subnetIDs := parseMemberList(vals, "SubnetIds")

	assocs, err := h.Backend.DisassociateTransitGatewayMulticastDomain(domainID, attachmentID, subnetIDs)
	if err != nil {
		return nil, err
	}

	return &disassociateTransitGatewayMulticastDomainResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		Associations: assocsToAggregate(assocs),
	}, nil
}

func (h *Handler) handleGetTransitGatewayMulticastDomainAssociations(
	vals url.Values,
	reqID string,
) (any, error) {
	domainID := vals.Get("TransitGatewayMulticastDomainId")
	assocs := h.Backend.GetTransitGatewayMulticastDomainAssociations(domainID)

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	assocs, nextToken = pageSlice(assocs, offset, maxResults)

	resp := &getTransitGatewayMulticastDomainAssociationsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		NextToken: nextToken,
	}

	for _, a := range assocs {
		resp.MulticastDomainAssociations.Items = append(
			resp.MulticastDomainAssociations.Items,
			tgwMulticastGetAssociationItem{
				ResourceType:               tgwResourceTypeVPC,
				TransitGatewayAttachmentID: a.TransitGatewayAttachmentID,
				Subnet: subnetAssociationItem{
					State:    a.State,
					SubnetID: a.SubnetID,
				},
			},
		)
	}

	return resp, nil
}

// ---- Handlers: multicast group members / sources ----

func (h *Handler) handleRegisterTransitGatewayMulticastGroupMembers(
	vals url.Values,
	reqID string,
) (any, error) {
	domainID := vals.Get("TransitGatewayMulticastDomainId")
	groupIP := vals.Get("GroupIpAddress")
	eniIDs := parseMemberList(vals, "NetworkInterfaceIds")

	membership, err := h.Backend.RegisterTransitGatewayMulticastGroupMembers(domainID, groupIP, eniIDs)
	if err != nil {
		return nil, err
	}

	return &registerTransitGatewayMulticastGroupMembersResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Registered: tgwMulticastGroupRegisteredItem{
			GroupIPAddress:                  membership.GroupIPAddress,
			RegisteredNetworkInterfaceIDs:   membership.NetworkInterfaceIDs,
			TransitGatewayMulticastDomainID: membership.TransitGatewayMulticastDomainID,
		},
	}, nil
}

func (h *Handler) handleDeregisterTransitGatewayMulticastGroupMembers(
	vals url.Values,
	reqID string,
) (any, error) {
	domainID := vals.Get("TransitGatewayMulticastDomainId")
	groupIP := vals.Get("GroupIpAddress")
	eniIDs := parseMemberList(vals, "NetworkInterfaceIds")

	membership, err := h.Backend.DeregisterTransitGatewayMulticastGroupMembers(domainID, groupIP, eniIDs)
	if err != nil {
		return nil, err
	}

	return &deregisterTransitGatewayMulticastGroupMembersResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Deregistered: tgwMulticastGroupDeregisteredItem{
			DeregisteredNetworkInterfaceIDs: membership.NetworkInterfaceIDs,
			GroupIPAddress:                  membership.GroupIPAddress,
			TransitGatewayMulticastDomainID: membership.TransitGatewayMulticastDomainID,
		},
	}, nil
}

func (h *Handler) handleRegisterTransitGatewayMulticastGroupSources(
	vals url.Values,
	reqID string,
) (any, error) {
	domainID := vals.Get("TransitGatewayMulticastDomainId")
	groupIP := vals.Get("GroupIpAddress")
	eniIDs := parseMemberList(vals, "NetworkInterfaceIds")

	membership, err := h.Backend.RegisterTransitGatewayMulticastGroupSources(domainID, groupIP, eniIDs)
	if err != nil {
		return nil, err
	}

	return &registerTransitGatewayMulticastGroupSourcesResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Registered: tgwMulticastGroupRegisteredItem{
			GroupIPAddress:                  membership.GroupIPAddress,
			RegisteredNetworkInterfaceIDs:   membership.NetworkInterfaceIDs,
			TransitGatewayMulticastDomainID: membership.TransitGatewayMulticastDomainID,
		},
	}, nil
}

func (h *Handler) handleDeregisterTransitGatewayMulticastGroupSources(
	vals url.Values,
	reqID string,
) (any, error) {
	domainID := vals.Get("TransitGatewayMulticastDomainId")
	groupIP := vals.Get("GroupIpAddress")
	eniIDs := parseMemberList(vals, "NetworkInterfaceIds")

	membership, err := h.Backend.DeregisterTransitGatewayMulticastGroupSources(domainID, groupIP, eniIDs)
	if err != nil {
		return nil, err
	}

	return &deregisterTransitGatewayMulticastGroupSourcesResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Deregistered: tgwMulticastGroupDeregisteredItem{
			DeregisteredNetworkInterfaceIDs: membership.NetworkInterfaceIDs,
			GroupIPAddress:                  membership.GroupIPAddress,
			TransitGatewayMulticastDomainID: membership.TransitGatewayMulticastDomainID,
		},
	}, nil
}

func (h *Handler) handleSearchTransitGatewayMulticastGroups(vals url.Values, reqID string) (any, error) {
	domainID := vals.Get("TransitGatewayMulticastDomainId")
	entries := h.Backend.SearchTransitGatewayMulticastGroups(domainID)

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	entries, nextToken = pageSlice(entries, offset, maxResults)

	resp := &searchTransitGatewayMulticastGroupsResponse{Xmlns: ec2XMLNS, RequestID: reqID, NextToken: nextToken}

	for _, e := range entries {
		item := tgwMulticastGroupItem{
			GroupIPAddress:     e.GroupIPAddress,
			NetworkInterfaceID: e.NetworkInterfaceID,
			ResourceID:         e.ResourceID,
			ResourceType:       e.ResourceType,
			GroupMember:        e.IsMember,
			GroupSource:        e.IsSource,
		}

		if e.IsMember {
			item.MemberType = "static"
		}

		if e.IsSource {
			item.SourceType = "static"
		}

		resp.MulticastGroups.Items = append(resp.MulticastGroups.Items, item)
	}

	return resp, nil
}

// ---- Handlers: metering policies ----

func (h *Handler) handleCreateTransitGatewayMeteringPolicy(vals url.Values, reqID string) (any, error) {
	tgwID := vals.Get("TransitGatewayId")
	middleboxIDs := parseMemberList(vals, "MiddleboxAttachmentId")
	tags := parseTagSpecificationPlural(vals, "transit-gateway-metering-policy")

	policy, err := h.Backend.CreateTransitGatewayMeteringPolicy(tgwID, middleboxIDs, tags)
	if err != nil {
		return nil, err
	}

	return &createTransitGatewayMeteringPolicyResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Policy:    tgwMeteringPolicyToItem(policy, tags),
	}, nil
}

func (h *Handler) handleDescribeTransitGatewayMeteringPolicies(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "TransitGatewayMeteringPolicyIds")
	policies := h.Backend.DescribeTransitGatewayMeteringPolicies(ids)

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	policies, nextToken = pageSlice(policies, offset, maxResults)

	resp := &describeTransitGatewayMeteringPoliciesResponse{Xmlns: ec2XMLNS, RequestID: reqID, NextToken: nextToken}

	for _, p := range policies {
		resp.Policies.Items = append(resp.Policies.Items, tgwMeteringPolicyToItem(p, h.Backend.TagsForResource(p.ID)))
	}

	return resp, nil
}

func (h *Handler) handleDeleteTransitGatewayMeteringPolicy(vals url.Values, reqID string) (any, error) {
	id := vals.Get("TransitGatewayMeteringPolicyId")

	existing := h.Backend.DescribeTransitGatewayMeteringPolicies([]string{id})
	tags := h.Backend.TagsForResource(id)

	if err := h.Backend.DeleteTransitGatewayMeteringPolicy(id); err != nil {
		return nil, err
	}

	var item tgwMeteringPolicyItem

	if len(existing) > 0 {
		item = tgwMeteringPolicyToItem(existing[0], tags)
		item.State = tgwRouteStateDeleted
	}

	return &deleteTransitGatewayMeteringPolicyResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Policy:    item,
	}, nil
}

// ---- Handlers: metering policy entries ----

func (h *Handler) handleCreateTransitGatewayMeteringPolicyEntry(
	vals url.Values,
	reqID string,
) (any, error) {
	policyID := vals.Get("TransitGatewayMeteringPolicyId")
	ruleNumber, _ := strconv.Atoi(vals.Get("PolicyRuleNumber"))

	entry := &TransitGatewayMeteringPolicyEntry{
		PolicyRuleNumber:                        ruleNumber,
		MeteredAccount:                          vals.Get("MeteredAccount"),
		DestinationCidrBlock:                    vals.Get("DestinationCidrBlock"),
		DestinationPortRange:                    vals.Get("DestinationPortRange"),
		DestinationTransitGatewayAttachmentID:   vals.Get("DestinationTransitGatewayAttachmentId"),
		DestinationTransitGatewayAttachmentType: vals.Get("DestinationTransitGatewayAttachmentType"),
		Protocol:                                vals.Get("Protocol"),
		SourceCidrBlock:                         vals.Get("SourceCidrBlock"),
		SourcePortRange:                         vals.Get("SourcePortRange"),
		SourceTransitGatewayAttachmentID:        vals.Get("SourceTransitGatewayAttachmentId"),
		SourceTransitGatewayAttachmentType:      vals.Get("SourceTransitGatewayAttachmentType"),
	}

	stored, err := h.Backend.CreateTransitGatewayMeteringPolicyEntry(policyID, entry)
	if err != nil {
		return nil, err
	}

	return &createTransitGatewayMeteringPolicyEntryResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Entry:     tgwMeteringPolicyEntryToItem(stored),
	}, nil
}

func (h *Handler) handleDeleteTransitGatewayMeteringPolicyEntry(
	vals url.Values,
	reqID string,
) (any, error) {
	policyID := vals.Get("TransitGatewayMeteringPolicyId")
	ruleNumber, _ := strconv.Atoi(vals.Get("PolicyRuleNumber"))

	entry, err := h.Backend.DeleteTransitGatewayMeteringPolicyEntry(policyID, ruleNumber)
	if err != nil {
		return nil, err
	}

	return &deleteTransitGatewayMeteringPolicyEntryResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Entry:     tgwMeteringPolicyEntryToItem(entry),
	}, nil
}
