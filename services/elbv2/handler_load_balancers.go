package elbv2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"sort"
)

func (h *Handler) handleCreateLoadBalancer(vals url.Values) (any, error) {
	name := vals.Get("Name")
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	subnets := parseMembers(vals, "Subnets.member")
	sgs := parseMembers(vals, "SecurityGroups.member")
	tagKVs := parseTagKVs(vals)

	subnetMappings := parseSubnetMappings(vals)

	lb, err := h.Backend.CreateLoadBalancer(CreateLoadBalancerInput{
		Name:           name,
		Scheme:         vals.Get("Scheme"),
		Type:           vals.Get("Type"),
		IPAddressType:  vals.Get("IpAddressType"),
		Subnets:        subnets,
		SubnetMappings: subnetMappings,
		SecurityGroups: sgs,
		Tags:           tagKVs,
	})
	if err != nil {
		return nil, err
	}

	return &createLoadBalancerResponse{
		Xmlns: elbv2XMLNS,
		Result: createLoadBalancerResult{
			LoadBalancers: xmlLoadBalancerList{
				Members: []xmlLoadBalancer{toXMLLoadBalancer(lb)},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-create-" + name},
	}, nil
}

func (h *Handler) handleDeleteLoadBalancer(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteLoadBalancer(lbArn); err != nil {
		return nil, err
	}

	return &deleteLoadBalancerResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-delete-lb"},
	}, nil
}

func (h *Handler) handleDescribeLoadBalancers(vals url.Values) (any, error) {
	arns := parseMembers(vals, "LoadBalancerArns.member")
	names := parseMembers(vals, "Names.member")

	lbs, err := h.Backend.DescribeLoadBalancers(arns, names)
	if err != nil {
		return nil, err
	}

	marker, pageSize := parsePagination(vals)

	startIdx := 0
	if marker != "" {
		for i, lb := range lbs {
			if lb.LoadBalancerArn == marker {
				startIdx = i + 1

				break
			}
		}
	}

	lbs = lbs[startIdx:]

	var nextMarker string
	if len(lbs) > pageSize {
		nextMarker = lbs[pageSize-1].LoadBalancerArn
		lbs = lbs[:pageSize]
	}

	members := make([]xmlLoadBalancer, 0, len(lbs))
	for i := range lbs {
		members = append(members, toXMLLoadBalancer(&lbs[i]))
	}

	return &describeLoadBalancersResponse{
		Xmlns: elbv2XMLNS,
		Result: describeLoadBalancersResult{
			NextMarker:    nextMarker,
			LoadBalancers: xmlLoadBalancerList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-lbs"},
	}, nil
}

func (h *Handler) handleModifyLoadBalancerAttributes(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	attrs := parseKVAttrs(vals, "Attributes.member")

	lb, err := h.Backend.ModifyLoadBalancerAttributes(lbArn, attrs)
	if err != nil {
		return nil, err
	}

	members := sortedLBAttributes(lb.Attributes)

	return &modifyLoadBalancerAttributesResponse{
		Xmlns:            elbv2XMLNS,
		Result:           modifyLoadBalancerAttributesResult{Attributes: xmlLBAttributeList{Members: members}},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-lb-attrs"},
	}, nil
}

func (h *Handler) handleDescribeLoadBalancerAttributes(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	lbs, err := h.Backend.DescribeLoadBalancers([]string{lbArn}, nil)
	if err != nil {
		return nil, err
	}

	if len(lbs) == 0 {
		return nil, ErrLoadBalancerNotFound
	}

	members := make([]xmlLBAttribute, 0, len(lbs[0].Attributes))
	for k, v := range lbs[0].Attributes {
		members = append(members, xmlLBAttribute{Key: k, Value: v})
	}

	sort.Slice(members, func(i, j int) bool { return members[i].Key < members[j].Key })

	return &describeLoadBalancerAttributesResponse{
		Xmlns: elbv2XMLNS,
		Result: describeLoadBalancerAttributesResult{
			Attributes: xmlLBAttributeList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-lb-attrs"},
	}, nil
}

func (h *Handler) handleSetSecurityGroups(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	sgs := parseMembers(vals, "SecurityGroups.member")

	lb, err := h.Backend.SetSecurityGroups(lbArn, sgs)
	if err != nil {
		return nil, err
	}

	members := make([]xmlStringValue, 0, len(lb.SecurityGroups))
	for _, sg := range lb.SecurityGroups {
		members = append(members, xmlStringValue{Value: sg})
	}

	return &setSecurityGroupsResponse{
		Xmlns: elbv2XMLNS,
		Result: setSecurityGroupsResult{
			SecurityGroupIDs:                 xmlStringList{Members: members},
			EnforceInboundRulesOnPrivateLink: "off",
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-set-sgs"},
	}, nil
}

func (h *Handler) handleSetSubnets(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	// Prefer SubnetMappings; fall back to plain Subnets.member list.
	mappings := parseSubnetMappings(vals)
	if len(mappings) == 0 {
		for _, s := range parseMembers(vals, "Subnets.member") {
			mappings = append(mappings, SubnetMapping{SubnetID: s})
		}
	}

	lb, err := h.Backend.SetSubnets(lbArn, mappings)
	if err != nil {
		return nil, err
	}

	azMembers := make([]xmlAZMapping, 0, len(lb.AvailabilityZones))
	for _, az := range lb.AvailabilityZones {
		azMembers = append(azMembers, xmlAZMapping(az))
	}

	return &setSubnetsResponse{
		Xmlns: elbv2XMLNS,
		Result: setSubnetsResult{
			AvailabilityZones:            xmlAZMappingList{Members: azMembers},
			IPAddressType:                lb.IPAddressType,
			EnablePrefixForIpv6SourceNat: "off",
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-set-subnets"},
	}, nil
}

func (h *Handler) handleSetIPAddressType(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	ipType := vals.Get("IpAddressType")
	if ipType == "" {
		return nil, fmt.Errorf("%w: IpAddressType is required", ErrInvalidParameter)
	}

	lb, err := h.Backend.SetIPAddressType(lbArn, ipType)
	if err != nil {
		return nil, err
	}

	return &setIPAddressTypeResponse{
		Xmlns:            elbv2XMLNS,
		Result:           setIPAddressTypeResult{IPAddressType: lb.IPAddressType},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-set-ip-type"},
	}, nil
}

func (h *Handler) handleModifyIPPools(vals url.Values) (any, error) {
	lbArn := vals.Get("LoadBalancerArn")
	if lbArn == "" {
		return nil, fmt.Errorf("%w: LoadBalancerArn is required", ErrInvalidParameter)
	}

	var ipv4PoolID *string

	if raw := vals.Get("IpamPools.Ipv4IpamPoolId"); raw != "" {
		ipv4PoolID = &raw
	}

	removeIPv4 := false

	for _, v := range parseMembers(vals, "RemoveIpamPools.member") {
		if v == ipAddressTypeIPv4 {
			removeIPv4 = true
		}
	}

	lb, err := h.Backend.ModifyIPPools(lbArn, ipv4PoolID, removeIPv4)
	if err != nil {
		return nil, err
	}

	result := modifyIPPoolsResult{}
	if lb.IPv4IPAMPoolID != "" {
		result.IpamPools = &xmlIpamPools{Ipv4IpamPoolID: lb.IPv4IPAMPoolID}
	}

	return &modifyIPPoolsResponse{
		Xmlns:            elbv2XMLNS,
		Result:           result,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-ip-pools"},
	}, nil
}

// parseTargets extracts target descriptions from Targets.member.N.Id/Port form values.
// parseSubnetMappings extracts SubnetMappings.member.N.* from form values.
func parseSubnetMappings(vals url.Values) []SubnetMapping {
	var out []SubnetMapping

	for i := 1; ; i++ {
		subnetID := vals.Get(fmt.Sprintf("SubnetMappings.member.%d.SubnetId", i))
		if subnetID == "" {
			break
		}

		out = append(out, SubnetMapping{
			SubnetID:           subnetID,
			AllocationID:       vals.Get(fmt.Sprintf("SubnetMappings.member.%d.AllocationId", i)),
			PrivateIPv4Address: vals.Get(fmt.Sprintf("SubnetMappings.member.%d.PrivateIPv4Address", i)),
			IPv6Address:        vals.Get(fmt.Sprintf("SubnetMappings.member.%d.IPv6Address", i)),
		})
	}

	return out
}

func toXMLLoadBalancer(lb *LoadBalancer) xmlLoadBalancer {
	azs := make([]xmlAZMapping, 0, len(lb.AvailabilityZones))
	for _, az := range lb.AvailabilityZones {
		azs = append(azs, xmlAZMapping(az))
	}

	sgs := make([]xmlStringValue, 0, len(lb.SecurityGroups))
	for _, sg := range lb.SecurityGroups {
		sgs = append(sgs, xmlStringValue{Value: sg})
	}

	xlb := xmlLoadBalancer{
		LoadBalancerArn:       lb.LoadBalancerArn,
		LoadBalancerName:      lb.LoadBalancerName,
		DNSName:               lb.DNSName,
		CanonicalHostedZoneID: lb.CanonicalHostedZoneID,
		CreatedTime:           lb.CreatedTime.UTC().Format("2006-01-02T15:04:05Z"),
		Scheme:                lb.Scheme,
		Type:                  lb.Type,
		IPAddressType:         lb.IPAddressType,
		VpcID:                 lb.VpcID,
		State:                 xmlLoadBalancerState{Code: lb.State.Code, Reason: lb.State.Description},
		AvailabilityZones:     xmlAZMappingList{Members: azs},
		SecurityGroups:        xmlStringList{Members: sgs},
	}

	if lb.IPv4IPAMPoolID != "" {
		xlb.IpamPools = &xmlIpamPools{Ipv4IpamPoolID: lb.IPv4IPAMPoolID}
	}

	return xlb
}

type xmlLoadBalancerState struct {
	Code   string `xml:"Code"`
	Reason string `xml:"Reason,omitempty"`
}

type xmlAZMapping struct {
	ZoneName string `xml:"ZoneName"`
	SubnetID string `xml:"SubnetId,omitempty"`
}

type xmlAZMappingList struct {
	Members []xmlAZMapping `xml:"member"`
}

type xmlLoadBalancer struct {
	IpamPools             *xmlIpamPools        `xml:"IpamPools,omitempty"`
	State                 xmlLoadBalancerState `xml:"State"`
	CanonicalHostedZoneID string               `xml:"CanonicalHostedZoneId"`
	LoadBalancerArn       string               `xml:"LoadBalancerArn"`
	CreatedTime           string               `xml:"CreatedTime"`
	Scheme                string               `xml:"Scheme"`
	Type                  string               `xml:"Type"`
	IPAddressType         string               `xml:"IpAddressType"`
	VpcID                 string               `xml:"VpcId"`
	DNSName               string               `xml:"DNSName"`
	LoadBalancerName      string               `xml:"LoadBalancerName"`
	AvailabilityZones     xmlAZMappingList     `xml:"AvailabilityZones"`
	SecurityGroups        xmlStringList        `xml:"SecurityGroups"`
}

type xmlLoadBalancerList struct {
	Members []xmlLoadBalancer `xml:"member"`
}

type createLoadBalancerResult struct {
	LoadBalancers xmlLoadBalancerList `xml:"LoadBalancers"`
}

type createLoadBalancerResponse struct {
	XMLName          xml.Name                 `xml:"CreateLoadBalancerResponse"`
	Xmlns            string                   `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata      `xml:"ResponseMetadata"`
	Result           createLoadBalancerResult `xml:"CreateLoadBalancerResult"`
}

type deleteLoadBalancerResponse struct {
	Result           deleteResultXML     `xml:"DeleteLoadBalancerResult"`
	XMLName          xml.Name            `xml:"DeleteLoadBalancerResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type describeLoadBalancersResult struct {
	NextMarker    string              `xml:"NextMarker,omitempty"`
	LoadBalancers xmlLoadBalancerList `xml:"LoadBalancers"`
}

type describeLoadBalancersResponse struct {
	XMLName          xml.Name                    `xml:"DescribeLoadBalancersResponse"`
	Xmlns            string                      `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata         `xml:"ResponseMetadata"`
	Result           describeLoadBalancersResult `xml:"DescribeLoadBalancersResult"`
}

type xmlLBAttribute struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type xmlLBAttributeList struct {
	Members []xmlLBAttribute `xml:"member"`
}

type modifyLoadBalancerAttributesResult struct {
	Attributes xmlLBAttributeList `xml:"Attributes"`
}

type modifyLoadBalancerAttributesResponse struct {
	XMLName          xml.Name                           `xml:"ModifyLoadBalancerAttributesResponse"`
	Xmlns            string                             `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                `xml:"ResponseMetadata"`
	Result           modifyLoadBalancerAttributesResult `xml:"ModifyLoadBalancerAttributesResult"`
}

type describeLoadBalancerAttributesResult struct {
	Attributes xmlLBAttributeList `xml:"Attributes"`
}

type describeLoadBalancerAttributesResponse struct {
	XMLName          xml.Name                             `xml:"DescribeLoadBalancerAttributesResponse"`
	Xmlns            string                               `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                  `xml:"ResponseMetadata"`
	Result           describeLoadBalancerAttributesResult `xml:"DescribeLoadBalancerAttributesResult"`
}

type setSecurityGroupsResult struct {
	EnforceInboundRulesOnPrivateLink string        `xml:"EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic,omitempty"`
	SecurityGroupIDs                 xmlStringList `xml:"SecurityGroupIds"`
}

type setSecurityGroupsResponse struct {
	XMLName          xml.Name                `xml:"SetSecurityGroupsResponse"`
	Xmlns            string                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata     `xml:"ResponseMetadata"`
	Result           setSecurityGroupsResult `xml:"SetSecurityGroupsResult"`
}

type setSubnetsResult struct {
	IPAddressType                string           `xml:"IpAddressType,omitempty"`
	EnablePrefixForIpv6SourceNat string           `xml:"EnablePrefixForIpv6SourceNat,omitempty"`
	AvailabilityZones            xmlAZMappingList `xml:"AvailabilityZones"`
}

type setSubnetsResponse struct {
	XMLName          xml.Name            `xml:"SetSubnetsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
	Result           setSubnetsResult    `xml:"SetSubnetsResult"`
}

type setIPAddressTypeResult struct {
	IPAddressType string `xml:"IpAddressType"`
}

type setIPAddressTypeResponse struct {
	XMLName          xml.Name               `xml:"SetIpAddressTypeResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	Result           setIPAddressTypeResult `xml:"SetIpAddressTypeResult"`
	ResponseMetadata xmlResponseMetadata    `xml:"ResponseMetadata"`
}

type xmlIpamPools struct {
	Ipv4IpamPoolID string `xml:"Ipv4IpamPoolId,omitempty"`
}

type modifyIPPoolsResult struct {
	IpamPools *xmlIpamPools `xml:"IpamPools,omitempty"`
}

type modifyIPPoolsResponse struct {
	Result           modifyIPPoolsResult `xml:"ModifyIpPoolsResult"`
	XMLName          xml.Name            `xml:"ModifyIpPoolsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

// sortedLBAttributes converts a map to a sorted xmlLBAttribute slice.
func sortedLBAttributes(m map[string]string) []xmlLBAttribute {
	out := make([]xmlLBAttribute, 0, len(m))
	for k, v := range m {
		out = append(out, xmlLBAttribute{Key: k, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })

	return out
}
