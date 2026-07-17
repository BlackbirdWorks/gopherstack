package elb

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

func (h *Handler) handleCreateLoadBalancer(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	listeners, err := parseListeners(vals)
	if err != nil {
		return nil, err
	}

	azs := parseMembers(vals, "AvailabilityZones.member")
	sgs := parseMembers(vals, "SecurityGroups.member")
	subnets := parseMembers(vals, "Subnets.member")
	scheme := vals.Get("Scheme")

	lb, createErr := h.Backend.CreateLoadBalancer(ctx, CreateLoadBalancerInput{
		LoadBalancerName:  name,
		Scheme:            scheme,
		AvailabilityZones: azs,
		SecurityGroups:    sgs,
		Subnets:           subnets,
		Listeners:         listeners,
	})
	if createErr != nil {
		return nil, createErr
	}

	// AWS allows passing initial Tags at CreateLoadBalancer time.
	if initialTags := parseTagKVs(vals, "Tags.member"); len(initialTags) > 0 {
		if tagErr := h.Backend.AddTags(ctx, []string{name}, initialTags); tagErr != nil {
			return nil, tagErr
		}
	}

	return &createLoadBalancerResponse{
		Xmlns: elbXMLNS,
		Result: createLoadBalancerResult{
			DNSName: lb.DNSName,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-" + name},
	}, nil
}

func (h *Handler) handleDeleteLoadBalancer(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("LoadBalancerName")
	if name == "" {
		return nil, fmt.Errorf("%w: LoadBalancerName is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteLoadBalancer(ctx, name); err != nil {
		return nil, err
	}

	return &deleteLoadBalancerResponse{
		Xmlns:            elbXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-delete-" + name},
	}, nil
}

func (h *Handler) handleDescribeLoadBalancers(ctx context.Context, vals url.Values) (any, error) {
	names := parseMembers(vals, "LoadBalancerNames.member")

	lbs, err := h.Backend.DescribeLoadBalancers(ctx, names)
	if err != nil {
		return nil, err
	}

	// Pagination: Marker (name of last LB on previous page) and PageSize (1-400).
	const maxPageSize = 400

	pageSize := maxPageSize

	if ps := vals.Get("PageSize"); ps != "" {
		if n, parseErr := strconv.Atoi(ps); parseErr == nil && n > 0 && n <= maxPageSize {
			pageSize = n
		}
	}

	marker := vals.Get("Marker")
	startIdx := 0

	if marker != "" {
		offset, decErr := decodePageMarker(marker)
		if decErr != nil {
			return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, decErr.Error())
		}

		startIdx = offset
	}

	if startIdx > len(lbs) {
		startIdx = len(lbs)
	}

	lbs = lbs[startIdx:]

	nextMarker := ""

	if len(lbs) > pageSize {
		nextMarker = encodePageMarker(startIdx + pageSize)
		lbs = lbs[:pageSize]
	}

	members := make([]xmlLoadBalancerDescription, 0, len(lbs))
	for i := range lbs {
		members = append(members, toXMLLoadBalancer(&lbs[i]))
	}

	return &describeLoadBalancersResponse{
		Xmlns: elbXMLNS,
		Result: describeLoadBalancersResult{
			LoadBalancerDescriptions: xmlLoadBalancerList{Members: members},
			NextMarker:               nextMarker,
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-describe"},
	}, nil
}

func (h *Handler) handleDescribeAccountLimits(ctx context.Context, _ url.Values) (any, error) {
	limits, err := h.Backend.DescribeAccountLimits(ctx)
	if err != nil {
		return nil, err
	}

	xmlLimits := make([]xmlAccountLimit, 0, len(limits))
	for _, l := range limits {
		xmlLimits = append(xmlLimits, xmlAccountLimit(l))
	}

	return &describeAccountLimitsResponse{
		Xmlns: elbXMLNS,
		Result: describeAccountLimitsResult{
			Limits: xmlAccountLimitList{Members: xmlLimits},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elb-acctlimits"},
	}, nil
}

// encodePageMarker encodes an integer offset as an opaque base64 page marker.
func encodePageMarker(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// decodePageMarker decodes a page marker produced by encodePageMarker.
func decodePageMarker(marker string) (int, error) {
	b, err := base64.StdEncoding.DecodeString(marker)
	if err != nil {
		return 0, fmt.Errorf("invalid Marker: %w", err)
	}

	n, err := strconv.Atoi(string(b))
	if err != nil {
		return 0, fmt.Errorf("invalid Marker payload: %w", err)
	}

	if n < 0 {
		return 0, fmt.Errorf("%w: invalid Marker: negative offset", ErrInvalidParameter)
	}

	return n, nil
}

// computeSourceSecurityGroup returns the SourceSecurityGroup XML value for lb.
// VPC LBs use the account ID as owner-alias; EC2-Classic LBs use the well-known
// Amazon-managed group "amazon-elb" / "amazon-elb-sg".
func computeSourceSecurityGroup(lb *LoadBalancer) xmlSourceSecurityGroup {
	if lb.IsVPC {
		return xmlSourceSecurityGroup{
			GroupName:  "default",
			OwnerAlias: lb.AccountID,
		}
	}

	return xmlSourceSecurityGroup{
		GroupName:  "amazon-elb-sg",
		OwnerAlias: "amazon-elb",
	}
}

// toXMLLoadBalancer converts a LoadBalancer to its XML representation.
func toXMLLoadBalancer(lb *LoadBalancer) xmlLoadBalancerDescription {
	azs := make([]xmlStringValue, 0, len(lb.AvailabilityZones))
	for _, az := range lb.AvailabilityZones {
		azs = append(azs, xmlStringValue{Value: az})
	}

	sgs := make([]xmlStringValue, 0, len(lb.SecurityGroups))
	for _, sg := range lb.SecurityGroups {
		sgs = append(sgs, xmlStringValue{Value: sg})
	}

	subnets := make([]xmlStringValue, 0, len(lb.Subnets))
	for _, s := range lb.Subnets {
		subnets = append(subnets, xmlStringValue{Value: s})
	}

	listeners := make([]xmlListenerDescription, 0, len(lb.Listeners))
	for _, l := range lb.Listeners {
		policyMembers := make([]xmlStringValue, 0, len(l.PolicyNames))
		for _, p := range l.PolicyNames {
			policyMembers = append(policyMembers, xmlStringValue{Value: p})
		}

		listeners = append(listeners, xmlListenerDescription{
			Listener: xmlListener{
				Protocol:         l.Protocol,
				InstanceProtocol: l.InstanceProtocol,
				LoadBalancerPort: l.LoadBalancerPort,
				InstancePort:     l.InstancePort,
				SSLCertificateID: l.SSLCertificateID,
			},
			PolicyNames: xmlStringValueList{Members: policyMembers},
		})
	}

	bsds := make([]xmlBackendServerDescription, 0, len(lb.BackendServerDescriptions))
	for _, bsd := range lb.BackendServerDescriptions {
		policyMembers := make([]xmlStringValue, 0, len(bsd.PolicyNames))
		for _, p := range bsd.PolicyNames {
			policyMembers = append(policyMembers, xmlStringValue{Value: p})
		}

		bsds = append(bsds, xmlBackendServerDescription{
			InstancePort: bsd.InstancePort,
			PolicyNames:  xmlStringValueList{Members: policyMembers},
		})
	}

	instances := toXMLInstances(lb.Instances)

	hc := xmlHealthCheck{}
	if lb.HealthCheck != nil {
		hc = toXMLHealthCheck(lb.HealthCheck)
	}

	return xmlLoadBalancerDescription{
		LoadBalancerName:          lb.LoadBalancerName,
		DNSName:                   lb.DNSName,
		CanonicalHostedZoneName:   lb.CanonicalHostedZoneName,
		CanonicalHostedZoneNameID: lb.CanonicalHostedZoneNameID,
		CreatedTime:               lb.CreatedTime.UTC().Format(time.RFC3339),
		Scheme:                    lb.Scheme,
		VPCId:                     lb.VPCId,
		SourceSecurityGroup:       computeSourceSecurityGroup(lb),
		AvailabilityZones:         xmlStringValueList{Members: azs},
		SecurityGroups:            xmlStringValueList{Members: sgs},
		Subnets:                   xmlStringValueList{Members: subnets},
		ListenerDescriptions:      xmlListenerDescriptionList{Members: listeners},
		BackendServerDescriptions: xmlBackendServerDescriptionList{Members: bsds},
		Instances:                 xmlInstanceList{Members: instances},
		HealthCheck:               hc,
	}
}

type xmlSourceSecurityGroup struct {
	GroupName  string `xml:"GroupName"`
	OwnerAlias string `xml:"OwnerAlias"`
}

type xmlListener struct {
	Protocol         string `xml:"Protocol"`
	InstanceProtocol string `xml:"InstanceProtocol"`
	SSLCertificateID string `xml:"SSLCertificateId,omitempty"`
	LoadBalancerPort int32  `xml:"LoadBalancerPort"`
	InstancePort     int32  `xml:"InstancePort"`
}

type xmlListenerDescription struct {
	Listener    xmlListener        `xml:"Listener"`
	PolicyNames xmlStringValueList `xml:"PolicyNames"`
}

type xmlListenerDescriptionList struct {
	Members []xmlListenerDescription `xml:"member"`
}

type xmlBackendServerDescription struct {
	PolicyNames  xmlStringValueList `xml:"PolicyNames"`
	InstancePort int32              `xml:"InstancePort"`
}

type xmlBackendServerDescriptionList struct {
	Members []xmlBackendServerDescription `xml:"member"`
}

type xmlLoadBalancerDescription struct {
	LoadBalancerName          string                          `xml:"LoadBalancerName"`
	DNSName                   string                          `xml:"DNSName"`
	CanonicalHostedZoneName   string                          `xml:"CanonicalHostedZoneName"`
	CanonicalHostedZoneNameID string                          `xml:"CanonicalHostedZoneNameID"`
	CreatedTime               string                          `xml:"CreatedTime"`
	Scheme                    string                          `xml:"Scheme"`
	VPCId                     string                          `xml:"VPCId,omitempty"`
	AvailabilityZones         xmlStringValueList              `xml:"AvailabilityZones"`
	SecurityGroups            xmlStringValueList              `xml:"SecurityGroups"`
	Subnets                   xmlStringValueList              `xml:"Subnets"`
	SourceSecurityGroup       xmlSourceSecurityGroup          `xml:"SourceSecurityGroup"`
	ListenerDescriptions      xmlListenerDescriptionList      `xml:"ListenerDescriptions"`
	BackendServerDescriptions xmlBackendServerDescriptionList `xml:"BackendServerDescriptions"`
	Instances                 xmlInstanceList                 `xml:"Instances"`
	HealthCheck               xmlHealthCheck                  `xml:"HealthCheck"`
}

type xmlLoadBalancerList struct {
	Members []xmlLoadBalancerDescription `xml:"member"`
}

type createLoadBalancerResult struct {
	DNSName string `xml:"DNSName"`
}

type createLoadBalancerResponse struct {
	XMLName          xml.Name                 `xml:"CreateLoadBalancerResponse"`
	Xmlns            string                   `xml:"xmlns,attr"`
	Result           createLoadBalancerResult `xml:"CreateLoadBalancerResult"`
	ResponseMetadata xmlResponseMetadata      `xml:"ResponseMetadata"`
}

type deleteLoadBalancerResult struct{}

type deleteLoadBalancerResponse struct {
	XMLName          xml.Name                 `xml:"DeleteLoadBalancerResponse"`
	Xmlns            string                   `xml:"xmlns,attr"`
	Result           deleteLoadBalancerResult `xml:"DeleteLoadBalancerResult"`
	ResponseMetadata xmlResponseMetadata      `xml:"ResponseMetadata"`
}

type describeLoadBalancersResult struct {
	NextMarker               string              `xml:"NextMarker,omitempty"`
	LoadBalancerDescriptions xmlLoadBalancerList `xml:"LoadBalancerDescriptions"`
}

type describeLoadBalancersResponse struct {
	XMLName          xml.Name                    `xml:"DescribeLoadBalancersResponse"`
	Xmlns            string                      `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata         `xml:"ResponseMetadata"`
	Result           describeLoadBalancersResult `xml:"DescribeLoadBalancersResult"`
}

type xmlAccountLimit struct {
	Name string `xml:"Name"`
	Max  string `xml:"Max"`
}

type xmlAccountLimitList struct {
	Members []xmlAccountLimit `xml:"member"`
}

type describeAccountLimitsResult struct {
	Limits xmlAccountLimitList `xml:"Limits"`
}

type describeAccountLimitsResponse struct {
	XMLName          xml.Name                    `xml:"DescribeAccountLimitsResponse"`
	Xmlns            string                      `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata         `xml:"ResponseMetadata"`
	Result           describeAccountLimitsResult `xml:"DescribeAccountLimitsResult"`
}
