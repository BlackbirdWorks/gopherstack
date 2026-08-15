package elbv2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"sort"
	"strconv"
)

func (h *Handler) handleCreateTargetGroup(vals url.Values) (any, error) {
	name := vals.Get("Name")
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	// Lambda target groups do not require a port.
	targetType := vals.Get("TargetType")

	port, err := parseTGPort(targetType, vals.Get("Port"))
	if err != nil {
		return nil, err
	}

	tagKVs := parseTagKVs(vals)

	hcInterval, err := parseOptionalInt32(vals, "HealthCheckIntervalSeconds")
	if err != nil {
		return nil, fmt.Errorf("%w: invalid HealthCheckIntervalSeconds", ErrInvalidParameter)
	}

	hcTimeout, err := parseOptionalInt32(vals, "HealthCheckTimeoutSeconds")
	if err != nil {
		return nil, fmt.Errorf("%w: invalid HealthCheckTimeoutSeconds", ErrInvalidParameter)
	}

	healthyThreshold, err := parseOptionalInt32(vals, "HealthyThresholdCount")
	if err != nil {
		return nil, fmt.Errorf("%w: invalid HealthyThresholdCount", ErrInvalidParameter)
	}

	unhealthyThreshold, err := parseOptionalInt32(vals, "UnhealthyThresholdCount")
	if err != nil {
		return nil, fmt.Errorf("%w: invalid UnhealthyThresholdCount", ErrInvalidParameter)
	}

	hcEnabledStr := vals.Get("HealthCheckEnabled")
	var hcEnabled bool
	if hcEnabledStr == "" {
		hcEnabled = vals.Get("TargetType") != targetTypeLambda
	} else {
		hcEnabled = hcEnabledStr != attrValueFalse
	}

	tg, createErr := h.Backend.CreateTargetGroup(CreateTargetGroupInput{
		Name:                name,
		Protocol:            vals.Get("Protocol"),
		ProtocolVersion:     vals.Get("ProtocolVersion"),
		Port:                port,
		VpcID:               vals.Get("VpcId"),
		TargetType:          vals.Get("TargetType"),
		Tags:                tagKVs,
		HealthCheckProtocol: vals.Get("HealthCheckProtocol"),
		HealthCheckPort:     vals.Get("HealthCheckPort"),
		HealthCheckPath:     vals.Get("HealthCheckPath"),
		Matcher: Matcher{
			HTTPCode: vals.Get("Matcher.HTTPCode"),
			GrpcCode: vals.Get("Matcher.GrpcCode"),
		},
		HealthCheckIntervalSeconds: hcInterval,
		HealthCheckTimeoutSeconds:  hcTimeout,
		HealthyThresholdCount:      healthyThreshold,
		UnhealthyThresholdCount:    unhealthyThreshold,
		HealthCheckEnabled:         hcEnabled,
	})
	if createErr != nil {
		return nil, createErr
	}

	return &createTargetGroupResponse{
		Xmlns: elbv2XMLNS,
		Result: createTargetGroupResult{
			TargetGroups: xmlTargetGroupList{
				Members: []xmlTargetGroup{toXMLTargetGroup(tg)},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-create-tg-" + name},
	}, nil
}

func (h *Handler) handleDeleteTargetGroup(vals url.Values) (any, error) {
	tgArn := vals.Get("TargetGroupArn")
	if tgArn == "" {
		return nil, fmt.Errorf("%w: TargetGroupArn is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteTargetGroup(tgArn); err != nil {
		return nil, err
	}

	return &deleteTargetGroupResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-delete-tg"},
	}, nil
}

func (h *Handler) handleDescribeTargetGroups(vals url.Values) (any, error) {
	arns := parseMembers(vals, "TargetGroupArns.member")
	names := parseMembers(vals, "Names.member")
	lbArn := vals.Get("LoadBalancerArn")

	tgs, err := h.Backend.DescribeTargetGroups(arns, names, lbArn)
	if err != nil {
		return nil, err
	}

	marker, pageSize := parsePagination(vals)

	startIdx := 0
	if marker != "" {
		for i, tg := range tgs {
			if tg.TargetGroupArn == marker {
				startIdx = i + 1

				break
			}
		}
	}

	tgs = tgs[startIdx:]

	var nextMarker string
	if len(tgs) > pageSize {
		nextMarker = tgs[pageSize-1].TargetGroupArn
		tgs = tgs[:pageSize]
	}

	members := make([]xmlTargetGroup, 0, len(tgs))
	for i := range tgs {
		members = append(members, toXMLTargetGroup(&tgs[i]))
	}

	return &describeTargetGroupsResponse{
		Xmlns: elbv2XMLNS,
		Result: describeTargetGroupsResult{
			NextMarker:   nextMarker,
			TargetGroups: xmlTargetGroupList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-tgs"},
	}, nil
}

func (h *Handler) handleModifyTargetGroup(vals url.Values) (any, error) {
	tgArn := vals.Get("TargetGroupArn")
	if tgArn == "" {
		return nil, fmt.Errorf("%w: TargetGroupArn is required", ErrInvalidParameter)
	}

	hcInterval, mErr := parseOptionalInt32(vals, "HealthCheckIntervalSeconds")
	if mErr != nil {
		return nil, fmt.Errorf("%w: invalid HealthCheckIntervalSeconds", ErrInvalidParameter)
	}

	hcTimeout, mErr := parseOptionalInt32(vals, "HealthCheckTimeoutSeconds")
	if mErr != nil {
		return nil, fmt.Errorf("%w: invalid HealthCheckTimeoutSeconds", ErrInvalidParameter)
	}

	healthyThreshold, mErr := parseOptionalInt32(vals, "HealthyThresholdCount")
	if mErr != nil {
		return nil, fmt.Errorf("%w: invalid HealthyThresholdCount", ErrInvalidParameter)
	}

	unhealthyThreshold, mErr := parseOptionalInt32(vals, "UnhealthyThresholdCount")
	if mErr != nil {
		return nil, fmt.Errorf("%w: invalid UnhealthyThresholdCount", ErrInvalidParameter)
	}

	// HealthCheckEnabled is optional: only update the field when the parameter is present.
	var hcEnabled *bool
	if hce := vals.Get("HealthCheckEnabled"); hce != "" {
		v := hce == attrValueTrue
		hcEnabled = &v
	}

	tg, err := h.Backend.ModifyTargetGroup(ModifyTargetGroupInput{
		TargetGroupArn:      tgArn,
		HealthCheckProtocol: vals.Get("HealthCheckProtocol"),
		HealthCheckPort:     vals.Get("HealthCheckPort"),
		HealthCheckPath:     vals.Get("HealthCheckPath"),
		Matcher: Matcher{
			HTTPCode: vals.Get("Matcher.HTTPCode"),
			GrpcCode: vals.Get("Matcher.GrpcCode"),
		},
		HealthCheckEnabled:         hcEnabled,
		HealthCheckIntervalSeconds: hcInterval,
		HealthCheckTimeoutSeconds:  hcTimeout,
		HealthyThresholdCount:      healthyThreshold,
		UnhealthyThresholdCount:    unhealthyThreshold,
	})
	if err != nil {
		return nil, err
	}

	return &modifyTargetGroupResponse{
		Xmlns: elbv2XMLNS,
		Result: modifyTargetGroupResult{
			TargetGroups: xmlTargetGroupList{
				Members: []xmlTargetGroup{toXMLTargetGroup(tg)},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-tg"},
	}, nil
}

func (h *Handler) handleModifyTargetGroupAttributes(vals url.Values) (any, error) {
	tgArn := vals.Get("TargetGroupArn")
	if tgArn == "" {
		return nil, fmt.Errorf("%w: TargetGroupArn is required", ErrInvalidParameter)
	}

	attrs := parseKVAttrs(vals, "Attributes.member")

	tg, err := h.Backend.ModifyTargetGroupAttributes(tgArn, attrs)
	if err != nil {
		return nil, err
	}

	members := sortedTGAttributes(tg.TargetGroupAttributes)

	return &modifyTargetGroupAttributesResponse{
		Xmlns:            elbv2XMLNS,
		Result:           modifyTargetGroupAttributesResult{Attributes: xmlTGAttributeList{Members: members}},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-tg-attrs"},
	}, nil
}

func (h *Handler) handleDescribeTargetGroupAttributes(vals url.Values) (any, error) {
	tgArn := vals.Get("TargetGroupArn")
	if tgArn == "" {
		return nil, fmt.Errorf("%w: TargetGroupArn is required", ErrInvalidParameter)
	}

	attrs, err := h.Backend.DescribeTargetGroupAttributes(tgArn)
	if err != nil {
		return nil, err
	}

	members := make([]xmlTGAttribute, 0, len(attrs))
	for k, v := range attrs {
		members = append(members, xmlTGAttribute{Key: k, Value: v})
	}

	sort.Slice(members, func(i, j int) bool { return members[i].Key < members[j].Key })

	return &describeTargetGroupAttributesResponse{
		Xmlns: elbv2XMLNS,
		Result: describeTargetGroupAttributesResult{
			Attributes: xmlTGAttributeList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-tg-attrs"},
	}, nil
}

// parseTGPort parses the Port form value for a CreateTargetGroup request.
// Lambda target groups do not require a port; all other types do.
func parseTGPort(targetType, portStr string) (int32, error) {
	if targetType == targetTypeLambda {
		if portStr == "" {
			return 0, nil
		}
	} else if portStr == "" {
		return 0, fmt.Errorf("%w: Port is required", ErrInvalidParameter)
	}

	p, err := parseInt32(portStr)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid Port", ErrInvalidParameter)
	}

	if targetType != targetTypeLambda {
		if vErr := validatePort(p); vErr != nil {
			return 0, vErr
		}
	}

	return p, nil
}

// parseOptionalInt32 parses an integer form field, returning an error only when
// the field is present but cannot be parsed. An absent field returns (0, nil).
func parseOptionalInt32(vals url.Values, key string) (int32, error) {
	s := vals.Get(key)
	if s == "" {
		return 0, nil
	}

	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, err
	}

	return int32(n), nil
}

func toXMLTargetGroup(tg *TargetGroup) xmlTargetGroup {
	xtg := xmlTargetGroup{
		TargetGroupArn:             tg.TargetGroupArn,
		TargetGroupName:            tg.TargetGroupName,
		Protocol:                   tg.Protocol,
		ProtocolVersion:            tg.ProtocolVersion,
		Port:                       tg.Port,
		VpcID:                      tg.VpcID,
		TargetType:                 tg.TargetType,
		HealthCheckProtocol:        tg.HealthCheckProtocol,
		HealthCheckPort:            tg.HealthCheckPort,
		HealthCheckPath:            tg.HealthCheckPath,
		HealthCheckEnabled:         tg.HealthCheckEnabled,
		HealthCheckIntervalSeconds: tg.HealthCheckIntervalSeconds,
		HealthCheckTimeoutSeconds:  tg.HealthCheckTimeoutSeconds,
		HealthyThresholdCount:      tg.HealthyThresholdCount,
		UnhealthyThresholdCount:    tg.UnhealthyThresholdCount,
		CrossZoneLoadBalancing:     tg.CrossZoneLoadBalancing,
	}

	if tg.Matcher.HTTPCode != "" || tg.Matcher.GrpcCode != "" {
		xtg.Matcher = &xmlMatcher{
			HTTPCode: tg.Matcher.HTTPCode,
			GrpcCode: tg.Matcher.GrpcCode,
		}
	} else if tg.HealthCheckProtocol == "HTTP" || tg.HealthCheckProtocol == "HTTPS" {
		// Default matcher for HTTP/HTTPS health checks.
		xtg.Matcher = &xmlMatcher{HTTPCode: "200"}
	}

	if len(tg.LoadBalancerArns) > 0 {
		lbArns := make([]xmlStringValue, 0, len(tg.LoadBalancerArns))
		for _, a := range tg.LoadBalancerArns {
			lbArns = append(lbArns, xmlStringValue{Value: a})
		}

		xtg.LoadBalancerArns = &xmlStringList{Members: lbArns}
	}

	return xtg
}

type xmlTargetGroup struct {
	Matcher                    *xmlMatcher    `xml:"Matcher,omitempty"`
	LoadBalancerArns           *xmlStringList `xml:"LoadBalancerArns,omitempty"`
	TargetGroupArn             string         `xml:"TargetGroupArn"`
	TargetGroupName            string         `xml:"TargetGroupName"`
	Protocol                   string         `xml:"Protocol"`
	ProtocolVersion            string         `xml:"ProtocolVersion,omitempty"`
	VpcID                      string         `xml:"VpcId,omitempty"`
	TargetType                 string         `xml:"TargetType"`
	HealthCheckProtocol        string         `xml:"HealthCheckProtocol"`
	HealthCheckPort            string         `xml:"HealthCheckPort"`
	HealthCheckPath            string         `xml:"HealthCheckPath,omitempty"`
	Port                       int32          `xml:"Port,omitempty"`
	HealthCheckIntervalSeconds int32          `xml:"HealthCheckIntervalSeconds,omitempty"`
	HealthCheckTimeoutSeconds  int32          `xml:"HealthCheckTimeoutSeconds,omitempty"`
	HealthyThresholdCount      int32          `xml:"HealthyThresholdCount,omitempty"`
	UnhealthyThresholdCount    int32          `xml:"UnhealthyThresholdCount,omitempty"`
	HealthCheckEnabled         bool           `xml:"HealthCheckEnabled"`
	CrossZoneLoadBalancing     bool           `xml:"CrossZoneLoadBalancing"`
}

type xmlTargetGroupList struct {
	Members []xmlTargetGroup `xml:"member"`
}

type createTargetGroupResult struct {
	TargetGroups xmlTargetGroupList `xml:"TargetGroups"`
}

type createTargetGroupResponse struct {
	XMLName          xml.Name                `xml:"CreateTargetGroupResponse"`
	Xmlns            string                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata     `xml:"ResponseMetadata"`
	Result           createTargetGroupResult `xml:"CreateTargetGroupResult"`
}

type deleteTargetGroupResponse struct {
	Result           emptyResultXML      `xml:"DeleteTargetGroupResult"`
	XMLName          xml.Name            `xml:"DeleteTargetGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type describeTargetGroupsResult struct {
	NextMarker   string             `xml:"NextMarker,omitempty"`
	TargetGroups xmlTargetGroupList `xml:"TargetGroups"`
}

type describeTargetGroupsResponse struct {
	XMLName          xml.Name                   `xml:"DescribeTargetGroupsResponse"`
	Xmlns            string                     `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata        `xml:"ResponseMetadata"`
	Result           describeTargetGroupsResult `xml:"DescribeTargetGroupsResult"`
}

type modifyTargetGroupResult struct {
	TargetGroups xmlTargetGroupList `xml:"TargetGroups"`
}

type modifyTargetGroupResponse struct {
	XMLName          xml.Name                `xml:"ModifyTargetGroupResponse"`
	Xmlns            string                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata     `xml:"ResponseMetadata"`
	Result           modifyTargetGroupResult `xml:"ModifyTargetGroupResult"`
}

type xmlTGAttribute struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type xmlTGAttributeList struct {
	Members []xmlTGAttribute `xml:"member"`
}

type modifyTargetGroupAttributesResult struct {
	Attributes xmlTGAttributeList `xml:"Attributes"`
}

type modifyTargetGroupAttributesResponse struct {
	XMLName          xml.Name                          `xml:"ModifyTargetGroupAttributesResponse"`
	Xmlns            string                            `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata               `xml:"ResponseMetadata"`
	Result           modifyTargetGroupAttributesResult `xml:"ModifyTargetGroupAttributesResult"`
}

type describeTargetGroupAttributesResult struct {
	Attributes xmlTGAttributeList `xml:"Attributes"`
}

type describeTargetGroupAttributesResponse struct {
	XMLName          xml.Name                            `xml:"DescribeTargetGroupAttributesResponse"`
	Xmlns            string                              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                 `xml:"ResponseMetadata"`
	Result           describeTargetGroupAttributesResult `xml:"DescribeTargetGroupAttributesResult"`
}

// sortedTGAttributes converts a map to a sorted xmlTGAttribute slice.
func sortedTGAttributes(m map[string]string) []xmlTGAttribute {
	out := make([]xmlTGAttribute, 0, len(m))
	for k, v := range m {
		out = append(out, xmlTGAttribute{Key: k, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })

	return out
}
