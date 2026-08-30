package autoscaling

import (
	"encoding/xml"
	"net/url"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultTSMaxRecords is DescribeTrafficSources's documented max page size
// (api_op_DescribeTrafficSources.go: "The maximum value is 50"); no distinct default is
// documented, so it's treated the same as the max, matching DescribeAutoScalingInstances.
const defaultTSMaxRecords = 50

func (h *Handler) handleAttachTrafficSources(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	tss := parseTrafficSources(vals)

	if err := h.Backend.AttachTrafficSources(groupName, tss); err != nil {
		return nil, err
	}

	return &attachTrafficSourcesResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-attach-traffic-sources"},
	}, nil
}

type attachTrafficSourcesResponse struct {
	XMLName          xml.Name            `xml:"AttachTrafficSourcesResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	Result           emptyResultXML      `xml:"AttachTrafficSourcesResult"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

func (h *Handler) handleDescribeTrafficSources(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	trafficSourceType := vals.Get("TrafficSourceType")

	sources, err := h.Backend.DescribeTrafficSources(groupName, trafficSourceType)
	if err != nil {
		return nil, err
	}

	maxRecords := defaultTSMaxRecords
	if v := vals.Get("MaxRecords"); v != "" {
		if n, parseErr := parseIntVal(v); parseErr == nil && n > 0 {
			maxRecords = min(int(n), defaultTSMaxRecords)
		}
	}

	p := page.New(sources, vals.Get("NextToken"), maxRecords, defaultTSMaxRecords)

	members := make([]xmlTrafficSourceState, 0, len(p.Data))
	for _, s := range p.Data {
		members = append(members, xmlTrafficSourceState(s))
	}

	return &describeTrafficSourcesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeTrafficSourcesResult{
			NextToken:      p.Next,
			TrafficSources: xmlTrafficSourceStateList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-traffic-sources"},
	}, nil
}

func (h *Handler) handleDetachTrafficSources(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	tss := parseTrafficSources(vals)

	if err := h.Backend.DetachTrafficSources(groupName, tss); err != nil {
		return nil, err
	}

	return &detachTrafficSourcesResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-detach-traffic-sources"},
	}, nil
}

type xmlTrafficSourceState struct {
	Identifier string `xml:"Identifier"`
	Type       string `xml:"Type"`
	State      string `xml:"State"`
}

type xmlTrafficSourceStateList struct {
	Members []xmlTrafficSourceState `xml:"member"`
}

type describeTrafficSourcesResult struct {
	NextToken      string                    `xml:"NextToken,omitempty"`
	TrafficSources xmlTrafficSourceStateList `xml:"TrafficSources"`
}

type describeTrafficSourcesResponse struct {
	XMLName          xml.Name                     `xml:"DescribeTrafficSourcesResponse"`
	Xmlns            string                       `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata          `xml:"ResponseMetadata"`
	Result           describeTrafficSourcesResult `xml:"DescribeTrafficSourcesResult"`
}

type detachTrafficSourcesResponse struct {
	XMLName          xml.Name            `xml:"DetachTrafficSourcesResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	Result           emptyResultXML      `xml:"DetachTrafficSourcesResult"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}
