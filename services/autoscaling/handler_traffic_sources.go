package autoscaling

import (
	"encoding/xml"
	"net/url"
)

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
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

func (h *Handler) handleDescribeTrafficSources(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	sources, err := h.Backend.DescribeTrafficSources(groupName)
	if err != nil {
		return nil, err
	}

	members := make([]xmlTrafficSourceState, 0, len(sources))
	for _, s := range sources {
		members = append(members, xmlTrafficSourceState(s))
	}

	return &describeTrafficSourcesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeTrafficSourcesResult{
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
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}
