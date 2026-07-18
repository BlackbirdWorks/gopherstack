package autoscaling

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleDescribeMetricCollectionTypes(_ url.Values) (any, error) {
	metrics, err := h.Backend.DescribeMetricCollectionTypes()
	if err != nil {
		return nil, err
	}

	members := make([]xmlMetricCollectionType, 0, len(metrics))
	for _, m := range metrics {
		members = append(members, xmlMetricCollectionType(m))
	}

	return &describeMetricCollectionTypesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeMetricCollectionTypesResult{
			Metrics: xmlMetricCollectionTypeList{Members: members},
			Granularities: xmlGranularityList{
				Members: []xmlGranularity{{Granularity: granularity1Minute}},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-metric-collection-types"},
	}, nil
}

func (h *Handler) handleEnableMetricsCollection(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	metrics := parseMembers(vals, "Metrics.member")
	granularity := vals.Get("Granularity")

	if err := h.Backend.EnableMetricsCollection(groupName, metrics, granularity); err != nil {
		return nil, err
	}

	return &enableMetricsCollectionResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-enable-metrics-collection"},
	}, nil
}

func (h *Handler) handleDisableMetricsCollection(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	metrics := parseMembers(vals, "Metrics.member")

	if err := h.Backend.DisableMetricsCollection(groupName, metrics); err != nil {
		return nil, err
	}

	return &disableMetricsCollectionResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-disable-metrics-collection"},
	}, nil
}

type xmlMetricCollectionType struct {
	Metric      string `xml:"Metric"`
	Granularity string `xml:"Granularity,omitempty"`
}

type xmlMetricCollectionTypeList struct {
	Members []xmlMetricCollectionType `xml:"member"`
}

type xmlGranularity struct {
	Granularity string `xml:"Granularity"`
}

type xmlGranularityList struct {
	Members []xmlGranularity `xml:"member"`
}

type describeMetricCollectionTypesResult struct {
	Metrics       xmlMetricCollectionTypeList `xml:"Metrics"`
	Granularities xmlGranularityList          `xml:"Granularities"`
}

type describeMetricCollectionTypesResponse struct {
	XMLName          xml.Name                            `xml:"DescribeMetricCollectionTypesResponse"`
	Xmlns            string                              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                 `xml:"ResponseMetadata"`
	Result           describeMetricCollectionTypesResult `xml:"DescribeMetricCollectionTypesResult"`
}

type enableMetricsCollectionResponse struct {
	XMLName          xml.Name            `xml:"EnableMetricsCollectionResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type disableMetricsCollectionResponse struct {
	XMLName          xml.Name            `xml:"DisableMetricsCollectionResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}
