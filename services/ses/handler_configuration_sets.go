package ses

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

func (h *Handler) handleCreateConfigurationSet(vals url.Values, reqID string) (any, error) {
	name := vals.Get("ConfigurationSet.Name")

	if err := h.Backend.CreateConfigurationSet(name); err != nil {
		return nil, err
	}

	return &createConfigurationSetResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleDeleteConfigurationSet(vals url.Values, reqID string) (any, error) {
	name := vals.Get("ConfigurationSetName")

	if err := h.Backend.DeleteConfigurationSet(name); err != nil {
		return nil, err
	}

	return &deleteConfigurationSetResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleListConfigurationSets(vals url.Values, reqID string) any {
	nextToken := vals.Get("NextToken")
	maxItems := 0

	if s := vals.Get("MaxItems"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxItems = n
		}
	}

	p := h.Backend.ListConfigurationSets(nextToken, maxItems)
	members := make([]xmlMember, 0, len(p.Data))

	for _, name := range p.Data {
		members = append(members, xmlMember{Value: name})
	}

	return &listConfigurationSetsResponse{
		Xmlns: sesXMLNS,
		Result: listConfigurationSetsResult{
			ConfigurationSets: xmlMemberList{Members: members},
			NextToken:         p.Next,
		},
		RequestID: reqID,
	}
}

type createConfigurationSetResponse struct {
	XMLName   xml.Name `xml:"CreateConfigurationSetResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type deleteConfigurationSetResponse struct {
	XMLName   xml.Name `xml:"DeleteConfigurationSetResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type listConfigurationSetsResult struct {
	NextToken         string        `xml:"NextToken,omitempty"`
	ConfigurationSets xmlMemberList `xml:"ConfigurationSets"`
}

type listConfigurationSetsResponse struct {
	XMLName   xml.Name                    `xml:"ListConfigurationSetsResponse"`
	Xmlns     string                      `xml:"xmlns,attr"`
	RequestID string                      `xml:"ResponseMetadata>RequestId"`
	Result    listConfigurationSetsResult `xml:"ListConfigurationSetsResult"`
}

func (h *Handler) handleCreateConfigurationSetTrackingOptions(vals url.Values, reqID string) (any, error) {
	configSetName := vals.Get("ConfigurationSetName")
	customRedirectDomain := vals.Get("TrackingOptions.CustomRedirectDomain")

	if err := h.Backend.CreateConfigurationSetTrackingOptions(configSetName, customRedirectDomain); err != nil {
		return nil, err
	}

	return &createConfigurationSetTrackingOptionsResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleDeleteConfigurationSetTrackingOptions(vals url.Values, reqID string) (any, error) {
	configSetName := vals.Get("ConfigurationSetName")

	if err := h.Backend.DeleteConfigurationSetTrackingOptions(configSetName); err != nil {
		return nil, err
	}

	return &deleteConfigurationSetTrackingOptionsResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

type createConfigurationSetTrackingOptionsResponse struct {
	XMLName   xml.Name `xml:"CreateConfigurationSetTrackingOptionsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type deleteConfigurationSetTrackingOptionsResponse struct {
	XMLName   xml.Name `xml:"DeleteConfigurationSetTrackingOptionsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleDescribeConfigurationSet(vals url.Values, reqID string) (any, error) {
	desc, err := h.Backend.DescribeConfigurationSet(vals.Get("ConfigurationSetName"))
	if err != nil {
		return nil, err
	}

	dests := make([]xmlEventDestination, 0, len(desc.EventDestinations))
	for _, d := range desc.EventDestinations {
		evTypes := make([]xmlMember, 0, len(d.MatchingEventTypes))
		for _, t := range d.MatchingEventTypes {
			evTypes = append(evTypes, xmlMember{Value: t})
		}

		dests = append(dests, xmlEventDestination{
			Name:               d.Name,
			Enabled:            d.Enabled,
			MatchingEventTypes: xmlMemberList{Members: evTypes},
			SNSTopicARN:        d.SNSTopicARN,
		})
	}

	result := describeConfigurationSetResult{
		ConfigurationSet:  xmlConfigurationSet{Name: desc.Name},
		EventDestinations: xmlEventDestinationList{Members: dests},
		ReputationOptions: &xmlReputationOptions{
			SendingEnabled:           desc.SendingEnabled,
			ReputationMetricsEnabled: desc.ReputationMetricsEnabled,
		},
	}

	if desc.TrackingOptions != nil {
		result.TrackingOptions = &xmlTrackingOptions{CustomRedirectDomain: desc.TrackingOptions.CustomRedirectDomain}
	}

	if desc.DeliveryOptions != nil {
		result.DeliveryOptions = &xmlDeliveryOptions{TLSPolicy: desc.DeliveryOptions.TLSPolicy}
	}

	return &describeConfigurationSetResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result:    result,
	}, nil
}

func (h *Handler) handlePutConfigurationSetDeliveryOptions(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.PutConfigurationSetDeliveryOptions(
		vals.Get("ConfigurationSetName"),
		vals.Get("DeliveryOptions.TlsPolicy"),
	); err != nil {
		return nil, err
	}

	return newEmptyResponseWithResult("PutConfigurationSetDeliveryOptions", reqID), nil
}

func (h *Handler) handleUpdateConfigurationSetReputationMetricsEnabled(vals url.Values, reqID string) (any, error) {
	enabled := vals.Get("Enabled") == boolTrue
	configSetName := vals.Get("ConfigurationSetName")

	if err := h.Backend.UpdateConfigurationSetReputationMetricsEnabled(configSetName, enabled); err != nil {
		return nil, err
	}

	return newEmptyResponse("UpdateConfigurationSetReputationMetricsEnabled", reqID), nil
}

func (h *Handler) handleUpdateConfigurationSetSendingEnabled(vals url.Values, reqID string) (any, error) {
	enabled := vals.Get("Enabled") == boolTrue
	if err := h.Backend.UpdateConfigurationSetSendingEnabled(vals.Get("ConfigurationSetName"), enabled); err != nil {
		return nil, err
	}

	return newEmptyResponse("UpdateConfigurationSetSendingEnabled", reqID), nil
}

func (h *Handler) handleUpdateConfigurationSetTrackingOptions(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.UpdateConfigurationSetTrackingOptions(
		vals.Get("ConfigurationSetName"),
		vals.Get("TrackingOptions.CustomRedirectDomain"),
	); err != nil {
		return nil, err
	}

	return newEmptyResponseWithResult("UpdateConfigurationSetTrackingOptions", reqID), nil
}

type xmlConfigurationSet struct {
	Name string `xml:"Name"`
}

type xmlTrackingOptions struct {
	CustomRedirectDomain string `xml:"CustomRedirectDomain,omitempty"`
}

type xmlDeliveryOptions struct {
	TLSPolicy string `xml:"TlsPolicy,omitempty"`
}

type xmlReputationOptions struct {
	SendingEnabled           bool `xml:"SendingEnabled"`
	ReputationMetricsEnabled bool `xml:"ReputationMetricsEnabled"`
}

type describeConfigurationSetResult struct {
	TrackingOptions   *xmlTrackingOptions     `xml:"TrackingOptions,omitempty"`
	DeliveryOptions   *xmlDeliveryOptions     `xml:"DeliveryOptions,omitempty"`
	ReputationOptions *xmlReputationOptions   `xml:"ReputationOptions,omitempty"`
	ConfigurationSet  xmlConfigurationSet     `xml:"ConfigurationSet"`
	EventDestinations xmlEventDestinationList `xml:"EventDestinations"`
}

type describeConfigurationSetResponse struct {
	XMLName   xml.Name                       `xml:"DescribeConfigurationSetResponse"`
	Xmlns     string                         `xml:"xmlns,attr"`
	RequestID string                         `xml:"ResponseMetadata>RequestId"`
	Result    describeConfigurationSetResult `xml:"DescribeConfigurationSetResult"`
}
