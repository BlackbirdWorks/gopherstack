package ses

import (
	"encoding/xml"
	"net/url"
	"slices"
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
	members := make([]xmlConfigurationSetMember, 0, len(p.Data))

	for _, name := range p.Data {
		members = append(members, xmlConfigurationSetMember{Name: name})
	}

	return &listConfigurationSetsResponse{
		Xmlns: sesXMLNS,
		Result: listConfigurationSetsResult{
			ConfigurationSets: xmlConfigurationSetList{Members: members},
			NextToken:         p.Next,
		},
		RequestID: reqID,
	}
}

type createConfigurationSetResult struct{}

type createConfigurationSetResponse struct {
	XMLName   xml.Name                     `xml:"CreateConfigurationSetResponse"`
	Xmlns     string                       `xml:"xmlns,attr"`
	Result    createConfigurationSetResult `xml:"CreateConfigurationSetResult"`
	RequestID string                       `xml:"ResponseMetadata>RequestId"`
}

type deleteConfigurationSetResult struct{}

type deleteConfigurationSetResponse struct {
	XMLName   xml.Name                     `xml:"DeleteConfigurationSetResponse"`
	Xmlns     string                       `xml:"xmlns,attr"`
	Result    deleteConfigurationSetResult `xml:"DeleteConfigurationSetResult"`
	RequestID string                       `xml:"ResponseMetadata>RequestId"`
}

// xmlConfigurationSetMember mirrors types.ConfigurationSet, which on the
// wire is an object carrying a single Name field, not a bare string --
// confirmed against awsAwsquery_deserializeDocumentConfigurationSet in the
// pinned SDK's deserializers.go. Emitting <member>name</member> as chardata
// (the generic xmlMemberList shape) leaves ConfigurationSet.Name nil for
// every item on a real client, because the deserializer only reads a
// nested <Name> child element.
type xmlConfigurationSetMember struct {
	Name string `xml:"Name"`
}

type xmlConfigurationSetList struct {
	Members []xmlConfigurationSetMember `xml:"member"`
}

type listConfigurationSetsResult struct {
	NextToken         string                  `xml:"NextToken,omitempty"`
	ConfigurationSets xmlConfigurationSetList `xml:"ConfigurationSets"`
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

type createConfigurationSetTrackingOptionsResult struct{}

type createConfigurationSetTrackingOptionsResponse struct {
	XMLName   xml.Name                                    `xml:"CreateConfigurationSetTrackingOptionsResponse"`
	Xmlns     string                                      `xml:"xmlns,attr"`
	Result    createConfigurationSetTrackingOptionsResult `xml:"CreateConfigurationSetTrackingOptionsResult"`
	RequestID string                                      `xml:"ResponseMetadata>RequestId"`
}

type deleteConfigurationSetTrackingOptionsResult struct{}

type deleteConfigurationSetTrackingOptionsResponse struct {
	XMLName   xml.Name                                    `xml:"DeleteConfigurationSetTrackingOptionsResponse"`
	Xmlns     string                                      `xml:"xmlns,attr"`
	Result    deleteConfigurationSetTrackingOptionsResult `xml:"DeleteConfigurationSetTrackingOptionsResult"`
	RequestID string                                      `xml:"ResponseMetadata>RequestId"`
}

// configSetAttr* mirror types.ConfigurationSetAttribute
// (aws-sdk-go-v2/service/ses/types/enums.go).
const (
	configSetAttrEventDestinations = "eventDestinations"
	configSetAttrTrackingOptions   = "trackingOptions"
	configSetAttrDeliveryOptions   = "deliveryOptions"
	configSetAttrReputationOptions = "reputationOptions"
)

func (h *Handler) handleDescribeConfigurationSet(vals url.Values, reqID string) (any, error) {
	desc, err := h.Backend.DescribeConfigurationSet(vals.Get("ConfigurationSetName"))
	if err != nil {
		return nil, err
	}

	attrs := parseSESMemberList(vals, "ConfigurationSetAttributeNames")
	wants := func(name string) bool {
		return slices.Contains(attrs, name)
	}

	result := describeConfigurationSetResult{ConfigurationSet: xmlConfigurationSet{Name: desc.Name}}

	if wants(configSetAttrEventDestinations) {
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
		result.EventDestinations = xmlEventDestinationList{Members: dests}
	}

	if wants(configSetAttrReputationOptions) {
		result.ReputationOptions = &xmlReputationOptions{
			SendingEnabled:           desc.SendingEnabled,
			ReputationMetricsEnabled: desc.ReputationMetricsEnabled,
		}
	}

	if wants(configSetAttrTrackingOptions) && desc.TrackingOptions != nil {
		result.TrackingOptions = &xmlTrackingOptions{CustomRedirectDomain: desc.TrackingOptions.CustomRedirectDomain}
	}

	if wants(configSetAttrDeliveryOptions) && desc.DeliveryOptions != nil {
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
