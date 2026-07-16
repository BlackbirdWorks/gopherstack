package ses

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleCreateConfigurationSetEventDestination(vals url.Values, reqID string) (any, error) {
	dest := EventDestination{
		Name:               vals.Get("EventDestination.Name"),
		Enabled:            vals.Get("EventDestination.Enabled") == boolTrue,
		MatchingEventTypes: parseSESMemberList(vals, "EventDestination.MatchingEventTypes"),
		SNSTopicARN:        vals.Get("EventDestination.SNSDestination.TopicARN"),
	}

	configSetName := vals.Get("ConfigurationSetName")

	if err := h.Backend.CreateConfigurationSetEventDestination(configSetName, dest); err != nil {
		return nil, err
	}

	return &createConfigurationSetEventDestinationResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleDeleteConfigurationSetEventDestination(vals url.Values, reqID string) (any, error) {
	configSetName := vals.Get("ConfigurationSetName")
	destName := vals.Get("EventDestinationName")

	if err := h.Backend.DeleteConfigurationSetEventDestination(configSetName, destName); err != nil {
		return nil, err
	}

	return &deleteConfigurationSetEventDestinationResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

type createConfigurationSetEventDestinationResponse struct {
	XMLName   xml.Name `xml:"CreateConfigurationSetEventDestinationResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type deleteConfigurationSetEventDestinationResponse struct {
	XMLName   xml.Name `xml:"DeleteConfigurationSetEventDestinationResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleUpdateConfigurationSetEventDestination(vals url.Values, reqID string) (any, error) {
	dest := EventDestination{
		Name:               vals.Get("EventDestination.Name"),
		Enabled:            vals.Get("EventDestination.Enabled") == boolTrue,
		MatchingEventTypes: parseSESMemberList(vals, "EventDestination.MatchingEventTypes"),
		SNSTopicARN:        vals.Get("EventDestination.SNSDestination.TopicARN"),
	}

	if err := h.Backend.UpdateConfigurationSetEventDestination(vals.Get("ConfigurationSetName"), dest); err != nil {
		return nil, err
	}

	return newEmptyResponseWithResult("UpdateConfigurationSetEventDestination", reqID), nil
}

type xmlEventDestination struct {
	Name               string        `xml:"Name"`
	SNSTopicARN        string        `xml:"SNSDestination>TopicARN,omitempty"`
	MatchingEventTypes xmlMemberList `xml:"MatchingEventTypes"`
	Enabled            bool          `xml:"Enabled"`
}

type xmlEventDestinationList struct {
	Members []xmlEventDestination `xml:"member"`
}
