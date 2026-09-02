package autoscaling

import (
	"encoding/xml"
	"net/url"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultNCMaxRecords and maxNCMaxRecords are DescribeNotificationConfigurations's documented
// default/max page size (api_op_DescribeNotificationConfigurations.go: "The default value is 50
// and the maximum value is 100").
const (
	defaultNCMaxRecords = 50
	maxNCMaxRecords     = 100
)

func (h *Handler) handleDescribeAutoScalingNotificationTypes(_ url.Values) (any, error) {
	types, err := h.Backend.DescribeAutoScalingNotificationTypes()
	if err != nil {
		return nil, err
	}

	members := make([]xmlStringValue, 0, len(types))
	for _, t := range types {
		members = append(members, xmlStringValue{Value: t})
	}

	return &describeAutoScalingNotificationTypesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeAutoScalingNotificationTypesResult{
			AutoScalingNotificationTypes: xmlStringValueList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-notification-types"},
	}, nil
}

func (h *Handler) handlePutNotificationConfiguration(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	topicARN := vals.Get("TopicARN")
	types := parseMembers(vals, "NotificationTypes.member")

	if err := h.Backend.PutNotificationConfiguration(groupName, topicARN, types); err != nil {
		return nil, err
	}

	return &putNotificationConfigurationResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-put-notification-configuration"},
	}, nil
}

func (h *Handler) handleDeleteNotificationConfiguration(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	topicARN := vals.Get("TopicARN")

	if err := h.Backend.DeleteNotificationConfiguration(groupName, topicARN); err != nil {
		return nil, err
	}

	return &deleteNotificationConfigurationResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-notification-configuration"},
	}, nil
}

func (h *Handler) handleDescribeNotificationConfigurations(vals url.Values) (any, error) {
	groupNames := parseMembers(vals, "AutoScalingGroupNames.member")

	configs, err := h.Backend.DescribeNotificationConfigurations(groupNames)
	if err != nil {
		return nil, err
	}

	maxRecords := defaultNCMaxRecords
	if v := vals.Get("MaxRecords"); v != "" {
		if n, parseErr := parseIntVal(v); parseErr == nil && n > 0 {
			maxRecords = min(int(n), maxNCMaxRecords)
		}
	}

	p := page.New(configs, vals.Get("NextToken"), maxRecords, defaultNCMaxRecords)

	members := make([]xmlNotificationConfiguration, 0, len(p.Data))
	for _, c := range p.Data {
		members = append(members, xmlNotificationConfiguration(c))
	}

	return &describeNotificationConfigurationsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeNotificationConfigurationsResult{
			NextToken:                  p.Next,
			NotificationConfigurations: xmlNotificationConfigurationList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-notification-configurations"},
	}, nil
}

type describeAutoScalingNotificationTypesResult struct {
	AutoScalingNotificationTypes xmlStringValueList `xml:"AutoScalingNotificationTypes"`
}

type describeAutoScalingNotificationTypesResponse struct {
	XMLName          xml.Name                                   `xml:"DescribeAutoScalingNotificationTypesResponse"`
	Xmlns            string                                     `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                        `xml:"ResponseMetadata"`
	Result           describeAutoScalingNotificationTypesResult `xml:"DescribeAutoScalingNotificationTypesResult"`
}

type putNotificationConfigurationResponse struct {
	XMLName          xml.Name            `xml:"PutNotificationConfigurationResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deleteNotificationConfigurationResponse struct {
	XMLName          xml.Name            `xml:"DeleteNotificationConfigurationResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlNotificationConfiguration struct {
	AutoScalingGroupName string `xml:"AutoScalingGroupName"`
	TopicARN             string `xml:"TopicARN"`
	NotificationType     string `xml:"NotificationType"`
}

type xmlNotificationConfigurationList struct {
	Members []xmlNotificationConfiguration `xml:"member"`
}

type describeNotificationConfigurationsResult struct {
	NextToken                  string                           `xml:"NextToken,omitempty"`
	NotificationConfigurations xmlNotificationConfigurationList `xml:"NotificationConfigurations"`
}

type describeNotificationConfigurationsResponse struct {
	XMLName          xml.Name                                 `xml:"DescribeNotificationConfigurationsResponse"`
	Xmlns            string                                   `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                      `xml:"ResponseMetadata"`
	Result           describeNotificationConfigurationsResult `xml:"DescribeNotificationConfigurationsResult"`
}
