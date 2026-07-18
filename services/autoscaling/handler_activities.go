package autoscaling

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleDescribeScalingActivities(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	activities, err := h.Backend.DescribeScalingActivities(groupName)
	if err != nil {
		return nil, err
	}

	// Apply MaxRecords if provided
	if maxStr := vals.Get("MaxRecords"); maxStr != "" {
		maxRecords, parseErr := parseIntVal(maxStr)
		if parseErr == nil && maxRecords > 0 && int(maxRecords) < len(activities) {
			activities = activities[:maxRecords]
		}
	}

	members := make([]xmlScalingActivity, 0, len(activities))
	for i := range activities {
		members = append(members, toXMLScalingActivity(&activities[i]))
	}

	return &describeScalingActivitiesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeScalingActivitiesResult{
			Activities: xmlScalingActivityList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-activities"},
	}, nil
}

type describeScalingActivitiesResult struct {
	NextToken  string                 `xml:"NextToken,omitempty"`
	Activities xmlScalingActivityList `xml:"Activities"`
}

type describeScalingActivitiesResponse struct {
	XMLName          xml.Name                        `xml:"DescribeScalingActivitiesResponse"`
	Xmlns            string                          `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata             `xml:"ResponseMetadata"`
	Result           describeScalingActivitiesResult `xml:"DescribeScalingActivitiesResult"`
}
