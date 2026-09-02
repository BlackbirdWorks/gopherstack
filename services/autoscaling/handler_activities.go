package autoscaling

import (
	"encoding/xml"
	"net/url"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (h *Handler) handleDescribeScalingActivities(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	statuses := scalingActivityStatusFilters(vals)

	activities, err := h.Backend.DescribeScalingActivities(groupName, statuses)
	if err != nil {
		return nil, err
	}

	maxRecords := defaultActivitiesMaxRecords
	if maxStr := vals.Get("MaxRecords"); maxStr != "" {
		if n, parseErr := parseIntVal(maxStr); parseErr == nil && n > 0 {
			maxRecords = int(n)
		}
	}

	p := page.New(activities, vals.Get("NextToken"), maxRecords, defaultActivitiesMaxRecords)

	members := make([]xmlScalingActivity, 0, len(p.Data))
	for i := range p.Data {
		members = append(members, toXMLScalingActivity(&p.Data[i]))
	}

	return &describeScalingActivitiesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeScalingActivitiesResult{
			NextToken:  p.Next,
			Activities: xmlScalingActivityList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-activities"},
	}, nil
}

// defaultActivitiesMaxRecords is DescribeScalingActivities's documented
// default/max page size (api_op_DescribeScalingActivities.go: "The default
// value is 100 and the maximum value is 100").
const defaultActivitiesMaxRecords = 100

// scalingActivityStatusFilters extracts the Values of every Filter named
// "Status" (api_op_DescribeScalingActivities.go's only enumerable Filter.Name
// this backend applies -- see PARITY.md for StartTimeLowerBound/UpperBound).
func scalingActivityStatusFilters(vals url.Values) []string {
	var statuses []string

	for _, f := range parseTagFilters(vals) {
		if f.Name == "Status" {
			statuses = append(statuses, f.Values...)
		}
	}

	return statuses
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
