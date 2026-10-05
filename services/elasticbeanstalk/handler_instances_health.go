package elasticbeanstalk

import (
	"context"
	"encoding/xml"
	"net/url"
)

// describeInstancesHealthResponse is the XML response for DescribeInstancesHealth.
type singleInstanceHealth struct {
	InstanceID   string `xml:"InstanceId"`
	HealthStatus string `xml:"HealthStatus"`
	Color        string `xml:"Color"`
}

type describeInstancesHealthResult struct {
	RefreshedAt        string                 `xml:"RefreshedAt"`
	InstanceHealthList []singleInstanceHealth `xml:"InstanceHealthList>member"`
}

type describeInstancesHealthResponse struct {
	XMLName                       xml.Name                      `xml:"DescribeInstancesHealthResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	ResponseMetadata              responseMetadata              `xml:"ResponseMetadata"`
	DescribeInstancesHealthResult describeInstancesHealthResult `xml:"DescribeInstancesHealthResult"`
}

// handleDescribeInstancesHealth always answers an empty InstanceHealthList:
// this backend never models EC2 instances (see handleRequestEnvironmentInfo's
// doc comment for the same disclosed gap) -- a structural limitation, not a
// dropped field. RefreshedAt is still emitted (using the same placeholder
// handleDescribeEnvironmentHealth uses): the real field is *time.Time, so
// never emitting it would decode as a nil pointer, unlike the always-empty
// list which a real client already expects to handle as zero-length.
func (h *Handler) handleDescribeInstancesHealth(ctx context.Context, vals url.Values) (any, error) {
	if err := h.checkOptionalEnvironment(ctx, vals); err != nil {
		return nil, err
	}

	return &describeInstancesHealthResponse{
		Xmlns: ebXMLNS,
		DescribeInstancesHealthResult: describeInstancesHealthResult{
			InstanceHealthList: []singleInstanceHealth{},
			RefreshedAt:        healthRefreshedAt,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-instances-health"},
	}, nil
}
