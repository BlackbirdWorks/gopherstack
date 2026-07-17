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
	InstanceHealthList []singleInstanceHealth `xml:"InstanceHealthList>member"`
}

type describeInstancesHealthResponse struct {
	XMLName                       xml.Name                      `xml:"DescribeInstancesHealthResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	ResponseMetadata              responseMetadata              `xml:"ResponseMetadata"`
	DescribeInstancesHealthResult describeInstancesHealthResult `xml:"DescribeInstancesHealthResult"`
}

func (h *Handler) handleDescribeInstancesHealth(_ context.Context, _ url.Values) (any, error) {
	return &describeInstancesHealthResponse{
		Xmlns: ebXMLNS,
		DescribeInstancesHealthResult: describeInstancesHealthResult{
			InstanceHealthList: []singleInstanceHealth{},
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-instances-health"},
	}, nil
}
