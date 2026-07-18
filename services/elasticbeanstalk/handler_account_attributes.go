package elasticbeanstalk

import (
	"context"
	"encoding/xml"
	"net/url"
)

// describeAccountAttributesResponse is the XML response for DescribeAccountAttributes.
type resourceQuota struct {
	Maximum int `xml:"Maximum"`
}

type resourceQuotas struct {
	ApplicationQuota           resourceQuota `xml:"ApplicationQuota"`
	ApplicationVersionQuota    resourceQuota `xml:"ApplicationVersionQuota"`
	ConfigurationTemplateQuota resourceQuota `xml:"ConfigurationTemplateQuota"`
	CustomPlatformQuota        resourceQuota `xml:"CustomPlatformQuota"`
	EnvironmentQuota           resourceQuota `xml:"EnvironmentQuota"`
}

type describeAccountAttributesResult struct {
	ResourceQuotas resourceQuotas `xml:"ResourceQuotas"`
}

type describeAccountAttributesResponse struct {
	XMLName                         xml.Name                        `xml:"DescribeAccountAttributesResponse"`
	Xmlns                           string                          `xml:"xmlns,attr"`
	ResponseMetadata                responseMetadata                `xml:"ResponseMetadata"`
	DescribeAccountAttributesResult describeAccountAttributesResult `xml:"DescribeAccountAttributesResult"`
}

func (h *Handler) handleDescribeAccountAttributes(_ context.Context, _ url.Values) (any, error) {
	return &describeAccountAttributesResponse{
		Xmlns: ebXMLNS,
		DescribeAccountAttributesResult: describeAccountAttributesResult{
			ResourceQuotas: resourceQuotas{
				ApplicationQuota:           resourceQuota{Maximum: quotaApplications},
				ApplicationVersionQuota:    resourceQuota{Maximum: quotaApplicationVersions},
				ConfigurationTemplateQuota: resourceQuota{Maximum: quotaConfigTemplates},
				CustomPlatformQuota:        resourceQuota{Maximum: quotaCustomPlatforms},
				EnvironmentQuota:           resourceQuota{Maximum: quotaEnvironments},
			},
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-account-attrs"},
	}, nil
}
