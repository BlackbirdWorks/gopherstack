package dashboard

import (
	"github.com/labstack/echo/v5"

	taggingbackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// taggedResourceView is the view model for a single tagged resource.
type taggedResourceView struct {
	ARN  string
	Tags []taggingKeyValue
}

// taggingKeyValue is a single tag key-value pair for template rendering.
type taggingKeyValue struct {
	Key   string
	Value string
}

// resourcegroupstaggingapiIndexData is the template data for the Resource Groups Tagging API index page.
type resourcegroupstaggingapiIndexData struct {
	PageData

	Resources []taggedResourceView
}

// resourcegroupstaggingapiIndex renders the Resource Groups Tagging API dashboard index,
// displaying all tagged resources discovered by registered resource providers.

func (h *DashboardHandler) resourcegroupstaggingapiIndex(c *echo.Context) error {
	w := c.Response()

	snippet := &SnippetData{
		ID:    "resourcegroupstaggingapi-operations",
		Title: "Using Resource Groups Tagging API",
		Cli:   `aws resourcegroupstaggingapi get-resources --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Resource Groups Tagging API
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:8000"}, nil
        }),
    ),
)
if err != nil {
    log.Fatal(err)
}
client := resourcegroupstaggingapi.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Resource Groups Tagging API
import boto3

client = boto3.client('resourcegroupstaggingapi', endpoint_url='http://localhost:8000')`,
	}

	if h.ResourceGroupsTaggingOps == nil {
		h.renderTemplate(w, "resourcegroupstaggingapi/index.html", resourcegroupstaggingapiIndexData{
			PageData: PageData{Title: "Resource Groups Tagging API", ActiveTab: "resourcegroupstaggingapi",
				Snippet: snippet},
			Resources: []taggedResourceView{},
		})

		return nil
	}

	out := h.ResourceGroupsTaggingOps.Backend.GetResources(&taggingbackend.GetResourcesInput{})

	views := make([]taggedResourceView, 0, len(out.ResourceTagMappingList))
	for _, r := range out.ResourceTagMappingList {
		pairs := make([]taggingKeyValue, 0, len(r.Tags))
		for _, t := range r.Tags {
			pairs = append(pairs, taggingKeyValue{Key: t.Key, Value: t.Value})
		}

		views = append(views, taggedResourceView{
			ARN:  r.ResourceARN,
			Tags: pairs,
		})
	}

	h.renderTemplate(w, "resourcegroupstaggingapi/index.html", resourcegroupstaggingapiIndexData{
		PageData: PageData{Title: "Resource Groups Tagging API", ActiveTab: "resourcegroupstaggingapi",
			Snippet: snippet},
		Resources: views,
	})

	return nil
}
