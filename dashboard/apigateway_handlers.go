package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
)

// apiGatewayIndexData is the template data for the API Gateway list page.
type apiGatewayIndexData struct {
	PageData

	APIs []apigwbackend.RestAPI
}

// apiGatewayDetailData is the template data for the API Gateway detail page.
type apiGatewayDetailData struct {
	PageData

	API       *apigwbackend.RestAPI
	Resources []apigwbackend.Resource
	Stages    []apigwbackend.Stage
}

func (h *DashboardHandler) apiGatewayIndex(c *echo.Context) error {
	if h.APIGatewayOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	apis, _, _ := h.APIGatewayOps.Backend.GetRestAPIs(0, "")
	data := apiGatewayIndexData{
		PageData: PageData{Title: "API Gateway", ActiveTab: "apigateway",
			Snippet: &SnippetData{
				ID:    "apigateway-operations",
				Title: "Using Apigateway",
				Cli:   `aws apigateway help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Apigateway
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
client := apigateway.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Apigateway
import boto3

client = boto3.client('apigateway', endpoint_url='http://localhost:8000')`,
			}},
		APIs: apis,
	}

	h.renderTemplate(c.Response(), "apigateway/index.html", data)

	return nil
}

func (h *DashboardHandler) apiGatewayDetail(c *echo.Context) error {
	if h.APIGatewayOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	id := c.Request().URL.Query().Get("id")
	if id == "" {
		return c.String(http.StatusBadRequest, "Missing id")
	}

	api, err := h.APIGatewayOps.Backend.GetRestAPI(id)
	if err != nil {
		return c.String(http.StatusNotFound, "API not found")
	}

	resources, _, _ := h.APIGatewayOps.Backend.GetResources(id, "", 0)
	stages, _ := h.APIGatewayOps.Backend.GetStages(id)
	data := apiGatewayDetailData{
		PageData: PageData{Title: "API Gateway — " + api.Name, ActiveTab: "apigateway",
			Snippet: &SnippetData{
				ID:    "apigateway-operations",
				Title: "Using Apigateway",
				Cli:   `aws apigateway help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Apigateway
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
client := apigateway.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Apigateway
import boto3

client = boto3.client('apigateway', endpoint_url='http://localhost:8000')`,
			}},
		API:       api,
		Resources: resources,
		Stages:    stages,
	}

	h.renderTemplate(c.Response(), "apigateway/detail.html", data)

	return nil
}
