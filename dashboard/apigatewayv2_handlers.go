package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	apigwv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// apiGatewayV2IndexData is the template data for the API Gateway V2 list page.
type apiGatewayV2IndexData struct {
	PageData

	APIs []apigwv2backend.API
}

// apiGatewayV2DetailData is the template data for the API Gateway V2 detail page.
type apiGatewayV2DetailData struct {
	PageData

	API          *apigwv2backend.API
	Routes       []apigwv2backend.Route
	Stages       []apigwv2backend.Stage
	Integrations []apigwv2backend.Integration
}

func (h *DashboardHandler) apiGatewayV2Snippet() *SnippetData {
	return &SnippetData{
		ID:    "apigatewayv2-operations",
		Title: "Using API Gateway V2",
		Cli: `# Create an HTTP API
aws apigatewayv2 create-api \
  --name my-http-api \
  --protocol-type HTTP \
  --endpoint-url http://localhost:8000

# List APIs
aws apigatewayv2 get-apis \
  --endpoint-url http://localhost:8000

# Create a stage
aws apigatewayv2 create-stage \
  --api-id <api-id> \
  --stage-name '$default' \
  --auto-deploy \
  --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for API Gateway V2
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
if err != nil {
    log.Fatal(err)
}
client := apigatewayv2.NewFromConfig(cfg, func(o *apigatewayv2.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})

// Create an HTTP API
out, err := client.CreateApi(context.TODO(), &apigatewayv2.CreateApiInput{
    Name:         aws.String("my-http-api"),
    ProtocolType: types.ProtocolTypeHttp,
})`,
		Python: `# Initialize boto3 client for API Gateway V2
import boto3

client = boto3.client('apigatewayv2', endpoint_url='http://localhost:8000')

# Create an HTTP API
response = client.create_api(
    Name='my-http-api',
    ProtocolType='HTTP'
)
api_id = response['ApiId']

# Create a stage
client.create_stage(
    ApiId=api_id,
    StageName='$default',
    AutoDeploy=True
)`,
	}
}

func (h *DashboardHandler) apiGatewayV2Index(c *echo.Context) error {
	if h.APIGatewayV2Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	apis, _ := h.APIGatewayV2Ops.Backend.GetAPIs()
	data := apiGatewayV2IndexData{
		PageData: PageData{
			Title:     "API Gateway V2",
			ActiveTab: "apigatewayv2",
			Snippet:   h.apiGatewayV2Snippet(),
		},
		APIs: apis,
	}

	h.renderTemplate(c.Response(), "apigatewayv2/index.html", data)

	return nil
}

func (h *DashboardHandler) apiGatewayV2Detail(c *echo.Context) error {
	if h.APIGatewayV2Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	id := c.Request().URL.Query().Get("id")
	if id == "" {
		return c.String(http.StatusBadRequest, "Missing id")
	}

	api, err := h.APIGatewayV2Ops.Backend.GetAPI(id)
	if err != nil {
		return c.String(http.StatusNotFound, "API not found")
	}

	routes, _ := h.APIGatewayV2Ops.Backend.GetRoutes(id)
	stages, _ := h.APIGatewayV2Ops.Backend.GetStages(id)
	integrations, _ := h.APIGatewayV2Ops.Backend.GetIntegrations(id)

	data := apiGatewayV2DetailData{
		PageData: PageData{
			Title:     "API Gateway V2 — " + api.Name,
			ActiveTab: "apigatewayv2",
			Snippet:   h.apiGatewayV2Snippet(),
		},
		API:          api,
		Routes:       routes,
		Stages:       stages,
		Integrations: integrations,
	}

	h.renderTemplate(c.Response(), "apigatewayv2/detail.html", data)

	return nil
}
