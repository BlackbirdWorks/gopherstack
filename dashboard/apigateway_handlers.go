package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	apigwbackend "github.com/blackbirdworks/gopherstack/apigateway"
)

func (h *DashboardHandler) apiGatewayIndex(c *echo.Context) error {
	if h.APIGatewayOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	apis, _, _ := h.APIGatewayOps.Backend.GetRestAPIs(0, "")
	data := struct {
		PageData

		APIs []apigwbackend.RestAPI
	}{
		PageData: PageData{Title: "API Gateway", ActiveTab: "apigateway",
			Snippet: &SnippetData{
				ID:    "apigateway-operations",
				Title: "Using Apigateway",
				Cli:   "aws apigateway help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Apigateway */",
				Python: "# Write boto3 code for ApiGateway\nimport boto3\n" +
					"client = boto3.client('apigateway', endpoint_url='http://localhost:8000')",
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
	data := struct {
		PageData

		API       *apigwbackend.RestAPI
		Resources []apigwbackend.Resource
		Stages    []apigwbackend.Stage
	}{
		PageData: PageData{Title: "API Gateway — " + api.Name, ActiveTab: "apigateway",
			Snippet: &SnippetData{
				ID:    "apigateway-operations",
				Title: "Using Apigateway",
				Cli:   "aws apigateway help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Apigateway */",
				Python: `# Write boto3 code for Apigateway
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
