package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	pkgslogger "github.com/blackbirdworks/gopherstack/pkgs/logger"
	apigwmgmtbackend "github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
)

// apiGatewayManagementAPIIndexData is the template data for the API Gateway Management API dashboard.
type apiGatewayManagementAPIIndexData struct {
	PageData

	Connections []apigwmgmtbackend.Connection
}

// apiGatewayManagementAPISnippet returns code snippet data for API Gateway Management API operations.
func (h *DashboardHandler) apiGatewayManagementAPISnippet() *SnippetData {
	return &SnippetData{
		ID:    "apigatewaymanagementapi-operations",
		Title: "Using API Gateway Management API",
		Cli: `# Post data to a WebSocket connection
aws apigatewaymanagementapi post-to-connection \
  --connection-id "abc123xyz" \
  --data '{"message":"hello"}' \
  --endpoint-url http://localhost:8000

# Get connection info
aws apigatewaymanagementapi get-connection \
  --connection-id "abc123xyz" \
  --endpoint-url http://localhost:8000

# Delete a connection
aws apigatewaymanagementapi delete-connection \
  --connection-id "abc123xyz" \
  --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for API Gateway Management API
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
if err != nil {
    log.Fatal(err)
}
client := apigatewaymanagementapi.NewFromConfig(cfg, func(o *apigatewaymanagementapi.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})

// Post data to a connection
_, err = client.PostToConnection(context.TODO(), &apigatewaymanagementapi.PostToConnectionInput{
    ConnectionId: aws.String("abc123xyz"),
    Data:         []byte(` + "`" + `{"message":"hello"}` + "`" + `),
})

// Get connection info
out, err := client.GetConnection(context.TODO(), &apigatewaymanagementapi.GetConnectionInput{
    ConnectionId: aws.String("abc123xyz"),
})`,
		Python: `# Initialize boto3 client for API Gateway Management API
import boto3

client = boto3.client(
    'apigatewaymanagementapi',
    endpoint_url='http://localhost:8000',
    region_name='us-east-1',
)

# Post data to a connection
client.post_to_connection(
    ConnectionId='abc123xyz',
    Data=b'{"message":"hello"}',
)

# Get connection info
response = client.get_connection(ConnectionId='abc123xyz')`,
	}
}

// apiGatewayManagementAPIIndex handles GET /dashboard/apigatewaymanagementapi.
func (h *DashboardHandler) apiGatewayManagementAPIIndex(c *echo.Context) error {
	w := c.Response()
	ctx := c.Request().Context()

	var connections []apigwmgmtbackend.Connection

	if h.APIGatewayManagementAPIOps == nil {
		pkgslogger.Load(ctx).WarnContext(ctx, "API Gateway Management API handler not available")
	} else {
		connections = h.APIGatewayManagementAPIOps.Backend.ListConnections()
	}

	h.renderTemplate(w, "apigatewaymanagementapi/index.html", apiGatewayManagementAPIIndexData{
		PageData: PageData{
			Title:     "API Gateway Management API",
			ActiveTab: "apigatewaymanagementapi",
			Snippet:   h.apiGatewayManagementAPISnippet(),
		},
		Connections: connections,
	})

	return nil
}

// apiGatewayManagementAPICreateConnection handles POST /dashboard/apigatewaymanagementapi/connection/create.
func (h *DashboardHandler) apiGatewayManagementAPICreateConnection(c *echo.Context) error {
	if h.APIGatewayManagementAPIOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	connectionID := c.Request().FormValue("connectionId")
	sourceIP := c.Request().FormValue("sourceIp")
	userAgent := c.Request().FormValue("userAgent")

	if connectionID == "" {
		return c.String(http.StatusBadRequest, "connectionId is required")
	}

	if sourceIP == "" {
		sourceIP = "127.0.0.1"
	}

	if userAgent == "" {
		userAgent = "test-client/1.0"
	}

	if _, err := h.APIGatewayManagementAPIOps.Backend.CreateConnection(connectionID, sourceIP, userAgent); err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Redirect(http.StatusFound, "/dashboard/apigatewaymanagementapi")
}

// apiGatewayManagementAPIDeleteConnection handles POST /dashboard/apigatewaymanagementapi/connection/delete.
func (h *DashboardHandler) apiGatewayManagementAPIDeleteConnection(c *echo.Context) error {
	if h.APIGatewayManagementAPIOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	connectionID := c.Request().FormValue("connectionId")
	if connectionID == "" {
		return c.String(http.StatusBadRequest, "connectionId is required")
	}

	if err := h.APIGatewayManagementAPIOps.Backend.DeleteConnection(connectionID); err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Redirect(http.StatusFound, "/dashboard/apigatewaymanagementapi")
}
