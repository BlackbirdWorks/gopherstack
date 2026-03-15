package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	pkgslogger "github.com/blackbirdworks/gopherstack/pkgs/logger"
	appconfigbackend "github.com/blackbirdworks/gopherstack/services/appconfig"
)

// appConfigIndexData is the template data for the AppConfig dashboard.
type appConfigIndexData struct {
	PageData

	Applications []appconfigbackend.Application
}

// appConfigSnippet returns code snippet data for AppConfig operations.
func (h *DashboardHandler) appConfigSnippet() *SnippetData {
	return &SnippetData{
		ID:    "appconfig-operations",
		Title: "Using AppConfig",
		Cli: `# Create an AppConfig application
aws appconfig create-application \
  --name my-app \
  --endpoint-url http://localhost:8000

# Create an environment
aws appconfig create-environment \
  --application-id <app-id> \
  --name production \
  --endpoint-url http://localhost:8000

# Create a deployment strategy
aws appconfig create-deployment-strategy \
  --name my-strategy \
  --deployment-duration-in-minutes 0 \
  --final-bake-time-in-minutes 0 \
  --growth-factor 100 \
  --replicate-to NONE \
  --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for AppConfig
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
if err != nil {
    log.Fatal(err)
}
client := appconfig.NewFromConfig(cfg, func(o *appconfig.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})

// Create an application
out, err := client.CreateApplication(context.TODO(), &appconfig.CreateApplicationInput{
    Name: aws.String("my-app"),
})`,
		Python: `# Initialize boto3 client for AppConfig
import boto3

client = boto3.client(
    'appconfig',
    endpoint_url='http://localhost:8000',
    region_name='us-east-1',
)

# Create an application
response = client.create_application(Name='my-app')
app_id = response['Id']

# Create an environment
client.create_environment(
    ApplicationId=app_id,
    Name='production',
)`,
	}
}

// appConfigIndex handles GET /dashboard/appconfig.
func (h *DashboardHandler) appConfigIndex(c *echo.Context) error {
	w := c.Response()
	ctx := c.Request().Context()

	var applications []appconfigbackend.Application

	if h.AppConfigOps == nil {
		pkgslogger.Load(ctx).WarnContext(ctx, "AppConfig handler not available")
	} else {
		applications, _ = h.AppConfigOps.Backend.ListApplications("", 0)
	}

	h.renderTemplate(w, "appconfig/index.html", appConfigIndexData{
		PageData: PageData{
			Title:     "AppConfig",
			ActiveTab: "appconfig",
			Snippet:   h.appConfigSnippet(),
		},
		Applications: applications,
	})

	return nil
}

// appConfigCreateApplication handles POST /dashboard/appconfig/application/create.
func (h *DashboardHandler) appConfigCreateApplication(c *echo.Context) error {
	if h.AppConfigOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	name := c.Request().FormValue("name")
	description := c.Request().FormValue("description")

	if name == "" {
		return c.String(http.StatusBadRequest, "name is required")
	}

	if _, err := h.AppConfigOps.Backend.CreateApplication(name, description); err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Redirect(http.StatusFound, "/dashboard/appconfig")
}

// appConfigDeleteApplication handles POST /dashboard/appconfig/application/delete.
func (h *DashboardHandler) appConfigDeleteApplication(c *echo.Context) error {
	if h.AppConfigOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	applicationID := c.Request().FormValue("applicationId")
	if applicationID == "" {
		return c.String(http.StatusBadRequest, "applicationId is required")
	}

	if err := h.AppConfigOps.Backend.DeleteApplication(applicationID); err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Redirect(http.StatusFound, "/dashboard/appconfig")
}
