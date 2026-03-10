package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	pkgslogger "github.com/blackbirdworks/gopherstack/pkgs/logger"
	appconfigdatabackend "github.com/blackbirdworks/gopherstack/services/appconfigdata"
)

// appConfigDataIndexData is the template data for the AppConfigData dashboard.
type appConfigDataIndexData struct {
	PageData

	Profiles []appconfigdatabackend.ConfigurationProfile
	Sessions []appconfigdatabackend.Session
}

// appConfigDataSnippet returns code snippet data for AppConfigData operations.
func (h *DashboardHandler) appConfigDataSnippet() *SnippetData {
	return &SnippetData{
		ID:    "appconfigdata-operations",
		Title: "Using AppConfigData",
		Cli: `# Start a configuration session
aws appconfigdata start-configuration-session \
  --application-identifier "my-app" \
  --environment-identifier "prod" \
  --configuration-profile-identifier "my-profile" \
  --endpoint-url http://localhost:8000

# Get the latest configuration using the session token
aws appconfigdata get-latest-configuration \
  --configuration-token "<token-from-start-session>" \
  --configuration /dev/stdout \
  --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for AppConfigData
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
if err != nil {
    log.Fatal(err)
}
client := appconfigdata.NewFromConfig(cfg, func(o *appconfigdata.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})

// Start a configuration session
sessionOut, err := client.StartConfigurationSession(context.TODO(),
    &appconfigdata.StartConfigurationSessionInput{
        ApplicationIdentifier:          aws.String("my-app"),
        EnvironmentIdentifier:          aws.String("prod"),
        ConfigurationProfileIdentifier: aws.String("my-profile"),
    },
)

// Get the latest configuration
configOut, err := client.GetLatestConfiguration(context.TODO(),
    &appconfigdata.GetLatestConfigurationInput{
        ConfigurationToken: sessionOut.InitialConfigurationToken,
    },
)
fmt.Println(string(configOut.Configuration))`,
		Python: `# Initialize boto3 client for AppConfigData
import boto3

client = boto3.client(
    'appconfigdata',
    endpoint_url='http://localhost:8000',
    region_name='us-east-1',
)

# Start a configuration session
response = client.start_configuration_session(
    ApplicationIdentifier='my-app',
    EnvironmentIdentifier='prod',
    ConfigurationProfileIdentifier='my-profile',
)
token = response['InitialConfigurationToken']

# Get the latest configuration
config_response = client.get_latest_configuration(
    ConfigurationToken=token,
)
print(config_response['Configuration'].read())`,
	}
}

// appConfigDataIndex handles GET /dashboard/appconfigdata.
func (h *DashboardHandler) appConfigDataIndex(c *echo.Context) error {
	w := c.Response()
	ctx := c.Request().Context()

	var profiles []appconfigdatabackend.ConfigurationProfile
	var sessions []appconfigdatabackend.Session

	if h.AppConfigDataOps == nil {
		pkgslogger.Load(ctx).WarnContext(ctx, "AppConfigData handler not available")
	} else {
		profiles = h.AppConfigDataOps.Backend.ListProfiles()
		sessions = h.AppConfigDataOps.Backend.ListSessions()
	}

	h.renderTemplate(w, "appconfigdata/index.html", appConfigDataIndexData{
		PageData: PageData{
			Title:     "AppConfigData",
			ActiveTab: "appconfigdata",
			Snippet:   h.appConfigDataSnippet(),
		},
		Profiles: profiles,
		Sessions: sessions,
	})

	return nil
}

// appConfigDataSetConfiguration handles POST /dashboard/appconfigdata/configuration/set.
func (h *DashboardHandler) appConfigDataSetConfiguration(c *echo.Context) error {
	if h.AppConfigDataOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	app := c.Request().FormValue("applicationIdentifier")
	env := c.Request().FormValue("environmentIdentifier")
	profile := c.Request().FormValue("configurationProfileIdentifier")
	content := c.Request().FormValue("content")
	contentType := c.Request().FormValue("contentType")

	if app == "" || env == "" || profile == "" {
		return c.String(
			http.StatusBadRequest,
			"applicationIdentifier, environmentIdentifier, and configurationProfileIdentifier are required",
		)
	}

	if contentType == "" {
		contentType = "application/json"
	}

	h.AppConfigDataOps.Backend.SetConfiguration(app, env, profile, content, contentType)

	return c.Redirect(http.StatusFound, "/dashboard/appconfigdata")
}

// appConfigDataDeleteProfile handles POST /dashboard/appconfigdata/configuration/delete.
func (h *DashboardHandler) appConfigDataDeleteProfile(c *echo.Context) error {
	if h.AppConfigDataOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	app := c.Request().FormValue("applicationIdentifier")
	env := c.Request().FormValue("environmentIdentifier")
	profile := c.Request().FormValue("configurationProfileIdentifier")

	if app == "" || env == "" || profile == "" {
		return c.String(
			http.StatusBadRequest,
			"applicationIdentifier, environmentIdentifier, and configurationProfileIdentifier are required",
		)
	}

	h.AppConfigDataOps.Backend.DeleteProfile(app, env, profile)

	return c.Redirect(http.StatusFound, "/dashboard/appconfigdata")
}
