package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// cloudcontrolResourceView is the view model for a single CloudControl managed resource.
type cloudcontrolResourceView struct {
	TypeName   string
	Identifier string
	Properties string
}

// cloudcontrolIndexData is the template data for the CloudControl dashboard index page.
type cloudcontrolIndexData struct {
	PageData

	Resources []cloudcontrolResourceView
}

func cloudcontrolSnippet() *SnippetData {
	return &SnippetData{
		ID:    "cloudcontrol-operations",
		Title: "Using CloudControl API",
		Cli: `aws cloudcontrol list-resources \
    --type-name "AWS::Logs::LogGroup" \
    --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for CloudControl API
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
)
if err != nil {
    log.Fatal(err)
}
client := cloudcontrolapi.NewFromConfig(cfg, func(o *cloudcontrolapi.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})`,
		Python: `# Initialize boto3 client for CloudControl API
import boto3

client = boto3.client('cloudcontrol', endpoint_url='http://localhost:8000')`,
	}
}

// cloudcontrolIndex renders the CloudControl dashboard index, listing all managed resources.
func (h *DashboardHandler) cloudcontrolIndex(c *echo.Context) error {
	w := c.Response()

	if h.CloudControlOps == nil {
		h.renderTemplate(w, "cloudcontrol/index.html", cloudcontrolIndexData{
			PageData:  PageData{Title: "CloudControl API", ActiveTab: "cloudcontrol", Snippet: cloudcontrolSnippet()},
			Resources: []cloudcontrolResourceView{},
		})

		return nil
	}

	all := h.CloudControlOps.Backend.ListAllResources()
	views := make([]cloudcontrolResourceView, 0, len(all))

	for _, r := range all {
		views = append(views, cloudcontrolResourceView{
			TypeName:   r.TypeName,
			Identifier: r.Identifier,
			Properties: r.Properties,
		})
	}

	h.renderTemplate(w, "cloudcontrol/index.html", cloudcontrolIndexData{
		PageData:  PageData{Title: "CloudControl API", ActiveTab: "cloudcontrol", Snippet: cloudcontrolSnippet()},
		Resources: views,
	})

	return nil
}

// cloudcontrolDelete handles POST /dashboard/cloudcontrol/delete.
func (h *DashboardHandler) cloudcontrolDelete(c *echo.Context) error {
	if h.CloudControlOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		h.Logger.Error("failed to parse form", "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	typeName := c.Request().FormValue("typeName")
	identifier := c.Request().FormValue("identifier")

	if typeName == "" || identifier == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.CloudControlOps.Backend.DeleteResource(typeName, identifier); err != nil {
		h.Logger.Error(
			"failed to delete cloudcontrol resource",
			"typeName",
			typeName,
			"identifier",
			identifier,
			"error",
			err,
		)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/cloudcontrol")
}

// setupCloudControlRoutes registers CloudControl API dashboard routes.
func (h *DashboardHandler) setupCloudControlRoutes() {
	h.SubRouter.GET("/dashboard/cloudcontrol", h.cloudcontrolIndex)
	h.SubRouter.POST("/dashboard/cloudcontrol/delete", h.cloudcontrolDelete)
}
