package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	mwaabackend "github.com/blackbirdworks/gopherstack/services/mwaa"
)

// mwaaEnvironmentView is the view model for a single MWAA environment row.
type mwaaEnvironmentView struct {
	Name             string
	ARN              string
	Status           string
	AirflowVersion   string
	EnvironmentClass string
}

// mwaaIndexData is the template data for the MWAA dashboard page.
type mwaaIndexData struct {
	PageData

	Environments []mwaaEnvironmentView
}

// mwaaSnippet returns the shared SnippetData for the MWAA dashboard.
func mwaaSnippet() *SnippetData {
	return &SnippetData{
		ID:    "mwaa-operations",
		Title: "Using MWAA",
		Cli:   `aws mwaa list-environments --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for MWAA
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
client := mwaa.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for MWAA
import boto3

client = boto3.client('mwaa', endpoint_url='http://localhost:8000')`,
	}
}

// setupMWAARoutes registers all MWAA dashboard routes.
func (h *DashboardHandler) setupMWAARoutes() {
	h.SubRouter.GET("/dashboard/mwaa", h.mwaaIndex)
	h.SubRouter.POST("/dashboard/mwaa/environment/create", h.mwaaCreateEnvironment)
	h.SubRouter.POST("/dashboard/mwaa/environment/delete", h.mwaaDeleteEnvironment)
}

// mwaaIndex renders the main MWAA dashboard page.
func (h *DashboardHandler) mwaaIndex(c *echo.Context) error {
	w := c.Response()

	if h.MWAAOps == nil {
		h.renderTemplate(w, "mwaa/index.html", mwaaIndexData{
			PageData: PageData{
				Title:     "MWAA",
				ActiveTab: "mwaa",
				Snippet:   mwaaSnippet(),
			},
			Environments: []mwaaEnvironmentView{},
		})

		return nil
	}

	ctx := c.Request().Context()

	names, err := h.MWAAOps.Backend.ListEnvironments()
	if err != nil {
		h.Logger.ErrorContext(ctx, "mwaa: failed to list environments", "error", err)

		names = nil
	}

	views := make([]mwaaEnvironmentView, 0, len(names))

	for _, name := range names {
		env, getErr := h.MWAAOps.Backend.GetEnvironment(name)
		if getErr != nil {
			continue
		}

		views = append(views, mwaaEnvironmentView{
			Name:             env.Name,
			ARN:              env.ARN,
			Status:           env.Status,
			AirflowVersion:   env.AirflowVersion,
			EnvironmentClass: env.EnvironmentClass,
		})
	}

	h.renderTemplate(w, "mwaa/index.html", mwaaIndexData{
		PageData: PageData{
			Title:     "MWAA Environments",
			ActiveTab: "mwaa",
			Snippet:   mwaaSnippet(),
		},
		Environments: views,
	})

	return nil
}

// mwaaCreateEnvironment handles POST /dashboard/mwaa/environment/create.
func (h *DashboardHandler) mwaaCreateEnvironment(c *echo.Context) error {
	if h.MWAAOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ctx := c.Request().Context()

	req := &mwaabackend.ExportedCreateEnvironmentRequest{
		DagS3Path:        "dags/",
		ExecutionRoleArn: "arn:aws:iam::" + h.GlobalConfig.AccountID + ":role/mwaa-role",
		SourceBucketArn:  "arn:aws:s3:::mwaa-bucket-" + name,
	}

	if _, err := h.MWAAOps.Backend.CreateEnvironment(
		h.GlobalConfig.Region,
		h.GlobalConfig.AccountID,
		name,
		req,
	); err != nil {
		h.Logger.ErrorContext(ctx, "mwaa: failed to create environment", "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/mwaa")
}

// mwaaDeleteEnvironment handles POST /dashboard/mwaa/environment/delete.
func (h *DashboardHandler) mwaaDeleteEnvironment(c *echo.Context) error {
	if h.MWAAOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ctx := c.Request().Context()

	if _, err := h.MWAAOps.Backend.DeleteEnvironment(name); err != nil {
		h.Logger.ErrorContext(ctx, "mwaa: failed to delete environment", "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/mwaa")
}
