package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// elasticbeanstalkApplicationView is the view model for an EB application.
type elasticbeanstalkApplicationView struct {
	Name        string
	ARN         string
	Description string
}

// elasticbeanstalkEnvironmentView is the view model for an EB environment.
type elasticbeanstalkEnvironmentView struct {
	ApplicationName string
	Name            string
	ID              string
	ARN             string
	SolutionStack   string
	Status          string
	Health          string
}

// elasticbeanstalkIndexData is the template data for the Elastic Beanstalk dashboard index page.
type elasticbeanstalkIndexData struct {
	PageData

	Applications []elasticbeanstalkApplicationView
	Environments []elasticbeanstalkEnvironmentView
}

// elasticbeanstalkIndex renders the Elastic Beanstalk dashboard index page.
func (h *DashboardHandler) elasticbeanstalkIndex(c *echo.Context) error {
	w := c.Response()

	snippet := &SnippetData{
		ID:    "elasticbeanstalk-operations",
		Title: "Using AWS Elastic Beanstalk",
		Cli: `aws elasticbeanstalk describe-applications \
    --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Elastic Beanstalk
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
client := elasticbeanstalk.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Elastic Beanstalk
import boto3

client = boto3.client('elasticbeanstalk', endpoint_url='http://localhost:8000')`,
	}

	if h.ElasticbeanstalkOps == nil {
		h.renderTemplate(w, "elasticbeanstalk/index.html", elasticbeanstalkIndexData{
			PageData: PageData{
				Title:     "Elastic Beanstalk",
				ActiveTab: "elasticbeanstalk",
				Snippet:   snippet,
			},
			Applications: []elasticbeanstalkApplicationView{},
			Environments: []elasticbeanstalkEnvironmentView{},
		})

		return nil
	}

	apps := h.ElasticbeanstalkOps.Backend.DescribeApplications(nil)
	appViews := make([]elasticbeanstalkApplicationView, 0, len(apps))

	for _, app := range apps {
		appViews = append(appViews, elasticbeanstalkApplicationView{
			Name:        app.ApplicationName,
			ARN:         app.ApplicationARN,
			Description: app.Description,
		})
	}

	envs := h.ElasticbeanstalkOps.Backend.DescribeEnvironments("", nil, nil)
	envViews := make([]elasticbeanstalkEnvironmentView, 0, len(envs))

	for _, env := range envs {
		envViews = append(envViews, elasticbeanstalkEnvironmentView{
			ApplicationName: env.ApplicationName,
			Name:            env.EnvironmentName,
			ID:              env.EnvironmentID,
			ARN:             env.EnvironmentARN,
			SolutionStack:   env.SolutionStackName,
			Status:          env.Status,
			Health:          env.Health,
		})
	}

	h.renderTemplate(w, "elasticbeanstalk/index.html", elasticbeanstalkIndexData{
		PageData: PageData{
			Title:     "Elastic Beanstalk",
			ActiveTab: "elasticbeanstalk",
			Snippet:   snippet,
		},
		Applications: appViews,
		Environments: envViews,
	})

	return nil
}

// elasticbeanstalkCreateApplication handles POST /dashboard/elasticbeanstalk/applications/create.
func (h *DashboardHandler) elasticbeanstalkCreateApplication(c *echo.Context) error {
	if h.ElasticbeanstalkOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	description := c.Request().FormValue("description")

	_, err := h.ElasticbeanstalkOps.Backend.CreateApplication(name, description, nil)
	if err != nil {
		h.Logger.Error("failed to create application", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/elasticbeanstalk")
}

// elasticbeanstalkDeleteApplication handles POST /dashboard/elasticbeanstalk/applications/delete.
func (h *DashboardHandler) elasticbeanstalkDeleteApplication(c *echo.Context) error {
	if h.ElasticbeanstalkOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.ElasticbeanstalkOps.Backend.DeleteApplication(name); err != nil {
		h.Logger.Error("failed to delete application", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/elasticbeanstalk")
}

// elasticbeanstalkCreateEnvironment handles POST /dashboard/elasticbeanstalk/environments/create.
func (h *DashboardHandler) elasticbeanstalkCreateEnvironment(c *echo.Context) error {
	if h.ElasticbeanstalkOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	appName := c.Request().FormValue("appName")
	envName := c.Request().FormValue("name")

	if appName == "" || envName == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	solutionStack := c.Request().FormValue("solutionStack")

	_, err := h.ElasticbeanstalkOps.Backend.CreateEnvironment(appName, envName, solutionStack, "", nil)
	if err != nil {
		h.Logger.Error("failed to create environment", "appName", appName, "envName", envName, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/elasticbeanstalk")
}

// elasticbeanstalkTerminateEnvironment handles POST /dashboard/elasticbeanstalk/environments/terminate.
func (h *DashboardHandler) elasticbeanstalkTerminateEnvironment(c *echo.Context) error {
	if h.ElasticbeanstalkOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	appName := c.Request().FormValue("appName")
	envName := c.Request().FormValue("name")

	if envName == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.ElasticbeanstalkOps.Backend.TerminateEnvironment(appName, envName); err != nil {
		h.Logger.Error("failed to terminate environment", "appName", appName, "envName", envName, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/elasticbeanstalk")
}

// setupElasticbeanstalkRoutes registers routes for the Elastic Beanstalk dashboard.
func (h *DashboardHandler) setupElasticbeanstalkRoutes() {
	h.SubRouter.GET("/dashboard/elasticbeanstalk", h.elasticbeanstalkIndex)
	h.SubRouter.POST("/dashboard/elasticbeanstalk/applications/create", h.elasticbeanstalkCreateApplication)
	h.SubRouter.POST("/dashboard/elasticbeanstalk/applications/delete", h.elasticbeanstalkDeleteApplication)
	h.SubRouter.POST("/dashboard/elasticbeanstalk/environments/create", h.elasticbeanstalkCreateEnvironment)
	h.SubRouter.POST("/dashboard/elasticbeanstalk/environments/terminate", h.elasticbeanstalkTerminateEnvironment)
}
