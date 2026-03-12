package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
)

// glueDatabaseView is the view model for a single Glue database.
type glueDatabaseView struct {
	Name      string
	CatalogID string
	ARN       string
}

// glueCrawlerView is the view model for a single Glue crawler.
type glueCrawlerView struct {
	Name         string
	DatabaseName string
	State        string
	ARN          string
}

// glueJobView is the view model for a single Glue job.
type glueJobView struct {
	Name        string
	Role        string
	GlueVersion string
	WorkerType  string
	ARN         string
}

// glueIndexData is the template data for the Glue dashboard page.
type glueIndexData struct {
	PageData

	Databases []glueDatabaseView
	Crawlers  []glueCrawlerView
	Jobs      []glueJobView
}

// setupGlueRoutes registers all Glue dashboard routes.
func (h *DashboardHandler) setupGlueRoutes() {
	h.SubRouter.GET("/dashboard/glue", h.glueIndex)
	h.SubRouter.POST("/dashboard/glue/databases/create", h.glueCreateDatabase)
	h.SubRouter.POST("/dashboard/glue/databases/delete", h.glueDeleteDatabase)
	h.SubRouter.POST("/dashboard/glue/jobs/create", h.glueCreateJob)
	h.SubRouter.POST("/dashboard/glue/jobs/delete", h.glueDeleteJob)
}

// glueIndex renders the Glue dashboard index page.
func (h *DashboardHandler) glueIndex(c *echo.Context) error {
	w := c.Response()

	snippet := &SnippetData{
		ID:    "glue-operations",
		Title: "Using AWS Glue",
		Cli: `aws glue get-databases \
    --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Glue
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
client := glue.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Glue
import boto3

client = boto3.client('glue', endpoint_url='http://localhost:8000')`,
	}

	if h.GlueOps == nil {
		h.renderTemplate(w, "glue/index.html", glueIndexData{
			PageData: PageData{
				Title:     "Glue",
				ActiveTab: "glue",
				Snippet:   snippet,
			},
			Databases: []glueDatabaseView{},
			Crawlers:  []glueCrawlerView{},
			Jobs:      []glueJobView{},
		})

		return nil
	}

	databases := h.GlueOps.Backend.GetDatabases()
	dbViews := make([]glueDatabaseView, 0, len(databases))

	for _, db := range databases {
		dbViews = append(dbViews, glueDatabaseView{
			Name:      db.Name,
			CatalogID: db.CatalogID,
			ARN:       db.ARN,
		})
	}

	crawlers := h.GlueOps.Backend.GetCrawlers()
	crawlerViews := make([]glueCrawlerView, 0, len(crawlers))

	for _, c := range crawlers {
		crawlerViews = append(crawlerViews, glueCrawlerView{
			Name:         c.Name,
			DatabaseName: c.DatabaseName,
			State:        c.State,
			ARN:          c.ARN,
		})
	}

	jobs := h.GlueOps.Backend.GetJobs()
	jobViews := make([]glueJobView, 0, len(jobs))

	for _, j := range jobs {
		jobViews = append(jobViews, glueJobView{
			Name:        j.Name,
			Role:        j.Role,
			GlueVersion: j.GlueVersion,
			WorkerType:  j.WorkerType,
			ARN:         j.ARN,
		})
	}

	h.renderTemplate(w, "glue/index.html", glueIndexData{
		PageData: PageData{
			Title:     "Glue",
			ActiveTab: "glue",
			Snippet:   snippet,
		},
		Databases: dbViews,
		Crawlers:  crawlerViews,
		Jobs:      jobViews,
	})

	return nil
}

// glueCreateDatabase handles POST /dashboard/glue/databases/create.
func (h *DashboardHandler) glueCreateDatabase(c *echo.Context) error {
	if h.GlueOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	description := c.Request().FormValue("description")

	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.GlueOps.Backend.CreateDatabase(gluebackend.DatabaseInput{
		Name:        name,
		Description: description,
	}, nil)
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/glue")
}

// glueDeleteDatabase handles POST /dashboard/glue/databases/delete.
func (h *DashboardHandler) glueDeleteDatabase(c *echo.Context) error {
	if h.GlueOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.GlueOps.Backend.DeleteDatabase(name); err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/glue")
}

// glueCreateJob handles POST /dashboard/glue/jobs/create.
func (h *DashboardHandler) glueCreateJob(c *echo.Context) error {
	if h.GlueOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	role := c.Request().FormValue("role")
	scriptLocation := c.Request().FormValue("script_location")
	glueVersion := c.Request().FormValue("glue_version")

	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if glueVersion == "" {
		glueVersion = "4.0"
	}

	_, err := h.GlueOps.Backend.CreateJob(gluebackend.Job{
		Name:        name,
		Role:        role,
		GlueVersion: glueVersion,
		Command: gluebackend.JobCommand{
			Name:           "glueetl",
			ScriptLocation: scriptLocation,
		},
	})
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/glue")
}

// glueDeleteJob handles POST /dashboard/glue/jobs/delete.
func (h *DashboardHandler) glueDeleteJob(c *echo.Context) error {
	if h.GlueOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.GlueOps.Backend.DeleteJob(name); err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/glue")
}
