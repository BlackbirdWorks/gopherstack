package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// efsFileSystemView is the view model for a single EFS file system.
type efsFileSystemView struct {
	ID              string
	ARN             string
	CreationToken   string
	PerformanceMode string
	ThroughputMode  string
	LifeCycleState  string
}

// efsIndexData is the template data for the EFS index page.
type efsIndexData struct {
	PageData

	FileSystems []efsFileSystemView
}

// efsSnippet returns the shared SnippetData for the EFS dashboard pages.
func efsSnippet() *SnippetData {
	return &SnippetData{
		ID:    "efs-operations",
		Title: "Using EFS",
		Cli:   `aws efs describe-file-systems --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for EFS
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
client := efs.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for EFS
import boto3

client = boto3.client('efs', endpoint_url='http://localhost:8000')`,
	}
}

// efsIndex renders the EFS dashboard index.
func (h *DashboardHandler) efsIndex(c *echo.Context) error {
	w := c.Response()

	if h.EFSOps == nil {
		h.renderTemplate(w, "efs/index.html", efsIndexData{
			PageData:    PageData{Title: "EFS File Systems", ActiveTab: "efs", Snippet: efsSnippet()},
			FileSystems: []efsFileSystemView{},
		})

		return nil
	}

	fsList, _ := h.EFSOps.Backend.DescribeFileSystems("")
	views := make([]efsFileSystemView, 0, len(fsList))

	for _, fs := range fsList {
		views = append(views, efsFileSystemView{
			ID:              fs.FileSystemID,
			ARN:             fs.FileSystemArn,
			CreationToken:   fs.CreationToken,
			PerformanceMode: fs.PerformanceMode,
			ThroughputMode:  fs.ThroughputMode,
			LifeCycleState:  fs.LifeCycleState,
		})
	}

	h.renderTemplate(w, "efs/index.html", efsIndexData{
		PageData:    PageData{Title: "EFS File Systems", ActiveTab: "efs", Snippet: efsSnippet()},
		FileSystems: views,
	})

	return nil
}

// efsCreateFileSystem handles POST /dashboard/efs/filesystem/create.
func (h *DashboardHandler) efsCreateFileSystem(c *echo.Context) error {
	if h.EFSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	token := c.Request().FormValue("creation_token")
	if token == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.EFSOps.Backend.CreateFileSystem(token, "", "", false, nil)
	if err != nil {
		h.Logger.Error("failed to create EFS file system", "token", token, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/efs")
}

// efsDeleteFileSystem handles POST /dashboard/efs/filesystem/delete.
func (h *DashboardHandler) efsDeleteFileSystem(c *echo.Context) error {
	if h.EFSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := c.Request().FormValue("id")
	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.EFSOps.Backend.DeleteFileSystem(id); err != nil {
		h.Logger.Error("failed to delete EFS file system", "id", id, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/efs")
}
