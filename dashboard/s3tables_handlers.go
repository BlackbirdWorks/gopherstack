package dashboard

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// s3tablesBucketView is the view model for a single S3 Tables bucket row.
type s3tablesBucketView struct {
	ARN  string
	Name string
}

// s3tablesIndexData is the template data for the S3 Tables dashboard page.
type s3tablesIndexData struct {
	PageData

	TableBuckets []s3tablesBucketView
}

// s3tablesSnippet returns the shared SnippetData for the S3 Tables dashboard.
func s3tablesSnippet() *SnippetData {
	return &SnippetData{
		ID:    "s3tables-operations",
		Title: "Using Amazon S3 Tables",
		Cli:   `aws s3tables list-table-buckets --endpoint-url http://localhost:8000`,
		Go: `// Initialize the S3 Tables client using AWS SDK v2.
import (
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/s3tables"
)

client := s3tables.NewFromConfig(cfg, func(o *s3tables.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})`,
		Python: `# Initialize boto3 client for S3 Tables
import boto3

client = boto3.client('s3tables', endpoint_url='http://localhost:8000')`,
	}
}

// setupS3TablesRoutes registers all S3 Tables dashboard routes.
func (h *DashboardHandler) setupS3TablesRoutes() {
	h.SubRouter.GET("/dashboard/s3tables", h.s3tablesIndex)
	h.SubRouter.POST("/dashboard/s3tables/create", h.s3tablesCreate)
	h.SubRouter.POST("/dashboard/s3tables/delete", h.s3tablesDelete)
}

// s3tablesIndex renders the main S3 Tables dashboard page.
func (h *DashboardHandler) s3tablesIndex(c *echo.Context) error {
	w := c.Response()

	if h.S3TablesOps == nil {
		h.renderTemplate(w, "s3tables/index.html", s3tablesIndexData{
			PageData: PageData{
				Title:     "S3 Tables",
				ActiveTab: "s3tables",
				Snippet:   s3tablesSnippet(),
			},
			TableBuckets: []s3tablesBucketView{},
		})

		return nil
	}

	list := h.S3TablesOps.Backend.ListTableBuckets()
	views := make([]s3tablesBucketView, 0, len(list))

	for _, tb := range list {
		views = append(views, s3tablesBucketView{
			ARN:  tb.ARN,
			Name: tb.Name,
		})
	}

	h.renderTemplate(w, "s3tables/index.html", s3tablesIndexData{
		PageData: PageData{
			Title:     "S3 Tables",
			ActiveTab: "s3tables",
			Snippet:   s3tablesSnippet(),
		},
		TableBuckets: views,
	})

	return nil
}

// s3tablesCreate handles POST /dashboard/s3tables/create.
func (h *DashboardHandler) s3tablesCreate(c *echo.Context) error {
	if h.S3TablesOps == nil {
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

	if _, err := h.S3TablesOps.Backend.CreateTableBucket(name); err != nil {
		h.Logger.ErrorContext(ctx, "s3tables: failed to create table bucket", "name", name, "error", err)

		if errors.Is(err, awserr.ErrConflict) {
			return c.NoContent(http.StatusConflict)
		}

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/s3tables")
}

// s3tablesDelete handles POST /dashboard/s3tables/delete.
//
//nolint:dupl // intentional: each handler has unique service data despite similar structure
func (h *DashboardHandler) s3tablesDelete(c *echo.Context) error {
	if h.S3TablesOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	bucketARN := c.Request().FormValue("arn")
	if bucketARN == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ctx := c.Request().Context()

	if err := h.S3TablesOps.Backend.DeleteTableBucket(bucketARN); err != nil {
		h.Logger.ErrorContext(ctx, "s3tables: failed to delete table bucket", "arn", bucketARN, "error", err)

		if errors.Is(err, awserr.ErrNotFound) {
			return c.NoContent(http.StatusNotFound)
		}

		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/s3tables")
}
