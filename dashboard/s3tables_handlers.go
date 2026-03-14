package dashboard

import (
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// s3tablesBucketView is the view model for a single S3 Tables bucket row.
type s3tablesBucketView struct {
	ARN       string
	Name      string
	CreatedAt string
	Region    string
}

// s3tablesTableView is the view model for a single S3 Tables table row.
type s3tablesTableView struct {
	ARN            string
	Name           string
	Namespace      string
	TableBucketARN string
	Format         string
	CreatedAt      string
}

// s3tablesIndexData is the template data for the S3 Tables dashboard page.
type s3tablesIndexData struct {
	PageData

	TableBuckets []s3tablesBucketView
	Tables       []s3tablesTableView
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
	h.SubRouter.POST("/dashboard/s3tables/bucket/create", h.s3tablesCreateBucket)
	h.SubRouter.POST("/dashboard/s3tables/bucket/delete", h.s3tablesDeleteBucket)
	h.SubRouter.POST("/dashboard/s3tables/table/create", h.s3tablesCreateTable)
	h.SubRouter.POST("/dashboard/s3tables/table/delete", h.s3tablesDeleteTable)
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
			Tables:       []s3tablesTableView{},
		})

		return nil
	}

	bucketList := h.S3TablesOps.Backend.ListTableBuckets()
	bucketViews := make([]s3tablesBucketView, 0, len(bucketList))

	for _, tb := range bucketList {
		bucketViews = append(bucketViews, s3tablesBucketView{
			ARN:       tb.ARN,
			Name:      tb.Name,
			CreatedAt: tb.CreatedAt.Format("2006-01-02 15:04:05"),
			Region:    h.S3TablesOps.Backend.Region(),
		})
	}

	// Collect all tables across all buckets.
	var tableViews []s3tablesTableView

	for _, tb := range bucketList {
		tables, err := h.S3TablesOps.Backend.ListTables(tb.ARN, "")
		if err != nil {
			continue
		}

		for _, t := range tables {
			tableViews = append(tableViews, s3tablesTableView{
				ARN:            t.ARN,
				Name:           t.Name,
				Namespace:      strings.Join(t.Namespace, "."),
				TableBucketARN: t.TableBucketARN,
				Format:         t.Format,
				CreatedAt:      t.CreatedAt.Format("2006-01-02 15:04:05"),
			})
		}
	}

	if tableViews == nil {
		tableViews = []s3tablesTableView{}
	}

	h.renderTemplate(w, "s3tables/index.html", s3tablesIndexData{
		PageData: PageData{
			Title:     "S3 Tables",
			ActiveTab: "s3tables",
			Snippet:   s3tablesSnippet(),
		},
		TableBuckets: bucketViews,
		Tables:       tableViews,
	})

	return nil
}

// s3tablesCreateBucket handles POST /dashboard/s3tables/bucket/create.
func (h *DashboardHandler) s3tablesCreateBucket(c *echo.Context) error {
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

// s3tablesDeleteBucket handles POST /dashboard/s3tables/bucket/delete.
//
//nolint:dupl // intentional: each handler has unique service data despite similar structure
func (h *DashboardHandler) s3tablesDeleteBucket(c *echo.Context) error {
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

// s3tablesCreateTable handles POST /dashboard/s3tables/table/create.
func (h *DashboardHandler) s3tablesCreateTable(c *echo.Context) error {
	if h.S3TablesOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	bucketARN := c.Request().FormValue("bucket_arn")
	namespace := c.Request().FormValue("namespace")
	tableName := c.Request().FormValue("name")

	if bucketARN == "" || namespace == "" || tableName == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ctx := c.Request().Context()

	if _, err := h.S3TablesOps.Backend.CreateTable(bucketARN, []string{namespace}, tableName, "ICEBERG"); err != nil {
		h.Logger.ErrorContext(ctx, "s3tables: failed to create table", "name", tableName, "error", err)

		if errors.Is(err, awserr.ErrConflict) {
			return c.NoContent(http.StatusConflict)
		}

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/s3tables")
}

// s3tablesDeleteTable handles POST /dashboard/s3tables/table/delete.
func (h *DashboardHandler) s3tablesDeleteTable(c *echo.Context) error {
	if h.S3TablesOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	bucketARN := c.Request().FormValue("bucket_arn")
	namespace := c.Request().FormValue("namespace")
	tableName := c.Request().FormValue("name")

	if bucketARN == "" || namespace == "" || tableName == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ctx := c.Request().Context()

	if err := h.S3TablesOps.Backend.DeleteTable(bucketARN, []string{namespace}, tableName); err != nil {
		h.Logger.ErrorContext(ctx, "s3tables: failed to delete table", "name", tableName, "error", err)

		if errors.Is(err, awserr.ErrNotFound) {
			return c.NoContent(http.StatusNotFound)
		}

		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/s3tables")
}
