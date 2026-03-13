package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// textractJobView is the view model for a single Textract async job.
type textractJobView struct {
	JobID   string
	JobType string
	Status  string
}

// textractIndexData is the template data for the Textract index page.
type textractIndexData struct {
	PageData

	Jobs []textractJobView
}

func textractSnippet() *SnippetData {
	return &SnippetData{
		ID:    "textract-operations",
		Title: "Using Textract",
		Cli: `aws textract detect-document-text \
  --document '{"S3Object":{"Bucket":"my-bucket","Name":"doc.pdf"}}' \
  --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Textract
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithBaseEndpoint("http://localhost:8000"),
)
if err != nil {
    log.Fatal(err)
}
client := textract.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Textract
import boto3

client = boto3.client('textract', endpoint_url='http://localhost:8000')
response = client.detect_document_text(
    Document={'S3Object': {'Bucket': 'my-bucket', 'Name': 'doc.pdf'}}
)`,
	}
}

// setupTextractRoutes registers the Textract dashboard routes.
func (h *DashboardHandler) setupTextractRoutes() {
	h.SubRouter.GET("/dashboard/textract", h.textractIndex)
	h.SubRouter.POST("/dashboard/textract/start-analysis", h.textractStartAnalysis)
	h.SubRouter.POST("/dashboard/textract/start-detection", h.textractStartDetection)
}

// textractIndex renders the Textract dashboard index.
func (h *DashboardHandler) textractIndex(c *echo.Context) error {
	w := c.Response()

	pageData := PageData{Title: "Textract Jobs", ActiveTab: "textract", Snippet: textractSnippet()}

	if h.TextractOps == nil {
		h.renderTemplate(w, "textract/index.html", textractIndexData{
			PageData: pageData,
			Jobs:     []textractJobView{},
		})

		return nil
	}

	jobs := h.TextractOps.Backend.ListJobs()
	views := make([]textractJobView, 0, len(jobs))

	for _, j := range jobs {
		views = append(views, textractJobView{
			JobID:   j.JobID,
			JobType: j.JobType,
			Status:  j.JobStatus,
		})
	}

	h.renderTemplate(w, "textract/index.html", textractIndexData{
		PageData: pageData,
		Jobs:     views,
	})

	return nil
}

// textractStartJob is a helper that starts an async Textract job.
func (h *DashboardHandler) textractStartJob(
	c *echo.Context,
	start func(uri string) error,
) error {
	if h.TextractOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	bucket := c.Request().FormValue("bucket")
	key := c.Request().FormValue("key")

	if bucket == "" || key == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := start("s3://" + bucket + "/" + key); err != nil {
		h.Logger.Error("failed to start textract job", "bucket", bucket, "key", key, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/textract")
}

// textractStartAnalysis handles POST /dashboard/textract/start-analysis.
func (h *DashboardHandler) textractStartAnalysis(c *echo.Context) error {
	return h.textractStartJob(c, func(uri string) error {
		_, err := h.TextractOps.Backend.StartDocumentAnalysis(uri)

		return err
	})
}

// textractStartDetection handles POST /dashboard/textract/start-detection.
func (h *DashboardHandler) textractStartDetection(c *echo.Context) error {
	return h.textractStartJob(c, func(uri string) error {
		_, err := h.TextractOps.Backend.StartDocumentTextDetection(uri)

		return err
	})
}
