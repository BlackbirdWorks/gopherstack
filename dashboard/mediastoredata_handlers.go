package dashboard

import (
	"time"

	"github.com/labstack/echo/v5"
)

// mediastoredataObjectView is the view model for a single MediaStore Data object row.
type mediastoredataObjectView struct {
	Name          string
	ContentType   string
	ETag          string
	LastModified  string
	ContentLength int64
}

// mediastoredataIndexData is the template data for the MediaStore Data dashboard page.
type mediastoredataIndexData struct {
	PageData

	Objects []mediastoredataObjectView
}

// mediastoredataSnippet returns the shared SnippetData for the MediaStore Data dashboard.
func mediastoredataSnippet() *SnippetData {
	return &SnippetData{
		ID:    "mediastoredata-operations",
		Title: "Using MediaStore Data",
		Cli:   `aws mediastore-data list-items --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for MediaStore Data
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
)
if err != nil {
    log.Fatal(err)
}
client := mediastoredata.NewFromConfig(cfg, func(o *mediastoredata.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})`,
		Python: `# Initialize boto3 client for MediaStore Data
import boto3

client = boto3.client('mediastore-data', endpoint_url='http://localhost:8000')`,
	}
}

// setupMediaStoreDataRoutes registers all MediaStore Data dashboard routes.
func (h *DashboardHandler) setupMediaStoreDataRoutes() {
	h.SubRouter.GET("/dashboard/mediastoredata", h.mediastoredataIndex)
}

// mediastoredataIndex renders the main MediaStore Data dashboard page.
func (h *DashboardHandler) mediastoredataIndex(c *echo.Context) error {
	w := c.Response()

	if h.MediaStoreDataOps == nil {
		h.renderTemplate(w, "mediastoredata/index.html", mediastoredataIndexData{
			PageData: PageData{
				Title:     "MediaStore Data Objects",
				ActiveTab: "mediastoredata",
				Snippet:   mediastoredataSnippet(),
			},
			Objects: []mediastoredataObjectView{},
		})

		return nil
	}

	items := h.MediaStoreDataOps.Backend.ListAllObjects()
	views := make([]mediastoredataObjectView, 0, len(items))

	for _, item := range items {
		views = append(views, mediastoredataObjectView{
			Name:          item.Name,
			ContentType:   item.ContentType,
			ETag:          item.ETag,
			ContentLength: item.ContentLength,
			LastModified:  item.LastModified.UTC().Format(time.RFC3339),
		})
	}

	h.renderTemplate(w, "mediastoredata/index.html", mediastoredataIndexData{
		PageData: PageData{
			Title:     "MediaStore Data Objects",
			ActiveTab: "mediastoredata",
			Snippet:   mediastoredataSnippet(),
		},
		Objects: views,
	})

	return nil
}
