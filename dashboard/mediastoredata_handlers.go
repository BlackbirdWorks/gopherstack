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
		const (
			demoVideoSize = int64(10 * 1024 * 1024) // 10 MiB
			demoAudioSize = int64(5 * 1024 * 1024)  // 5 MiB
		)

		h.renderTemplate(w, "mediastoredata/index.html", mediastoredataIndexData{
			PageData: PageData{
				Title:     "MediaStore Data Objects",
				ActiveTab: "mediastoredata",
				Snippet:   mediastoredataSnippet(),
			},
			Objects: []mediastoredataObjectView{
				{
					Name:          "videos/demo-clip.mp4",
					ContentType:   "video/mp4",
					ETag:          `"a3f8b2c1d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1"`,
					ContentLength: demoVideoSize,
					LastModified:  "2026-01-01T12:00:00Z",
				},
				{
					Name:          "audio/demo-track.mp3",
					ContentType:   "audio/mpeg",
					ETag:          `"b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4"`,
					ContentLength: demoAudioSize,
					LastModified:  "2026-01-02T08:30:00Z",
				},
			},
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
