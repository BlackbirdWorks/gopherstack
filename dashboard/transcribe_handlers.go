package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// transcribeJobView is the view model for a single transcription job.
type transcribeJobView struct {
	JobName  string
	Status   string
	Language string
	URI      string
}

// transcribeIndexData is the template data for the Transcribe index page.
type transcribeIndexData struct {
	PageData

	Jobs []transcribeJobView
}

// transcribeIndex renders the Transcribe dashboard index.
func (h *DashboardHandler) transcribeIndex(c *echo.Context) error {
	w := c.Response()

	if h.TranscribeOps == nil {
		h.renderTemplate(w, "transcribe/index.html", transcribeIndexData{
			PageData: PageData{Title: "Transcribe Jobs", ActiveTab: "transcribe",
		Snippet: &SnippetData{
			ID:    "transcribe-operations",
			Title: "Using Transcribe",
			Cli:   "aws transcribe help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Transcribe */",
			Python: "# Write boto3 code for Transcribe\nimport boto3\nclient = boto3.client('transcribe', endpoint_url='http://localhost:8000')",
		},},
			Jobs:     []transcribeJobView{},
		})

		return nil
	}

	jobs := h.TranscribeOps.Backend.ListTranscriptionJobs("")
	views := make([]transcribeJobView, 0, len(jobs))

	for _, j := range jobs {
		views = append(views, transcribeJobView{
			JobName:  j.JobName,
			Status:   j.JobStatus,
			Language: j.LanguageCode,
			URI:      j.MediaFileURI,
		})
	}

	h.renderTemplate(w, "transcribe/index.html", transcribeIndexData{
		PageData: PageData{Title: "Transcribe Jobs", ActiveTab: "transcribe",
		Snippet: &SnippetData{
			ID:    "transcribe-operations",
			Title: "Using Transcribe",
			Cli:   "aws transcribe help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Transcribe */",
			Python: `# Write boto3 code for Transcribe
import boto3
client = boto3.client('transcribe', endpoint_url='http://localhost:8000')`,
		},},
		Jobs:     views,
	})

	return nil
}

// transcribeStartJob handles POST /dashboard/transcribe/start.
func (h *DashboardHandler) transcribeStartJob(c *echo.Context) error {
	if h.TranscribeOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	jobName := c.Request().FormValue("jobName")
	languageCode := c.Request().FormValue("languageCode")
	mediaFileURI := c.Request().FormValue("mediaFileUri")

	if jobName == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.TranscribeOps.Backend.StartTranscriptionJob(jobName, languageCode, mediaFileURI); err != nil {
		h.Logger.Error("failed to start transcription job", "name", jobName, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/transcribe")
}
