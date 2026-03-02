package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// consoleIndex renders the Live API Console page.
func (h *DashboardHandler) consoleIndex(c *echo.Context) error {
	w := c.Response()

	data := struct {
		PageData //nolint:embeddedstructfieldcheck
	}{
		PageData: PageData{
			Title:     "Live Console",
			ActiveTab: "console",
		
		Snippet: &SnippetData{
			ID:    "console-operations",
			Title: "Using Live Console",
			Cli:   "aws console help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Live Console */",
			Python: "# Write boto3 code for Live Console\nimport boto3\nclient = boto3.client('console', endpoint_url='http://localhost:8000')",
		},
},
	}

	h.renderTemplate(w, "apiconsole.html", data)

	return nil
}

// consoleAPI returns the latest captured requests as JSON.
func (h *DashboardHandler) consoleAPI(c *echo.Context) error {
	requests := logger.GlobalRingBuffer.GetAll()

	// Reverse to show newest first
	for i := 0; i < len(requests)/2; i++ {
		j := len(requests) - i - 1
		requests[i], requests[j] = requests[j], requests[i]
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"requests": requests,
	})
}
