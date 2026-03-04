package dashboard

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// consoleIndex renders the Live API Console page.
func (h *DashboardHandler) consoleIndex(c *echo.Context) error {
	w := c.Response()

	data := struct {
		PageData
	}{
		PageData: PageData{
			Title:     "Live Console",
			ActiveTab: "console",

			Snippet: &SnippetData{
				ID:    "console-operations",
				Title: "Using Live Console",
				Cli:   "aws console help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Live Console */",
				Python: "# Write boto3 code for Live Console\nimport boto3\n" +
					"client = boto3.client('console', endpoint_url='http://localhost:8000')",
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
	for i := range len(requests) / 2 {
		j := len(requests) - i - 1
		requests[i], requests[j] = requests[j], requests[i]
	}

	return c.JSON(http.StatusOK, map[string]any{
		"requests": requests,
	})
}

// consoleAPIStream creates a Server-Sent Events (SSE) stream for new console requests.
func (h *DashboardHandler) consoleAPIStream(c *echo.Context) error {
	c.Response().Header().Set(echo.HeaderContentType, "text/event-stream")
	c.Response().Header().Set(echo.HeaderCacheControl, "no-cache")
	c.Response().Header().Set(echo.HeaderConnection, "keep-alive")

	w := c.Response()
	w.WriteHeader(http.StatusOK)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	ch := logger.GlobalRingBuffer.Subscribe()

	defer logger.GlobalRingBuffer.Unsubscribe(ch)

	encoder := json.NewEncoder(w)

	// Send a keep-alive ping every 2 seconds to prevent browser/proxy timeouts.
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.Request().Context().Done():
			return nil
		case <-ticker.C:
			w.Write([]byte(":\n\n"))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		case req := <-ch:
			w.Write([]byte("data: "))
			if err := encoder.Encode(req); err != nil {
				h.Logger.Error("failed to encode event stream data", "err", err)
				return err
			}
			w.Write([]byte("\n\n"))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}
}
