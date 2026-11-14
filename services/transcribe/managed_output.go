package transcribe

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const managedOutputPath = "/_gopherstack/transcribe/output/"

const managedPathParts = 5

type managedFetcher interface {
	ManagedOutput(kind, name, token, file string) ([]byte, bool)
}

func isManagedOutputRequest(r *http.Request) bool {
	return r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, managedOutputPath)
}

// ServeManagedOutput serves a service-managed transcript, standing in for the presigned S3 URL real Transcribe returns.
func (h *Handler) ServeManagedOutput(c *echo.Context) error {
	parts := strings.Split(strings.TrimPrefix(c.Request().URL.Path, managedOutputPath), "/")
	if len(parts) != managedPathParts {
		return c.NoContent(http.StatusNotFound)
	}

	region, kind, name, token, file := parts[0], parts[1], parts[2], parts[3], parts[4]

	f, ok := h.RegionHandler(region).Backend.(managedFetcher)
	if !ok {
		return c.NoContent(http.StatusNotFound)
	}

	data, found := f.ManagedOutput(kind, name, token, file)
	if !found {
		return c.NoContent(http.StatusForbidden)
	}

	ctype := "application/json"
	if strings.HasSuffix(file, ".vtt") {
		ctype = "text/vtt"
	} else if strings.HasSuffix(file, ".srt") {
		ctype = "application/x-subrip"
	}

	return c.Blob(http.StatusOK, ctype, data)
}
