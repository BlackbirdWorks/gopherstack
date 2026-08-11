package codebuild

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func doTestRequest(t *testing.T, h *Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "CodeBuild_20161006."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

func createWhiteboxTestProject(t *testing.T, h *Handler, name string) {
	t.Helper()

	rec := doTestRequest(t, h, "CreateProject", map[string]any{
		"name":      name,
		"source":    map[string]any{"type": "NO_SOURCE"},
		"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{
			"type":        "LINUX_CONTAINER",
			"image":       "aws/codebuild/standard:5.0",
			"computeType": "BUILD_GENERAL1_SMALL",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func setProjectTimestamps(b *InMemoryBackend, name string, created, lastModified time.Time) {
	b.mu.Lock("test.setProjectTimestamps")
	defer b.mu.Unlock()

	if p, ok := b.projects.Get(name); ok {
		p.Created = float64(created.Unix())
		p.LastModified = float64(lastModified.Unix())
	}
}

// TestHandler_ListProjects_SortByCreatedTime verifies sortBy=CREATED_TIME
// orders projects by creation time rather than by name. It needs direct
// access to the unexported project timestamp fields to get a deterministic
// ordering independent of wall-clock creation time.
func TestHandler_ListProjects_SortByCreatedTime(t *testing.T) {
	t.Parallel()

	h := NewHandler(NewInMemoryBackend("000000000000", "us-east-1"))
	createWhiteboxTestProject(t, h, "created-z")
	createWhiteboxTestProject(t, h, "created-a")
	createWhiteboxTestProject(t, h, "created-m")

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	setProjectTimestamps(h.Backend, "created-z", base, base)
	setProjectTimestamps(h.Backend, "created-a", base.Add(time.Hour), base.Add(time.Hour))
	setProjectTimestamps(h.Backend, "created-m", base.Add(2*time.Hour), base.Add(2*time.Hour))

	rec := doTestRequest(t, h, "ListProjects", map[string]any{"sortBy": "CREATED_TIME"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Projects []string `json:"projects"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Equal(t, []string{"created-z", "created-a", "created-m"}, out.Projects,
		"CREATED_TIME order must ignore name ordering")
}
