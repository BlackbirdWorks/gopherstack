package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/teststack"
)

// TestDashboard_Textract_Index covers the Textract dashboard index handler.
func TestDashboard_Textract_Index(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *teststack.Stack)
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "index renders page header",
			wantCode:     http.StatusOK,
			wantContains: "Textract Jobs",
		},
		{
			name: "index shows analysis job",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.TextractHandler.Backend.StartDocumentAnalysis("s3://bucket/doc.pdf")
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "DocumentAnalysis",
		},
		{
			name: "index shows text detection job",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.TextractHandler.Backend.StartDocumentTextDetection("s3://bucket/page.png")
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "TextDetection",
		},
		{
			name:         "index shows empty state",
			wantCode:     http.StatusOK,
			wantContains: "No Textract jobs found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/textract", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_Textract_StartAnalysis covers the start document analysis handler.
func TestDashboard_Textract_StartAnalysis(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		key      string
		wantCode int
	}{
		{
			name:     "valid request redirects",
			bucket:   "my-bucket",
			key:      "invoice.pdf",
			wantCode: http.StatusSeeOther,
		},
		{
			name:     "missing bucket returns bad request",
			bucket:   "",
			key:      "invoice.pdf",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing key returns bad request",
			bucket:   "my-bucket",
			key:      "",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			form := url.Values{}
			if tt.bucket != "" {
				form.Set("bucket", tt.bucket)
			}

			if tt.key != "" {
				form.Set("key", tt.key)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/textract/start-analysis",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

// TestDashboard_Textract_StartDetection covers the start text detection handler.
func TestDashboard_Textract_StartDetection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		key      string
		wantCode int
	}{
		{
			name:     "valid request redirects",
			bucket:   "my-bucket",
			key:      "page.png",
			wantCode: http.StatusSeeOther,
		},
		{
			name:     "missing bucket returns bad request",
			bucket:   "",
			key:      "page.png",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing key returns bad request",
			bucket:   "my-bucket",
			key:      "",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			form := url.Values{}
			if tt.bucket != "" {
				form.Set("bucket", tt.bucket)
			}

			if tt.key != "" {
				form.Set("key", tt.key)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/textract/start-detection",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}
