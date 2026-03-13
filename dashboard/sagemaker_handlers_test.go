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

// TestDashboard_SageMaker_Index covers the SageMaker dashboard index handler.
func TestDashboard_SageMaker_Index(t *testing.T) {
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
			wantContains: "SageMaker Models",
		},
		{
			name: "index renders created model",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.SageMakerHandler.Backend.CreateModel(
					"dashboard-test-model",
					"arn:aws:iam::000000000000:role/test",
					nil,
					nil,
					nil,
				)
				require.NoError(t, err)
			},
			wantCode:     http.StatusOK,
			wantContains: "dashboard-test-model",
		},
		{
			name:         "index shows no models message when empty",
			wantCode:     http.StatusOK,
			wantContains: "No models found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/sagemaker", nil)
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
			assert.Contains(t, w.Body.String(), tt.wantContains)
		})
	}
}

// TestDashboard_SageMaker_Create covers the SageMaker model create handler.
func TestDashboard_SageMaker_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *teststack.Stack)
		formValues map[string]string
		name       string
		wantCode   int
	}{
		{
			name: "create model redirects",
			formValues: map[string]string{
				"name": "new-model",
			},
			wantCode: http.StatusSeeOther,
		},
		{
			name:       "missing name returns bad request",
			formValues: map[string]string{},
			wantCode:   http.StatusBadRequest,
		},
		{
			name: "duplicate model name returns bad request",
			setup: func(t *testing.T, s *teststack.Stack) {
				t.Helper()

				_, err := s.SageMakerHandler.Backend.CreateModel("dup-model", "", nil, nil, nil)
				require.NoError(t, err)
			},
			formValues: map[string]string{
				"name": "dup-model",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)

			if tt.setup != nil {
				tt.setup(t, s)
			}

			form := url.Values{}
			for k, v := range tt.formValues {
				form.Set(k, v)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/sagemaker/create",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

// TestDashboard_SageMaker_Delete covers the SageMaker model delete handler.
func TestDashboard_SageMaker_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *teststack.Stack) string
		name     string
		wantCode int
	}{
		{
			name: "delete existing model redirects",
			setup: func(t *testing.T, s *teststack.Stack) string {
				t.Helper()

				m, err := s.SageMakerHandler.Backend.CreateModel("del-model", "", nil, nil, nil)
				require.NoError(t, err)

				return m.ModelName
			},
			wantCode: http.StatusSeeOther,
		},
		{
			name: "missing name returns bad request",
			setup: func(_ *testing.T, _ *teststack.Stack) string {
				return ""
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "not found model returns 404",
			setup: func(_ *testing.T, _ *teststack.Stack) string {
				return "does-not-exist"
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t)
			modelName := tt.setup(t, s)

			form := url.Values{}
			if modelName != "" {
				form.Set("name", modelName)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/sagemaker/delete",
				strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(s.Dashboard, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}
