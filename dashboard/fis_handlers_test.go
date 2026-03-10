package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	fisbackend "github.com/blackbirdworks/gopherstack/services/fis"
)

// TestDashboard_FIS_Index verifies the FIS dashboard index page renders correctly.
func TestDashboard_FIS_Index(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*fisbackend.Handler)
		name     string
		wantBody string
		wantCode int
	}{
		{
			name:     "renders empty state",
			setup:    nil,
			wantCode: http.StatusOK,
			wantBody: "Experiment Templates",
		},
		{
			name: "shows experiment template",
			setup: func(h *fisbackend.Handler) {
				_, err := h.Backend.CreateExperimentTemplate(
					&fisbackend.ExportedCreateTemplateRequest{
						Description: "Test template",
						RoleArn:     "arn:aws:iam::000000000000:role/fis-role",
						Actions: map[string]fisbackend.ExportedActionDTO{
							"action1": {
								ActionID:   "aws:fis:inject-api-throttle-error",
								Parameters: map[string]string{"service": "s3", "duration": "PT5M"},
							},
						},
						StopConditions: []fisbackend.ExportedStopConditionDTO{{Source: "none"}},
						Tags:           map[string]string{},
					},
					"000000000000", "us-east-1",
				)
				require.NoError(t, err)
			},
			wantCode: http.StatusOK,
			wantBody: "Test template",
		},
		{
			name:     "shows action catalog",
			setup:    nil,
			wantCode: http.StatusOK,
			wantBody: "Action Catalog",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fisHndlr := fisbackend.NewHandler(fisbackend.NewInMemoryBackend("000000000000", "us-east-1"))
			if tt.setup != nil {
				tt.setup(fisHndlr)
			}

			h := dashboard.NewHandler(dashboard.Config{
				FISOps: fisHndlr,
			})

			req := httptest.NewRequest(http.MethodGet, "/dashboard/fis", nil)
			w := httptest.NewRecorder()
			serveHandler(h, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			if tt.wantBody != "" {
				assert.Contains(t, w.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestDashboard_FIS_CreateTemplate verifies the create template flow.
func TestDashboard_FIS_CreateTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		formValues   url.Values
		name         string
		wantLocation string
		wantCode     int
	}{
		{
			name: "creates template and redirects",
			formValues: url.Values{
				"description": {"E2E throttle test"},
				"roleArn":     {"arn:aws:iam::000000000000:role/fis-role"},
				"actionId":    {"aws:fis:inject-api-throttle-error"},
				"actionName":  {"throttle"},
				"service":     {"s3"},
				"duration":    {"PT5M"},
			},
			wantCode:     http.StatusFound,
			wantLocation: "/dashboard/fis",
		},
		{
			name:         "returns 400 when description is missing",
			formValues:   url.Values{"roleArn": {"arn:aws:iam::000000000000:role/r"}, "actionId": {"aws:fis:wait"}},
			wantCode:     http.StatusBadRequest,
			wantLocation: "",
		},
		{
			name: "returns 503 when FIS handler is nil",
			formValues: url.Values{
				"description": {"x"},
				"roleArn":     {"arn:aws:iam::000000000000:role/r"},
				"actionId":    {"aws:fis:wait"},
			},
			wantCode:     http.StatusServiceUnavailable,
			wantLocation: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *dashboard.DashboardHandler
			if tt.name == "returns 503 when FIS handler is nil" {
				h = dashboard.NewHandler(dashboard.Config{})
			} else {
				fisHndlr := fisbackend.NewHandler(fisbackend.NewInMemoryBackend("000000000000", "us-east-1"))
				h = dashboard.NewHandler(dashboard.Config{FISOps: fisHndlr})
			}

			body := tt.formValues.Encode()
			req := httptest.NewRequest(http.MethodPost, "/dashboard/fis/templates/create",
				strings.NewReader(body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(h, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			if tt.wantLocation != "" {
				assert.Equal(t, tt.wantLocation, w.Header().Get("Location"))
			}
		})
	}
}

// TestDashboard_FIS_StartExperiment verifies the start experiment flow.
func TestDashboard_FIS_StartExperiment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*fisbackend.Handler) string
		name         string
		wantLocation string
		wantCode     int
	}{
		{
			name: "starts experiment and redirects",
			setup: func(h *fisbackend.Handler) string {
				tpl, err := h.Backend.CreateExperimentTemplate(
					&fisbackend.ExportedCreateTemplateRequest{
						Description: "wait template",
						RoleArn:     "arn:aws:iam::000000000000:role/r",
						Actions: map[string]fisbackend.ExportedActionDTO{
							"wait": {
								ActionID:   "aws:fis:wait",
								Parameters: map[string]string{"duration": "PT1M"},
							},
						},
						StopConditions: []fisbackend.ExportedStopConditionDTO{{Source: "none"}},
						Tags:           map[string]string{},
					},
					"000000000000", "us-east-1",
				)
				require.NoError(t, err)

				return tpl.ID
			},
			wantCode:     http.StatusFound,
			wantLocation: "/dashboard/fis",
		},
		{
			name:         "returns 400 when templateId is missing",
			setup:        func(_ *fisbackend.Handler) string { return "" },
			wantCode:     http.StatusBadRequest,
			wantLocation: "",
		},
		{
			name: "returns 503 when FIS handler is nil",
			setup: func(_ *fisbackend.Handler) string {
				return "any-id"
			},
			wantCode:     http.StatusServiceUnavailable,
			wantLocation: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *dashboard.DashboardHandler
			var templateID string

			if tt.name == "returns 503 when FIS handler is nil" {
				h = dashboard.NewHandler(dashboard.Config{})
				templateID = "any-id"
			} else {
				fisHndlr := fisbackend.NewHandler(fisbackend.NewInMemoryBackend("000000000000", "us-east-1"))
				templateID = tt.setup(fisHndlr)
				h = dashboard.NewHandler(dashboard.Config{FISOps: fisHndlr})
			}

			body := url.Values{"templateId": {templateID}}.Encode()
			req := httptest.NewRequest(http.MethodPost, "/dashboard/fis/experiments/start",
				strings.NewReader(body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(h, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			if tt.wantLocation != "" {
				assert.Equal(t, tt.wantLocation, w.Header().Get("Location"))
			}
		})
	}
}

// TestDashboard_FIS_StopExperiment verifies the stop experiment flow.
func TestDashboard_FIS_StopExperiment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*fisbackend.Handler) string
		name         string
		wantLocation string
		wantCode     int
	}{
		{
			name: "stops running experiment and redirects",
			setup: func(h *fisbackend.Handler) string {
				tpl, err := h.Backend.CreateExperimentTemplate(
					&fisbackend.ExportedCreateTemplateRequest{
						Description: "wait tpl",
						RoleArn:     "arn:aws:iam::000000000000:role/r",
						Actions: map[string]fisbackend.ExportedActionDTO{
							"wait": {
								ActionID:   "aws:fis:wait",
								Parameters: map[string]string{"duration": "PT5M"},
							},
						},
						StopConditions: []fisbackend.ExportedStopConditionDTO{{Source: "none"}},
						Tags:           map[string]string{},
					},
					"000000000000", "us-east-1",
				)
				require.NoError(t, err)

				exp, startErr := h.Backend.StartExperiment(
					t.Context(),
					&fisbackend.ExportedStartExperimentRequest{
						ExperimentTemplateID: tpl.ID,
						Tags:                 map[string]string{},
					},
					"000000000000", "us-east-1",
				)
				require.NoError(t, startErr)

				return exp.ID
			},
			wantCode:     http.StatusFound,
			wantLocation: "/dashboard/fis",
		},
		{
			name:         "returns 400 when id is missing",
			setup:        func(_ *fisbackend.Handler) string { return "" },
			wantCode:     http.StatusBadRequest,
			wantLocation: "",
		},
		{
			name: "returns 503 when FIS handler is nil",
			setup: func(_ *fisbackend.Handler) string {
				return "any-id"
			},
			wantCode:     http.StatusServiceUnavailable,
			wantLocation: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *dashboard.DashboardHandler
			var experimentID string

			if tt.name == "returns 503 when FIS handler is nil" {
				h = dashboard.NewHandler(dashboard.Config{})
				experimentID = "any-id"
			} else {
				fisHndlr := fisbackend.NewHandler(fisbackend.NewInMemoryBackend("000000000000", "us-east-1"))
				experimentID = tt.setup(fisHndlr)
				h = dashboard.NewHandler(dashboard.Config{FISOps: fisHndlr})
			}

			body := url.Values{"id": {experimentID}}.Encode()
			req := httptest.NewRequest(http.MethodPost, "/dashboard/fis/experiments/stop",
				strings.NewReader(body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(h, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			if tt.wantLocation != "" {
				assert.Equal(t, tt.wantLocation, w.Header().Get("Location"))
			}
		})
	}
}
func TestDashboard_FIS_DeleteTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*fisbackend.Handler) string
		name         string
		wantLocation string
		wantCode     int
	}{
		{
			name: "deletes existing template and redirects",
			setup: func(h *fisbackend.Handler) string {
				tpl, err := h.Backend.CreateExperimentTemplate(
					&fisbackend.ExportedCreateTemplateRequest{
						Description: "to delete",
						RoleArn:     "arn:aws:iam::000000000000:role/r",
						Actions: map[string]fisbackend.ExportedActionDTO{
							"a": {
								ActionID:   "aws:fis:wait",
								Parameters: map[string]string{"duration": "PT1M"},
							},
						},
						StopConditions: []fisbackend.ExportedStopConditionDTO{{Source: "none"}},
						Tags:           map[string]string{},
					},
					"000000000000", "us-east-1",
				)
				require.NoError(t, err)

				return tpl.ID
			},
			wantCode:     http.StatusFound,
			wantLocation: "/dashboard/fis",
		},
		{
			name:         "returns 400 when ID is empty",
			setup:        func(_ *fisbackend.Handler) string { return "" },
			wantCode:     http.StatusBadRequest,
			wantLocation: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fisHndlr := fisbackend.NewHandler(fisbackend.NewInMemoryBackend("000000000000", "us-east-1"))
			id := tt.setup(fisHndlr)

			h := dashboard.NewHandler(dashboard.Config{FISOps: fisHndlr})

			body := url.Values{"id": {id}}.Encode()
			req := httptest.NewRequest(http.MethodPost, "/dashboard/fis/templates/delete",
				strings.NewReader(body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(h, w, req)

			assert.Equal(t, tt.wantCode, w.Code)
			if tt.wantLocation != "" {
				assert.Equal(t, tt.wantLocation, w.Header().Get("Location"))
			}
		})
	}
}
