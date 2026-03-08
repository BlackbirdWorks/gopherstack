package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
)

// TestDashboard_Chaos_Index tests the Chaos Engineering dashboard index page.
func TestDashboard_Chaos_Index(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*chaos.FaultStore)
		name     string
		wantBody string
		wantCode int
	}{
		{
			name:     "renders when fault store is present",
			setup:    nil,
			wantCode: http.StatusOK,
			wantBody: "Chaos Engineering",
		},
		{
			name: "shows active fault rule",
			setup: func(fs *chaos.FaultStore) {
				fs.AppendRules([]chaos.FaultRule{
					{
						Service:     "s3",
						Probability: 1.0,
						Error:       &chaos.FaultError{StatusCode: 503, Code: "ServiceUnavailable"},
					},
				})
			},
			wantCode: http.StatusOK,
			wantBody: "s3",
		},
		{
			name: "shows 100% probability rule",
			setup: func(fs *chaos.FaultStore) {
				fs.AppendRules([]chaos.FaultRule{
					{Service: "dynamodb", Probability: 1.0},
				})
			},
			wantCode: http.StatusOK,
			wantBody: "dynamodb",
		},
		{
			name: "shows partial probability rule",
			setup: func(fs *chaos.FaultStore) {
				fs.AppendRules([]chaos.FaultRule{
					{Service: "sqs", Probability: 0.3},
				})
			},
			wantCode: http.StatusOK,
			wantBody: "sqs",
		},
		{
			name:     "returns 503 when fault store is nil",
			setup:    nil,
			wantCode: http.StatusServiceUnavailable,
			wantBody: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.name == "returns 503 when fault store is nil" {
				h := dashboard.NewHandler(dashboard.Config{})
				req := httptest.NewRequest(http.MethodGet, "/dashboard/chaos", nil)
				w := httptest.NewRecorder()
				serveHandler(h, w, req)
				require.Equal(t, tt.wantCode, w.Code)

				return
			}

			stack := newStack(t)
			if tt.setup != nil {
				tt.setup(stack.FaultStore)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/chaos", nil)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			require.Equal(t, tt.wantCode, w.Code)
			if tt.wantBody != "" {
				assert.Contains(t, w.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestDashboard_Chaos_RulesFragment tests the HTMX rules fragment endpoint.
func TestDashboard_Chaos_RulesFragment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*chaos.FaultStore)
		name     string
		wantBody string
		wantCode int
	}{
		{
			name:     "empty rules shows empty state",
			wantCode: http.StatusOK,
			wantBody: "No active fault rules",
		},
		{
			name: "shows rule in table",
			setup: func(fs *chaos.FaultStore) {
				fs.AppendRules([]chaos.FaultRule{
					{Service: "lambda", Region: "us-east-1", Probability: 0.5},
				})
			},
			wantCode: http.StatusOK,
			wantBody: "lambda",
		},
		{
			name: "wildcard fields shown as asterisk",
			setup: func(fs *chaos.FaultStore) {
				fs.AppendRules([]chaos.FaultRule{{Probability: 1.0}})
			},
			wantCode: http.StatusOK,
			wantBody: "*",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			if tt.setup != nil {
				tt.setup(stack.FaultStore)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/chaos/rules", nil)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			require.Equal(t, tt.wantCode, w.Code)
			if tt.wantBody != "" {
				assert.Contains(t, w.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestDashboard_Chaos_ActivityFragment tests the HTMX activity fragment endpoint.
func TestDashboard_Chaos_ActivityFragment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*chaos.FaultStore)
		name     string
		wantBody string
		wantCode int
	}{
		{
			name:     "empty activity shows placeholder",
			wantCode: http.StatusOK,
			wantBody: "No activity yet",
		},
		{
			name: "shows triggered event",
			setup: func(fs *chaos.FaultStore) {
				fs.RecordActivity(chaos.ActivityEvent{
					Service:      "s3",
					Operation:    "PutObject",
					Region:       "us-east-1",
					FaultApplied: "ServiceUnavailable",
					Probability:  1.0,
					Triggered:    true,
				})
			},
			wantCode: http.StatusOK,
			wantBody: "ServiceUnavailable",
		},
		{
			name: "shows missed event",
			setup: func(fs *chaos.FaultStore) {
				fs.RecordActivity(chaos.ActivityEvent{
					Service:     "dynamodb",
					Operation:   "GetItem",
					Region:      "us-east-1",
					Probability: 0.1,
					Triggered:   false,
				})
			},
			wantCode: http.StatusOK,
			wantBody: "dynamodb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			if tt.setup != nil {
				tt.setup(stack.FaultStore)
			}

			req := httptest.NewRequest(http.MethodGet, "/dashboard/chaos/activity", nil)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			require.Equal(t, tt.wantCode, w.Code)
			if tt.wantBody != "" {
				assert.Contains(t, w.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestDashboard_Chaos_AddFault tests adding a fault rule via form POST.
func TestDashboard_Chaos_AddFault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form        string
		name        string
		wantService string
		wantCode    int
		wantRules   int
	}{
		{
			name: "add basic fault rule",
			form: "service=s3&region=us-east-1&operation=PutObject" +
				"&probability=100&statusCode=503&errorCode=ServiceUnavailable",
			wantCode:    http.StatusOK,
			wantService: "s3",
			wantRules:   1,
		},
		{
			name:      "add wildcard fault rule",
			form:      "service=&region=&operation=&probability=50&statusCode=500&errorCode=InternalServerError",
			wantCode:  http.StatusOK,
			wantRules: 1,
		},
		{
			name:      "add second rule stacks",
			form:      "service=dynamodb&probability=30&statusCode=400&errorCode=ThrottlingException",
			wantCode:  http.StatusOK,
			wantRules: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)

			req := httptest.NewRequest(http.MethodPost, "/dashboard/chaos/faults",
				strings.NewReader(tt.form))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			require.Equal(t, tt.wantCode, w.Code)
			require.Len(t, stack.FaultStore.GetRules(), tt.wantRules)

			if tt.wantService != "" {
				assert.Equal(t, tt.wantService, stack.FaultStore.GetRules()[0].Service)
			}
		})
	}
}

// TestDashboard_Chaos_DeleteFault tests deleting a fault rule.
func TestDashboard_Chaos_DeleteFault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		body      string
		wantCode  int
		wantRules int
	}{
		{
			name:      "delete existing rule",
			body:      `{"service":"s3","region":"","operation":""}`,
			wantCode:  http.StatusOK,
			wantRules: 0,
		},
		{
			name:      "delete non-existent rule is no-op",
			body:      `{"service":"dynamodb","region":"","operation":""}`,
			wantCode:  http.StatusOK,
			wantRules: 1,
		},
		{
			name:      "invalid JSON returns 400",
			body:      "not-json",
			wantCode:  http.StatusBadRequest,
			wantRules: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			stack.FaultStore.AppendRules([]chaos.FaultRule{{Service: "s3", Probability: 1.0}})

			req := httptest.NewRequest(http.MethodDelete, "/dashboard/chaos/faults",
				strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			require.Equal(t, tt.wantCode, w.Code)
			assert.Len(t, stack.FaultStore.GetRules(), tt.wantRules)
		})
	}
}

// TestDashboard_Chaos_ClearFaults tests clearing all fault rules.
func TestDashboard_Chaos_ClearFaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*chaos.FaultStore)
		name      string
		wantCode  int
		wantRules int
	}{
		{
			name: "clears all rules",
			setup: func(fs *chaos.FaultStore) {
				fs.AppendRules([]chaos.FaultRule{
					{Service: "s3"},
					{Service: "dynamodb"},
					{Service: "sqs"},
				})
			},
			wantCode:  http.StatusOK,
			wantRules: 0,
		},
		{
			name:      "clear when already empty is no-op",
			setup:     nil,
			wantCode:  http.StatusOK,
			wantRules: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			if tt.setup != nil {
				tt.setup(stack.FaultStore)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/chaos/faults/clear", nil)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			require.Equal(t, tt.wantCode, w.Code)
			assert.Len(t, stack.FaultStore.GetRules(), tt.wantRules)
		})
	}
}

// TestDashboard_Chaos_SetEffects tests updating network effects.
func TestDashboard_Chaos_SetEffects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form          string
		name          string
		wantIndicator string
		wantLatency   int
		wantJitter    int
		wantCode      int
	}{
		{
			name:          "set latency and jitter",
			form:          "latency=200&jitter=50",
			wantCode:      http.StatusOK,
			wantLatency:   200,
			wantJitter:    50,
			wantIndicator: "200ms latency",
		},
		{
			name:          "set only latency",
			form:          "latency=500",
			wantCode:      http.StatusOK,
			wantLatency:   500,
			wantIndicator: "500ms latency",
		},
		{
			name:          "set zero latency clears effects",
			form:          "latency=0&jitter=0",
			wantCode:      http.StatusOK,
			wantLatency:   0,
			wantIndicator: "No delay active",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)

			req := httptest.NewRequest(http.MethodPost, "/dashboard/chaos/effects",
				strings.NewReader(tt.form))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			require.Equal(t, tt.wantCode, w.Code)

			effects := stack.FaultStore.GetEffects()
			assert.Equal(t, tt.wantLatency, effects.Latency)
			if tt.wantJitter > 0 {
				assert.Equal(t, tt.wantJitter, effects.Jitter)
			}

			if tt.wantIndicator != "" {
				assert.Contains(t, w.Body.String(), tt.wantIndicator)
			}
		})
	}
}

// TestDashboard_Chaos_ResetEffects tests resetting network effects to zero.
func TestDashboard_Chaos_ResetEffects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*chaos.FaultStore)
		name     string
		wantCode int
	}{
		{
			name: "reset active effects to zero",
			setup: func(fs *chaos.FaultStore) {
				fs.SetEffects(chaos.NetworkEffects{Latency: 500, Jitter: 100})
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "reset when already zero is no-op",
			setup:    nil,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stack := newStack(t)
			if tt.setup != nil {
				tt.setup(stack.FaultStore)
			}

			req := httptest.NewRequest(http.MethodPost, "/dashboard/chaos/effects/reset", nil)
			w := httptest.NewRecorder()
			serveHandler(stack.Dashboard, w, req)

			require.Equal(t, tt.wantCode, w.Code)
			assert.Equal(t, 0, stack.FaultStore.GetEffects().Latency)
			assert.Equal(t, 0, stack.FaultStore.GetEffects().Jitter)
			assert.Contains(t, w.Body.String(), "No delay active")
		})
	}
}

// TestDashboard_Chaos_NilFaultStore tests behavior when FaultStore is nil.
func TestDashboard_Chaos_NilFaultStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "GET /dashboard/chaos", method: http.MethodGet, path: "/dashboard/chaos"},
		{name: "GET /dashboard/chaos/rules", method: http.MethodGet, path: "/dashboard/chaos/rules"},
		{name: "GET /dashboard/chaos/activity", method: http.MethodGet, path: "/dashboard/chaos/activity"},
		{name: "POST /dashboard/chaos/faults", method: http.MethodPost, path: "/dashboard/chaos/faults",
			body: "service=s3&probability=100&statusCode=503&errorCode=ServiceUnavailable"},
		{name: "POST /dashboard/chaos/faults/clear", method: http.MethodPost, path: "/dashboard/chaos/faults/clear"},
		{name: "POST /dashboard/chaos/effects", method: http.MethodPost, path: "/dashboard/chaos/effects",
			body: "latency=100"},
		{name: "POST /dashboard/chaos/effects/reset", method: http.MethodPost, path: "/dashboard/chaos/effects/reset"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{})

			var body strings.Reader
			if tt.body != "" {
				body = *strings.NewReader(tt.body)
			}

			var req *http.Request
			if tt.body != "" {
				req = httptest.NewRequest(tt.method, tt.path, &body)
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			} else {
				req = httptest.NewRequest(tt.method, tt.path, nil)
			}

			w := httptest.NewRecorder()
			serveHandler(h, w, req)

			assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		})
	}
}

// TestChaos_DeleteFault_NilFaultStore tests DELETE with nil FaultStore.
func TestChaos_DeleteFault_NilFaultStore(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	req := httptest.NewRequest(http.MethodDelete, "/dashboard/chaos/faults",
		strings.NewReader(`{"service":"s3"}`))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	serveHandler(h, w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// TestChaos_HelperFunctions tests the toRuleDisplayList and toActivityDisplayList helpers.
func TestChaos_HelperFunctions(t *testing.T) {
	t.Parallel()

	t.Run("probability display", func(t *testing.T) {
		t.Parallel()

		stack := newStack(t)
		stack.FaultStore.AppendRules([]chaos.FaultRule{
			{Service: "s3", Probability: 0.5},
			{Service: "dynamodb", Probability: 1.0},
			{Service: "sqs", Probability: 0.0},
		})

		req := httptest.NewRequest(http.MethodGet, "/dashboard/chaos/rules", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		assert.Contains(t, body, "50%")
		assert.Contains(t, body, "100%")
		assert.Contains(t, body, "s3")
	})

	t.Run("activity display with timestamps", func(t *testing.T) {
		t.Parallel()

		stack := newStack(t)
		stack.FaultStore.RecordActivity(chaos.ActivityEvent{
			Service:      "kinesis",
			Operation:    "PutRecord",
			Region:       "eu-west-1",
			FaultApplied: "ServiceUnavailable",
			Probability:  1.0,
			Triggered:    true,
		})

		req := httptest.NewRequest(http.MethodGet, "/dashboard/chaos/activity", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		assert.Contains(t, body, "kinesis")
		assert.Contains(t, body, "PutRecord")
		assert.Contains(t, body, "eu-west-1")
	})
}

// TestChaos_GetActivity_APIEndpoint tests the GET /_gopherstack/chaos/activity endpoint.
func TestChaos_GetActivity_APIEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*chaos.FaultStore)
		name       string
		wantEvents int
	}{
		{
			name:       "empty activity log",
			wantEvents: 0,
		},
		{
			name: "returns recorded events",
			setup: func(fs *chaos.FaultStore) {
				fs.RecordActivity(chaos.ActivityEvent{
					Service:   "s3",
					Triggered: true,
				})
				fs.RecordActivity(chaos.ActivityEvent{
					Service:   "dynamodb",
					Triggered: false,
				})
			},
			wantEvents: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs := chaos.NewFaultStore()
			if tt.setup != nil {
				tt.setup(fs)
			}

			events := fs.GetActivity()
			require.Len(t, events, tt.wantEvents)
		})
	}
}
