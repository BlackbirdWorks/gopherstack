package eventbridge_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// mockConfigProvider implements config.Provider for testing.
type mockConfigProvider struct{}

func (m *mockConfigProvider) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{AccountID: "111111111111", Region: "eu-west-1"}
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantName string
	}{
		{
			name:     "returns EventBridge",
			wantName: "EventBridge",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := &eventbridge.Provider{}
			assert.Equal(t, tt.wantName, p.Name())
		})
	}
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ctx      *service.AppContext
		wantName string
	}{
		{
			name: "with config",
			ctx: &service.AppContext{
				Logger: slog.Default(),
				Config: &mockConfigProvider{},
			},
			wantName: "EventBridge",
		},
		{
			name: "without config",
			ctx:  &service.AppContext{Logger: slog.Default()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := &eventbridge.Provider{}
			svc, err := p.Init(tt.ctx)
			require.NoError(t, err)
			require.NotNil(t, svc)
			if tt.wantName != "" {
				assert.Equal(t, tt.wantName, svc.Name())
			}
		})
	}
}

func TestHandler_Metadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want  any
		check func(h *eventbridge.Handler) any
		name  string
	}{
		{
			name:  "name is EventBridge",
			check: func(h *eventbridge.Handler) any { return h.Name() },
			want:  "EventBridge",
		},
		{
			name:  "match priority is 100",
			check: func(h *eventbridge.Handler) any { return h.MatchPriority() },
			want:  100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
			assert.Equal(t, tt.want, tt.check(h))
		})
	}
}

func TestHandler_RouteMatcherCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "matches EventBridge target",
			target:    "AmazonEventBridge.CreateEventBus",
			wantMatch: true,
		},
		{
			name:      "does not match non-EventBridge target",
			target:    "AmazonSQS.CreateQueue",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
			matcher := h.RouteMatcher()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			assert.Equal(t, tt.wantMatch, matcher(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func TestHandler_ExtractOperationCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		wantOp string
	}{
		{
			name:   "extracts PutEvents operation",
			target: "AmazonEventBridge.PutEvents",
			wantOp: "PutEvents",
		},
		{
			name:   "returns Unknown for missing target",
			target: "",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}
			assert.Equal(t, tt.wantOp, h.ExtractOperation(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		body      string
		wantRes   string
		wantEmpty bool
	}{
		{
			name:    "extracts Name field",
			body:    `{"Name":"my-bus"}`,
			wantRes: "my-bus",
		},
		{
			name:    "extracts Rule field",
			body:    `{"Rule":"my-rule"}`,
			wantRes: "my-rule",
		},
		{
			name:      "returns empty when no Name or Rule",
			body:      `{}`,
			wantEmpty: true,
		},
		{
			name:      "returns empty for invalid JSON",
			body:      `not-json`,
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			got := h.ExtractResource(e.NewContext(req, httptest.NewRecorder()))
			if tt.wantEmpty {
				assert.Empty(t, got)
			} else {
				assert.Equal(t, tt.wantRes, got)
			}
		})
	}
}

func TestHandler_InvalidTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		target   string
		body     string
		wantCode int
	}{
		{
			name:     "invalid target returns bad request",
			target:   "InvalidTarget",
			body:     "{}",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := echo.New()

			h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_NotFoundErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   string
	}{
		{name: "delete nonexistent event bus", action: "DeleteEventBus", body: `{"Name":"nonexistent"}`},
		{name: "describe nonexistent event bus", action: "DescribeEventBus", body: `{"Name":"nonexistent"}`},
		{name: "delete nonexistent rule", action: "DeleteRule", body: `{"Name":"r","EventBusName":"default"}`},
		{name: "describe nonexistent rule", action: "DescribeRule", body: `{"Name":"r","EventBusName":"default"}`},
		{name: "enable nonexistent rule", action: "EnableRule", body: `{"Name":"r","EventBusName":"default"}`},
		{name: "disable nonexistent rule", action: "DisableRule", body: `{"Name":"r","EventBusName":"default"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := makeRequest(t, tt.action, tt.body)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		body     string
		wantCode int
	}{
		{
			// JSON parse errors are mapped to 500 InternalServerError
			name:     "invalid JSON returns internal server error",
			action:   "CreateEventBus",
			body:     `not-json`,
			wantCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := makeRequest(t, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_ListWithPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupAction string
		action      string
		body        string
		setupBodies []string
		wantCode    int
	}{
		{
			name:        "list rules with name prefix",
			setupAction: "PutRule",
			setupBodies: []string{
				`{"Name":"alpha-rule","EventBusName":"default","EventPattern":"{}","State":"ENABLED"}`,
				`{"Name":"beta-rule","EventBusName":"default","EventPattern":"{}","State":"ENABLED"}`,
			},
			action:   "ListRules",
			body:     `{"EventBusName":"default","NamePrefix":"alpha"}`,
			wantCode: http.StatusOK,
		},
		{
			name:        "list event buses with name prefix",
			setupAction: "CreateEventBus",
			setupBodies: []string{
				`{"Name":"prod-bus"}`,
				`{"Name":"dev-bus"}`,
			},
			action:   "ListEventBuses",
			body:     `{"NamePrefix":"prod"}`,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := echo.New()

			bk := eventbridge.NewInMemoryBackend()
			h := eventbridge.NewHandler(bk)
			for _, setupBody := range tt.setupBodies {
				makeRequestWithHandler(t, h, e, tt.setupAction, setupBody)
			}
			rec := makeRequestWithHandler(t, h, e, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_ListTargetsByRule_RuleNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		body     string
		wantCode int
	}{
		{
			// ListTargetsByRule with a nonexistent rule returns empty list (200), not 404
			name:     "list targets for nonexistent rule returns empty list",
			action:   "ListTargetsByRule",
			body:     `{"Rule":"nonexistent","EventBusName":"default"}`,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := makeRequest(t, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_PutEvents_Empty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "empty entries returns OK",
			body:     `{"Entries":[]}`,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := makeRequest(t, "PutEvents", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_PutRule_MissingName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "missing name returns bad request",
			body:     `{"EventBusName":"default"}`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := makeRequest(t, "PutRule", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_CreateEventBus_AlreadyExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		busBody  string
		wantCode int
	}{
		{
			name:     "conflict when creating duplicate bus",
			busBody:  `{"Name":"dup-bus"}`,
			wantCode: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := echo.New()

			bk := eventbridge.NewInMemoryBackend()
			h := eventbridge.NewHandler(bk)
			makeRequestWithHandler(t, h, e, "CreateEventBus", tt.busBody)
			rec := makeRequestWithHandler(t, h, e, "CreateEventBus", tt.busBody)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DeleteDefaultBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "cannot delete default bus",
			body:     `{"Name":"default"}`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := makeRequest(t, "DeleteEventBus", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
