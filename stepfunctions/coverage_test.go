package stepfunctions_test

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
	"github.com/blackbirdworks/gopherstack/stepfunctions"
)

// mockSFNConfig implements config.Provider for testing.
type mockSFNConfig struct{}

func (m *mockSFNConfig) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{AccountID: "111111111111", Region: "eu-west-1"}
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantName string
	}{
		{
			name:     "returns_step_functions",
			wantName: "StepFunctions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &stepfunctions.Provider{}
			assert.Equal(t, tt.wantName, p.Name())
		})
	}
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   any
		wantName string
	}{
		{
			name:     "with_config",
			config:   &mockSFNConfig{},
			wantName: "StepFunctions",
		},
		{
			name:     "without_config",
			config:   nil,
			wantName: "StepFunctions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &stepfunctions.Provider{}
			ctx := &service.AppContext{
				Logger: slog.Default(),
				Config: tt.config,
			}

			svc, err := p.Init(ctx)
			require.NoError(t, err)
			require.NotNil(t, svc)
			assert.Equal(t, tt.wantName, svc.Name())
		})
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantPrio int
	}{
		{
			name:     "returns_100",
			wantPrio: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend())
			assert.Equal(t, tt.wantPrio, h.MatchPriority())
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "sfn_target_matches",
			target:    "AmazonStates.CreateStateMachine",
			wantMatch: true,
		},
		{
			name:      "sqs_target_no_match",
			target:    "AmazonSQS.CreateQueue",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend())
			matcher := h.RouteMatcher()
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			assert.Equal(t, tt.wantMatch, matcher(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		wantOp string
	}{
		{
			name:   "known_operation",
			target: "AmazonStates.ListStateMachines",
			wantOp: "ListStateMachines",
		},
		{
			name:   "no_target_header",
			target: "",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend())
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
		name         string
		body         string
		wantResource string
	}{
		{
			name:         "name_field",
			body:         `{"name":"my-sm"}`,
			wantResource: "my-sm",
		},
		{
			name:         "state_machine_arn",
			body:         `{"stateMachineArn":"arn:aws:states:us-east-1:123:stateMachine:test"}`,
			wantResource: "arn:aws:states:us-east-1:123:stateMachine:test",
		},
		{
			name:         "execution_arn",
			body:         `{"executionArn":"arn:aws:states:us-east-1:123:execution:test:exec1"}`,
			wantResource: "arn:aws:states:us-east-1:123:execution:test:exec1",
		},
		{
			name:         "empty_body",
			body:         `{}`,
			wantResource: "",
		},
		{
			name:         "bad_json",
			body:         `not-json`,
			wantResource: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend())
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", stringReader(tt.body))
			assert.Equal(t, tt.wantResource, h.ExtractResource(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func stringReader(s string) *strings.Reader {
	return strings.NewReader(s)
}
