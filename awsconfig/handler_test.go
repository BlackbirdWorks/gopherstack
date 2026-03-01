package awsconfig_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/awsconfig"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func newTestAWSConfigHandler(t *testing.T) *awsconfig.Handler {
	t.Helper()

	return awsconfig.NewHandler(awsconfig.NewInMemoryBackend(), slog.Default())
}

func doAWSConfigRequest(t *testing.T, h *awsconfig.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "StarlingDoveService."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestAWSConfigHandler_PutConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     any
		wantCode int
	}{
		{
			name: "success",
			body: map[string]any{
				"ConfigurationRecorder": map[string]any{
					"name":    "default",
					"roleARN": "arn:aws:iam::000000000000:role/config",
				},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			rec := doAWSConfigRequest(t, h, "PutConfigurationRecorder", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DescribeConfigurationRecorders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(t *testing.T, h *awsconfig.Handler)
		wantCode     int
		wantContains []string
	}{
		{
			name: "with_recorder",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutConfigurationRecorder", map[string]any{
					"ConfigurationRecorder": map[string]any{"name": "default", "roleARN": "arn:aws:iam::000000000000:role/config"},
				})
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"ConfigurationRecorders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DescribeConfigurationRecorders", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestAWSConfigHandler_StartConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutConfigurationRecorder", map[string]any{
					"ConfigurationRecorder": map[string]any{"name": "default", "roleARN": "arn:aws:iam::000000000000:role/config"},
				})
			},
			body:     map[string]any{"ConfigurationRecorderName": "default"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"ConfigurationRecorderName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "StartConfigurationRecorder", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_PutDeliveryChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     any
		wantCode int
	}{
		{
			name: "success",
			body: map[string]any{
				"DeliveryChannel": map[string]any{
					"name":         "default",
					"s3BucketName": "my-bucket",
					"snsTopicARN":  "arn:aws:sns:us-east-1:000000000000:my-topic",
				},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			rec := doAWSConfigRequest(t, h, "PutDeliveryChannel", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DescribeDeliveryChannels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(t *testing.T, h *awsconfig.Handler)
		wantCode     int
		wantContains []string
	}{
		{
			name: "with_channel",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutDeliveryChannel", map[string]any{
					"DeliveryChannel": map[string]any{"name": "default", "s3BucketName": "my-bucket", "snsTopicARN": ""},
				})
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DeliveryChannels"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DescribeDeliveryChannels", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestAWSConfigHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "unknown_action",
			action:   "UnknownAction",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			rec := doAWSConfigRequest(t, h, tt.action, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "match",
			target:    "StarlingDoveService.PutConfigurationRecorder",
			wantMatch: true,
		},
		{
			name:      "no_match",
			target:    "Kinesis_20131202.CreateStream",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestAWSConfigProvider_Name(t *testing.T) {
	t.Parallel()

	p := &awsconfig.Provider{}
	assert.Equal(t, "AWSConfig", p.Name())
}

func TestAWSConfigHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	assert.Equal(t, "AWSConfig", h.Name())
}

func TestAWSConfigHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "PutConfigurationRecorder")
	assert.Contains(t, ops, "DescribeConfigurationRecorders")
	assert.Contains(t, ops, "StartConfigurationRecorder")
	assert.Contains(t, ops, "PutDeliveryChannel")
	assert.Contains(t, ops, "DescribeDeliveryChannels")
}

func TestAWSConfigHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestAWSConfigHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "with_target",
			target: "StarlingDoveService.PutConfigurationRecorder",
			want:   "PutConfigurationRecorder",
		},
		{
			name:   "no_target",
			target: "",
			want:   "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestAWSConfigHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   string
		want   string
	}{
		{
			name:   "PutConfigurationRecorder",
			action: "PutConfigurationRecorder",
			body:   `{"ConfigurationRecorder":{"name":"my-recorder"}}`,
			want:   "my-recorder",
		},
		{
			name:   "StartConfigurationRecorder",
			action: "StartConfigurationRecorder",
			body:   `{"ConfigurationRecorderName":"my-recorder"}`,
			want:   "my-recorder",
		},
		{
			name:   "DescribeConfigurationRecorders_with_names",
			action: "DescribeConfigurationRecorders",
			body:   `{"ConfigurationRecorderNames":["r1"]}`,
			want:   "r1",
		},
		{
			name:   "DescribeConfigurationRecorders_without_names",
			action: "DescribeConfigurationRecorders",
			body:   `{}`,
			want:   "",
		},
		{
			name:   "PutDeliveryChannel",
			action: "PutDeliveryChannel",
			body:   `{"DeliveryChannel":{"name":"my-channel"}}`,
			want:   "my-channel",
		},
		{
			name:   "DescribeDeliveryChannels_with_names",
			action: "DescribeDeliveryChannels",
			body:   `{"DeliveryChannelNames":["ch1"]}`,
			want:   "ch1",
		},
		{
			name:   "DescribeDeliveryChannels_without_names",
			action: "DescribeDeliveryChannels",
			body:   `{}`,
			want:   "",
		},
		{
			name:   "default_fallback",
			action: "UnknownOp",
			body:   `{"ConfigurationRecorderName":"fallback"}`,
			want:   "fallback",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("X-Amz-Target", "StarlingDoveService."+tt.action)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestAWSConfigProvider_Init(t *testing.T) {
	t.Parallel()

	p := &awsconfig.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "AWSConfig", svc.Name())
}
