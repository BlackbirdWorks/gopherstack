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

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func newTestAWSConfigHandler(t *testing.T) *awsconfig.Handler {
	t.Helper()

	return awsconfig.NewHandler(awsconfig.NewInMemoryBackend())
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

func TestAWSConfigHandler_ExtractResource_NewOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         string
		operation    string
		name         string
		wantResource string
	}{
		{
			name:         "DeleteConfigRule",
			operation:    "DeleteConfigRule",
			body:         `{"ConfigRuleName":"my-rule"}`,
			wantResource: "my-rule",
		},
		{
			name:         "DeleteConfigurationAggregator",
			operation:    "DeleteConfigurationAggregator",
			body:         `{"ConfigurationAggregatorName":"my-agg"}`,
			wantResource: "my-agg",
		},
		{
			name:         "DeleteConformancePack",
			operation:    "DeleteConformancePack",
			body:         `{"ConformancePackName":"my-pack"}`,
			wantResource: "my-pack",
		},
		{
			name:         "DeleteOrganizationConfigRule",
			operation:    "DeleteOrganizationConfigRule",
			body:         `{"OrganizationConfigRuleName":"org-rule"}`,
			wantResource: "org-rule",
		},
		{
			name:         "DeleteOrganizationConformancePack",
			operation:    "DeleteOrganizationConformancePack",
			body:         `{"OrganizationConformancePackName":"org-pack"}`,
			wantResource: "org-pack",
		},
		{
			name:         "AssociateResourceTypes",
			operation:    "AssociateResourceTypes",
			body:         `{"ConfigurationRecorderArn":"arn:aws:config:us-east-1:000:config-recorder/default"}`,
			wantResource: "arn:aws:config:us-east-1:000:config-recorder/default",
		},
		{
			name:         "StopConfigurationRecorder",
			operation:    "StopConfigurationRecorder",
			body:         `{"ConfigurationRecorderName":"my-recorder"}`,
			wantResource: "my-recorder",
		},
		{
			name:         "GetConnector",
			operation:    "GetConnector",
			body:         `{"Arn":"arn:aws:config:us-east-1:000:connector/my-connector"}`,
			wantResource: "arn:aws:config:us-east-1:000:connector/my-connector",
		},
		{
			name:         "DeleteConnector",
			operation:    "DeleteConnector",
			body:         `{"Arn":"arn:aws:config:us-east-1:000:connector/my-connector"}`,
			wantResource: "arn:aws:config:us-east-1:000:connector/my-connector",
		},
		{
			name:         "PutThirdPartyServiceLinkedConfigurationRecorder",
			operation:    "PutThirdPartyServiceLinkedConfigurationRecorder",
			body:         `{"ServicePrincipal":"thirdparty.amazonaws.com"}`,
			wantResource: "thirdparty.amazonaws.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("X-Amz-Target", "StarlingDoveService."+tt.operation)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantResource, h.ExtractResource(c))
		})
	}
}

func TestAWSConfigHandler_ExtractResource_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   string
		want   string
	}{
		{
			name:   "PutConfigurationRecorder_invalid_json",
			action: "PutConfigurationRecorder",
			body:   `not-valid-json`,
			want:   "",
		},
		{
			name:   "StartConfigurationRecorder_invalid_json",
			action: "StartConfigurationRecorder",
			body:   `not-valid-json`,
			want:   "",
		},
		{
			name:   "PutDeliveryChannel_invalid_json",
			action: "PutDeliveryChannel",
			body:   `not-valid-json`,
			want:   "",
		},
		{
			name:   "DeleteConfigurationRecorder_fallback",
			action: "DeleteConfigurationRecorder",
			body:   `{"ConfigurationRecorderName":"rec1"}`,
			want:   "rec1",
		},
		{
			name:   "DeleteConfigurationRecorder_invalid_json",
			action: "DeleteConfigurationRecorder",
			body:   `not-valid-json`,
			want:   "",
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
