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

func TestAWSConfigHandler_PutConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
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
		setup        func(t *testing.T, h *awsconfig.Handler)
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "with_recorder",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutConfigurationRecorder", map[string]any{
					"ConfigurationRecorder": map[string]any{
						"name":    "default",
						"roleARN": "arn:aws:iam::000000000000:role/config",
					},
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
		body     any
		setup    func(t *testing.T, h *awsconfig.Handler)
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutConfigurationRecorder", map[string]any{
					"ConfigurationRecorder": map[string]any{
						"name":    "default",
						"roleARN": "arn:aws:iam::000000000000:role/config",
					},
				})
				doAWSConfigRequest(t, h, "PutDeliveryChannel", map[string]any{
					"DeliveryChannel": map[string]any{
						"name":         "default",
						"s3BucketName": "my-bucket",
					},
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
		body     any
		name     string
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
		setup        func(t *testing.T, h *awsconfig.Handler)
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "with_channel",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutDeliveryChannel", map[string]any{
					"DeliveryChannel": map[string]any{
						"name":         "default",
						"s3BucketName": "my-bucket",
						"snsTopicARN":  "",
					},
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

func TestAWSConfigHandler_DescribeConfigRulesAndCompliance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		name      string
		action    string
		wantField string
		wantCode  int
	}{
		{
			name:      "describe_config_rules_empty",
			action:    "DescribeConfigRules",
			body:      map[string]any{},
			wantCode:  http.StatusOK,
			wantField: "ConfigRules",
		},
		{
			name:      "describe_config_rules_with_names",
			action:    "DescribeConfigRules",
			body:      map[string]any{"ConfigRuleNames": []string{"my-rule"}},
			wantCode:  http.StatusOK,
			wantField: "ConfigRules",
		},
		{
			name:      "get_compliance_details_empty",
			action:    "GetComplianceDetailsByConfigRule",
			body:      map[string]any{"ConfigRuleName": "my-rule"},
			wantCode:  http.StatusOK,
			wantField: "EvaluationResults",
		},
		{
			name:      "get_compliance_details_no_name",
			action:    "GetComplianceDetailsByConfigRule",
			body:      map[string]any{},
			wantCode:  http.StatusOK,
			wantField: "EvaluationResults",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			rec := doAWSConfigRequest(t, h, tt.action, tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotNil(t, out[tt.wantField])
		})
	}
}

func TestAWSConfigHandler_DescribeConfigurationRecorderStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(t *testing.T, h *awsconfig.Handler)
		name             string
		wantCode         int
		wantRecordingLen int
		wantRecording    bool
	}{
		{
			name:             "empty_returns_empty_list",
			wantCode:         http.StatusOK,
			wantRecordingLen: 0,
		},
		{
			name: "pending_recorder_is_not_recording",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutConfigurationRecorder", map[string]any{
					"ConfigurationRecorder": map[string]any{"name": "default", "roleARN": "arn:aws:iam::123:role/r"},
				})
			},
			wantCode:         http.StatusOK,
			wantRecordingLen: 1,
			wantRecording:    false,
		},
		{
			name: "active_recorder_is_recording",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				doAWSConfigRequest(t, h, "PutConfigurationRecorder", map[string]any{
					"ConfigurationRecorder": map[string]any{"name": "default", "roleARN": "arn:aws:iam::123:role/r"},
				})
				doAWSConfigRequest(t, h, "PutDeliveryChannel", map[string]any{
					"DeliveryChannel": map[string]any{"name": "default", "s3BucketName": "my-bucket"},
				})
				doAWSConfigRequest(t, h, "StartConfigurationRecorder", map[string]any{
					"ConfigurationRecorderName": "default",
				})
			},
			wantCode:         http.StatusOK,
			wantRecordingLen: 1,
			wantRecording:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DescribeConfigurationRecorderStatus", map[string]any{})

			assert.Equal(t, tt.wantCode, rec.Code)

			var out struct {
				ConfigurationRecordersStatus []struct {
					Name      string `json:"name"`
					Recording bool   `json:"recording"`
				} `json:"ConfigurationRecordersStatus"`
			}

			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Len(t, out.ConfigurationRecordersStatus, tt.wantRecordingLen)

			if tt.wantRecordingLen > 0 {
				assert.Equal(t, tt.wantRecording, out.ConfigurationRecordersStatus[0].Recording)
			}
		})
	}
}

func TestAWSConfigHandler_AssociateResourceTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success_with_arn",
			body: map[string]any{
				"ConfigurationRecorderArn": "arn:aws:config:us-east-1:000000000000:config-recorder/default",
				"ResourceTypes":            []string{"AWS::EC2::Instance"},
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"ConfigurationRecorder", "AWS::EC2::Instance"},
		},
		{
			name: "empty_resource_types",
			body: map[string]any{
				"ConfigurationRecorderArn": "arn:aws:config:us-east-1:000000000000:config-recorder/default",
				"ResourceTypes":            []string{},
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"ConfigurationRecorder"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			require.NoError(t, h.Backend.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/r", nil))

			rec := doAWSConfigRequest(t, h, "AssociateResourceTypes", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestAWSConfigHandler_AssociateResourceTypes_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	rec := doAWSConfigRequest(t, h, "AssociateResourceTypes", map[string]any{
		"ConfigurationRecorderArn": "arn:aws:config:us-east-1:000000000000:config-recorder/unknown",
		"ResourceTypes":            []string{"AWS::EC2::Instance"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchConfigurationRecorderException")
}

func TestAWSConfigHandler_BatchGetAggregateResourceConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "returns_unprocessed_identifiers",
			body: map[string]any{
				"ConfigurationAggregatorName": "my-aggregator",
				"ResourceIdentifiers": []map[string]any{
					{
						"SourceAccountId": "000000000000",
						"SourceRegion":    "us-east-1",
						"ResourceId":      "i-1234567890abcdef0",
						"ResourceType":    "AWS::EC2::Instance",
					},
				},
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"BaseConfigurationItems", "UnprocessedResourceIdentifiers"},
		},
		{
			name: "empty_identifiers",
			body: map[string]any{
				"ConfigurationAggregatorName": "my-aggregator",
				"ResourceIdentifiers":         []any{},
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"BaseConfigurationItems"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			rec := doAWSConfigRequest(t, h, "BatchGetAggregateResourceConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestAWSConfigHandler_BatchGetResourceConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "returns_unprocessed_keys",
			body: map[string]any{
				"ResourceKeys": []map[string]any{
					{
						"resourceType": "AWS::EC2::Instance",
						"resourceId":   "i-1234567890abcdef0",
					},
				},
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"BaseConfigurationItems", "UnprocessedResourceKeys"},
		},
		{
			name: "empty_resource_keys",
			body: map[string]any{
				"ResourceKeys": []any{},
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"BaseConfigurationItems"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			rec := doAWSConfigRequest(t, h, "BatchGetResourceConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestAWSConfigHandler_DeleteAggregationAuthorization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutAggregationAuthorization("123456789012", "us-east-1"))
			},
			body: map[string]any{
				"AuthorizedAccountId": "123456789012",
				"AuthorizedAwsRegion": "us-east-1",
			},
			wantCode: http.StatusOK,
		},
		{
			// Real AWS Config's DeleteAggregationAuthorization is idempotent, so
			// deleting a nonexistent authorization also returns 200.
			name: "not_found_is_idempotent",
			body: map[string]any{
				"AuthorizedAccountId": "999999999999",
				"AuthorizedAwsRegion": "us-west-2",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DeleteAggregationAuthorization", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DeleteConfigRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "my-rule"}))
			},
			body:     map[string]any{"ConfigRuleName": "my-rule"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"ConfigRuleName": "nonexistent-rule"},
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

			rec := doAWSConfigRequest(t, h, "DeleteConfigRule", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DeleteConfigurationAggregator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConfigurationAggregator("my-aggregator", nil, nil))
			},
			body:     map[string]any{"ConfigurationAggregatorName": "my-aggregator"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"ConfigurationAggregatorName": "nonexistent"},
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

			rec := doAWSConfigRequest(t, h, "DeleteConfigurationAggregator", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DeleteConformancePack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConformancePack("my-pack", "", ""))
			},
			body:     map[string]any{"ConformancePackName": "my-pack"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"ConformancePackName": "nonexistent"},
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

			rec := doAWSConfigRequest(t, h, "DeleteConformancePack", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DeleteEvaluationResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success_for_existing_rule",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "my-rule"}))
			},
			body:     map[string]any{"ConfigRuleName": "my-rule"},
			wantCode: http.StatusOK,
		},
		{
			name:     "nonexistent_rule_not_found",
			body:     map[string]any{"ConfigRuleName": "my-rule"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "empty_rule_name",
			body:     map[string]any{"ConfigRuleName": ""},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DeleteEvaluationResults", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DeleteOrganizationConfigRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutOrganizationConfigRule("org-rule"))
			},
			body:     map[string]any{"OrganizationConfigRuleName": "org-rule"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"OrganizationConfigRuleName": "nonexistent"},
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

			rec := doAWSConfigRequest(t, h, "DeleteOrganizationConfigRule", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_DeleteOrganizationConformancePack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutOrganizationConformancePack("org-pack"))
			},
			body:     map[string]any{"OrganizationConformancePackName": "org-pack"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"OrganizationConformancePackName": "nonexistent"},
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

			rec := doAWSConfigRequest(t, h, "DeleteOrganizationConformancePack", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_StopConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConfigurationRecorder("default", "arn:aws:iam::000:role/r", nil))
				require.NoError(t, h.Backend.PutDeliveryChannel("default", "my-bucket", "", "", nil))
				require.NoError(t, h.Backend.StartConfigurationRecorder("default"))
			},
			body:     map[string]any{"ConfigurationRecorderName": "default"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"ConfigurationRecorderName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "empty_name_returns_400",
			body:     map[string]any{"ConfigurationRecorderName": ""},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "StopConfigurationRecorder", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestAWSConfigHandler_PutConfigurationRecorder_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name: "empty_name_returns_400",
			body: map[string]any{
				"ConfigurationRecorder": map[string]any{"name": "", "roleARN": "arn:aws:iam::000:role/r"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty_role_arn_returns_400",
			body: map[string]any{
				"ConfigurationRecorder": map[string]any{"name": "default", "roleARN": ""},
			},
			wantCode: http.StatusBadRequest,
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

func TestAWSConfigHandler_PutDeliveryChannel_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name: "empty_name_returns_400",
			body: map[string]any{
				"DeliveryChannel": map[string]any{"name": "", "s3BucketName": "my-bucket"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty_bucket_returns_400",
			body: map[string]any{
				"DeliveryChannel": map[string]any{"name": "default", "s3BucketName": ""},
			},
			wantCode: http.StatusBadRequest,
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

func TestAWSConfigHandler_DescribeConfigurationRecorders_NameFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		name      string
		wantCode  int
		wantCount int
	}{
		{
			name:      "no_filter_returns_all",
			body:      map[string]any{},
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
		{
			name:      "filter_one_recorder",
			body:      map[string]any{"ConfigurationRecorderNames": []string{"rec-a"}},
			wantCode:  http.StatusOK,
			wantCount: 1,
		},
		{
			name:      "filter_nonexistent",
			body:      map[string]any{"ConfigurationRecorderNames": []string{"no-such"}},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			require.NoError(t, h.Backend.PutConfigurationRecorder("rec-a", "arn:aws:iam::123:role/r", nil))
			require.NoError(t, h.Backend.PutConfigurationRecorder("rec-b", "arn:aws:iam::123:role/r", nil))

			rec := doAWSConfigRequest(t, h, "DescribeConfigurationRecorders", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var out map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			var recorders []any
			require.NoError(t, json.Unmarshal(out["ConfigurationRecorders"], &recorders))
			assert.Len(t, recorders, tt.wantCount)
		})
	}
}

func TestAWSConfigHandler_DescribeDeliveryChannels_NameFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		name      string
		wantCode  int
		wantCount int
	}{
		{
			name:      "no_filter_returns_all",
			body:      map[string]any{},
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
		{
			name:      "filter_one_channel",
			body:      map[string]any{"DeliveryChannelNames": []string{"ch-a"}},
			wantCode:  http.StatusOK,
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			require.NoError(t, h.Backend.PutDeliveryChannel("ch-a", "bucket-a", "", "", nil))
			require.NoError(t, h.Backend.PutDeliveryChannel("ch-b", "bucket-b", "", "", nil))

			rec := doAWSConfigRequest(t, h, "DescribeDeliveryChannels", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var out map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			var channels []any
			require.NoError(t, json.Unmarshal(out["DeliveryChannels"], &channels))
			assert.Len(t, channels, tt.wantCount)
		})
	}
}

func TestAWSConfigHandler_DescribeConfigRules_BackedByStorage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, h *awsconfig.Handler)
		body      any
		name      string
		wantCode  int
		wantCount int
	}{
		{
			name:      "empty_returns_no_rules",
			body:      map[string]any{},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
		{
			name: "returns_stored_rules",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-x"}))
				require.NoError(t, h.Backend.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-y"}))
			},
			body:      map[string]any{},
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
		{
			name: "filter_by_name",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-1"}))
				require.NoError(t, h.Backend.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-2"}))
			},
			body:      map[string]any{"ConfigRuleNames": []string{"rule-1"}},
			wantCode:  http.StatusOK,
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DescribeConfigRules", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var out map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			var rules []any
			require.NoError(t, json.Unmarshal(out["ConfigRules"], &rules))
			assert.Len(t, rules, tt.wantCount)
		})
	}
}

func TestAWSConfigHandler_ErrorTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		name         string
		operation    string
		wantContains string
		wantCode     int
	}{
		{
			name:         "delete_configuration_recorder_not_found_type",
			operation:    "DeleteConfigurationRecorder",
			body:         map[string]any{"ConfigurationRecorderName": "nonexistent"},
			wantCode:     http.StatusNotFound,
			wantContains: "NoSuchConfigurationRecorderException",
		},
		{
			name:         "delete_delivery_channel_not_found_type",
			operation:    "DeleteDeliveryChannel",
			body:         map[string]any{"DeliveryChannelName": "nonexistent"},
			wantCode:     http.StatusNotFound,
			wantContains: "NoSuchDeliveryChannelException",
		},
		{
			name:         "delete_config_rule_not_found_type",
			operation:    "DeleteConfigRule",
			body:         map[string]any{"ConfigRuleName": "nonexistent"},
			wantCode:     http.StatusNotFound,
			wantContains: "NoSuchConfigRuleException",
		},
		{
			name:         "delete_aggregator_not_found_type",
			operation:    "DeleteConfigurationAggregator",
			body:         map[string]any{"ConfigurationAggregatorName": "nonexistent"},
			wantCode:     http.StatusNotFound,
			wantContains: "NoSuchConfigurationAggregatorException",
		},
		{
			name:         "delete_conformance_pack_not_found_type",
			operation:    "DeleteConformancePack",
			body:         map[string]any{"ConformancePackName": "nonexistent"},
			wantCode:     http.StatusNotFound,
			wantContains: "NoSuchConformancePackException",
		},
		{
			name:         "delete_org_config_rule_not_found_type",
			operation:    "DeleteOrganizationConfigRule",
			body:         map[string]any{"OrganizationConfigRuleName": "nonexistent"},
			wantCode:     http.StatusNotFound,
			wantContains: "NoSuchOrganizationConfigRuleException",
		},
		{
			name:         "delete_org_conformance_pack_not_found_type",
			operation:    "DeleteOrganizationConformancePack",
			body:         map[string]any{"OrganizationConformancePackName": "nonexistent"},
			wantCode:     http.StatusNotFound,
			wantContains: "NoSuchOrganizationConformancePackException",
		},
		{
			name:         "start_recorder_no_delivery_channel_400",
			operation:    "StartConfigurationRecorder",
			body:         map[string]any{"ConfigurationRecorderName": "default"},
			wantCode:     http.StatusBadRequest,
			wantContains: "NoAvailableDeliveryChannelException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.operation == "StartConfigurationRecorder" {
				require.NoError(t, h.Backend.PutConfigurationRecorder("default", "arn:aws:iam::000:role/r", nil))
			}

			rec := doAWSConfigRequest(t, h, tt.operation, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
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

func TestAWSConfigHandler_AssociateResourceTypes_EmptyARN(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	rec := doAWSConfigRequest(t, h, "AssociateResourceTypes", map[string]any{
		"ConfigurationRecorderArn": "",
		"ResourceTypes":            []string{"AWS::EC2::Instance"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

func TestAWSConfigHandler_PutConfigRule_ValidationAndDescribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name:     "empty_name_returns_400",
			body:     map[string]any{"ConfigRuleName": ""},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			// Call PutConfigRule directly via backend (handler not wired for PutConfigRule in HTTP yet)
			ruleName := tt.body.(map[string]any)["ConfigRuleName"].(string)
			err := h.Backend.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: ruleName})
			require.Error(t, err)
			assert.ErrorIs(t, err, awsconfig.ErrValidation)
		})
	}
}
