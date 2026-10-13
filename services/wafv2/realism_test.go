package wafv2_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decodeError(t *testing.T, body []byte) (string, string) {
	t.Helper()

	var out struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(body, &out))

	return out.Type, out.Message
}

func TestListPagingValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req        map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name: "limit too high", op: "ListWebACLs",
			req: map[string]any{"Scope": "REGIONAL", "Limit": 101}, wantStatus: 400,
		},
		{
			name: "limit negative", op: "ListIPSets",
			req: map[string]any{"Scope": "REGIONAL", "Limit": -1}, wantStatus: 400,
		},
		{
			name: "bad marker", op: "ListRuleGroups",
			req: map[string]any{"Scope": "REGIONAL", "NextMarker": "!!!"}, wantStatus: 400,
		},
		{
			name: "marker too long", op: "ListRegexPatternSets",
			req:        map[string]any{"Scope": "REGIONAL", "NextMarker": strings.Repeat("QQ==", 100)},
			wantStatus: 400,
		},
		{
			name: "valid limit", op: "ListWebACLs",
			req: map[string]any{"Scope": "REGIONAL", "Limit": 100}, wantStatus: 200,
		},
		{
			name: "valid marker", op: "ListIPSets",
			req: map[string]any{"Scope": "REGIONAL", "NextMarker": "YQ=="}, wantStatus: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doWafv2Request(t, newTestHandler(t), tt.op, tt.req)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus == http.StatusBadRequest {
				code, _ := decodeError(t, rec.Body.Bytes())
				assert.Equal(t, "WAFInvalidParameterException", code)
			}
		})
	}
}

func TestLoggingDestinationNaming(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		dest string
		ok   bool
	}{
		{name: "s3 prefixed", dest: "arn:aws:s3:::aws-waf-logs-bucket", ok: true},
		{name: "s3 prefixed with key", dest: "arn:aws:s3:::aws-waf-logs-bucket/prefix", ok: true},
		{name: "s3 unprefixed", dest: "arn:aws:s3:::my-bucket"},
		{
			name: "firehose prefixed", ok: true,
			dest: "arn:aws:firehose:us-east-1:000000000000:deliverystream/aws-waf-logs-s",
		},
		{name: "firehose unprefixed", dest: "arn:aws:firehose:us-east-1:000000000000:deliverystream/stream"},
		{
			name: "log group prefixed", ok: true,
			dest: "arn:aws:logs:us-east-1:000000000000:log-group:aws-waf-logs-g:*",
		},
		{name: "log group unprefixed", dest: "arn:aws:logs:us-east-1:000000000000:log-group:group:*"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			acl := doWafv2Request(t, h, "CreateWebACL", map[string]any{
				"Name": "acl", "Scope": "REGIONAL", "DefaultAction": map[string]any{"Allow": map[string]any{}},
				"VisibilityConfig": map[string]any{
					"SampledRequestsEnabled": true, "CloudWatchMetricsEnabled": true, "MetricName": "m",
				},
			})
			require.Equal(t, http.StatusOK, acl.Code, acl.Body.String())

			var created struct {
				Summary struct {
					ARN string `json:"ARN"`
				} `json:"Summary"`
			}
			require.NoError(t, json.Unmarshal(acl.Body.Bytes(), &created))

			rec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
				"LoggingConfiguration": map[string]any{
					"ResourceArn": created.Summary.ARN, "LogDestinationConfigs": []string{tt.dest},
				},
			})

			if tt.ok {
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return
			}

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			code, _ := decodeError(t, rec.Body.Bytes())
			assert.Equal(t, "WAFInvalidParameterException", code)
		})
	}
}

func xssRule(i int) map[string]any {
	return map[string]any{
		"Name": fmt.Sprintf("r%d", i), "Priority": i,
		"Action": map[string]any{"Block": map[string]any{}},
		"Statement": map[string]any{"XssMatchStatement": map[string]any{
			"FieldToMatch":        map[string]any{"UriPath": map[string]any{}},
			"TextTransformations": []map[string]any{{"Priority": 0, "Type": "NONE"}},
		}},
		"VisibilityConfig": map[string]any{
			"SampledRequestsEnabled": true, "CloudWatchMetricsEnabled": true, "MetricName": fmt.Sprintf("r%d", i),
		},
	}
}

func TestWebACLCapacityLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rules      int
		wantStatus int
	}{
		{name: "within limit", rules: 5, wantStatus: http.StatusOK},
		{name: "over limit", rules: 38, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rules := make([]map[string]any, 0, tt.rules)
			for i := range tt.rules {
				rules = append(rules, xssRule(i))
			}

			rec := doWafv2Request(t, newTestHandler(t), "CreateWebACL", map[string]any{
				"Name": "acl", "Scope": "REGIONAL", "DefaultAction": map[string]any{"Allow": map[string]any{}},
				"Rules": rules,
				"VisibilityConfig": map[string]any{
					"SampledRequestsEnabled": true, "CloudWatchMetricsEnabled": true, "MetricName": "m",
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus != http.StatusOK {
				code, _ := decodeError(t, rec.Body.Bytes())
				assert.Equal(t, "WAFLimitsExceededException", code)
			}
		})
	}
}

func TestErrorMessageOmitsCode(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doWafv2Request(t, h, "GetWebACL", map[string]any{
		"Name": "x", "Scope": "REGIONAL", "Id": "11111111-1111-1111-1111-111111111111",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	code, msg := decodeError(t, rec.Body.Bytes())
	assert.Equal(t, "WAFNonexistentItemException", code)
	assert.NotContains(t, msg, "WAFNonexistentItemException")
}
