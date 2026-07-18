package wafv2_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

func TestHandler_DeleteLoggingConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		body       func(arnStr string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *wafv2.Handler) string {
				w, _ := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "ALLOW", nil)
				arnStr := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
				require.NoError(t, h.Backend.PutLoggingConfiguration(
					context.Background(),
					arnStr,
					json.RawMessage(`{"ResourceArn":"`+arnStr+`"}`),
				))

				return arnStr
			},
			body: func(arnStr string) map[string]any {
				return map[string]any{"ResourceArn": arnStr}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_arn",
			setup: func(_ *wafv2.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/nonexistent/badid"
			},
			body: func(arnStr string) map[string]any {
				return map[string]any{"ResourceArn": arnStr}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arnStr := tt.setup(h)
			rec := doWafv2Request(t, h, "DeleteLoggingConfiguration", tt.body(arnStr))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestLoggingConfigurationFullRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a WebACL to get a real ARN.
	createRec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":             "log-acl",
		"Scope":            "REGIONAL",
		"DefaultAction":    map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{"MetricName": "log-acl"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	webACLARN := createResp["Summary"].(map[string]any)["ARN"].(string)

	// Put a full logging configuration.
	loggingConfig := map[string]any{
		"ResourceArn":           webACLARN,
		"LogDestinationConfigs": []string{"arn:aws:firehose:us-east-1:000000000000:deliverystream/my-stream"},
		"RedactedFields":        []any{},
	}
	putRec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
		"LoggingConfiguration": loggingConfig,
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	// Verify the full config is round-tripped in Get.
	getRec := doWafv2Request(t, h, "GetLoggingConfiguration", map[string]any{
		"ResourceArn": webACLARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	cfg, ok := getResp["LoggingConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, webACLARN, cfg["ResourceArn"])

	dests, ok := cfg["LogDestinationConfigs"].([]any)
	require.True(t, ok)
	require.Len(t, dests, 1)

	// Verify ListLoggingConfigurations returns entries.
	listRec := doWafv2Request(t, h, "ListLoggingConfigurations", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	configs, ok := listResp["LoggingConfigurations"].([]any)
	require.True(t, ok)
	assert.Len(t, configs, 1)
}

// ---- Gap 14: GetWebACLForResource returns Rules ----------------------------

func TestLoggingConfig_FirehoseDestination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, webACLARN := createWebACLHelper(t, h, "log-firehose-acl", "REGIONAL")

	rec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
		"LoggingConfiguration": map[string]any{
			"ResourceArn":           webACLARN,
			"LogDestinationConfigs": []string{"arn:aws:firehose:us-east-1:000000000000:deliverystream/waf-logs"},
			"RedactedFields":        []any{},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "Firehose destination: %s", rec.Body.String())

	getRec := doWafv2Request(t, h, "GetLoggingConfiguration", map[string]any{
		"ResourceArn": webACLARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	cfg := resp["LoggingConfiguration"].(map[string]any)
	dests := cfg["LogDestinationConfigs"].([]any)
	require.Len(t, dests, 1)
	assert.Contains(t, dests[0].(string), "arn:aws:firehose:")
}

func TestLoggingConfig_S3Destination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, webACLARN := createWebACLHelper(t, h, "log-s3-acl", "REGIONAL")

	rec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
		"LoggingConfiguration": map[string]any{
			"ResourceArn":           webACLARN,
			"LogDestinationConfigs": []string{"arn:aws:s3:::my-waf-log-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "S3 destination: %s", rec.Body.String())

	getRec := doWafv2Request(t, h, "GetLoggingConfiguration", map[string]any{
		"ResourceArn": webACLARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	cfg := resp["LoggingConfiguration"].(map[string]any)
	dests := cfg["LogDestinationConfigs"].([]any)
	require.Len(t, dests, 1)
	assert.Contains(t, dests[0].(string), "arn:aws:s3:::")
}

func TestLoggingConfig_CloudWatchDestination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, webACLARN := createWebACLHelper(t, h, "log-cw-acl", "REGIONAL")

	rec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
		"LoggingConfiguration": map[string]any{
			"ResourceArn":           webACLARN,
			"LogDestinationConfigs": []string{"arn:aws:logs:us-east-1:000000000000:log-group:aws-waf-logs-test"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "CloudWatch destination: %s", rec.Body.String())

	getRec := doWafv2Request(t, h, "GetLoggingConfiguration", map[string]any{
		"ResourceArn": webACLARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	cfg := resp["LoggingConfiguration"].(map[string]any)
	dests := cfg["LogDestinationConfigs"].([]any)
	require.Len(t, dests, 1)
	assert.Contains(t, dests[0].(string), "arn:aws:logs:")
}

func TestLoggingConfig_InvalidDestinationRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		dest string
	}{
		{name: "dynamodb", dest: "arn:aws:dynamodb:us-east-1:000000000000:table/WafLogs"},
		{name: "sns", dest: "arn:aws:sns:us-east-1:000000000000:waf-alerts"},
		{name: "sqs", dest: "arn:aws:sqs:us-east-1:000000000000:waf-queue"},
		{name: "plain_string", dest: "not-an-arn"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, webACLARN := createWebACLHelper(t, h, "log-invalid-acl-"+tt.name, "REGIONAL")

			rec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
				"LoggingConfiguration": map[string]any{
					"ResourceArn":           webACLARN,
					"LogDestinationConfigs": []string{tt.dest},
				},
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code, "destination %q should be rejected", tt.dest)
		})
	}
}

func TestLoggingConfig_DeleteAndGetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, webACLARN := createWebACLHelper(t, h, "log-delete-acl", "REGIONAL")

	// Put a config.
	rec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
		"LoggingConfiguration": map[string]any{
			"ResourceArn":           webACLARN,
			"LogDestinationConfigs": []string{"arn:aws:s3:::my-log-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete it.
	delRec := doWafv2Request(t, h, "DeleteLoggingConfiguration", map[string]any{
		"ResourceArn": webACLARN,
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Subsequent Get should return not-found.
	getRec := doWafv2Request(t, h, "GetLoggingConfiguration", map[string]any{
		"ResourceArn": webACLARN,
	})
	assert.Equal(t, http.StatusBadRequest, getRec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestLoggingConfig_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "DeleteLoggingConfiguration", map[string]any{
		"ResourceArn": "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/no-such/xyz",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

func TestLoggingConfig_FullFieldRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, webACLARN := createWebACLHelper(t, h, "log-full-acl", "REGIONAL")

	loggingConfig := map[string]any{
		"ResourceArn":           webACLARN,
		"LogDestinationConfigs": []string{"arn:aws:firehose:us-east-1:000000000000:deliverystream/waf-stream"},
		"RedactedFields": []map[string]any{
			{
				"SingleHeader": map[string]any{"Name": "authorization"},
			},
		},
		"LoggingFilter": map[string]any{
			"DefaultBehavior": "KEEP",
			"Filters": []map[string]any{
				{
					"Behavior":    "DROP",
					"Requirement": "MEETS_ANY",
					"Conditions": []map[string]any{
						{
							"ActionCondition": map[string]any{"Action": "BLOCK"},
						},
					},
				},
			},
		},
	}

	putRec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
		"LoggingConfiguration": loggingConfig,
	})
	require.Equal(t, http.StatusOK, putRec.Code, "full field put: %s", putRec.Body.String())

	getRec := doWafv2Request(t, h, "GetLoggingConfiguration", map[string]any{
		"ResourceArn": webACLARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	cfg := resp["LoggingConfiguration"].(map[string]any)

	// RedactedFields should round-trip.
	redacted, ok := cfg["RedactedFields"].([]any)
	require.True(t, ok, "RedactedFields should be present")
	assert.Len(t, redacted, 1)

	// LoggingFilter should round-trip.
	filter, ok := cfg["LoggingFilter"].(map[string]any)
	require.True(t, ok, "LoggingFilter should be present")
	assert.Equal(t, "KEEP", filter["DefaultBehavior"])
}

// ---- Permission policy lifecycle --------------------------------------------

func TestHandler_PutAndGetLoggingConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		putResourceArn string
		getResourceArn string
		wantPutStatus  int
		wantGetStatus  int
	}{
		{
			name:           "put_and_get_success",
			putResourceArn: "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/test/abc",
			getResourceArn: "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/test/abc",
			wantPutStatus:  http.StatusOK,
			wantGetStatus:  http.StatusOK,
		},
		{
			name:           "get_missing_resource_arn",
			getResourceArn: "",
			wantGetStatus:  http.StatusBadRequest,
		},
		{
			name:           "get_not_found",
			getResourceArn: "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/test/notexist",
			wantGetStatus:  http.StatusBadRequest,
		},
		{
			name:          "put_missing_resource_arn",
			wantPutStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.putResourceArn != "" || tt.wantPutStatus == http.StatusBadRequest {
				var putBody any
				if tt.putResourceArn != "" {
					putBody = map[string]any{
						"LoggingConfiguration": map[string]any{"ResourceArn": tt.putResourceArn},
					}
				} else {
					putBody = map[string]any{
						"LoggingConfiguration": map[string]any{},
					}
				}

				rec := doWafv2Request(t, h, "PutLoggingConfiguration", putBody)
				assert.Equal(t, tt.wantPutStatus, rec.Code)
			}

			if tt.getResourceArn != "" || tt.wantGetStatus == http.StatusBadRequest {
				var getBody any
				if tt.getResourceArn != "" {
					getBody = map[string]any{"ResourceArn": tt.getResourceArn}
				} else {
					getBody = map[string]any{}
				}

				rec := doWafv2Request(t, h, "GetLoggingConfiguration", getBody)
				assert.Equal(t, tt.wantGetStatus, rec.Code)
			}
		})
	}
}

func TestListLoggingConfigurations_ReturnsStoredConfigs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	arn1 := "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/acl-a/id1"
	arn2 := "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/acl-b/id2"

	for _, a := range []string{arn1, arn2} {
		rec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
			"LoggingConfiguration": map[string]any{
				"ResourceArn":           a,
				"LogDestinationConfigs": []string{"arn:aws:s3:::my-log-bucket"},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doWafv2Request(t, h, "ListLoggingConfigurations", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	configs, _ := resp["LoggingConfigurations"].([]any)
	assert.Len(t, configs, 2, "ListLoggingConfigurations should return 2 stored configs")
}

// ---- GetWebACLForResource returns not-found when no association exists -------
