package waf_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoggingConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"logging config CRUD"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newWAFHandler(t)

			aclID := wafCreateWebACL(t, h, "LoggedACL")
			aclARN := "arn:aws:waf::123456789012:webacl/" + aclID

			// Put
			firehoseARN := "arn:aws:firehose:us-east-1:123456789012:deliverystream/waf-logs"
			rec := wafDo(t, h, "PutLoggingConfiguration", map[string]any{
				"LoggingConfiguration": map[string]any{
					"ResourceArn":           aclARN,
					"LogDestinationConfigs": []string{firehoseARN},
					"RedactedFields": []map[string]any{
						{"Type": "HEADER", "Data": "cookie"},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)
			var putResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
			assert.NotNil(t, putResp["LoggingConfiguration"])

			// Get
			rec = wafDo(t, h, "GetLoggingConfiguration", map[string]any{"ResourceArn": aclARN})
			require.Equal(t, http.StatusOK, rec.Code)
			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			cfgMap := getResp["LoggingConfiguration"].(map[string]any)
			assert.Equal(t, aclARN, cfgMap["ResourceArn"])

			// List
			rec = wafDo(t, h, "ListLoggingConfigurations", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			cfgs := listResp["LoggingConfigurations"].([]any)
			assert.Len(t, cfgs, 1)

			// Delete
			rec = wafDo(t, h, "DeleteLoggingConfiguration", map[string]any{"ResourceArn": aclARN})
			require.Equal(t, http.StatusOK, rec.Code)

			// Get after delete → not found
			rec = wafDo(t, h, "GetLoggingConfiguration", map[string]any{"ResourceArn": aclARN})
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			// List after delete → empty
			rec = wafDo(t, h, "ListLoggingConfigurations", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			cfgs = listResp["LoggingConfigurations"].([]any)
			assert.Empty(t, cfgs)
		})
	}
}

func TestLoggingConfigurationNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "GetLoggingConfiguration not-found",
			action: "GetLoggingConfiguration",
			body:   map[string]any{"ResourceArn": "arn:aws:waf::123:webacl/no-such"},
		},
		{
			name:   "DeleteLoggingConfiguration not-found",
			action: "DeleteLoggingConfiguration",
			body:   map[string]any{"ResourceArn": "arn:aws:waf::123:webacl/no-such"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newWAFHandler(t)
			rec := wafDo(t, h, tc.action, tc.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}
