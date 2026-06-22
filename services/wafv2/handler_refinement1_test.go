package wafv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

// createRegexPatternSetHelper creates a regex pattern set with REGIONAL scope and returns its ID.
func createRegexPatternSetHelper(t *testing.T, h *wafv2.Handler, name string) string {
	t.Helper()

	rec := doWafv2Request(t, h, "CreateRegexPatternSet", map[string]any{
		"Name":  name,
		"Scope": "REGIONAL",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summary, ok := resp["Summary"].(map[string]any)
	require.True(t, ok)

	id, ok := summary["Id"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}

// createRuleGroupHelper creates a rule group with REGIONAL scope and returns its ID and ARN.
func createRuleGroupHelper(t *testing.T, h *wafv2.Handler, name string) (string, string) {
	t.Helper()

	rec := doWafv2Request(t, h, "CreateRuleGroup", map[string]any{
		"Name":     name,
		"Scope":    "REGIONAL",
		"Capacity": 10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summary, ok := resp["Summary"].(map[string]any)
	require.True(t, ok)

	id, ok := summary["Id"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	arn, ok := summary["ARN"].(string)
	require.True(t, ok)

	return id, arn
}

// createWebACLHelper creates a web ACL and returns its ID and ARN.
func createWebACLHelper(t *testing.T, h *wafv2.Handler, name, scope string) (string, string) {
	t.Helper()

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":  name,
		"Scope": scope,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summary, ok := resp["Summary"].(map[string]any)
	require.True(t, ok)

	id, ok := summary["Id"].(string)
	require.True(t, ok)

	arn, ok := summary["ARN"].(string)
	require.True(t, ok)

	return id, arn
}

func TestHandler_GetRegexPatternSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupName  string
		requestID  string
		wantField  string
		wantStatus int
	}{
		{
			name:       "found",
			setupName:  "my-regex",
			wantStatus: http.StatusOK,
			wantField:  "RegexPatternSet",
		},
		{
			name:       "missing_id",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			requestID:  "nonexistent-id",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := tt.requestID
			if tt.setupName != "" {
				id = createRegexPatternSetHelper(t, h, tt.setupName)
			}

			var body any
			if id != "" {
				body = map[string]any{"Id": id, "Scope": "REGIONAL"}
			} else {
				body = map[string]any{}
			}

			rec := doWafv2Request(t, h, "GetRegexPatternSet", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, tt.wantField)
			}
		})
	}
}

func TestHandler_ListRegexPatternSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scope      string
		setup      []string
		wantCount  int
		wantStatus int
	}{
		{
			name:       "empty",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "list_all",
			setup:      []string{"rps-a", "rps-b"},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "filter_by_scope",
			setup:      []string{"rps-a", "rps-b"},
			scope:      "REGIONAL",
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "filter_no_match",
			setup:      []string{"rps-a"},
			scope:      "CLOUDFRONT",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range tt.setup {
				createRegexPatternSetHelper(t, h, name)
			}

			rec := doWafv2Request(t, h, "ListRegexPatternSets", map[string]any{"Scope": tt.scope})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			items, ok := resp["RegexPatternSets"].([]any)
			require.True(t, ok)
			assert.Len(t, items, tt.wantCount)
		})
	}
}

func TestHandler_UpdateRegexPatternSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupName   string
		requestID   string
		description string
		wantStatus  int
	}{
		{
			name:        "update_description",
			setupName:   "my-rps",
			description: "updated description",
			wantStatus:  http.StatusOK,
		},
		{
			name:       "missing_id",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			requestID:  "nonexistent",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := tt.requestID
			if tt.setupName != "" {
				id = createRegexPatternSetHelper(t, h, tt.setupName)
			}

			var body any
			if id != "" {
				body = map[string]any{"Id": id, "Description": tt.description}
			} else {
				body = map[string]any{}
			}

			rec := doWafv2Request(t, h, "UpdateRegexPatternSet", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "NextLockToken")
			}
		})
	}
}

func TestHandler_GetRuleGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupName  string
		requestID  string
		wantField  string
		wantStatus int
	}{
		{
			name:       "found",
			setupName:  "my-rg",
			wantStatus: http.StatusOK,
			wantField:  "RuleGroup",
		},
		{
			name:       "missing_id",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			requestID:  "nonexistent-id",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := tt.requestID
			if tt.setupName != "" {
				id, _ = createRuleGroupHelper(t, h, tt.setupName)
			}

			var body any
			if id != "" {
				body = map[string]any{"Id": id}
			} else {
				body = map[string]any{}
			}

			rec := doWafv2Request(t, h, "GetRuleGroup", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, tt.wantField)
			}
		})
	}
}

func TestHandler_ListRuleGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scope      string
		setup      []string
		wantCount  int
		wantStatus int
	}{
		{
			name:       "empty",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "two_groups",
			setup:      []string{"rg-a", "rg-b"},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "filter_scope_match",
			setup:      []string{"rg-a"},
			scope:      "REGIONAL",
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "filter_scope_no_match",
			setup:      []string{"rg-a"},
			scope:      "CLOUDFRONT",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range tt.setup {
				createRuleGroupHelper(t, h, name)
			}

			rec := doWafv2Request(t, h, "ListRuleGroups", map[string]any{"Scope": tt.scope})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			items, ok := resp["RuleGroups"].([]any)
			require.True(t, ok)
			assert.Len(t, items, tt.wantCount)
		})
	}
}

func TestHandler_UpdateRuleGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupName   string
		requestID   string
		description string
		wantStatus  int
	}{
		{
			name:        "update_description",
			setupName:   "my-rg",
			description: "updated",
			wantStatus:  http.StatusOK,
		},
		{
			name:       "missing_id",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			requestID:  "nonexistent",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := tt.requestID
			if tt.setupName != "" {
				id, _ = createRuleGroupHelper(t, h, tt.setupName)
			}

			var body any
			if id != "" {
				body = map[string]any{"Id": id, "Description": tt.description}
			} else {
				body = map[string]any{}
			}

			rec := doWafv2Request(t, h, "UpdateRuleGroup", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "NextLockToken")
			}
		})
	}
}

func TestHandler_ListAPIKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scope      string
		setupScope string
		setupCount int
		wantCount  int
		wantStatus int
	}{
		{
			name:       "empty",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "list_all",
			setupScope: "REGIONAL",
			setupCount: 2,
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "filter_scope_match",
			setupScope: "REGIONAL",
			setupCount: 1,
			scope:      "REGIONAL",
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "filter_scope_no_match",
			setupScope: "REGIONAL",
			setupCount: 1,
			scope:      "CLOUDFRONT",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for range tt.setupCount {
				rec := doWafv2Request(t, h, "CreateAPIKey", map[string]any{
					"Scope":        tt.setupScope,
					"TokenDomains": []string{"example.com"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doWafv2Request(t, h, "ListAPIKeys", map[string]any{"Scope": tt.scope})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			items, ok := resp["APIKeys"].([]any)
			require.True(t, ok)
			assert.Len(t, items, tt.wantCount)
		})
	}
}

func TestHandler_GetDecryptedAPIKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scope      string
		apiKey     string
		createKey  bool
		wantStatus int
	}{
		{
			name:       "found",
			scope:      "REGIONAL",
			createKey:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_scope",
			apiKey:     "somekey",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_apikey",
			scope:      "REGIONAL",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			scope:      "REGIONAL",
			apiKey:     "nonexistent-key",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			apiKey := tt.apiKey
			if tt.createKey {
				rec := doWafv2Request(t, h, "CreateAPIKey", map[string]any{
					"Scope":        "REGIONAL",
					"TokenDomains": []string{"example.com"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				apiKey, _ = resp["APIKey"].(string)
				require.NotEmpty(t, apiKey)
			}

			body := map[string]any{}
			if tt.scope != "" {
				body["Scope"] = tt.scope
			}
			if apiKey != "" {
				body["APIKey"] = apiKey
			}

			rec := doWafv2Request(t, h, "GetDecryptedAPIKey", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "Scope")
				assert.Contains(t, resp, "TokenDomains")
			}
		})
	}
}

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

func TestHandler_PutAndGetPermissionPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		putResourceArn string
		putPolicy      string
		getResourceArn string
		wantPutStatus  int
		wantGetStatus  int
	}{
		{
			name:           "put_and_get_success",
			putResourceArn: "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/test/abc",
			putPolicy:      `{"Version":"2012-10-17"}`,
			getResourceArn: "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/test/abc",
			wantPutStatus:  http.StatusOK,
			wantGetStatus:  http.StatusOK,
		},
		{
			name:          "put_missing_resource_arn",
			wantPutStatus: http.StatusBadRequest,
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.putResourceArn != "" || tt.wantPutStatus == http.StatusBadRequest {
				var putBody any
				if tt.putResourceArn != "" {
					putBody = map[string]any{
						"ResourceArn": tt.putResourceArn,
						"Policy":      tt.putPolicy,
					}
				} else {
					putBody = map[string]any{}
				}

				rec := doWafv2Request(t, h, "PutPermissionPolicy", putBody)
				assert.Equal(t, tt.wantPutStatus, rec.Code)
			}

			if tt.getResourceArn != "" || tt.wantGetStatus == http.StatusBadRequest {
				var getBody any
				if tt.getResourceArn != "" {
					getBody = map[string]any{"ResourceArn": tt.getResourceArn}
				} else {
					getBody = map[string]any{}
				}

				rec := doWafv2Request(t, h, "GetPermissionPolicy", getBody)
				assert.Equal(t, tt.wantGetStatus, rec.Code)

				if tt.wantGetStatus == http.StatusOK {
					var resp map[string]any
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
					assert.Contains(t, resp, "Policy")
				}
			}
		})
	}
}

func TestHandler_ListResourcesForWebACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		webACLArn     string
		associations  []string
		wantStatus    int
		wantResources int
	}{
		{
			name:       "missing_webacl_arn",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "webacl_not_found",
			webACLArn:  "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/notexist/xxx",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:          "no_associations",
			wantStatus:    http.StatusOK,
			wantResources: 0,
		},
		{
			name:          "with_association",
			associations:  []string{"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/test/1234"},
			wantStatus:    http.StatusOK,
			wantResources: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			webACLArn := tt.webACLArn
			if tt.wantStatus == http.StatusOK || len(tt.associations) > 0 {
				_, webACLArn = createWebACLHelper(t, h, "test-acl", "REGIONAL")

				for _, resourceArn := range tt.associations {
					rec := doWafv2Request(t, h, "AssociateWebACL", map[string]any{
						"WebACLArn":   webACLArn,
						"ResourceArn": resourceArn,
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}
			}

			var body any
			if webACLArn != "" {
				body = map[string]any{"WebACLArn": webACLArn}
			} else {
				body = map[string]any{}
			}

			rec := doWafv2Request(t, h, "ListResourcesForWebACL", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				resources, ok := resp["ResourceArns"].([]any)
				require.True(t, ok)
				assert.Len(t, resources, tt.wantResources)
			}
		})
	}
}

func TestHandler_ScopeValidation_CreateWebACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scope      string
		wantStatus int
	}{
		{name: "regional_valid", scope: "REGIONAL", wantStatus: http.StatusOK},
		{name: "cloudfront_valid", scope: "CLOUDFRONT", wantStatus: http.StatusOK},
		{name: "invalid_scope", scope: "INVALID", wantStatus: http.StatusBadRequest},
		{name: "empty_scope", scope: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
				"Name":  "test-acl",
				"Scope": tt.scope,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ScopeValidation_CreateIPSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		scope            string
		ipAddressVersion string
		wantStatus       int
	}{
		{name: "regional_ipv4", scope: "REGIONAL", ipAddressVersion: "IPV4", wantStatus: http.StatusOK},
		{name: "cloudfront_ipv6", scope: "CLOUDFRONT", ipAddressVersion: "IPV6", wantStatus: http.StatusOK},
		{name: "invalid_scope", scope: "BAD", ipAddressVersion: "IPV4", wantStatus: http.StatusBadRequest},
		{
			name:             "invalid_ip_version",
			scope:            "REGIONAL",
			ipAddressVersion: "BADVERSION",
			wantStatus:       http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CreateIPSet", map[string]any{
				"Name":             "test-ipset",
				"Scope":            tt.scope,
				"IPAddressVersion": tt.ipAddressVersion,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ScopeValidation_CreateRegexPatternSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scope      string
		wantStatus int
	}{
		{name: "regional_valid", scope: "REGIONAL", wantStatus: http.StatusOK},
		{name: "cloudfront_valid", scope: "CLOUDFRONT", wantStatus: http.StatusOK},
		{name: "invalid_scope", scope: "BAD", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CreateRegexPatternSet", map[string]any{
				"Name":  "test-rps",
				"Scope": tt.scope,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ScopeValidation_CreateRuleGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scope      string
		wantStatus int
	}{
		{name: "regional_valid", scope: "REGIONAL", wantStatus: http.StatusOK},
		{name: "cloudfront_valid", scope: "CLOUDFRONT", wantStatus: http.StatusOK},
		{name: "invalid_scope", scope: "BAD", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CreateRuleGroup", map[string]any{
				"Name":     "test-rg",
				"Scope":    tt.scope,
				"Capacity": 10,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestProvider_Init_NilCtx(t *testing.T) {
	t.Parallel()

	p := &wafv2.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, wafv2.ErrNilAppContext)
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a WebACL.
	doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":  "test-acl",
		"Scope": "REGIONAL",
	})

	// Verify it exists.
	rec := doWafv2Request(t, h, "ListWebACLs", map[string]any{"Scope": "REGIONAL"})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ := resp["WebACLs"].([]any)
	assert.Len(t, items, 1)

	// Reset.
	h.Reset()

	// Verify it's gone.
	rec = doWafv2Request(t, h, "ListWebACLs", map[string]any{"Scope": "REGIONAL"})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ = resp["WebACLs"].([]any)
	assert.Empty(t, items)
}

func TestBackend_AccountID(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("123456789012", "us-west-2")
	assert.Equal(t, "123456789012", b.AccountID())
}

func TestHandler_TagResource_RegexPatternSet(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("000000000000", "us-east-1")

	rps := &wafv2.RegexPatternSet{
		ID:    "rps-id-1",
		Name:  "test-rps",
		Scope: "REGIONAL",
	}
	wafv2.AddRegexPatternSetInternal(b, rps)

	hb := wafv2.NewHandler(b)
	arnStr := b.RegexPatternSetARN(rps.Name, rps.ID, rps.Scope)

	rec := doWafv2Request(t, hb, "TagResource", map[string]any{
		"ResourceARN": arnStr,
		"Tags":        []map[string]string{{"Key": "env", "Value": "test"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doWafv2Request(t, hb, "ListTagsForResource", map[string]any{
		"ResourceARN": arnStr,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	tagInfo, ok := resp["TagInfoForResource"].(map[string]any)
	require.True(t, ok)
	tagList, ok := tagInfo["TagList"].([]any)
	require.True(t, ok)
	assert.Len(t, tagList, 1)
}

func TestHandler_TagResource_RuleGroup(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("000000000000", "us-east-1")
	rg := &wafv2.RuleGroup{
		ID:    "rg-id-1",
		Name:  "test-rg",
		Scope: "REGIONAL",
	}
	wafv2.AddRuleGroupInternal(b, rg)

	h := wafv2.NewHandler(b)
	arnStr := b.RuleGroupARN(rg.Name, rg.ID, rg.Scope)

	rec := doWafv2Request(t, h, "TagResource", map[string]any{
		"ResourceARN": arnStr,
		"Tags":        []map[string]string{{"Key": "team", "Value": "security"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doWafv2Request(t, h, "UntagResource", map[string]any{
		"ResourceARN": arnStr,
		"TagKeys":     []string{"team"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteWebACL_CascadeLogging(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a WebACL.
	_, webACLArn := createWebACLHelper(t, h, "test-acl", "REGIONAL")

	// Put logging config for it.
	rec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
		"LoggingConfiguration": map[string]any{"ResourceArn": webACLArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete the WebACL.
	var createResp map[string]any
	rec2 := doWafv2Request(t, h, "ListWebACLs", map[string]any{"Scope": "REGIONAL"})
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &createResp))
	webACLs, _ := createResp["WebACLs"].([]any)
	require.Len(t, webACLs, 1)
	webACL := webACLs[0].(map[string]any)
	webACLID := webACL["Id"].(string)

	rec = doWafv2Request(t, h, "DeleteWebACL", map[string]any{"Id": webACLID})
	require.Equal(t, http.StatusOK, rec.Code)

	// The logging config should be gone.
	rec = doWafv2Request(t, h, "GetLoggingConfiguration", map[string]any{"ResourceArn": webACLArn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Reset_EdgesAndCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(h *wafv2.Handler)
		verify func(t *testing.T, b *wafv2.InMemoryBackend)
		name   string
	}{
		{
			name:  "reset_empty_backend",
			setup: func(_ *wafv2.Handler) {},
			verify: func(t *testing.T, b *wafv2.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 0, wafv2.WebACLCount(b))
				assert.Equal(t, 0, wafv2.IPSetCount(b))
				assert.Equal(t, 0, wafv2.RegexPatternSetCount(b))
				assert.Equal(t, 0, wafv2.RuleGroupCount(b))
				assert.Equal(t, 0, wafv2.APIKeyCount(b))
			},
		},
		{
			name: "reset_after_partial_creation",
			setup: func(h *wafv2.Handler) {
				doWafv2Request(t, h, "CreateWebACL", map[string]any{"Name": "a", "Scope": "REGIONAL"})
				doWafv2Request(
					t,
					h,
					"CreateIPSet",
					map[string]any{"Name": "b", "Scope": "REGIONAL", "IPAddressVersion": "IPV4"},
				)
				doWafv2Request(t, h, "CreateRegexPatternSet", map[string]any{"Name": "c", "Scope": "REGIONAL"})
			},
			verify: func(t *testing.T, b *wafv2.InMemoryBackend) {
				t.Helper()
				assert.Equal(t, 0, wafv2.WebACLCount(b))
				assert.Equal(t, 0, wafv2.IPSetCount(b))
				assert.Equal(t, 0, wafv2.RegexPatternSetCount(b))
				assert.Equal(t, 0, wafv2.AssociationCount(b))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := wafv2.NewInMemoryBackend("000000000000", "us-east-1")
			h := wafv2.NewHandler(b)
			tt.setup(h)
			h.Reset()
			tt.verify(t, b)
		})
	}
}

func TestCheckCapacity_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rules      []map[string]any
		wantExact  int
		wantStatus int
	}{
		{
			name:       "nil_rules",
			rules:      nil,
			wantExact:  0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty_rules",
			rules:      []map[string]any{},
			wantExact:  0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "single_rule",
			rules:      []map[string]any{{"Name": "r1"}},
			wantExact:  1,
			wantStatus: http.StatusOK,
		},
		{
			name: "five_rules",
			rules: []map[string]any{
				{"Name": "r1"},
				{"Name": "r2"},
				{"Name": "r3"},
				{"Name": "r4"},
				{"Name": "r5"},
			},
			wantExact:  5,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_scope",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var body map[string]any
			if tt.name == "missing_scope" {
				body = map[string]any{"Rules": tt.rules}
			} else {
				body = map[string]any{"Scope": "REGIONAL", "Rules": tt.rules}
			}

			rec := doWafv2Request(t, h, "CheckCapacity", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var result map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))

				consumed, ok := result["ConsumedCapacity"].(float64)
				require.True(t, ok)
				assert.Equal(t, tt.wantExact, int(consumed))
			}
		})
	}
}

func TestSnapshotRestore_RoundTrip(t *testing.T) {
	t.Parallel()

	b1 := wafv2.NewInMemoryBackend("123456789012", "eu-west-1")
	h1 := wafv2.NewHandler(b1)

	_, arnWebACL := createWebACLHelper(t, h1, "my-acl", "REGIONAL")
	doWafv2Request(t, h1, "CreateIPSet", map[string]any{
		"Name": "my-ipset", "Scope": "REGIONAL", "IPAddressVersion": "IPV4",
	})
	createRegexPatternSetHelper(t, h1, "my-rps")
	createRuleGroupHelper(t, h1, "my-rg")

	// Put logging config.
	rec := doWafv2Request(t, h1, "PutLoggingConfiguration", map[string]any{
		"LoggingConfiguration": map[string]any{"ResourceArn": arnWebACL},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	snap := b1.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := wafv2.NewInMemoryBackend("123456789012", "eu-west-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, wafv2.WebACLCount(b2))
	assert.Equal(t, 1, wafv2.IPSetCount(b2))
	assert.Equal(t, 1, wafv2.RegexPatternSetCount(b2))
	assert.Equal(t, 1, wafv2.RuleGroupCount(b2))

	// Logging config should be present after restore.
	h2 := wafv2.NewHandler(b2)
	rec = doWafv2Request(t, h2, "GetLoggingConfiguration", map[string]any{"ResourceArn": arnWebACL})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestSnapshotRestore_NilMaps(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("000000000000", "us-east-1")
	snap := []byte(`{"webACLs":null,"ipSets":null,"accountID":"123","region":"us-east-1"}`)
	require.NoError(t, b.Restore(t.Context(), snap))

	// After restore with nil maps, counts should be 0 (not panic).
	assert.Equal(t, 0, wafv2.WebACLCount(b))
	assert.Equal(t, 0, wafv2.IPSetCount(b))
	assert.Equal(t, 0, wafv2.RegexPatternSetCount(b))
}
