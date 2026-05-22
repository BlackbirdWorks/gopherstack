package route53resolver_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

func TestTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name: "tags_stored_and_listed",
			body: map[string]any{
				"ResourceArn": "arn:aws:route53resolver:us-east-1:000000000000:resolver-endpoint/rslvr-in-aabbccdd",
				"Tags":        []map[string]string{{"Key": "env", "Value": "prod"}},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "empty_body",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "TagResource", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		wantTags int
	}{
		{
			name:     "returns_tags_after_tagging",
			wantCode: http.StatusOK,
			wantTags: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create an endpoint to get its ARN.
			createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
				"Name":      "test-ep",
				"Direction": "INBOUND",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			ep := createResp["ResolverEndpoint"].(map[string]any)
			epARN := ep["Arn"].(string)

			// Tag the resource.
			tagRec := doRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": epARN,
				"Tags":        []map[string]string{{"Key": "env", "Value": "test"}},
			})
			require.Equal(t, http.StatusOK, tagRec.Code)

			// List tags.
			listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceArn": epARN,
			})
			assert.Equal(t, tt.wantCode, listRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
			tags := resp["Tags"].([]any)
			assert.Len(t, tags, tt.wantTags)
		})
	}
}

func TestUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		removeKey string
		wantCode  int
		wantTags  int
	}{
		{
			name:      "removes_tag",
			wantCode:  http.StatusOK,
			wantTags:  1,
			removeKey: "env",
		},
		{
			name:      "removes_nonexistent_key_is_ok",
			wantCode:  http.StatusOK,
			wantTags:  2,
			removeKey: "missing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create an endpoint.
			createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
				"Name":      "untag-ep",
				"Direction": "INBOUND",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			ep := createResp["ResolverEndpoint"].(map[string]any)
			epARN := ep["Arn"].(string)

			// Tag with two keys.
			tagRec := doRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": epARN,
				"Tags": []map[string]string{
					{"Key": "env", "Value": "test"},
					{"Key": "team", "Value": "platform"},
				},
			})
			require.Equal(t, http.StatusOK, tagRec.Code)

			// Untag.
			untagRec := doRequest(t, h, "UntagResource", map[string]any{
				"ResourceArn": epARN,
				"TagKeys":     []string{tt.removeKey},
			})
			assert.Equal(t, tt.wantCode, untagRec.Code)

			// List remaining tags.
			listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"ResourceArn": epARN,
			})
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
			tags := resp["Tags"].([]any)
			assert.Len(t, tags, tt.wantTags)
		})
	}
}

func TestDeleteResolverEndpoint_CascadeDeletesRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantRulesAfter int
		createRule     bool
	}{
		{
			name:           "cascade_deletes_associated_rule",
			createRule:     true,
			wantRulesAfter: 0,
		},
		{
			name:           "no_rules_to_cascade",
			createRule:     false,
			wantRulesAfter: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create endpoint.
			epRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
				"Name":      "cascade-ep",
				"Direction": "OUTBOUND",
			})
			require.Equal(t, http.StatusOK, epRec.Code)

			var epResp map[string]any
			require.NoError(t, json.Unmarshal(epRec.Body.Bytes(), &epResp))
			ep := epResp["ResolverEndpoint"].(map[string]any)
			epID := ep["Id"].(string)

			if tt.createRule {
				// Create a rule referencing the endpoint.
				ruleRec := doRequest(t, h, "CreateResolverRule", map[string]any{
					"Name":               "test-rule",
					"DomainName":         "example.com.",
					"RuleType":           "FORWARD",
					"ResolverEndpointId": epID,
					"TargetIps":          []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
				})
				require.Equal(t, http.StatusOK, ruleRec.Code)
			}

			// Delete the endpoint.
			delRec := doRequest(t, h, "DeleteResolverEndpoint", map[string]any{
				"ResolverEndpointId": epID,
			})
			assert.Equal(t, http.StatusOK, delRec.Code)

			// List rules — should be empty.
			listRec := doRequest(t, h, "ListResolverRules", map[string]any{})
			require.Equal(t, http.StatusOK, listRec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
			rules := listResp["ResolverRules"].([]any)
			assert.Len(t, rules, tt.wantRulesAfter)
		})
	}
}

func TestCreateResolverRule_EndpointValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		endpointID string
		wantCode   int
	}{
		{
			name:       "valid_endpoint",
			wantCode:   http.StatusOK,
			endpointID: "", // set after creating endpoint
		},
		{
			name:       "missing_endpoint_returns_not_found",
			endpointID: "rslvr-out-notexist",
			wantCode:   http.StatusNotFound,
		},
		{
			name:       "empty_endpoint_id_is_ok",
			endpointID: "",
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			endpointID := tt.endpointID
			if tt.name == "valid_endpoint" {
				// Create a real endpoint first.
				epRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
					"Name":      "ep-for-rule",
					"Direction": "OUTBOUND",
				})
				require.Equal(t, http.StatusOK, epRec.Code)

				var epResp map[string]any
				require.NoError(t, json.Unmarshal(epRec.Body.Bytes(), &epResp))
				ep := epResp["ResolverEndpoint"].(map[string]any)
				endpointID = ep["Id"].(string)
			}

			rec := doRequest(t, h, "CreateResolverRule", map[string]any{
				"Name":               "forward-rule",
				"DomainName":         "example.com.",
				"RuleType":           "FORWARD",
				"ResolverEndpointId": endpointID,
				"TargetIps":          []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create endpoint + rule.
	epRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "persist-ep",
		"Direction": "INBOUND",
	})
	require.Equal(t, http.StatusOK, epRec.Code)

	var epResp map[string]any
	require.NoError(t, json.Unmarshal(epRec.Body.Bytes(), &epResp))
	ep := epResp["ResolverEndpoint"].(map[string]any)
	epARN := ep["Arn"].(string)

	// Tag the endpoint.
	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": epARN,
		"Tags":        []map[string]string{{"Key": "persist-key", "Value": "persist-value"}},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	// Snapshot and restore to a new handler.
	snap := h.Snapshot()
	require.NotEmpty(t, snap)

	h2 := route53resolver.NewHandler(route53resolver.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(snap))

	// Tags must survive round-trip.
	listRec := doRequest(t, h2, "ListTagsForResource", map[string]any{
		"ResourceArn": epARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags := listResp["Tags"].([]any)
	assert.Len(t, tags, 1)
}
