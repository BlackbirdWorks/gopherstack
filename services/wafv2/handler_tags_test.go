package wafv2_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

func TestHandler_TagResource_and_ListTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		name       string
		wantTag    string
		tags       []map[string]string
		wantStatus int
	}{
		{
			name: "tags_flow",
			setup: func(h *wafv2.Handler) string {
				w, _ := wafv2.CreateWebACLSimple(h.Backend, "tagged-acl", "REGIONAL", "", "ALLOW", nil)

				return h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
			},
			tags:       []map[string]string{{"Key": "env", "Value": "prod"}},
			wantStatus: http.StatusOK,
			wantTag:    "env",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arnStr := tt.setup(h)

			tagRec := doWafv2Request(t, h, "TagResource", map[string]any{
				"ResourceARN": arnStr,
				"Tags":        tt.tags,
			})
			assert.Equal(t, tt.wantStatus, tagRec.Code)

			listRec := doWafv2Request(t, h, "ListTagsForResource", map[string]any{
				"ResourceARN": arnStr,
			})
			assert.Equal(t, http.StatusOK, listRec.Code)

			var result map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &result))

			info, ok := result["TagInfoForResource"].(map[string]any)
			require.True(t, ok)

			tagList, ok := info["TagList"].([]any)
			require.True(t, ok)
			require.NotEmpty(t, tagList)

			firstTag, ok := tagList[0].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantTag, firstTag["Key"])
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		name       string
		tagKeys    []string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *wafv2.Handler) string {
				w, _ := wafv2.CreateWebACLSimple(
					h.Backend,
					"tagged-acl",
					"REGIONAL",
					"",
					"ALLOW",
					map[string]string{"env": "prod", "team": "ops"},
				)
				arnStr := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)

				return arnStr
			},
			tagKeys:    []string{"env"},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_resource_arn",
			setup: func(_ *wafv2.Handler) string {
				return ""
			},
			tagKeys:    []string{"env"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/nonexistent/badid"
			},
			tagKeys:    []string{"env"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "ipset_success",
			setup: func(h *wafv2.Handler) string {
				s, _ := h.Backend.CreateIPSet(
					context.Background(),
					"tagged-set",
					"REGIONAL",
					"",
					"IPV4",
					nil,
					map[string]string{"env": "prod"},
				)
				arnStr := h.Backend.IPSetARN(s.Name, s.ID, s.Scope)

				return arnStr
			},
			tagKeys:    []string{"env"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(h)

			body := map[string]any{"TagKeys": tt.tagKeys}
			if resourceARN != "" {
				body["ResourceARN"] = resourceARN
			}

			rec := doWafv2Request(t, h, "UntagResource", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_TagResource_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name:       "missing_resource_arn",
			body:       map[string]any{"Tags": []map[string]string{{"Key": "k", "Value": "v"}}},
			wantStatus: http.StatusBadRequest,
			wantType:   "WAFInvalidParameterException",
		},
		{
			name: "not_found",
			body: map[string]any{
				"ResourceARN": "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/nonexistent/badid",
				"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "WAFNonexistentItemException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "TagResource", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var result map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
			assert.Equal(t, tt.wantType, result["__type"])
		})
	}
}

func TestHandler_ListTagsForResource_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name:       "missing_resource_arn",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantType:   "WAFInvalidParameterException",
		},
		{
			name: "not_found",
			body: map[string]any{
				"ResourceARN": "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/nonexistent/badid",
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "WAFNonexistentItemException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "ListTagsForResource", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var result map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
			assert.Equal(t, tt.wantType, result["__type"])
		})
	}
}

func TestBackend_TagResource_IPSet(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("000000000000", "us-east-1")
	s, err := b.CreateIPSet(context.Background(), "my-set", "REGIONAL", "", "IPV4", nil, nil)
	require.NoError(t, err)

	arnStr := b.IPSetARN(s.Name, s.ID, s.Scope)
	require.NoError(t, b.TagResource(context.Background(), arnStr, map[string]string{"env": "test"}))

	tags, err := b.ListTagsForResource(context.Background(), arnStr)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
}

func TestTagValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a WebACL to tag.
	createRec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":             "tag-test-acl",
		"Scope":            "REGIONAL",
		"DefaultAction":    map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{"MetricName": "tag-test-acl"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	arnStr := createResp["Summary"].(map[string]any)["ARN"].(string)

	// More than 50 tags should fail.
	tags := make([]map[string]string, 51)
	for i := range tags {
		tags[i] = map[string]string{"Key": fmt.Sprintf("key%d", i), "Value": "v"}
	}

	tooManyRec := doWafv2Request(t, h, "TagResource", map[string]any{
		"ResourceARN": arnStr,
		"Tags":        tags,
	})
	assert.Equal(t, http.StatusBadRequest, tooManyRec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(tooManyRec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFTagOperationException", errResp["__type"])

	// Reserved prefix "aws:" should fail.
	reservedRec := doWafv2Request(t, h, "TagResource", map[string]any{
		"ResourceARN": arnStr,
		"Tags":        []map[string]string{{"Key": "aws:reserved", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, reservedRec.Code)

	var reservedErr map[string]any
	require.NoError(t, json.Unmarshal(reservedRec.Body.Bytes(), &reservedErr))
	assert.Equal(t, "WAFTagOperationException", reservedErr["__type"])
}

// ---- Gap 21: List pagination -----------------------------------------------

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

// TestHandler_ListTagsForResource_Pagination verifies ListTagsForResource
// honors Limit/NextMarker instead of always returning the full tag set,
// matching the real ListTagsForResourceInput/Output shape (both fields are
// documented on the real SDK's api_op_ListTagsForResource.go).
func TestHandler_ListTagsForResource_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	w, err := wafv2.CreateWebACLSimple(h.Backend, "paginated-tags-acl", "REGIONAL", "", "ALLOW", nil)
	require.NoError(t, err)

	arnStr := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)

	tagRec := doWafv2Request(t, h, "TagResource", map[string]any{
		"ResourceARN": arnStr,
		"Tags": []map[string]string{
			{"Key": "a", "Value": "1"},
			{"Key": "b", "Value": "2"},
			{"Key": "c", "Value": "3"},
		},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	// First page: Limit 2 returns exactly 2 tags plus a NextMarker.
	firstRec := doWafv2Request(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": arnStr,
		"Limit":       2,
	})
	require.Equal(t, http.StatusOK, firstRec.Code, firstRec.Body.String())

	var first map[string]any
	require.NoError(t, json.Unmarshal(firstRec.Body.Bytes(), &first))

	firstInfo, ok := first["TagInfoForResource"].(map[string]any)
	require.True(t, ok)
	firstList, ok := firstInfo["TagList"].([]any)
	require.True(t, ok)
	assert.Len(t, firstList, 2)

	marker, ok := first["NextMarker"].(string)
	require.True(t, ok, "NextMarker should be present when more tags remain")
	assert.NotEmpty(t, marker)

	// Second page: the remaining tag, no further NextMarker.
	secondRec := doWafv2Request(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": arnStr,
		"Limit":       2,
		"NextMarker":  marker,
	})
	require.Equal(t, http.StatusOK, secondRec.Code, secondRec.Body.String())

	var second map[string]any
	require.NoError(t, json.Unmarshal(secondRec.Body.Bytes(), &second))

	secondInfo, ok := second["TagInfoForResource"].(map[string]any)
	require.True(t, ok)
	secondList, ok := secondInfo["TagList"].([]any)
	require.True(t, ok)
	assert.Len(t, secondList, 1)
	_, hasMarker := second["NextMarker"]
	assert.False(t, hasMarker, "NextMarker should be absent once all tags are returned")

	// No Limit: full tag set in one page, no NextMarker.
	allRec := doWafv2Request(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": arnStr,
	})
	require.Equal(t, http.StatusOK, allRec.Code)

	var all map[string]any
	require.NoError(t, json.Unmarshal(allRec.Body.Bytes(), &all))
	allInfo, ok := all["TagInfoForResource"].(map[string]any)
	require.True(t, ok)
	allList, ok := allInfo["TagList"].([]any)
	require.True(t, ok)
	assert.Len(t, allList, 3)
}
