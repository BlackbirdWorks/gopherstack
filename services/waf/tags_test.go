package waf_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWAF_Tags_TagAndList(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	aclID := wafCreateWebACL(t, h, "tagged-acl")

	// Get the ARN
	rec := wafDo(t, h, "GetWebACL", map[string]any{"WebACLId": aclID})
	require.Equal(t, http.StatusOK, rec.Code)
	var aclResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &aclResp))
	arn := aclResp["WebACL"].(map[string]any)["WebACLArn"].(string)
	require.NotEmpty(t, arn)

	// TagResource
	rec = wafDo(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags": []map[string]any{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "security"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource
	rec = wafDo(t, h, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	info := listResp["TagInfoForResource"].(map[string]any)
	assert.Equal(t, arn, info["ResourceARN"])
	tagList := info["TagList"].([]any)
	require.Len(t, tagList, 2)

	// Collect tags
	tagMap := make(map[string]string)
	for _, t := range tagList {
		tMap := t.(map[string]any)
		tagMap[tMap["Key"].(string)] = tMap["Value"].(string)
	}

	assert.Equal(t, "prod", tagMap["env"])
	assert.Equal(t, "security", tagMap["team"])
}

func TestWAF_Tags_UntagResource(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	aclID := wafCreateWebACL(t, h, "untag-acl")

	rec := wafDo(t, h, "GetWebACL", map[string]any{"WebACLId": aclID})
	var aclResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &aclResp))
	arn := aclResp["WebACL"].(map[string]any)["WebACLArn"].(string)

	// Tag
	rec = wafDo(t, h, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags": []map[string]any{
			{"Key": "k1", "Value": "v1"},
			{"Key": "k2", "Value": "v2"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Untag k1
	rec = wafDo(t, h, "UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []string{"k1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify only k2 remains
	rec = wafDo(t, h, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tagList := listResp["TagInfoForResource"].(map[string]any)["TagList"].([]any)
	require.Len(t, tagList, 1)
	assert.Equal(t, "k2", tagList[0].(map[string]any)["Key"])
}

func TestWAF_Tags_CreateWebACLWithTags(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	token := wafGetToken(t, h)
	rec := wafDo(t, h, "CreateWebACL", map[string]any{
		"ChangeToken":   token,
		"Name":          "initial-tags-acl",
		"MetricName":    "initialTagsMetric",
		"DefaultAction": map[string]any{"Type": "ALLOW"},
		"Tags": []map[string]any{
			{"Key": "cost-center", "Value": "eng"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn := resp["WebACL"].(map[string]any)["WebACLArn"].(string)

	rec = wafDo(t, h, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tagList := listResp["TagInfoForResource"].(map[string]any)["TagList"].([]any)
	require.Len(t, tagList, 1)
	assert.Equal(t, "cost-center", tagList[0].(map[string]any)["Key"])
}
