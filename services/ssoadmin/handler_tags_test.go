package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTagKeyValidation verifies that tags with aws: prefix are rejected.
func TestTagKeyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tagKey     string
		wantStatus int
	}{
		{
			name:       "valid tag key accepted",
			tagKey:     "Environment",
			wantStatus: http.StatusOK,
		},
		{
			name:       "aws: prefix tag key rejected",
			tagKey:     "aws:Reserved",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "AWS: prefix tag key rejected",
			tagKey:     "AWS:Reserved",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "tag-validation-inst")
			rec := doRequest(t, h, "CreatePermissionSet", map[string]any{
				"InstanceArn": instanceArn,
				"Name":        "TagPS",
				"Tags":        []map[string]any{{"Key": tt.tagKey, "Value": "val"}},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestTagResourceApplication verifies that TagResource, UntagResource,
// and ListTagsForResource work on applications.
func TestTagResourceApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tagKey     string
		tagValue   string
		wantStatus int
	}{
		{
			name:       "tag application resource",
			tagKey:     "env",
			tagValue:   "prod",
			wantStatus: http.StatusOK,
		},
		{
			name:       "tag with multiple tags",
			tagKey:     "team",
			tagValue:   "platform",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "tag-app-inst")
			rec := doRequest(t, h, "CreateApplication", map[string]any{
				"InstanceArn":            instanceArn,
				"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
				"Name":                   "TagApp",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			appArn := parseResponse(t, rec)["ApplicationArn"].(string)

			// Tag it.
			tagRec := doRequest(t, h, "TagResource", map[string]any{
				"InstanceArn": instanceArn,
				"ResourceArn": appArn,
				"Tags":        []map[string]any{{"Key": tt.tagKey, "Value": tt.tagValue}},
			})
			assert.Equal(t, tt.wantStatus, tagRec.Code)

			// List tags and verify.
			listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"InstanceArn": instanceArn,
				"ResourceArn": appArn,
			})
			require.Equal(t, http.StatusOK, listRec.Code)
			resp := parseResponse(t, listRec)
			tagList := resp["Tags"].([]any)
			found := false
			for _, item := range tagList {
				m := item.(map[string]any)
				if m["Key"] == tt.tagKey {
					found = true
					assert.Equal(t, tt.tagValue, m["Value"])
				}
			}
			assert.True(t, found, "expected tag key %q in response", tt.tagKey)
		})
	}
}

// TestUntagResourceApplication verifies that UntagResource works on applications.
func TestUntagResourceApplication(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "untag-app-inst")
	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
		"Name":                   "UntagApp",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	appArn := parseResponse(t, rec)["ApplicationArn"].(string)

	// Tag it first.
	_ = doRequest(t, h, "TagResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": appArn,
		"Tags":        []map[string]any{{"Key": "env", "Value": "test"}},
	})

	// Untag it.
	untagRec := doRequest(t, h, "UntagResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": appArn,
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, untagRec.Code)

	// Verify tag is removed.
	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": appArn,
	})
	resp := parseResponse(t, listRec)
	tagList := resp["Tags"].([]any)
	for _, item := range tagList {
		m := item.(map[string]any)
		assert.NotEqual(t, "env", m["Key"])
	}
}

// TestTagResourceInstance verifies that TagResource works on instances.
func TestTagResourceInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "tag-instance")

	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": instanceArn,
		"Tags":        []map[string]any{{"Key": "owner", "Value": "team-a"}},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": instanceArn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	resp := parseResponse(t, listRec)
	tagList := resp["Tags"].([]any)
	found := false
	for _, item := range tagList {
		m := item.(map[string]any)
		if m["Key"] == "owner" {
			found = true
			assert.Equal(t, "team-a", m["Value"])
		}
	}
	assert.True(t, found)
}

// TestTagResourceTooManyTags verifies that adding >50 tags returns 400.
func TestTagResourceTooManyTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "tag-limit-inst")

	// Add 50 tags first.
	tags := make([]map[string]any, 50)
	for i := range tags {
		tags[i] = map[string]any{"Key": "k" + string(rune('a'+i%26)) + string(rune('0'+i/26)), "Value": "v"}
	}
	rec := doRequest(t, h, "TagResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": instanceArn,
		"Tags":        tags,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Adding 1 more tag should fail with the real ssoadmin exception type --
	// TooManyTagsException is not in the ssoadmin error model; AWS returns
	// ServiceQuotaExceededException for this limit (see types/errors.go).
	rec = doRequest(t, h, "TagResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": instanceArn,
		"Tags":        []map[string]any{{"Key": "extra", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	resp := parseResponse(t, rec)
	assert.Equal(t, "ServiceQuotaExceededException", resp["__type"])
}

func TestTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTags   map[string]string
		wantAfterU map[string]string
		name       string
		tags       []map[string]string
		untagKeys  []string
	}{
		{
			name: "tag, list, and untag resource",
			tags: []map[string]string{
				{"Key": "env", "Value": "prod"},
				{"Key": "team", "Value": "platform"},
			},
			untagKeys:  []string{"env"},
			wantTags:   map[string]string{"env": "prod", "team": "platform"},
			wantAfterU: map[string]string{"team": "platform"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")
			psArn := createPermissionSet(t, h, instanceArn, "PS")

			tagData := make([]any, 0, len(tt.tags))
			for _, tag := range tt.tags {
				tagData = append(tagData, tag)
			}

			tagRec := doRequest(t, h, "TagResource", map[string]any{
				"InstanceArn": instanceArn,
				"ResourceArn": psArn,
				"Tags":        tagData,
			})
			require.Equal(t, http.StatusOK, tagRec.Code)

			listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"InstanceArn": instanceArn,
				"ResourceArn": psArn,
			})
			require.Equal(t, http.StatusOK, listRec.Code)
			listResp := parseResponse(t, listRec)
			tagList, ok := listResp["Tags"].([]any)
			require.True(t, ok)
			gotTags := make(map[string]string, len(tagList))
			for _, item := range tagList {
				t2 := item.(map[string]any)
				gotTags[t2["Key"].(string)] = t2["Value"].(string)
			}
			assert.Equal(t, tt.wantTags, gotTags)

			untagRec := doRequest(t, h, "UntagResource", map[string]any{
				"InstanceArn": instanceArn,
				"ResourceArn": psArn,
				"TagKeys":     tt.untagKeys,
			})
			require.Equal(t, http.StatusOK, untagRec.Code)

			listRec2 := doRequest(t, h, "ListTagsForResource", map[string]any{
				"InstanceArn": instanceArn,
				"ResourceArn": psArn,
			})
			require.Equal(t, http.StatusOK, listRec2.Code)
			listResp2 := parseResponse(t, listRec2)
			tagList2, ok := listResp2["Tags"].([]any)
			require.True(t, ok)
			gotTags2 := make(map[string]string, len(tagList2))
			for _, item := range tagList2 {
				t2 := item.(map[string]any)
				gotTags2[t2["Key"].(string)] = t2["Value"].(string)
			}
			assert.Equal(t, tt.wantAfterU, gotTags2)
		})
	}
}

func TestTaggingOnInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "tag instance arn succeeds",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")

			tagRec := doRequest(t, h, "TagResource", map[string]any{
				"InstanceArn": instanceArn,
				"ResourceArn": instanceArn,
				"Tags":        []map[string]string{{"Key": "env", "Value": "test"}},
			})
			assert.Equal(t, tt.wantStatus, tagRec.Code)

			untagRec := doRequest(t, h, "UntagResource", map[string]any{
				"InstanceArn": instanceArn,
				"ResourceArn": instanceArn,
				"TagKeys":     []string{"env"},
			})
			assert.Equal(t, tt.wantStatus, untagRec.Code)

			listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"InstanceArn": instanceArn,
				"ResourceArn": instanceArn,
			})
			assert.Equal(t, tt.wantStatus, listRec.Code)
		})
	}
}

func TestTagging_ErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantType   string
		wantStatus int
	}{
		{
			name: "tag_resource_not_found",
			op:   "TagResource",
			body: map[string]any{
				"InstanceArn": "arn:aws:sso:::instance/ssoins-notfound",
				"ResourceArn": "arn:aws:sso:::permissionSet/ssoins-x/notfound",
				"Tags":        []map[string]string{},
			},
			wantStatus: http.StatusNotFound,
			wantType:   "ResourceNotFoundException",
		},
		{
			name: "untag_resource_not_found",
			op:   "UntagResource",
			body: map[string]any{
				"InstanceArn": "arn:aws:sso:::instance/ssoins-notfound",
				"ResourceArn": "arn:aws:sso:::permissionSet/ssoins-x/notfound",
				"TagKeys":     []string{"env"},
			},
			wantStatus: http.StatusNotFound,
			wantType:   "ResourceNotFoundException",
		},
		{
			name: "list_tags_resource_not_found",
			op:   "ListTagsForResource",
			body: map[string]any{
				"InstanceArn": "arn:aws:sso:::instance/ssoins-notfound",
				"ResourceArn": "arn:aws:sso:::permissionSet/ssoins-x/notfound",
			},
			wantStatus: http.StatusNotFound,
			wantType:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResponse(t, rec)
			assert.Equal(t, tt.wantType, resp["__type"])
		})
	}
}
