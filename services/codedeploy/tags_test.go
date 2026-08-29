package codedeploy_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_Tagging(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create app first.
	rec := doRequest(t, h, "CreateApplication", map[string]any{"applicationName": "tagged-app"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get the ARN.
	app, err := h.Backend.GetApplication("tagged-app")
	require.NoError(t, err)
	appARN := h.Backend.ApplicationARN(app.ApplicationName)

	// Tag the resource.
	rec = doRequest(t, h, "TagResource", map[string]any{
		"resourceArn": appARN,
		"tags": []map[string]string{
			{"Key": "env", "Value": "test"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// List tags.
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": appARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Untag.
	rec = doRequest(t, h, "UntagResource", map[string]any{
		"resourceArn": appARN,
		"tagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestTags_SortedListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("my-app", "Server", map[string]string{
		"zzz": "last",
		"aaa": "first",
		"mmm": "middle",
	})

	arn := h.Backend.ApplicationARN("my-app")
	rec := doRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Tags, 3)
	assert.Equal(t, "aaa", resp.Tags[0].Key)
	assert.Equal(t, "mmm", resp.Tags[1].Key)
	assert.Equal(t, "zzz", resp.Tags[2].Key)
}

func TestTags_OnDeploymentGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
	_, _ = createDG(h.Backend, "my-app", "my-dg", "", "", nil)

	dgArn := h.Backend.DeploymentGroupARN("my-app", "my-dg")

	// Tag the deployment group via TagResource
	rec := doRequest(t, h, "TagResource", map[string]any{
		"resourceArn": dgArn,
		"tags":        []map[string]string{{"Key": "env", "Value": "prod"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Read the tags back
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": dgArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Tags, 1)
	assert.Equal(t, "env", resp.Tags[0].Key)
	assert.Equal(t, "prod", resp.Tags[0].Value)
}

func TestTags_UntagDeploymentGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
	_, _ = createDG(h.Backend, "my-app", "my-dg", "", "", map[string]string{"env": "test", "team": "eng"})

	dgARN := h.Backend.DeploymentGroupARN("my-app", "my-dg")

	rec := doRequest(t, h, "UntagResource", map[string]any{
		"resourceArn": dgARN,
		"tagKeys":     []string{"team"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	kv, err := h.Backend.ListTagsForResource(dgARN)
	require.NoError(t, err)
	assert.NotContains(t, kv, "team")
	assert.Contains(t, kv, "env")
}

func TestTags_ResourceTagLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantErrType string
		tags        []map[string]string
		wantStatus  int
	}{
		{
			name: "within_limit",
			tags: []map[string]string{
				{"Key": "k1", "Value": "v1"},
				{"Key": "k2", "Value": "v2"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:        "reserved_prefix",
			tags:        []map[string]string{{"Key": "aws:tag", "Value": "v"}},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidTagsToAddException",
		},
		{
			name: "key_too_long",
			tags: []map[string]string{
				{"Key": strings.Repeat("k", 129), "Value": "v"},
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidTagsToAddException",
		},
		{
			name: "value_too_long",
			tags: []map[string]string{
				{"Key": "k", "Value": strings.Repeat("v", 257)},
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidTagsToAddException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, _ = h.Backend.CreateApplication("tag-app", "Server", nil)
			appARN := h.Backend.ApplicationARN("tag-app")

			rec := doRequest(t, h, "TagResource", map[string]any{
				"resourceArn": appARN,
				"tags":        tt.tags,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrType != "" {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantErrType, resp["__type"])
			}
		})
	}
}

func TestTags_ResourceExceedsMaxTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, _ = h.Backend.CreateApplication("tag-app2", "Server", nil)
	appARN := h.Backend.ApplicationARN("tag-app2")

	// Add 50 tags.
	tags := make([]map[string]string, 50)
	for i := range 50 {
		tags[i] = map[string]string{"Key": "k" + string(rune('a'+i%26)) + string(rune('0'+i/26)), "Value": "v"}
	}
	rec := doRequest(t, h, "TagResource", map[string]any{"resourceArn": appARN, "tags": tags})
	require.Equal(t, http.StatusOK, rec.Code)

	// Adding one more should fail.
	rec = doRequest(t, h, "TagResource", map[string]any{
		"resourceArn": appARN,
		"tags":        []map[string]string{{"Key": "extra-key", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	// TagResource's own deserializer models only InvalidTagsToAddException for
	// tag content, not TagLimitExceededException (that code belongs to
	// AddTagsToOnPremisesInstances/RemoveTagsFromOnPremisesInstances/
	// UpdateDeploymentGroup instead).
	assert.Equal(t, "InvalidTagsToAddException", resp["__type"])
}
