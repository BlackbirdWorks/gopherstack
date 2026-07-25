package mwaa_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_TagsFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		envName string
	}{
		{
			name:    "tag_list_untag",
			envName: "tag-test-env",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			// Create environment.
			rec := doMWAARequest(t, h, http.MethodPut, "/environments/"+tt.envName, map[string]any{
				"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
				"NetworkConfiguration": networkConfigBody(),
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			envARN := createResp["Arn"]
			require.NotEmpty(t, envARN)

			// TagResource.
			tagRec := doMWAARequest(t, h, http.MethodPost, "/tags/"+envARN, map[string]any{
				"Tags": map[string]string{"env": "test"},
			})
			assert.Equal(t, http.StatusOK, tagRec.Code)

			// ListTagsForResource.
			listTagRec := doMWAARequest(t, h, http.MethodGet, "/tags/"+envARN, nil)
			assert.Equal(t, http.StatusOK, listTagRec.Code)

			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(listTagRec.Body.Bytes(), &tagsResp))
			tags, ok := tagsResp["Tags"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "test", tags["env"])
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		envName     string
		tagKeys     []string
		wantStatus  int
		wantTagsLen int
	}{
		{
			name:        "removes_tag",
			envName:     "untag-env",
			tagKeys:     []string{"env"},
			wantStatus:  http.StatusOK,
			wantTagsLen: 1,
		},
		{
			name:        "removes_nonexistent_key_ok",
			envName:     "untag-env2",
			tagKeys:     []string{"missing"},
			wantStatus:  http.StatusOK,
			wantTagsLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			// Create environment.
			rec := doMWAARequest(t, h, http.MethodPut, "/environments/"+tt.envName, map[string]any{
				"DagS3Path":            "dags/",
				"ExecutionRoleArn":     "arn:r",
				"SourceBucketArn":      "arn:b",
				"NetworkConfiguration": networkConfigBody(),
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			envARN := createResp["Arn"]
			require.NotEmpty(t, envARN)

			// Add tags.
			tagRec := doMWAARequest(t, h, http.MethodPost, "/tags/"+envARN, map[string]any{
				"Tags": map[string]string{"env": "test", "team": "platform"},
			})
			require.Equal(t, http.StatusOK, tagRec.Code)

			// Untag.
			untagPath := "/tags/" + envARN + "?tagKeys=" + tt.tagKeys[0]
			var untagPathSb118 strings.Builder
			for _, k := range tt.tagKeys[1:] {
				untagPathSb118.WriteString("&tagKeys=" + k)
			}
			untagPath += untagPathSb118.String()
			untagRec := doMWAARequest(t, h, http.MethodDelete, untagPath, nil)
			assert.Equal(t, tt.wantStatus, untagRec.Code)

			// Verify tags.
			listRec := doMWAARequest(t, h, http.MethodGet, "/tags/"+envARN, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsResp))
			tags := tagsResp["Tags"].(map[string]any)
			assert.Len(t, tags, tt.wantTagsLen)
		})
	}
}

func TestListTagsForResource_HTTP_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	unknownARN := "arn:aws:airflow:us-east-1:123456789012:environment/does-not-exist"
	rec := doMWAARequest(t, h, http.MethodGet, "/tags/"+unknownARN, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestListTagsForResource_HTTP_EmptyTagsShape(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	createRec := doMWAARequest(t, h, http.MethodPut, "/environments/notags-env", map[string]any{
		"DagS3Path":            "dags/",
		"ExecutionRoleArn":     "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":      "arn:aws:s3:::b",
		"NetworkConfiguration": networkConfigBody(),
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	envARN := createResp["Arn"]

	rec := doMWAARequest(t, h, http.MethodGet, "/tags/"+envARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, ok := resp["Tags"]
	assert.True(t, ok, "response must have Tags key even when empty")
}

// ─────────────────────────────────────────────────────────────
// 9. LoggingConfiguration Enabled bool pointer round-trip
// ─────────────────────────────────────────────────────────────

func TestTagLimit_HTTP_TagResource_Exceeds(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-tag-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"NetworkConfiguration": networkConfigBody(),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	envARN := createResp["Arn"]

	// Build 51 tags.
	tags := make(map[string]string, 51)
	for i := range 51 {
		tags[strings.Repeat("t", i+1)] = "val"
	}

	tagRec := doMWAARequest(t, h, http.MethodPost, "/tags/"+envARN, map[string]any{"Tags": tags})
	assert.Equal(t, http.StatusBadRequest, tagRec.Code)
}

// ─────────────────────────────────────────────────────────────
// Gap 6: WebserverAccessMode validation in UpdateEnvironment
// ─────────────────────────────────────────────────────────────

func TestHandler_UntagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	untagPath := "/tags/arn:aws:airflow:us-east-1:123456789012:environment/nonexistent?tagKeys=env"
	rec := doMWAARequest(t, h, http.MethodDelete, untagPath, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ListTags_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(
		t,
		h,
		http.MethodGet,
		"/tags/arn:aws:airflow:us-east-1:123456789012:environment/nonexistent",
		nil,
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(
		t,
		h,
		http.MethodPost,
		"/tags/arn:aws:airflow:us-east-1:123456789012:environment/nonexistent",
		map[string]any{
			"Tags": map[string]string{"k": "v"},
		},
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestTags_HTTP_CreateWithTagsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/http-tags-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
		"NetworkConfiguration": networkConfigBody(),
		"Tags":                 map[string]string{"service": "airflow", "tier": "production"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	envARN := createResp["Arn"]

	// ListTagsForResource returns the creation tags.
	tagRec := doMWAARequest(t, h, http.MethodGet, "/tags/"+envARN, nil)
	require.Equal(t, http.StatusOK, tagRec.Code)

	var tagsResp struct {
		Tags map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(tagRec.Body.Bytes(), &tagsResp))
	assert.Equal(t, "airflow", tagsResp.Tags["service"])
	assert.Equal(t, "production", tagsResp.Tags["tier"])
}
