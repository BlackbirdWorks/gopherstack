package memorydb_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tag helpers

func tagEntry(key, value string) map[string]any {
	return map[string]any{"Key": key, "Value": value}
}

func tagsPayload(pairs ...map[string]any) []any {
	result := make([]any, 0, len(pairs))
	for _, p := range pairs {
		result = append(result, p)
	}

	return result
}

// -- Tag validation on TagResource -----------------------------------------------

func TestBatch2_TagResource_KeyValueConstraints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tags       []any
		wantStatus int
	}{
		{
			name:       "valid tag accepted",
			tags:       tagsPayload(tagEntry("env", "prod")),
			wantStatus: http.StatusOK,
		},
		{
			name:       "key starts with aws: rejected",
			tags:       tagsPayload(tagEntry("aws:reserved", "v")),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty key rejected",
			tags:       tagsPayload(tagEntry("", "v")),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "key 128 chars accepted",
			tags:       tagsPayload(tagEntry(strings.Repeat("k", 128), "v")),
			wantStatus: http.StatusOK,
		},
		{
			name:       "key 129 chars rejected",
			tags:       tagsPayload(tagEntry(strings.Repeat("k", 129), "v")),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "value 256 chars accepted",
			tags:       tagsPayload(tagEntry("k", strings.Repeat("v", 256))),
			wantStatus: http.StatusOK,
		},
		{
			name:       "value 257 chars rejected",
			tags:       tagsPayload(tagEntry("k", strings.Repeat("v", 257))),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "50 tags accepted",
			tags: func() []any {
				ts := make([]any, 50)
				for i := range ts {
					ts[i] = tagEntry(strings.Repeat("k", i+1), "v")
				}

				return ts
			}(),
			wantStatus: http.StatusOK,
		},
		{
			name: "51 tags rejected",
			tags: func() []any {
				ts := make([]any, 51)
				for i := range ts {
					ts[i] = tagEntry(strings.Repeat("k", i%128+1), "v")
				}

				return ts
			}(),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Create a cluster to get its ARN.
			resp, code := doCreateCluster(t, h, minimalClusterBody("tag-res-cl"))
			require.Equal(t, http.StatusOK, code)
			clusterARN, _ := resp["Cluster"].(map[string]any)["ARN"].(string)
			require.NotEmpty(t, clusterARN)

			rec := doRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": clusterARN,
				"Tags":        tt.tags,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)
		})
	}
}

// -- Tag validation on CreateCluster ---------------------------------------------

func TestBatch2_CreateCluster_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tags       []any
		wantStatus int
	}{
		{
			name:       "valid tags accepted",
			tags:       tagsPayload(tagEntry("env", "prod"), tagEntry("team", "platform")),
			wantStatus: http.StatusOK,
		},
		{
			name:       "aws: prefix rejected",
			tags:       tagsPayload(tagEntry("aws:reserved", "v")),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty key rejected",
			tags:       tagsPayload(tagEntry("", "v")),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "key too long rejected",
			tags:       tagsPayload(tagEntry(strings.Repeat("k", 129), "v")),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "value too long rejected",
			tags:       tagsPayload(tagEntry("k", strings.Repeat("v", 257))),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "51 tags rejected",
			tags: func() []any {
				ts := make([]any, 51)
				for i := range ts {
					ts[i] = tagEntry(strings.Repeat("k", i%128+1), "v")
				}

				return ts
			}(),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			body := minimalClusterBody("tag-cl-test")
			body["Tags"] = tt.tags
			_, code := doCreateCluster(t, h, body)
			assert.Equal(t, tt.wantStatus, code)
		})
	}
}

// -- Tag validation on CreateACL -------------------------------------------------

func TestBatch2_CreateACL_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tags       []any
		wantStatus int
	}{
		{
			name:       "valid tags accepted",
			tags:       tagsPayload(tagEntry("env", "test")),
			wantStatus: http.StatusOK,
		},
		{
			name:       "aws: prefix rejected",
			tags:       tagsPayload(tagEntry("aws:managed", "v")),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "key too long rejected",
			tags:       tagsPayload(tagEntry(strings.Repeat("k", 129), "v")),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "value too long rejected",
			tags:       tagsPayload(tagEntry("k", strings.Repeat("v", 257))),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateACL", map[string]any{
				"ACLName": "tag-acl-test",
				"Tags":    tt.tags,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)
		})
	}
}

// -- Tag validation on CreateUser ------------------------------------------------

func TestBatch2_CreateUser_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tags       []any
		wantStatus int
	}{
		{
			name:       "valid tags accepted",
			tags:       tagsPayload(tagEntry("owner", "alice")),
			wantStatus: http.StatusOK,
		},
		{
			name:       "aws: prefix rejected",
			tags:       tagsPayload(tagEntry("aws:owned", "v")),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty key rejected",
			tags:       tagsPayload(tagEntry("", "v")),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			body := map[string]any{
				"UserName":           "tag-user-test",
				"AccessString":       "on ~* &* +@all",
				"AuthenticationMode": map[string]any{"Type": "no-password-required"},
				"Tags":               tt.tags,
			}
			rec := doRequest(t, h, "CreateUser", body)
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)
		})
	}
}

// -- Tag validation on CreateParameterGroup --------------------------------------

func TestBatch2_CreateParameterGroup_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tags       []any
		wantStatus int
	}{
		{
			name:       "valid tags accepted",
			tags:       tagsPayload(tagEntry("env", "staging")),
			wantStatus: http.StatusOK,
		},
		{
			name:       "aws: prefix rejected",
			tags:       tagsPayload(tagEntry("aws:tag", "v")),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "value too long rejected",
			tags:       tagsPayload(tagEntry("k", strings.Repeat("v", 257))),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateParameterGroup", map[string]any{
				"ParameterGroupName": "tag-pg-test",
				"Family":             "memorydb_redis7",
				"Tags":               tt.tags,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)
		})
	}
}

// -- Tag validation on CreateSnapshot --------------------------------------------

func TestBatch2_CreateSnapshot_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tags       []any
		wantStatus int
	}{
		{
			name:       "valid tags accepted",
			tags:       tagsPayload(tagEntry("purpose", "backup")),
			wantStatus: http.StatusOK,
		},
		{
			name:       "aws: prefix rejected",
			tags:       tagsPayload(tagEntry("aws:created-by", "v")),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "key too long rejected",
			tags:       tagsPayload(tagEntry(strings.Repeat("k", 129), "v")),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			_, code := doCreateCluster(t, h, minimalClusterBody("snap-tag-cluster"))
			require.Equal(t, http.StatusOK, code)

			rec := doRequest(t, h, "CreateSnapshot", map[string]any{
				"ClusterName":  "snap-tag-cluster",
				"SnapshotName": "snap-tag-test",
				"Tags":         tt.tags,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)
		})
	}
}

// -- CreateCluster tags stored and retrievable via ListTags ----------------------

func TestBatch2_CreateCluster_TagsStored(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := minimalClusterBody("tagged-cluster")
	body["Tags"] = tagsPayload(
		tagEntry("env", "production"),
		tagEntry("team", "platform"),
	)

	resp, code := doCreateCluster(t, h, body)
	require.Equal(t, http.StatusOK, code)

	clusterARN, _ := resp["Cluster"].(map[string]any)["ARN"].(string)
	require.NotEmpty(t, clusterARN)

	rec := doRequest(t, h, "ListTags", map[string]any{"ResourceArn": clusterARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tagList, _ := listResp["TagList"].([]any)
	require.Len(t, tagList, 2)

	tagMap := make(map[string]string, len(tagList))
	for _, raw := range tagList {
		te, _ := raw.(map[string]any)
		k, _ := te["Key"].(string)
		v, _ := te["Value"].(string)
		tagMap[k] = v
	}

	assert.Equal(t, "production", tagMap["env"])
	assert.Equal(t, "platform", tagMap["team"])
}
