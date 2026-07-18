package memorydb_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTags   map[string]string
		name       string
		wantStatus int
	}{
		{
			name:       "list tag after create",
			wantStatus: http.StatusOK,
			wantTags:   map[string]string{"Env": "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "tag-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
				"Tags":        []map[string]any{{"Key": "Env", "Value": "test"}},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

			clusterMap := createResp["Cluster"].(map[string]any)
			clusterARN := clusterMap["ARN"].(string)

			listRec := doRequest(t, h, "ListTags", map[string]any{"ResourceArn": clusterARN})
			assert.Equal(t, tt.wantStatus, listRec.Code)

			if tt.wantStatus == http.StatusOK {
				var tagsResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsResp))
				tagList := tagsResp["TagList"].([]any)
				require.NotEmpty(t, tagList)
			}
		})
	}
}

// TestHandler_Tags_NotFound tests tag operations on unknown ARN.
func TestHandler_Tags_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "list tags unknown ARN",
			op:         "ListTags",
			body:       map[string]any{"ResourceArn": "arn:aws:memorydb:us-east-1:123:cluster/no-such"},
			wantStatus: 404,
		},
		{
			name: "tag resource unknown ARN",
			op:   "TagResource",
			body: map[string]any{
				"ResourceArn": "arn:aws:memorydb:us-east-1:123:cluster/no-such",
				"Tags":        []map[string]any{{"Key": "k", "Value": "v"}},
			},
			wantStatus: 404,
		},
		{
			name: "untag resource unknown ARN",
			op:   "UntagResource",
			body: map[string]any{
				"ResourceArn": "arn:aws:memorydb:us-east-1:123:cluster/no-such",
				"TagKeys":     []string{"k"},
			},
			wantStatus: 404,
		},
		{
			name:       "list tags missing ARN",
			op:         "ListTags",
			body:       map[string]any{},
			wantStatus: 400,
		},
		{
			name:       "tag resource missing ARN",
			op:         "TagResource",
			body:       map[string]any{"Tags": []map[string]any{{"Key": "k", "Value": "v"}}},
			wantStatus: 400,
		},
		{
			name:       "untag resource missing ARN",
			op:         "UntagResource",
			body:       map[string]any{"TagKeys": []string{"k"}},
			wantStatus: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_TagResource_KeyValueConstraints(t *testing.T) {
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

func TestHandler_CreateCluster_TagValidation(t *testing.T) {
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

func TestHandler_CreateACL_TagValidation(t *testing.T) {
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

func TestHandler_CreateUser_TagValidation(t *testing.T) {
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

func TestHandler_CreateParameterGroup_TagValidation(t *testing.T) {
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

func TestHandler_CreateSnapshot_TagValidation(t *testing.T) {
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

func TestHandler_CreateCluster_TagsStored(t *testing.T) {
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

// TestHandler_Tags_AllResources tests tag operations on all resource kinds.
func TestHandler_Tags_AllResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*memorydb.Handler) string // returns ARN
		name       string
		wantStatus int
	}{
		{
			name: "tags on subnet group",
			setup: func(h *memorydb.Handler) string {
				rec := doRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "tag-sg",
					"SubnetIds":       []string{"subnet-1"},
					"Tags":            []map[string]any{{"Key": "Env", "Value": "prod"}},
				})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["SubnetGroup"].(map[string]any)["ARN"].(string)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "tags on user",
			setup: func(h *memorydb.Handler) string {
				rec := doRequest(t, h, "CreateUser", map[string]any{
					"UserName":     "tag-user",
					"AccessString": "on ~*",
					"AuthenticationMode": map[string]any{
						"Type":      "password",
						"Passwords": []string{"pass1"},
					},
					"Tags": []map[string]any{{"Key": "Env", "Value": "prod"}},
				})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["User"].(map[string]any)["ARN"].(string)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "tags on parameter group",
			setup: func(h *memorydb.Handler) string {
				rec := doRequest(t, h, "CreateParameterGroup", map[string]any{
					"ParameterGroupName": "tag-pg",
					"Family":             "memorydb_redis7",
					"Tags":               []map[string]any{{"Key": "Env", "Value": "prod"}},
				})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["ParameterGroup"].(map[string]any)["ARN"].(string)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "tags on snapshot",
			setup: func(h *memorydb.Handler) string {
				// Pre-create the cluster first.
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "my-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				rec := doRequest(t, h, "CreateSnapshot", map[string]any{
					"SnapshotName": "tag-snap",
					"ClusterName":  "my-cluster",
					"Tags":         []map[string]any{{"Key": "Env", "Value": "prod"}},
				})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["Snapshot"].(map[string]any)["ARN"].(string)
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := tt.setup(h)

			rec := doRequest(t, h, "ListTags", map[string]any{"ResourceArn": arn})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			tagList := resp["TagList"].([]any)
			assert.NotEmpty(t, tagList)

			// Test TagResource
			tagRec := doRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": arn,
				"Tags":        []map[string]any{{"Key": "Team", "Value": "ops"}},
			})
			assert.Equal(t, http.StatusOK, tagRec.Code)

			// Test UntagResource
			untagRec := doRequest(t, h, "UntagResource", map[string]any{
				"ResourceArn": arn,
				"TagKeys":     []string{"Team"},
			})
			assert.Equal(t, http.StatusOK, untagRec.Code)
		})
	}
}

func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody map[string]any
		getARN     func(resp map[string]any) string
		name       string
		resource   string
		createOp   string
	}{
		{
			name:     "tag cluster",
			resource: "cluster",
			createOp: "CreateCluster",
			createBody: map[string]any{
				"ClusterName": "tag-test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			},
			getARN: func(r map[string]any) string {
				cl, _ := r["Cluster"].(map[string]any)
				arn, _ := cl["ARN"].(string)

				return arn
			},
		},
		{
			name:       "tag ACL",
			resource:   "acl",
			createOp:   "CreateACL",
			createBody: map[string]any{"ACLName": "tag-test-acl"},
			getARN: func(r map[string]any) string {
				acl, _ := r["ACL"].(map[string]any)
				arn, _ := acl["ARN"].(string)

				return arn
			},
		},
		{
			name:     "tag user",
			resource: "user",
			createOp: "CreateUser",
			createBody: map[string]any{
				"UserName":           "tag-test-user",
				"AccessString":       "on ~* &* +@all",
				"AuthenticationMode": map[string]any{"Type": "no-password-required"},
			},
			getARN: func(r map[string]any) string {
				u, _ := r["User"].(map[string]any)
				arn, _ := u["ARN"].(string)

				return arn
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequest(t, h, tt.createOp, tt.createBody)
			require.Equal(t, http.StatusOK, rec.Code, "create: %s", rec.Body)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			resourceARN := tt.getARN(createResp)
			require.NotEmpty(t, resourceARN)

			// Tag the resource.
			tagRec := doRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": resourceARN,
				"Tags": []any{
					map[string]any{"Key": "env", "Value": "test"},
					map[string]any{"Key": "team", "Value": "platform"},
				},
			})
			require.Equal(t, http.StatusOK, tagRec.Code, "tag: %s", tagRec.Body)

			// List tags.
			listRec := doRequest(t, h, "ListTags", map[string]any{"ResourceArn": resourceARN})
			require.Equal(t, http.StatusOK, listRec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
			tagList, _ := listResp["TagList"].([]any)
			assert.Len(t, tagList, 2)

			// Untag one.
			untagRec := doRequest(t, h, "UntagResource", map[string]any{
				"ResourceArn": resourceARN,
				"TagKeys":     []string{"env"},
			})
			require.Equal(t, http.StatusOK, untagRec.Code)

			// Verify only 1 tag remains.
			listRec2 := doRequest(t, h, "ListTags", map[string]any{"ResourceArn": resourceARN})
			require.Equal(t, http.StatusOK, listRec2.Code)
			require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listResp))
			tagList2, _ := listResp["TagList"].([]any)
			assert.Len(t, tagList2, 1)
		})
	}
}

// -- Reset clears and re-seeds everything ----------------------------------------

// TestHandler_TagValidation tests tag validation edge cases.
func TestHandler_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name: "too many tags rejected",
			op:   "CreateCluster",
			body: func() map[string]any {
				tags := make([]map[string]any, 51)
				for i := range tags {
					tags[i] = map[string]any{"Key": "key" + string(rune('a'+i%26)), "Value": "v"}
				}
				// Use unique keys for all 51
				for i := range tags {
					tags[i] = map[string]any{"Key": "uniquekey" + string(rune(i+65)), "Value": "v"}
				}

				return map[string]any{
					"ClusterName": "tagged-cl",
					"NodeType":    "db.r6g.large",
					"Tags":        tags,
				}
			}(),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "tag key with aws prefix rejected",
			op:   "CreateCluster",
			body: map[string]any{
				"ClusterName": "tagged-cl",
				"NodeType":    "db.r6g.large",
				"Tags":        []map[string]any{{"Key": "aws:restricted", "Value": "v"}},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "empty tag key rejected",
			op:   "CreateCluster",
			body: map[string]any{
				"ClusterName": "tagged-cl",
				"NodeType":    "db.r6g.large",
				"Tags":        []map[string]any{{"Key": "", "Value": "v"}},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_TagResource_Limit tests the tag limit enforcement on TagResource.
func TestHandler_TagResource_Limit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "adding tags beyond limit returns 400", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "tag-limit-cl",
				"NodeType":    "db.r6g.large",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			clusterMap := createResp["Cluster"].(map[string]any)
			arn := clusterMap["ARN"].(string)

			// Add 50 tags (the limit)
			tags := make([]map[string]any, 50)
			for i := range tags {
				tags[i] = map[string]any{
					"Key":   "tagkey" + string(rune('a'+i%26)) + string(rune('0'+i/26)),
					"Value": "v",
				}
			}
			for i := range tags {
				tags[i] = map[string]any{
					"Key":   "key-" + string(rune(i+65)),
					"Value": "v",
				}
			}

			// Tag with 50 unique keys
			tagList := make([]map[string]any, 50)
			for i := range tagList {
				tagList[i] = map[string]any{"Key": "k" + string(rune(i+65)), "Value": "v"}
			}
			doRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": arn,
				"Tags":        tagList,
			})

			// Now try to add one more tag → should exceed limit
			overRec := doRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": arn,
				"Tags":        []map[string]any{{"Key": "extra-key", "Value": "v"}},
			})
			assert.Equal(t, tt.wantStatus, overRec.Code)
		})
	}
}

func TestHandler_Tags_Extended(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*memorydb.Handler) string
		wantTags   map[string]string
		name       string
		wantStatus int
	}{
		{
			name: "list tags on cluster",
			setup: func(h *memorydb.Handler) string {
				cl := createClusterObj(t, h, map[string]any{
					"ClusterName": "tag-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
					"Tags":        []map[string]any{{"Key": "env", "Value": "prod"}},
				})

				return cl["ARN"].(string)
			},
			wantStatus: http.StatusOK,
			wantTags:   map[string]string{"env": "prod"},
		},
		{
			name: "tag resource",
			setup: func(h *memorydb.Handler) string {
				cl := createClusterObj(t, h, map[string]any{
					"ClusterName": "tag-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				arn := cl["ARN"].(string)
				doRequest(t, h, "TagResource", map[string]any{
					"ResourceArn": arn,
					"Tags":        []map[string]any{{"Key": "team", "Value": "backend"}},
				})

				return arn
			},
			wantStatus: http.StatusOK,
			wantTags:   map[string]string{"team": "backend"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(h)

			rec := doRequest(t, h, "ListTags", map[string]any{"ResourceArn": resourceARN})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tagList := resp["TagList"].([]any)
				tags := make(map[string]string)
				for _, t := range tagList {
					tag := t.(map[string]any)
					tags[tag["Key"].(string)] = tag["Value"].(string)
				}
				for k, v := range tt.wantTags {
					assert.Equal(t, v, tags[k])
				}
			}
		})
	}
}

// -- Endpoint addressing -------------------------------------------------------

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	cl := createClusterObj(t, h, map[string]any{
		"ClusterName": "tag-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
		"Tags":        []map[string]any{{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "backend"}},
	})
	resourceARN := cl["ARN"].(string)

	// Untag the "env" key.
	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": resourceARN,
		"TagKeys":     []string{"env"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List tags - should only have "team" left.
	rec2 := doRequest(t, h, "ListTags", map[string]any{"ResourceArn": resourceARN})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	tagList := resp["TagList"].([]any)
	assert.Len(t, tagList, 1)
	tag := tagList[0].(map[string]any)
	assert.Equal(t, "team", tag["Key"])
}

// -- BatchUpdateCluster --------------------------------------------------------
