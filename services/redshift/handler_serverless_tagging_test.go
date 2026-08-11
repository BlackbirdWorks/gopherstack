package redshift_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServerless_TagResource_RoundTrip proves creation-time Tags on
// CreateNamespace/CreateWorkgroup/CreateSnapshot actually reach the store
// (previously nowhere, since Namespace/Workgroup/Snapshot have no "tags"
// field of their own on the wire) and that TagResource/UntagResource mutate
// the same store ListTagsForResource reads from.
func TestServerless_TagResource_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "CreateNamespace", map[string]any{
		"namespaceName": "tag-ns",
		"tags": []map[string]any{
			{"key": "env", "value": "test"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	nsData, _ := createResp["namespace"].(map[string]any)
	require.NotNil(t, nsData)
	nsArn, _ := nsData["namespaceArn"].(string)
	require.NotEmpty(t, nsArn)

	// The namespace object itself never echoes tags (real Namespace has no
	// "tags" field) -- ListTagsForResource is the only real read path.
	assert.NotContains(t, nsData, "tags")

	rec = doServerlessOp(t, h, "ListTagsForResource", map[string]any{"resourceArn": nsArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&listResp))
	tagList, _ := listResp["tags"].([]any)
	require.Len(t, tagList, 1)

	first, _ := tagList[0].(map[string]any)
	require.NotNil(t, first)
	assert.Equal(t, "env", first["key"])
	assert.Equal(t, "test", first["value"])

	rec = doServerlessOp(t, h, "TagResource", map[string]any{
		"resourceArn": nsArn,
		"tags": []map[string]any{
			{"key": "team", "value": "platform"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "ListTagsForResource", map[string]any{"resourceArn": nsArn})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&listResp))
	tagList, _ = listResp["tags"].([]any)
	assert.Len(t, tagList, 2)

	rec = doServerlessOp(t, h, "UntagResource", map[string]any{
		"resourceArn": nsArn,
		"tagKeys":     []string{"team"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "ListTagsForResource", map[string]any{"resourceArn": nsArn})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&listResp))
	tagList, _ = listResp["tags"].([]any)
	require.Len(t, tagList, 1)

	remaining, _ := tagList[0].(map[string]any)
	require.NotNil(t, remaining)
	assert.Equal(t, "env", remaining["key"])
}

// TestServerless_TagResource_WorkgroupAndSnapshot proves CreateWorkgroup and
// CreateSnapshot's creation-time tags also reach the same store.
func TestServerless_TagResource_WorkgroupAndSnapshot(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "tag-ns2"})

	rec := doServerlessOp(t, h, "CreateWorkgroup", map[string]any{
		"workgroupName": "tag-wg",
		"namespaceName": "tag-ns2",
		"tags":          []map[string]any{{"key": "owner", "value": "team-a"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var wgResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&wgResp))
	wgData, _ := wgResp["workgroup"].(map[string]any)
	wgArn, _ := wgData["workgroupArn"].(string)
	require.NotEmpty(t, wgArn)

	rec = doServerlessOp(t, h, "ListTagsForResource", map[string]any{"resourceArn": wgArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&listResp))
	tagList, _ := listResp["tags"].([]any)
	require.Len(t, tagList, 1)

	rec = doServerlessOp(t, h, "CreateSnapshot", map[string]any{
		"snapshotName":  "tag-snap",
		"namespaceName": "tag-ns2",
		"tags":          []map[string]any{{"key": "owner", "value": "team-a"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var snapResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&snapResp))
	snapData, _ := snapResp["snapshot"].(map[string]any)
	snapArn, _ := snapData["snapshotArn"].(string)
	require.NotEmpty(t, snapArn)

	rec = doServerlessOp(t, h, "ListTagsForResource", map[string]any{"resourceArn": snapArn})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&listResp))
	tagList, _ = listResp["tags"].([]any)
	assert.Len(t, tagList, 1)
}

func TestServerless_TagResource_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		op   string
	}{
		{
			op:   "TagResource",
			body: map[string]any{"resourceArn": "arn:aws:redshift-serverless:us-east-1:000000000000:namespace/nope"},
		},
		{
			op: "UntagResource",
			body: map[string]any{
				"resourceArn": "arn:aws:redshift-serverless:us-east-1:000000000000:namespace/nope",
				"tagKeys":     []string{"env"},
			},
		},
		{
			op:   "ListTagsForResource",
			body: map[string]any{"resourceArn": "arn:aws:redshift-serverless:us-east-1:000000000000:namespace/nope"},
		},
	}

	for _, tt := range tests {
		t.Run("unknown resource "+tt.op, func(t *testing.T) {
			t.Parallel()

			h := newServerlessHandler()

			body := tt.body
			if tt.op == "TagResource" {
				body = map[string]any{
					"resourceArn": tt.body["resourceArn"],
					"tags":        []map[string]any{{"key": "env", "value": "prod"}},
				}
			}

			rec := doServerlessOp(t, h, tt.op, body)
			require.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
			assert.Equal(t, "ResourceNotFoundException", errResp["__type"])
		})
	}

	t.Run("tag resource missing tags", func(t *testing.T) {
		t.Parallel()

		h := newServerlessHandler()

		rec := doServerlessOp(t, h, "TagResource", map[string]any{
			"resourceArn": "arn:aws:redshift-serverless:us-east-1:000000000000:namespace/x",
		})
		require.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp map[string]any
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "ValidationException", errResp["__type"])
	})

	t.Run("untag resource missing tag keys", func(t *testing.T) {
		t.Parallel()

		h := newServerlessHandler()

		rec := doServerlessOp(t, h, "UntagResource", map[string]any{
			"resourceArn": "arn:aws:redshift-serverless:us-east-1:000000000000:namespace/x",
		})
		require.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp map[string]any
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
		assert.Equal(t, "ValidationException", errResp["__type"])
	})
}
