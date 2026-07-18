package dax_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// ---- Tags ----

func TestHandlerTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *dax.Handler) string
		tags       []map[string]string
		wantStatus int
	}{
		{
			name: "tag cluster",
			setup: func(t *testing.T, h *dax.Handler) string {
				t.Helper()
				rec := daxRequest(t, h, "CreateCluster", validClusterBody("tagged-cluster"))
				var resp map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["Cluster"].(map[string]any)["ClusterArn"].(string)
			},
			tags:       []map[string]string{{"Key": "env", "Value": "prod"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "resource not found",
			setup: func(_ *testing.T, _ *dax.Handler) string {
				return "arn:aws:dax:us-east-1:123456789012:cache/no-such"
			},
			tags:       []map[string]string{{"Key": "k", "Value": "v"}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			arn := tt.setup(t, h)

			rec := daxRequest(t, h, "TagResource", map[string]any{
				"ResourceName": arn,
				"Tags":         tt.tags,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandlerListTags(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	createRec := daxRequest(t, h, "CreateCluster", validClusterBody("tagged-cluster"))
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	clusterArn := createResp["Cluster"].(map[string]any)["ClusterArn"].(string)

	tagRec := daxRequest(t, h, "TagResource", map[string]any{
		"ResourceName": clusterArn,
		"Tags":         []map[string]string{{"Key": "env", "Value": "prod"}},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	listRec := daxRequest(t, h, "ListTags", map[string]any{"ResourceName": clusterArn})
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags := listResp["Tags"].([]any)
	assert.NotEmpty(t, tags)
}

// TestHandlerCreateClusterTagsAsArray verifies that CreateCluster accepts Tags as an array of
// {Key, Value} objects (the AWS SDK wire format) rather than a JSON map.
func TestHandlerCreateClusterTagsAsArray(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       map[string]any
		checkTag   string
		wantValue  string
		wantStatus int
	}{
		{
			name: "tags as array are stored correctly",
			body: map[string]any{
				"ClusterName":       "tagged-array",
				"NodeType":          "dax.r5.large",
				"IamRoleArn":        "arn:aws:iam::123456789012:role/DAXRole",
				"ReplicationFactor": 1,
				"Tags": []map[string]string{
					{"Key": "env", "Value": "prod"},
					{"Key": "team", "Value": "platform"},
				},
			},
			wantStatus: http.StatusOK,
			checkTag:   "env",
			wantValue:  "prod",
		},
		{
			name: "empty tags array is accepted",
			body: map[string]any{
				"ClusterName":       "no-tags",
				"NodeType":          "dax.r5.large",
				"IamRoleArn":        "arn:aws:iam::123456789012:role/DAXRole",
				"ReplicationFactor": 1,
				"Tags":              []map[string]string{},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "tag key starting with aws: is rejected",
			body: map[string]any{
				"ClusterName":       "bad-tag",
				"NodeType":          "dax.r5.large",
				"IamRoleArn":        "arn:aws:iam::123456789012:role/DAXRole",
				"ReplicationFactor": 1,
				"Tags":              []map[string]string{{"Key": "aws:reserved", "Value": "v"}},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "tag key empty is rejected",
			body: map[string]any{
				"ClusterName":       "bad-key",
				"NodeType":          "dax.r5.large",
				"IamRoleArn":        "arn:aws:iam::123456789012:role/DAXRole",
				"ReplicationFactor": 1,
				"Tags":              []map[string]string{{"Key": "", "Value": "v"}},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := daxRequest(t, h, "CreateCluster", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK && tt.checkTag != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				cluster := resp["Cluster"].(map[string]any)
				// Verify tags are present on the returned cluster
				rawTags := cluster["Tags"].([]any)
				found := false
				for _, rt := range rawTags {
					tag := rt.(map[string]any)
					if tag["Key"].(string) == tt.checkTag {
						assert.Equal(t, tt.wantValue, tag["Value"].(string))
						found = true
					}
				}
				assert.True(t, found, "tag %q not found in cluster response", tt.checkTag)
			}
		})
	}
}

// TestHandlerTagResourceValidation verifies that TagResource enforces AWS tag constraints.
func TestHandlerTagResourceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode string
		tags     []map[string]string
		wantOK   bool
	}{
		{
			name:   "valid tags accepted",
			tags:   []map[string]string{{"Key": "env", "Value": "prod"}},
			wantOK: true,
		},
		{
			name:     "key starts with aws: rejected",
			tags:     []map[string]string{{"Key": "aws:reserved", "Value": "v"}},
			wantCode: "InvalidParameterValueException",
		},
		{
			name:     "key too long rejected",
			tags:     []map[string]string{{"Key": strings.Repeat("k", 129), "Value": "v"}},
			wantCode: "InvalidParameterValueException",
		},
		{
			name:     "empty key rejected",
			tags:     []map[string]string{{"Key": "", "Value": "v"}},
			wantCode: "InvalidParameterValueException",
		},
		{
			name:     "value too long rejected",
			tags:     []map[string]string{{"Key": "k", "Value": strings.Repeat("v", 257)}},
			wantCode: "InvalidParameterValueException",
		},
		{
			name:   "value exactly 256 chars accepted",
			tags:   []map[string]string{{"Key": "k", "Value": strings.Repeat("v", 256)}},
			wantOK: true,
		},
		{
			name:   "key exactly 128 chars accepted",
			tags:   []map[string]string{{"Key": strings.Repeat("k", 128), "Value": "v"}},
			wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			createRec := daxRequest(t, h, "CreateCluster", validClusterBody("tag-val-cluster"))
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			arn := createResp["Cluster"].(map[string]any)["ClusterArn"].(string)

			rec := daxRequest(t, h, "TagResource", map[string]any{
				"ResourceName": arn,
				"Tags":         tt.tags,
			})

			if tt.wantOK {
				assert.Equal(t, http.StatusOK, rec.Code)
			} else {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantCode, errResp["__type"])
			}
		})
	}
}

// TestHandlerTagResourceQuotaExceeded verifies that adding tags beyond the 50-tag limit returns
// TagQuotaPerResourceExceeded, matching AWS behavior.
func TestHandlerTagResourceQuotaExceeded(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	createRec := daxRequest(t, h, "CreateCluster", validClusterBody("quota-cluster"))
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	arn := createResp["Cluster"].(map[string]any)["ClusterArn"].(string)

	// Add 50 tags in two batches of 25.
	firstBatch := make([]map[string]string, 25)
	for i := range firstBatch {
		firstBatch[i] = map[string]string{"Key": strings.Repeat("a", i+1), "Value": "v"}
	}

	rec := daxRequest(t, h, "TagResource", map[string]any{"ResourceName": arn, "Tags": firstBatch})
	require.Equal(t, http.StatusOK, rec.Code)

	secondBatch := make([]map[string]string, 25)
	for i := range secondBatch {
		secondBatch[i] = map[string]string{"Key": strings.Repeat("b", i+1), "Value": "v"}
	}

	rec = daxRequest(t, h, "TagResource", map[string]any{"ResourceName": arn, "Tags": secondBatch})
	require.Equal(t, http.StatusOK, rec.Code)

	// 51st unique tag must be rejected.
	rec = daxRequest(t, h, "TagResource", map[string]any{
		"ResourceName": arn,
		"Tags":         []map[string]string{{"Key": "overflow", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "TagQuotaPerResourceExceeded", errResp["__type"])
}

// TestHandlerTagResourceReturnsAllTags verifies that TagResource returns the complete tag set on
// the resource after the operation, not just the tags that were added.
func TestHandlerTagResourceReturnsAllTags(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	createRec := daxRequest(t, h, "CreateCluster", validClusterBody("return-tags"))
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	arn := createResp["Cluster"].(map[string]any)["ClusterArn"].(string)

	// Add first tag.
	rec1 := daxRequest(t, h, "TagResource", map[string]any{
		"ResourceName": arn,
		"Tags":         []map[string]string{{"Key": "k1", "Value": "v1"}},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	// Add second tag — response must include both tags.
	rec2 := daxRequest(t, h, "TagResource", map[string]any{
		"ResourceName": arn,
		"Tags":         []map[string]string{{"Key": "k2", "Value": "v2"}},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	rawTags := resp["Tags"].([]any)
	assert.Len(t, rawTags, 2, "TagResource must return all tags on the resource, not just added ones")

	keys := make(map[string]string)
	for _, rt := range rawTags {
		tag := rt.(map[string]any)
		keys[tag["Key"].(string)] = tag["Value"].(string)
	}

	assert.Equal(t, "v1", keys["k1"])
	assert.Equal(t, "v2", keys["k2"])
}

// TestHandlerUntagResourceReturnsRemainingTags verifies that UntagResource returns the remaining
// tags after removal, matching AWS DAX behavior.
func TestHandlerUntagResourceReturnsRemainingTags(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	createRec := daxRequest(t, h, "CreateCluster", validClusterBody("untag-return"))
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	arn := createResp["Cluster"].(map[string]any)["ClusterArn"].(string)

	tagRec := daxRequest(t, h, "TagResource", map[string]any{
		"ResourceName": arn,
		"Tags":         []map[string]string{{"Key": "keep", "Value": "yes"}, {"Key": "remove", "Value": "no"}},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	untagRec := daxRequest(t, h, "UntagResource", map[string]any{
		"ResourceName": arn,
		"TagKeys":      []string{"remove"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(untagRec.Body.Bytes(), &resp))

	rawTags, ok := resp["Tags"].([]any)
	require.True(t, ok, "UntagResource must return Tags in response")
	assert.Len(t, rawTags, 1)

	tag := rawTags[0].(map[string]any)
	assert.Equal(t, "keep", tag["Key"])
	assert.Equal(t, "yes", tag["Value"])
}
