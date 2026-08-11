package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/datasync"
)

func TestDataSync_LocationS3(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *datasync.Handler)
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateLocationS3 returns LocationArn",
			action: "CreateLocationS3",
			body: map[string]any{
				"S3BucketArn":  "arn:aws:s3:::my-bucket",
				"Subdirectory": "/prefix",
				"S3Config": map[string]any{
					"BucketAccessRoleArn": "arn:aws:iam::000000000000:role/Role",
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["LocationArn"], "arn:aws:datasync:us-east-1:000000000000:location/")
			},
		},
		{
			name:   "CreateLocationS3 missing S3BucketArn returns 400",
			action: "CreateLocationS3",
			body: map[string]any{
				"S3Config": map[string]any{
					"BucketAccessRoleArn": "arn:aws:iam::000000000000:role/Role",
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CreateLocationS3 missing S3Config returns 400",
			action:   "CreateLocationS3",
			body:     map[string]any{"S3BucketArn": "arn:aws:s3:::my-bucket"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DescribeLocationS3 unknown ARN returns 404",
			action:   "DescribeLocationS3",
			body:     map[string]any{"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteLocation unknown ARN returns 404",
			action:   "DeleteLocation",
			body:     map[string]any{"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "ListLocations empty returns empty list",
			action:   "ListLocations",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				locs, ok := resp["Locations"].([]any)
				require.True(t, ok)
				assert.Empty(t, locs)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}

			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestDataSync_LocationS3CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	locationArn := createTestLocationS3(t, h)
	assert.Equal(t, 1, datasync.LocationCount(h.Backend.(*datasync.InMemoryBackend)))

	// Describe
	rec := doRequest(t, h, "DescribeLocationS3", map[string]any{"LocationArn": locationArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Contains(t, descResp["LocationUri"].(string), "s3://my-test-bucket/")

	// List
	rec = doRequest(t, h, "ListLocations", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["Locations"], 1)

	// Delete
	rec = doRequest(t, h, "DeleteLocation", map[string]any{"LocationArn": locationArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, datasync.LocationCount(h.Backend.(*datasync.InMemoryBackend)))
}

func TestDataSync_UpdateLocationS3(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	locArn := createTestLocationS3(t, h)

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "update subdirectory and storage class",
			body: map[string]any{
				"LocationArn":    locArn,
				"Subdirectory":   "/new-subdir",
				"S3StorageClass": "STANDARD_IA",
				"S3Config":       map[string]any{"BucketAccessRoleArn": "arn:aws:iam::000000000000:role/NewRole"},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing LocationArn returns 400",
			body:     map[string]any{"Subdirectory": "/x"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "not found returns 404",
			body: map[string]any{
				"LocationArn": "arn:aws:datasync:us-east-1:000000000000:location/notexist",
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, "UpdateLocationS3", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestDataSync_LocationS3AgentArns covers the Outposts AgentArns field on
// CreateLocationS3/DescribeLocationS3Output, and confirms the real wire shape
// (AgentArns present, S3BucketArn/Subdirectory absent -- see
// describeLocationS3Output's doc comment).
func TestDataSync_LocationS3AgentArns(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	agentArn := createTestAgent(t, h)

	rec := doRequest(t, h, "CreateLocationS3", map[string]any{
		"S3BucketArn":  "arn:aws:s3:::outposts-bucket",
		"Subdirectory": "/data",
		"S3Config": map[string]any{
			"BucketAccessRoleArn": "arn:aws:iam::000000000000:role/Role",
		},
		"AgentArns": []string{agentArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn := createResp["LocationArn"].(string)

	rec = doRequest(t, h, "DescribeLocationS3", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	agentArns, ok := descResp["AgentArns"].([]any)
	require.True(t, ok)
	require.Len(t, agentArns, 1)
	assert.Equal(t, agentArn, agentArns[0])

	// Real DescribeLocationS3Output has neither S3BucketArn nor Subdirectory.
	assert.Nil(t, descResp["S3BucketArn"])
	assert.Nil(t, descResp["Subdirectory"])
}
