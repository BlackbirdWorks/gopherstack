package mediapackage_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

// pa1Handler builds a fresh handler for each parity test.
func pa1Handler(t *testing.T) *mediapackage.Handler {
	t.Helper()

	return mediapackage.NewHandler(mediapackage.NewInMemoryBackend("000000000000", "us-east-1"))
}

// pa1Do sends a request and returns the recorder.
func pa1Do(t *testing.T, h *mediapackage.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()

	rec := doRequest(t, h, method, path, body)

	if rec.Body.Len() == 0 {
		return rec.Code, nil
	}

	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)

	return rec.Code, out
}

// pa1CreateChannel creates a channel and returns its ID and ARN.
func pa1CreateChannel(t *testing.T, h *mediapackage.Handler, id string, tags map[string]any) (string, string) {
	t.Helper()

	body := map[string]any{"id": id}
	if tags != nil {
		body["tags"] = tags
	}

	code, resp := pa1Do(t, h, http.MethodPost, "/channels", body)
	require.Equal(t, http.StatusCreated, code)

	return resp["id"].(string), resp["arn"].(string)
}

// pa1CreateEndpoint creates an origin endpoint and returns its ARN.
func pa1CreateEndpoint(t *testing.T, h *mediapackage.Handler, channelID, epID string, tags map[string]any) string {
	t.Helper()

	body := map[string]any{"channelId": channelID, "id": epID}
	if tags != nil {
		body["tags"] = tags
	}

	code, resp := pa1Do(t, h, http.MethodPost, "/origin_endpoints", body)
	require.Equal(t, http.StatusCreated, code)

	return resp["arn"].(string)
}

// TestParity_Tags_CreatedAtChannelCreation verifies that tags supplied at channel
// creation are returned by ListTagsForResource (not just DescribeChannel).
func TestParity_Tags_CreatedAtChannelCreation(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // test-only struct; layout not performance-critical
		creationTags map[string]any
		name         string
		wantTags     map[string]string
	}{
		{
			name:         "tags at creation visible via ListTagsForResource",
			creationTags: map[string]any{"env": "prod", "team": "video"},
			wantTags:     map[string]string{"env": "prod", "team": "video"},
		},
		{
			name:         "no tags at creation returns empty map",
			creationTags: nil,
			wantTags:     map[string]string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			_, arn := pa1CreateChannel(t, h, "ch-tag-create", tc.creationTags)

			code, resp := pa1Do(t, h, http.MethodGet, "/tags/"+arn, nil)
			require.Equal(t, http.StatusOK, code)

			tags, ok := resp["tags"].(map[string]any)
			require.True(t, ok)

			got := make(map[string]string, len(tags))
			for k, v := range tags {
				got[k] = v.(string)
			}

			assert.Equal(t, tc.wantTags, got)
		})
	}
}

// TestParity_Tags_TagResourceReflectsInDescribeChannel verifies that tags added
// via TagResource appear in subsequent DescribeChannel responses.
func TestParity_Tags_TagResourceReflectsInDescribeChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addTags map[string]any
		name    string
		wantKey string
		wantVal string
	}{
		{
			name:    "TagResource tag visible in DescribeChannel",
			addTags: map[string]any{"env": "staging"},
			wantKey: "env",
			wantVal: "staging",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			_, arn := pa1CreateChannel(t, h, "ch-tag-describe", nil)

			// Add tag via TagResource
			code, _ := pa1Do(t, h, http.MethodPost, "/tags/"+arn, map[string]any{"tags": tc.addTags})
			require.Equal(t, http.StatusNoContent, code)

			// Check DescribeChannel shows the tag
			code, resp := pa1Do(t, h, http.MethodGet, "/channels/ch-tag-describe", nil)
			require.Equal(t, http.StatusOK, code)

			tags, ok := resp["tags"].(map[string]any)
			require.True(t, ok, "tags field should be present in DescribeChannel")
			assert.Equal(t, tc.wantVal, tags[tc.wantKey])
		})
	}
}

// TestParity_Tags_UntagResourceReflectsInDescribeChannel verifies that tags removed
// via UntagResource no longer appear in DescribeChannel.
func TestParity_Tags_UntagResourceReflectsInDescribeChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		initialTags map[string]any
		removeKey   string
		keepKey     string
	}{
		{
			name:        "UntagResource removes tag from DescribeChannel",
			initialTags: map[string]any{"env": "prod", "team": "video"},
			removeKey:   "env",
			keepKey:     "team",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			_, arn := pa1CreateChannel(t, h, "ch-untag-describe", tc.initialTags)

			// Remove one tag
			code, _ := pa1Do(t, h, http.MethodDelete, "/tags/"+arn+"?tagKeys="+tc.removeKey, nil)
			require.Equal(t, http.StatusNoContent, code)

			// Check DescribeChannel no longer shows removed tag
			code, resp := pa1Do(t, h, http.MethodGet, "/channels/ch-untag-describe", nil)
			require.Equal(t, http.StatusOK, code)

			tags, ok := resp["tags"].(map[string]any)
			require.True(t, ok)
			assert.NotContains(t, tags, tc.removeKey, "removed tag should not appear in DescribeChannel")
			assert.Contains(t, tags, tc.keepKey, "retained tag should still appear in DescribeChannel")
		})
	}
}

// TestParity_Tags_OriginEndpointCreationTags verifies that tags supplied at
// origin endpoint creation are returned by ListTagsForResource.
func TestParity_Tags_OriginEndpointCreationTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		creationTags map[string]any
		name         string
		wantCount    int
	}{
		{
			name:         "endpoint creation tags visible via ListTagsForResource",
			creationTags: map[string]any{"tier": "premium"},
			wantCount:    1,
		},
		{
			name:         "no endpoint creation tags returns empty",
			creationTags: nil,
			wantCount:    0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			chID, _ := pa1CreateChannel(t, h, "ch-ep-tags", nil)
			epARN := pa1CreateEndpoint(t, h, chID, "ep-tags", tc.creationTags)

			code, resp := pa1Do(t, h, http.MethodGet, "/tags/"+epARN, nil)
			require.Equal(t, http.StatusOK, code)

			tags, ok := resp["tags"].(map[string]any)
			require.True(t, ok)
			assert.Len(t, tags, tc.wantCount)
		})
	}
}

// TestParity_NotFound_ErrorType verifies that not-found responses include
// the __type field used by the AWS SDK for error classification.
func TestParity_NotFound_ErrorType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "describe missing channel includes __type", method: http.MethodGet, path: "/channels/no-such"},
		{name: "describe missing endpoint includes __type", method: http.MethodGet, path: "/origin_endpoints/no-such"},
		{name: "describe missing harvest job includes __type", method: http.MethodGet, path: "/harvest_jobs/no-such"},
		{
			name:   "describe missing packaging config includes __type",
			method: http.MethodGet,
			path:   "/packaging_configurations/no-such",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			code, resp := pa1Do(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, code)
			require.NotNil(t, resp)
			assert.Contains(t, resp, "__type", "__type field required for SDK error classification")
			assert.Contains(t, resp["__type"], "NotFoundException")
		})
	}
}

// TestParity_LifecyclePolicy_NoPolicy verifies that GetChannelLifecyclePolicy
// returns 404 when no policy has been set.
func TestParity_LifecyclePolicy_NoPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "get lifecycle policy with no policy set returns 404", wantCode: http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			pa1CreateChannel(t, h, "ch-no-policy", nil)

			code, _ := pa1Do(t, h, http.MethodGet, "/channels/ch-no-policy/lifecycle_policy", nil)
			assert.Equal(t, tc.wantCode, code)
		})
	}
}

// TestParity_LifecyclePolicy_RoundTrip verifies the full put-then-get cycle.
func TestParity_LifecyclePolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy   string
		name     string
		wantCode int
	}{
		{
			name:     "put and get lifecycle policy round-trips the value",
			policy:   `{"rules":[{"retention":{"unit":"DAYS","value":30}}]}`,
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			pa1CreateChannel(t, h, "ch-lc-rt", nil)

			code, _ := pa1Do(
				t,
				h,
				http.MethodPut,
				"/channels/ch-lc-rt/lifecycle_policy",
				map[string]any{"policy": tc.policy},
			)
			require.Equal(t, http.StatusOK, code)

			code, resp := pa1Do(t, h, http.MethodGet, "/channels/ch-lc-rt/lifecycle_policy", nil)
			require.Equal(t, tc.wantCode, code)
			assert.Equal(t, tc.policy, resp["policy"])
		})
	}
}

// TestParity_Channel_DeleteReturns202 verifies the delete channel returns 202 Accepted.
func TestParity_Channel_DeleteReturns202(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "delete channel returns 202 Accepted", wantCode: http.StatusAccepted},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			pa1CreateChannel(t, h, "ch-del-202", nil)

			code, _ := pa1Do(t, h, http.MethodDelete, "/channels/ch-del-202", nil)
			assert.Equal(t, tc.wantCode, code)
		})
	}
}

// TestParity_OriginEndpoint_DeleteReturns202 verifies delete endpoint returns 202.
func TestParity_OriginEndpoint_DeleteReturns202(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "delete origin endpoint returns 202 Accepted", wantCode: http.StatusAccepted},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			chID, _ := pa1CreateChannel(t, h, "ch-ep-del", nil)
			pa1CreateEndpoint(t, h, chID, "ep-del-202", nil)

			code, _ := pa1Do(t, h, http.MethodDelete, "/origin_endpoints/ep-del-202", nil)
			assert.Equal(t, tc.wantCode, code)
		})
	}
}

// TestParity_PackagingConfig_DeleteReturns202 verifies delete packaging config returns 202.
func TestParity_PackagingConfig_DeleteReturns202(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "delete packaging config returns 202 Accepted", wantCode: http.StatusAccepted},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)

			code, _ := pa1Do(t, h, http.MethodPost, "/packaging_configurations", map[string]any{
				"id":               "pc-del",
				"packagingGroupId": "g1",
			})
			require.Equal(t, http.StatusCreated, code)

			code, _ = pa1Do(t, h, http.MethodDelete, "/packaging_configurations/pc-del", nil)
			assert.Equal(t, tc.wantCode, code)
		})
	}
}

// TestParity_Tags_ListChannelsIncludesTags verifies that ListChannels response
// includes tags set at creation time.
func TestParity_Tags_ListChannelsIncludesTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantTag string
		wantVal string
	}{
		{
			name:    "list channels response includes creation tags",
			wantTag: "env",
			wantVal: "prod",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			pa1CreateChannel(t, h, "ch-list-tags", map[string]any{"env": "prod"})

			code, resp := pa1Do(t, h, http.MethodGet, "/channels", nil)
			require.Equal(t, http.StatusOK, code)

			channels, ok := resp["channels"].([]any)
			require.True(t, ok)
			require.Len(t, channels, 1)

			ch := channels[0].(map[string]any)
			tags, ok := ch["tags"].(map[string]any)
			require.True(t, ok, "tags should be present in ListChannels entry")
			assert.Equal(t, tc.wantVal, tags[tc.wantTag])
		})
	}
}

// TestParity_Channel_ARNShape verifies ARN format matches AWS pattern.
func TestParity_Channel_ARNShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		channelID  string
		wantPrefix string
	}{
		{
			name:       "channel ARN has correct prefix and resource",
			channelID:  "my-live-ch",
			wantPrefix: "arn:aws:mediapackage:us-east-1:000000000000:channels/my-live-ch",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			_, arn := pa1CreateChannel(t, h, tc.channelID, nil)
			assert.Equal(t, tc.wantPrefix, arn)
		})
	}
}

// TestParity_OriginEndpoint_DefaultFields verifies default field values on create.
func TestParity_OriginEndpoint_DefaultFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantOrigination  string
		wantManifestName string
		epID             string
	}{
		{
			name:             "origination defaults to ALLOW",
			wantOrigination:  "ALLOW",
			wantManifestName: "ep-defaults",
			epID:             "ep-defaults",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			chID, _ := pa1CreateChannel(t, h, "ch-defaults", nil)

			code, resp := pa1Do(t, h, http.MethodPost, "/origin_endpoints", map[string]any{
				"channelId": chID,
				"id":        tc.epID,
			})
			require.Equal(t, http.StatusCreated, code)

			assert.Equal(t, tc.wantOrigination, resp["origination"])
			assert.Equal(t, tc.wantManifestName, resp["manifestName"])
		})
	}
}

// TestParity_HarvestJob_RequiredFields verifies that missing required fields
// return 422 rather than 500.
func TestParity_HarvestJob_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing id returns 422",
			body: map[string]any{
				"originEndpointId": "ep",
				"startTime":        "2024-01-01T00:00:00Z",
				"endTime":          "2024-01-02T00:00:00Z",
				"s3Destination":    map[string]any{"bucketName": "b", "manifestKey": "m", "roleArn": "r"},
			},
			wantCode: http.StatusUnprocessableEntity,
		},
		{
			name: "missing originEndpointId returns 422",
			body: map[string]any{
				"id":            "job1",
				"startTime":     "2024-01-01T00:00:00Z",
				"endTime":       "2024-01-02T00:00:00Z",
				"s3Destination": map[string]any{"bucketName": "b", "manifestKey": "m", "roleArn": "r"},
			},
			wantCode: http.StatusUnprocessableEntity,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			code, _ := pa1Do(t, h, http.MethodPost, "/harvest_jobs", tc.body)
			assert.Equal(t, tc.wantCode, code)
		})
	}
}
