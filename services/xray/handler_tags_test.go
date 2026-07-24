package xray_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

// createTaggableGroup creates a real group via the handler and returns its ARN, so
// tag tests exercise TagResource/UntagResource/ListTagsForResource against a resource
// ARN that actually resolves -- real AWS returns ResourceNotFoundException for tag
// operations against an ARN that isn't a known group or sampling rule.
func createTaggableGroup(t *testing.T, h *xray.Handler, name string) string {
	t.Helper()

	rec := doXrayRequest(t, h, "/CreateGroup", map[string]any{"GroupName": name})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	group, ok := resp["Group"].(map[string]any)
	require.True(t, ok)

	arn, ok := group["GroupARN"].(string)
	require.True(t, ok)
	require.NotEmpty(t, arn)

	return arn
}

func TestHandler_ListTagsForResource_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		explicitARN    string
		wantStatus     int
		wantTags       int
		explicitStatus int
		useKnownGroup  bool
	}{
		{
			name:       "missing ResourceARN rejected",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:          "resource with no tags returns empty list",
			useKnownGroup: true,
			wantStatus:    http.StatusOK,
			wantTags:      0,
		},
		{
			name:        "unknown resource ARN returns 400 ResourceNotFoundException",
			explicitARN: "arn:aws:xray:us-east-1:123:group/default/does-not-exist",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := map[string]any{}

			switch {
			case tt.useKnownGroup:
				body["ResourceARN"] = createTaggableGroup(t, h, "g1")
			case tt.explicitARN != "":
				body["ResourceARN"] = tt.explicitARN
			}

			rec := doXrayRequest(t, h, "/ListTagsForResource", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tags, _ := resp["Tags"].([]any)
				assert.Len(t, tags, tt.wantTags)
				assert.Contains(t, resp, "NextToken")
			}
		})
	}
}

func TestHandler_ListTagsForResource_WithTags(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	arn := createTaggableGroup(t, h, "tagged")
	tags := map[string]string{
		"env":     "prod",
		"team":    "platform",
		"version": "v1",
		"owner":   "alice",
		"cost":    "high",
	}
	require.NoError(t, b.TagResource(arn, tags))

	rec := doXrayRequest(t, h, "/ListTagsForResource", map[string]any{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tagsList, _ := resp["Tags"].([]any)
	assert.Len(t, tagsList, len(tags))

	// Each tag should be a map with Key and Value
	for _, tagAny := range tagsList {
		tagMap, ok := tagAny.(map[string]any)
		require.True(t, ok)
		assert.Contains(t, tagMap, "Key")
		assert.Contains(t, tagMap, "Value")
	}
}

func TestTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{
			name: "single tag",
			tags: map[string]string{"env": "prod"},
		},
		{
			name: "multiple tags",
			tags: map[string]string{"env": "prod", "team": "platform", "cost-center": "eng"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := createTaggableGroup(t, h, "test-group")

			tagRec := doXrayRequest(t, h, "/TagResource", map[string]any{
				"ResourceARN": arn,
				"Tags":        tt.tags,
			})
			require.Equal(t, http.StatusOK, tagRec.Code)

			listRec := doXrayRequest(t, h, "/ListTagsForResource", map[string]any{
				"ResourceARN": arn,
			})
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

			tags, ok := resp["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tags, len(tt.tags))
		})
	}
}

func TestTags_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := createTaggableGroup(t, h, "untag-group")

	tagRec := doXrayRequest(t, h, "/TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags":        map[string]string{"key1": "val1", "key2": "val2"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	untagRec := doXrayRequest(t, h, "/UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []string{"key1"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	listRec := doXrayRequest(t, h, "/ListTagsForResource", map[string]any{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	tags, _ := resp["Tags"].([]any)
	assert.Len(t, tags, 1, "only key2 should remain after untagging key1")
}

func TestTags_TagResource_UnknownResourceReturns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/TagResource", map[string]any{
		"ResourceARN": "arn:aws:xray:us-east-1:000000000000:group/default/nope",
		"Tags":        map[string]string{"env": "prod"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

func TestTags_UntagResource_UnknownResourceReturns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/UntagResource", map[string]any{
		"ResourceARN": "arn:aws:xray:us-east-1:000000000000:group/default/nope",
		"TagKeys":     []string{"env"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

func TestTags_TagResource_ExceedsMaxTagsReturns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := createTaggableGroup(t, h, "many-tags")

	tags := make(map[string]string, 51)
	for i := range 51 {
		tags["k"+strconv.Itoa(i)] = "v"
	}

	rec := doXrayRequest(t, h, "/TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags":        tags,
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "TooManyTagsException", resp["__type"])
}
