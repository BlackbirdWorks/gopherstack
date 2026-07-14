package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_TagsWireKey verifies the JSON key carrying tags on the wire is
// lowercase "tags", not "Tags". The real MediaTailor restjson1 model gives
// every Tags member a "tags" locationName override (confirmed against
// aws-sdk-go-v2/service/mediatailor's (de)serializers and botocore's
// service-2.json); every other field stays PascalCase. Getting this wrong
// silently drops tags to/from a real SDK client since json.Unmarshal treats
// an unknown key as absent rather than erroring.
func TestHandler_TagsWireKey(t *testing.T) {
	t.Parallel()

	t.Run("CreateChannel request and response", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/channel/tagged-ch", map[string]any{
			"PlaybackMode": "LOOP",
			"tags":         map[string]any{"env": "prod"},
		})
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		// A PascalCase "Tags" key must NOT be how tags are surfaced.
		_, wrongKeyPresent := resp["Tags"]
		assert.False(t, wrongKeyPresent, "response must not use PascalCase Tags key")

		tags, ok := resp["tags"].(map[string]any)
		require.True(t, ok, "response must carry tags under lowercase 'tags' key")
		assert.Equal(t, "prod", tags["env"])
	})

	t.Run("PutPlaybackConfiguration request and response", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
			"Name": "tagged-cfg",
			"tags": map[string]any{"team": "media"},
		})
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		tags, ok := resp["tags"].(map[string]any)
		require.True(t, ok, "response must carry tags under lowercase 'tags' key")
		assert.Equal(t, "media", tags["team"])
	})

	t.Run("CreateSourceLocation request and response", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/sourceLocation/tagged-sl", map[string]any{
			"HttpConfiguration": map[string]any{"BaseUrl": "https://example.com"},
			"tags":              map[string]any{"owner": "video"},
		})
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		tags, ok := resp["tags"].(map[string]any)
		require.True(t, ok, "response must carry tags under lowercase 'tags' key")
		assert.Equal(t, "video", tags["owner"])
	})
}

// TestHandler_ChannelTimestamps_EpochSeconds verifies CreationTime and
// LastModifiedTime serialize as JSON numbers (epoch seconds), not RFC3339
// strings. The real restjson1 deserializer for these members is
// __timestampUnix; sending a string trips "expected __timestampUnix to be a
// JSON Number, got string instead" in a real SDK client.
func TestHandler_ChannelTimestamps_EpochSeconds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/channel/epoch-ch", map[string]any{
		"PlaybackMode": "LOOP",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	creationTime, ok := resp["CreationTime"].(float64)
	require.True(t, ok, "CreationTime must decode as a JSON number, got %T", resp["CreationTime"])
	assert.Positive(t, creationTime, "CreationTime must be a positive epoch-seconds value")

	lastModified, ok := resp["LastModifiedTime"].(float64)
	require.True(t, ok, "LastModifiedTime must decode as a JSON number, got %T", resp["LastModifiedTime"])
	assert.Positive(t, lastModified, "LastModifiedTime must be a positive epoch-seconds value")
}

// TestHandler_PaginationQueryParamCasing verifies MaxResults/NextToken are
// read regardless of casing. The real model binds them lowercase
// (maxResults/nextToken) for ListChannels, ListSourceLocations,
// ListVodSources, ListLiveSources, and GetChannelSchedule, but PascalCase
// (MaxResults/NextToken) for ListPlaybackConfigurations and ListFunctions
// (confirmed against aws-sdk-go-v2's httpbinding serializers and botocore's
// service-2.json). A handler that only checks one casing silently ignores
// pagination for the other set of operations.
func TestHandler_PaginationQueryParamCasing(t *testing.T) {
	t.Parallel()

	t.Run("ListChannels honors lowercase maxResults", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		for _, name := range []string{"ch-a", "ch-b", "ch-c"} {
			doRequest(t, h, http.MethodPost, "/channel/"+name, map[string]any{"PlaybackMode": "LOOP"})
		}

		rec := doRequestWithQuery(t, h, http.MethodGet, "/channels", "maxResults=1")
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		items, _ := resp["Items"].([]any)
		assert.Len(t, items, 1, "lowercase maxResults query param must limit the page size")
		assert.NotEmpty(t, resp["NextToken"], "a NextToken must be returned when more pages remain")
	})

	t.Run("ListPlaybackConfigurations honors PascalCase MaxResults", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		for _, name := range []string{"pc-a", "pc-b", "pc-c"} {
			doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{"Name": name})
		}

		rec := doRequestWithQuery(t, h, http.MethodGet, "/playbackConfigurations", "MaxResults=1")
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		items, _ := resp["Items"].([]any)
		assert.Len(t, items, 1, "PascalCase MaxResults query param must limit the page size")
		assert.NotEmpty(t, resp["NextToken"], "a NextToken must be returned when more pages remain")
	})

	t.Run("ListFunctions honors PascalCase MaxResults and actually paginates", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		for _, id := range []string{"fn-a", "fn-b", "fn-c"} {
			doRequest(t, h, http.MethodPut, "/function/"+id, map[string]any{"FunctionType": "AWS_LAMBDA"})
		}

		rec := doRequestWithQuery(t, h, http.MethodGet, "/functions", "MaxResults=1")
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		items, _ := resp["Items"].([]any)
		assert.Len(t, items, 1, "ListFunctions must actually paginate, not return every function")
		assert.NotEmpty(t, resp["NextToken"], "a NextToken must be returned when more pages remain")
	})

	t.Run("GetChannelSchedule honors lowercase maxResults and actually paginates", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/channel/sched-ch", map[string]any{"PlaybackMode": "LOOP"})
		doRequest(t, h, http.MethodPost, "/sourceLocation/sched-sl", map[string]any{
			"HttpConfiguration": map[string]any{"BaseUrl": "https://example.com"},
		})
		doRequest(t, h, http.MethodPost, "/sourceLocation/sched-sl/vodSource/sched-vs", map[string]any{
			"HttpPackageConfigurations": []any{
				map[string]any{"Path": "/", "SourceGroup": "hd", "Type": "HLS"},
			},
		})

		for _, name := range []string{"prog-a", "prog-b"} {
			doRequest(t, h, http.MethodPost, "/channel/sched-ch/program/"+name, map[string]any{
				"SourceLocationName": "sched-sl",
				"VodSourceName":      "sched-vs",
			})
		}

		rec := doRequestWithQuery(t, h, http.MethodGet, "/channel/sched-ch/schedule", "maxResults=1")
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		items, _ := resp["Items"].([]any)
		assert.Len(t, items, 1, "GetChannelSchedule must actually paginate, not return every program")
		assert.NotEmpty(t, resp["NextToken"], "a NextToken must be returned when more pages remain")
	})
}

// TestHandler_ListPrefetchSchedules_PostWithBodyPagination verifies
// ListPrefetchSchedules is routed as POST (not GET) on the bare
// /prefetchSchedule/{PlaybackConfigurationName} path, and that MaxResults/
// NextToken are read from the JSON request body rather than the query
// string — confirmed against aws-sdk-go-v2's serializer for
// ListPrefetchSchedulesInput.
func TestHandler_ListPrefetchSchedules_PostWithBodyPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{"Name": "pc1"})

	for _, name := range []string{"sched-a", "sched-b", "sched-c"} {
		doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1/"+name, nil)
	}

	// A GET on the list path must not be routed to ListPrefetchSchedules.
	getRec := doRequest(t, h, http.MethodGet, "/prefetchSchedule/pc1", nil)
	assert.NotEqual(t, http.StatusOK, getRec.Code, "GET must not serve ListPrefetchSchedules")

	rec := doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1", map[string]any{
		"MaxResults": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ := resp["Items"].([]any)
	assert.Len(t, items, 1, "body MaxResults must limit the page size")
	assert.NotEmpty(t, resp["NextToken"], "a NextToken must be returned when more pages remain")
}
