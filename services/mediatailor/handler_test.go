package mediatailor_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// --- shared test helpers ---

func newTestHandler(t *testing.T) *mediatailor.Handler {
	t.Helper()

	backend := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")

	return mediatailor.NewHandler(backend)
}

func doRequest(t *testing.T, h *mediatailor.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	if body != nil {
		req.ContentLength = int64(len(bodyBytes))
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// doRequestWithQuery is like doRequest but with a query string appended.
// Every call site issues a bodyless request (GET/DELETE with query
// parameters), so unlike doRequest this helper has no body parameter.
func doRequestWithQuery(
	t *testing.T,
	h *mediatailor.Handler,
	method, path, query string,
) *httptest.ResponseRecorder {
	t.Helper()

	fullPath := path
	if query != "" {
		fullPath = path + "?" + query
	}

	return doRequest(t, h, method, fullPath, nil)
}

// makeEchoContext creates an echo.Context for path-based dispatch testing.
func makeEchoContext(t *testing.T, method, path string) *echo.Context {
	t.Helper()

	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	return c
}

func createTestSourceLocation(t *testing.T, h *mediatailor.Handler) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1", map[string]any{
		"HttpConfiguration": map[string]any{"BaseUrl": "https://example.com"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createTestChannel(t *testing.T, h *mediatailor.Handler) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{"PlaybackMode": "LOOP"})
	require.Equal(t, http.StatusOK, rec.Code)
}

// testScheduleConfig returns a minimal, valid ScheduleConfiguration for
// CreateProgram: an ABSOLUTE transition starting at startMillis. Every
// backend/HTTP test that creates a program needs one, since real
// CreateProgramInput requires ScheduleConfiguration.Transition (both Type
// and RelativePosition are required smithy members, even though
// RelativePosition is meaningless for an ABSOLUTE transition).
func testScheduleConfig(startMillis int64) *mediatailor.ScheduleConfiguration {
	return &mediatailor.ScheduleConfiguration{
		Transition: mediatailor.Transition{
			Type:                     "ABSOLUTE",
			RelativePosition:         "AFTER_PROGRAM",
			ScheduledStartTimeMillis: startMillis,
			DurationMillis:           30000,
		},
	}
}

// testScheduleConfigBody is testScheduleConfig's JSON-body equivalent, for
// HTTP-level tests that POST to /channel/{ch}/program/{name}.
func testScheduleConfigBody(startMillis int64) map[string]any {
	return map[string]any{
		"ScheduleConfiguration": map[string]any{
			"Transition": map[string]any{
				"Type":                     "ABSOLUTE",
				"RelativePosition":         "AFTER_PROGRAM",
				"ScheduledStartTimeMillis": float64(startMillis),
				"DurationMillis":           float64(30000),
			},
		},
	}
}

func createTestPlaybackConfig(t *testing.T, h *mediatailor.Handler, name string) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
		"Name":                  name,
		"AdDecisionServerUrl":   "https://ads.example.com",
		"VideoContentSourceUrl": "https://video.example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// --- dispatch / routing metadata ---

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "MediaTailor", h.Name())
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
		"Name": "to-be-reset",
	})
	h.Reset()
	rec := doRequest(t, h, http.MethodGet, "/playbackConfiguration/to-be-reset", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.NotZero(t, h.MatchPriority())
}

func TestExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{name: "list alerts", method: http.MethodGet, path: "/alerts", wantOp: "ListAlerts"},
		{
			name:   "configure logs channel",
			method: http.MethodPut,
			path:   "/configureLogs/channel",
			wantOp: "ConfigureLogsForChannel",
		},
		{
			name:   "configure logs playback",
			method: http.MethodPut,
			path:   "/configureLogs/playbackConfiguration",
			wantOp: "ConfigureLogsForPlaybackConfiguration",
		},
		{
			name:   "tag path get",
			method: http.MethodGet,
			path:   "/tags/arn:aws:mediatailor:us-east-1:000000000000:playbackConfiguration/x",
			wantOp: "ListTagsForResource",
		},
		{
			name:   "tag path post",
			method: http.MethodPost,
			path:   "/tags/arn:aws:mediatailor:us-east-1:000000000000:playbackConfiguration/x",
			wantOp: "TagResource",
		},
		{
			name:   "tag path delete",
			method: http.MethodDelete,
			path:   "/tags/arn:aws:mediatailor:us-east-1:000000000000:playbackConfiguration/x",
			wantOp: "UntagResource",
		},
		{name: "list functions", method: http.MethodGet, path: "/functions", wantOp: "ListFunctions"},
		{name: "put function", method: http.MethodPut, path: "/function/fn1", wantOp: "PutFunction"},
		{name: "get function", method: http.MethodGet, path: "/function/fn1", wantOp: "GetFunction"},
		{name: "delete function", method: http.MethodDelete, path: "/function/fn1", wantOp: "DeleteFunction"},
		{name: "list channels", method: http.MethodGet, path: "/channels", wantOp: "ListChannels"},
		{name: "create channel", method: http.MethodPost, path: "/channel/ch1", wantOp: "CreateChannel"},
		{name: "update channel", method: http.MethodPut, path: "/channel/ch1", wantOp: "UpdateChannel"},
		{name: "describe channel", method: http.MethodGet, path: "/channel/ch1", wantOp: "DescribeChannel"},
		{name: "delete channel", method: http.MethodDelete, path: "/channel/ch1", wantOp: "DeleteChannel"},
		{name: "start channel", method: http.MethodPut, path: "/channel/ch1/start", wantOp: "StartChannel"},
		{name: "stop channel", method: http.MethodPut, path: "/channel/ch1/stop", wantOp: "StopChannel"},
		{
			name:   "get channel schedule",
			method: http.MethodGet,
			path:   "/channel/ch1/schedule",
			wantOp: "GetChannelSchedule",
		},
		{name: "put channel policy", method: http.MethodPut, path: "/channel/ch1/policy", wantOp: "PutChannelPolicy"},
		{name: "get channel policy", method: http.MethodGet, path: "/channel/ch1/policy", wantOp: "GetChannelPolicy"},
		{
			name:   "delete channel policy",
			method: http.MethodDelete,
			path:   "/channel/ch1/policy",
			wantOp: "DeleteChannelPolicy",
		},
		{name: "create program", method: http.MethodPost, path: "/channel/ch1/program/prog1", wantOp: "CreateProgram"},
		{
			name:   "describe program",
			method: http.MethodGet,
			path:   "/channel/ch1/program/prog1",
			wantOp: "DescribeProgram",
		},
		{name: "update program", method: http.MethodPut, path: "/channel/ch1/program/prog1", wantOp: "UpdateProgram"},
		{
			name:   "delete program",
			method: http.MethodDelete,
			path:   "/channel/ch1/program/prog1",
			wantOp: "DeleteProgram",
		},
		{
			name:   "list source locations",
			method: http.MethodGet,
			path:   "/sourceLocations",
			wantOp: "ListSourceLocations",
		},
		{
			name:   "create source location",
			method: http.MethodPost,
			path:   "/sourceLocation/sl1",
			wantOp: "CreateSourceLocation",
		},
		{
			name:   "describe source location",
			method: http.MethodGet,
			path:   "/sourceLocation/sl1",
			wantOp: "DescribeSourceLocation",
		},
		{
			name:   "update source location",
			method: http.MethodPut,
			path:   "/sourceLocation/sl1",
			wantOp: "UpdateSourceLocation",
		},
		{
			name:   "delete source location",
			method: http.MethodDelete,
			path:   "/sourceLocation/sl1",
			wantOp: "DeleteSourceLocation",
		},
		{
			name:   "list vod sources",
			method: http.MethodGet,
			path:   "/sourceLocation/sl1/vodSources",
			wantOp: "ListVodSources",
		},
		{
			name:   "create vod source",
			method: http.MethodPost,
			path:   "/sourceLocation/sl1/vodSource/vs1",
			wantOp: "CreateVodSource",
		},
		{
			name:   "describe vod source",
			method: http.MethodGet,
			path:   "/sourceLocation/sl1/vodSource/vs1",
			wantOp: "DescribeVodSource",
		},
		{
			name:   "update vod source",
			method: http.MethodPut,
			path:   "/sourceLocation/sl1/vodSource/vs1",
			wantOp: "UpdateVodSource",
		},
		{
			name:   "delete vod source",
			method: http.MethodDelete,
			path:   "/sourceLocation/sl1/vodSource/vs1",
			wantOp: "DeleteVodSource",
		},
		{
			name:   "list live sources",
			method: http.MethodGet,
			path:   "/sourceLocation/sl1/liveSources",
			wantOp: "ListLiveSources",
		},
		{
			name:   "create live source",
			method: http.MethodPost,
			path:   "/sourceLocation/sl1/liveSource/ls1",
			wantOp: "CreateLiveSource",
		},
		{
			name:   "describe live source",
			method: http.MethodGet,
			path:   "/sourceLocation/sl1/liveSource/ls1",
			wantOp: "DescribeLiveSource",
		},
		{
			name:   "update live source",
			method: http.MethodPut,
			path:   "/sourceLocation/sl1/liveSource/ls1",
			wantOp: "UpdateLiveSource",
		},
		{
			name:   "delete live source",
			method: http.MethodDelete,
			path:   "/sourceLocation/sl1/liveSource/ls1",
			wantOp: "DeleteLiveSource",
		},
		{
			name:   "list prefetch schedules",
			method: http.MethodPost,
			path:   "/prefetchSchedule/pc1",
			wantOp: "ListPrefetchSchedules",
		},
		{
			name:   "create prefetch schedule",
			method: http.MethodPost,
			path:   "/prefetchSchedule/pc1/sched1",
			wantOp: "CreatePrefetchSchedule",
		},
		{
			name:   "get prefetch schedule",
			method: http.MethodGet,
			path:   "/prefetchSchedule/pc1/sched1",
			wantOp: "GetPrefetchSchedule",
		},
		{
			name:   "delete prefetch schedule",
			method: http.MethodDelete,
			path:   "/prefetchSchedule/pc1/sched1",
			wantOp: "DeletePrefetchSchedule",
		},
		{name: "unknown path", method: http.MethodGet, path: "/unknown/path", wantOp: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			op := h.ExtractOperation(makeEchoContext(t, tt.method, tt.path))
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		service string
		want    bool
	}{
		{name: "playbackConfiguration matches", path: "/playbackConfiguration", want: true},
		{name: "playbackConfiguration sub matches", path: "/playbackConfiguration/my-cfg", want: true},
		{name: "playbackConfigurations matches", path: "/playbackConfigurations", want: true},
		{name: "channels matches", path: "/channels", service: "mediatailor", want: true},
		{name: "channels without mediatailor service does not match", path: "/channels", want: false},
		{name: "channel sub matches", path: "/channel/ch1", want: true},
		{name: "sourceLocations matches", path: "/sourceLocations", want: true},
		{name: "sourceLocation sub matches", path: "/sourceLocation/sl1", want: true},
		{name: "prefetchSchedule matches", path: "/prefetchSchedule/pc1", want: true},
		{name: "functions matches", path: "/functions", want: true},
		{name: "function sub matches", path: "/function/fn1", want: true},
		{name: "alerts matches", path: "/alerts", want: true},
		{name: "configureLogs sub matches", path: "/configureLogs/channel", want: true},
		{
			name: "mediatailor tag path matches",
			path: "/tags/arn:aws:mediatailor:us-east-1:000000000000:playbackConfiguration/x",
			want: true,
		},
		{
			name: "non-mediatailor tag path does not match",
			path: "/tags/arn:aws:fis:us-east-1:000000000000:experiment/x",
			want: false,
		},
		{name: "unknown path does not match", path: "/unknown", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			matcher := h.RouteMatcher()
			c := makeEchoContext(t, http.MethodGet, tt.path)

			if tt.service != "" {
				c.Request().Header.Set(
					"Authorization",
					"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-east-1/"+tt.service+"/aws4_request",
				)
			}

			got := matcher(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandleUnknownPath_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		method   string
		wantCode int
	}{
		{
			name:     "unknown path returns 404",
			path:     "/unknown/route",
			method:   http.MethodGet,
			wantCode: http.StatusNotFound,
		},
		{
			name:     "configureLogs with unknown suffix returns 404",
			path:     "/configureLogs/unknown",
			method:   http.MethodPut,
			wantCode: http.StatusNotFound,
		},
		{
			name:     "tag path with non-mediatailor arn returns 404",
			path:     "/tags/arn:aws:fis:us-east-1:000:experiment/x",
			method:   http.MethodGet,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandleInvalidJSONBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Send invalid JSON with non-zero content length.
	req := httptest.NewRequest(http.MethodPut, "/playbackConfiguration",
		bytes.NewReader([]byte("not-valid-json{{{")))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = 17

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- cross-cutting wire-format behavior ---

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

		startMillis := int64(1_700_000_000_000)
		for i, name := range []string{"prog-a", "prog-b"} {
			progBody := testScheduleConfigBody(startMillis + int64(i)*60_000)
			progBody["SourceLocationName"] = "sched-sl"
			progBody["VodSourceName"] = "sched-vs"

			progRec := doRequest(t, h, http.MethodPost, "/channel/sched-ch/program/"+name, progBody)
			require.Equal(t, http.StatusOK, progRec.Code, progRec.Body.String())
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
