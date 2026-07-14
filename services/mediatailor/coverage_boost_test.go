package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// --- Handler metadata ---

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns MediaTailor", want: "MediaTailor"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			assert.Equal(t, tt.want, h.Name())
		})
	}
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset clears data"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			// Create some data then reset
			doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
				"Name": "to-be-reset",
			})
			h.Reset()
			rec := doRequest(t, h, http.MethodGet, "/playbackConfiguration/to-be-reset", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "returns non-zero priority"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			assert.NotZero(t, h.MatchPriority())
		})
	}
}

// --- Backend metadata ---

func TestBackend_AccountIDAndRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
	}{
		{name: "returns configured account and region", accountID: "123456789012", region: "us-west-2"},
		{name: "returns defaults", accountID: "000000000000", region: "us-east-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := mediatailor.NewInMemoryBackend(tt.accountID, tt.region)
			assert.Equal(t, tt.accountID, b.AccountID())
			assert.Equal(t, tt.region, b.Region())
		})
	}
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset clears all stored data"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.PutPlaybackConfiguration("test", "https://ads.com", "https://video.com", nil)
			require.NoError(t, err)
			b.Reset()
			assert.Equal(t, 0, mediatailor.PlaybackConfigurationCount(b))
		})
	}
}

func TestBackend_SnapshotAndRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "snapshot captures state and restore restores it"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := mediatailor.NewInMemoryBackend("111111111111", "eu-west-1")
			_, err := b.PutPlaybackConfiguration("snap-cfg", "https://ads.com", "https://video.com", nil)
			require.NoError(t, err)

			snap := b.Snapshot(t.Context())
			require.NotEmpty(t, snap)

			b2 := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
			err = b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			assert.Equal(t, "111111111111", b2.AccountID())
			assert.Equal(t, "eu-west-1", b2.Region())
			assert.Equal(t, 1, mediatailor.PlaybackConfigurationCount(b2))

			cfg, err := b2.GetPlaybackConfiguration("snap-cfg")
			require.NoError(t, err)
			assert.Equal(t, "snap-cfg", cfg.Name)
		})
	}
}

func TestBackend_RestoreInvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "restore with invalid JSON returns error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
			err := b.Restore(t.Context(), []byte("not json"))
			require.Error(t, err)
		})
	}
}

// --- Provider ---

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns MediaTailor", want: "MediaTailor"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := &mediatailor.Provider{}
			assert.Equal(t, tt.want, p.Name())
		})
	}
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ctx     any
		name    string
		wantErr bool
	}{
		{name: "nil context returns error", ctx: nil, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := &mediatailor.Provider{}
			_, err := p.Init(nil)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, mediatailor.ErrNilAppContext)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// --- Tags ---

func TestTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tagBody  map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "tag existing resource succeeds",
			wantCode: http.StatusNoContent,
			tagBody:  map[string]any{"tags": map[string]any{"env": "prod"}},
		},
		{
			name:     "tag any arn succeeds (backend does not validate existence)",
			wantCode: http.StatusNoContent,
			tagBody:  map[string]any{"tags": map[string]any{"env": "prod"}},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var arn string
			if i == 0 {
				// Create a playback config and tag it
				rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
					"Name":                  "taggable",
					"AdDecisionServerUrl":   "https://ads.com",
					"VideoContentSourceUrl": "https://video.com",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				arn, _ = resp["PlaybackConfigurationArn"].(string)
			} else {
				arn = "arn:aws:mediatailor:us-east-1:000000000000:playbackConfiguration/nonexistent"
			}

			rec := doRequest(t, h, http.MethodPost, "/tags/"+arn, tt.tagBody)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "list tags on existing resource returns tags", wantCode: http.StatusOK},
		{name: "list tags on any arn returns ok (empty tags)", wantCode: http.StatusOK},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var arn string
			if i == 0 {
				rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
					"Name":                  "list-tags-cfg",
					"AdDecisionServerUrl":   "https://ads.com",
					"VideoContentSourceUrl": "https://video.com",
					"tags":                  map[string]any{"key1": "val1"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				arn, _ = resp["PlaybackConfigurationArn"].(string)
			} else {
				arn = "arn:aws:mediatailor:us-east-1:000000000000:playbackConfiguration/nonexistent"
			}

			rec := doRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "untag existing resource succeeds", wantCode: http.StatusNoContent},
		{name: "untag non-existent resource is idempotent", wantCode: http.StatusNoContent},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var arn string
			if i == 0 {
				rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
					"Name":                  "untag-cfg",
					"AdDecisionServerUrl":   "https://ads.com",
					"VideoContentSourceUrl": "https://video.com",
					"tags":                  map[string]any{"key1": "val1"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				arn, _ = resp["PlaybackConfigurationArn"].(string)

				// Actually tag it first
				doRequest(t, h, http.MethodPost, "/tags/"+arn, map[string]any{
					"tags": map[string]any{"key1": "val1"},
				})
			} else {
				arn = "arn:aws:mediatailor:us-east-1:000000000000:playbackConfiguration/nonexistent"
			}

			// Use direct request builder to add query params
			rec := doRequestWithQuery(t, h, http.MethodDelete, "/tags/"+arn, "tagKeys=key1")
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
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

// --- classifyPath edge cases via ExtractOperation / ExtractResource ---

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

// makeEchoContext creates an echo.Context for path-based dispatch testing.
func makeEchoContext(t *testing.T, method, path string) *echo.Context {
	t.Helper()

	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	return c
}

// --- List operations with next-token branch (needs multiple items) ---

func TestListPlaybackConfigurations_WithNextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "list returns items"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for i := range 3 {
				name := "cfg-" + string(rune('a'+i))
				doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
					"Name":                  name,
					"AdDecisionServerUrl":   "https://ads.com",
					"VideoContentSourceUrl": "https://video.com",
				})
			}

			rec := doRequest(t, h, http.MethodGet, "/playbackConfigurations", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			items, _ := resp["Items"].([]any)
			assert.Len(t, items, 3)
		})
	}
}

func TestListChannels_WithItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "list channels returns items"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for i := range 2 {
				name := "ch-" + string(rune('a'+i))
				doRequest(t, h, http.MethodPost, "/channel/"+name, map[string]any{
					"PlaybackMode": "LINEAR",
					"Outputs": []any{
						map[string]any{
							"ManifestName": "manifest",
							"SourceGroup":  "default",
						},
					},
				})
			}

			rec := doRequest(t, h, http.MethodGet, "/channels", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			items, _ := resp["Items"].([]any)
			assert.Len(t, items, 2)
		})
	}
}

func TestListSourceLocations_WithItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "list source locations returns items"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for i := range 2 {
				name := "sl-" + string(rune('a'+i))
				doRequest(t, h, http.MethodPost, "/sourceLocation/"+name, map[string]any{
					"HttpConfiguration": map[string]any{"BaseUrl": "https://example.com"},
				})
			}

			rec := doRequest(t, h, http.MethodGet, "/sourceLocations", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			items, _ := resp["Items"].([]any)
			assert.Len(t, items, 2)
		})
	}
}

func TestListVodSources_WithItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "list vod sources returns items"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			createTestSourceLocation(t, h)

			for i := range 2 {
				name := "vs-" + string(rune('a'+i))
				doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/vodSource/"+name, map[string]any{
					"HttpPackageConfigurations": []any{
						map[string]any{"Path": "/hls", "SourceGroup": "default", "Type": "HLS"},
					},
				})
			}

			rec := doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/vodSources", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			items, _ := resp["Items"].([]any)
			assert.Len(t, items, 2)
		})
	}
}

func TestListLiveSources_WithItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "list live sources returns items"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			createTestSourceLocation(t, h)

			for i := range 2 {
				name := "ls-" + string(rune('a'+i))
				doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/liveSource/"+name, map[string]any{
					"HttpPackageConfigurations": []any{
						map[string]any{"Path": "/hls", "SourceGroup": "default", "Type": "HLS"},
					},
				})
			}

			rec := doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/liveSources", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			items, _ := resp["Items"].([]any)
			assert.Len(t, items, 2)
		})
	}
}

func TestListPrefetchSchedules_WithItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "list prefetch schedules returns items"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// First create a playback configuration
			doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
				"Name":                  "pc1",
				"AdDecisionServerUrl":   "https://ads.com",
				"VideoContentSourceUrl": "https://video.com",
			})

			for i := range 2 {
				name := "sched-" + string(rune('a'+i))
				doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1/"+name, nil)
			}

			rec := doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			items, _ := resp["Items"].([]any)
			assert.Len(t, items, 2)
		})
	}
}

func TestListFunctions_WithItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "list functions returns items"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for i := range 2 {
				name := "fn-" + string(rune('a'+i))
				doRequest(t, h, http.MethodPut, "/function/"+name, map[string]any{
					"FunctionType": "CHANNEL_ASSEMBLY",
					"Description":  "test fn",
				})
			}

			rec := doRequest(t, h, http.MethodGet, "/functions", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			items, _ := resp["Items"].([]any)
			assert.Len(t, items, 2)
		})
	}
}

// --- Error branches ---

func TestHandleDeleteChannelPolicy_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "delete channel policy on missing channel returns 404", wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodDelete, "/channel/nonexistent/policy", nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandleUpdateProgram_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "update program on missing channel returns 404", wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPut, "/channel/nonexistent/program/prog1", nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandleGetChannelSchedule_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "get schedule on missing channel returns 404", wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/channel/nonexistent/schedule", nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandleGetChannelSchedule_WithItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "get channel schedule with programs returns items"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			createTestSourceLocation(t, h)

			// Create channel
			doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{
				"PlaybackMode": "LINEAR",
			})

			// Create program in channel
			doRequest(t, h, http.MethodPost, "/channel/ch1/program/prog1", map[string]any{
				"SourceLocationName": "sl1",
				"VodSourceName":      "vs1",
			})

			rec := doRequest(t, h, http.MethodGet, "/channel/ch1/schedule", nil)
			require.Equal(t, http.StatusOK, rec.Code)
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

func TestHandleConfigureLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		path     string
		wantCode int
	}{
		{
			name:     "configure logs for channel that exists",
			path:     "/configureLogs/channel",
			body:     map[string]any{"ChannelName": "ch1", "LogTypes": []any{"AS_RUN"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "configure logs for channel not found",
			path:     "/configureLogs/channel",
			body:     map[string]any{"ChannelName": "missing", "LogTypes": []any{"AS_RUN"}},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "configure logs for playback config that exists",
			path:     "/configureLogs/playbackConfiguration",
			body:     map[string]any{"PlaybackConfigurationName": "pc1", "PercentEnabled": float64(50)},
			wantCode: http.StatusOK,
		},
		{
			name:     "configure logs for playback config not found",
			path:     "/configureLogs/playbackConfiguration",
			body:     map[string]any{"PlaybackConfigurationName": "missing", "PercentEnabled": float64(50)},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Set up dependencies for success cases
			if tt.wantCode == http.StatusOK {
				if tt.path == "/configureLogs/channel" {
					doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{
						"PlaybackMode": "LINEAR",
					})
				} else {
					doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
						"Name":                  "pc1",
						"AdDecisionServerUrl":   "https://ads.com",
						"VideoContentSourceUrl": "https://video.com",
					})
				}
			}

			rec := doRequest(t, h, http.MethodPut, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandleInvalidJSONBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "invalid JSON body returns 400", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Send invalid JSON with non-zero content length
			req := httptest.NewRequest(http.MethodPut, "/playbackConfiguration",
				strings.NewReader("not-valid-json{{{"))
			req.Header.Set("Content-Type", "application/json")
			req.ContentLength = 17

			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestChannelOutput_WithHLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "channel output includes HLS playlist settings"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{
				"PlaybackMode": "LINEAR",
				"Outputs": []any{
					map[string]any{
						"ManifestName": "index",
						"SourceGroup":  "main",
						"HlsPlaylistSettings": map[string]any{
							"ManifestWindowSeconds": float64(30),
						},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			outputs, _ := resp["Outputs"].([]any)
			require.Len(t, outputs, 1)
			out := outputs[0].(map[string]any)
			assert.NotNil(t, out["HlsPlaylistSettings"])
		})
	}
}

func TestVodSource_DuplicateReturnsConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "duplicate vod source returns 409", wantCode: http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			createTestSourceLocation(t, h)

			doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/vodSource/vs1", map[string]any{
				"HttpPackageConfigurations": []any{
					map[string]any{"Path": "/hls", "SourceGroup": "default", "Type": "HLS"},
				},
			})

			rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/vodSource/vs1", map[string]any{
				"HttpPackageConfigurations": []any{
					map[string]any{"Path": "/hls", "SourceGroup": "default", "Type": "HLS"},
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestCreateChannel_MissingPlaybackMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "create channel without playback mode defaults to LOOP", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestCreateSourceLocation_MissingBaseURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "create source location without base URL returns 400", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1", map[string]any{})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestCreateVodSource_MissingSourceLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "create vod source with missing source location returns 404", wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/sourceLocation/missing/vodSource/vs1", map[string]any{
				"HttpPackageConfigurations": []any{
					map[string]any{"Path": "/hls", "SourceGroup": "default", "Type": "HLS"},
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandleListAlerts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "list alerts returns empty list", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/alerts", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			items, _ := resp["Items"].([]any)
			assert.Empty(t, items)
		})
	}
}

func TestDeleteChannelPolicy_WithExistingPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "delete existing channel policy succeeds", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Create channel
			doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{
				"PlaybackMode": "LINEAR",
			})

			// Add policy
			doRequest(t, h, http.MethodPut, "/channel/ch1/policy", map[string]any{
				"Policy": `{"Version":"2012-10-17"}`,
			})

			// Delete it
			rec := doRequest(t, h, http.MethodDelete, "/channel/ch1/policy", nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
