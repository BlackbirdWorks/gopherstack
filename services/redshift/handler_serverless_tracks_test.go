package redshift_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerless_GetTrack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantType   string
		wantStatus int
		wantFound  bool
	}{
		{
			name:       "current",
			body:       map[string]any{"trackName": "current"},
			wantStatus: http.StatusOK,
			wantFound:  true,
		},
		{
			name:       "trailing",
			body:       map[string]any{"trackName": "trailing"},
			wantStatus: http.StatusOK,
			wantFound:  true,
		},
		{
			name:       "unknown",
			body:       map[string]any{"trackName": "no-such-track"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
		{
			name:       "missing name",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newServerlessHandler()

			rec := doServerlessOp(t, h, "GetTrack", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

			if tt.wantFound {
				track, _ := resp["track"].(map[string]any)
				require.NotNil(t, track)
				assert.Equal(t, tt.body["trackName"], track["trackName"])
				assert.Equal(t, "1.0", track["workgroupVersion"])

				return
			}

			assert.Equal(t, tt.wantType, resp["__type"])
		})
	}
}

func TestServerless_ListTracks(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "ListTracks", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	tracks, _ := resp["tracks"].([]any)
	require.Len(t, tracks, 2, "real Redshift Serverless exposes exactly two maintenance tracks")

	names := make([]string, 0, len(tracks))
	for _, tr := range tracks {
		m, _ := tr.(map[string]any)
		require.NotNil(t, m)
		names = append(names, m["trackName"].(string))
	}

	assert.ElementsMatch(t, []string{"current", "trailing"}, names)
	assert.Nil(t, resp["nextToken"], "two tracks fit in one page, no nextToken expected")
}

func TestServerless_ListTracks_Pagination(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "ListTracks", map[string]any{"maxResults": 1})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&page1))

	tracks1, _ := page1["tracks"].([]any)
	require.Len(t, tracks1, 1)
	require.NotEmpty(t, page1["nextToken"])

	rec = doServerlessOp(t, h, "ListTracks", map[string]any{
		"maxResults": 1, "nextToken": page1["nextToken"],
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page2 map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&page2))

	tracks2, _ := page2["tracks"].([]any)
	require.Len(t, tracks2, 1)

	track1, _ := tracks1[0].(map[string]any)
	track2, _ := tracks2[0].(map[string]any)
	assert.NotEqual(t, track1["trackName"], track2["trackName"])
}
