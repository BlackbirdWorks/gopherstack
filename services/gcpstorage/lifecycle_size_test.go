package gcpstorage_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLifecycleSizeConditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		condition string
		want      []string
	}{
		{name: "above", condition: `"sizeAboveBytes":5`, want: []string{"big"}},
		{name: "above quoted", condition: `"sizeAboveBytes":"5"`, want: []string{"big"}},
		{name: "below", condition: `"sizeBelowBytes":5`, want: []string{"small"}},
		{name: "equal excluded", condition: `"sizeAboveBytes":2,"sizeBelowBytes":9`, want: []string{"mid"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, clk := newClockedSDK(t)
			b := e.bucket(t, "bkt", nil)

			write(t, b.Object("small"), []byte("ab"), 0, nil)
			write(t, b.Object("mid"), []byte("abcde"), 0, nil)
			write(t, b.Object("big"), []byte("abcdefghij"), 0, nil)

			body := `{"lifecycle":{"rule":[{"action":{"type":"Delete"},"condition":{` + tt.condition + `}}]}}`
			req, err := http.NewRequestWithContext(
				t.Context(), http.MethodPatch, e.url+"/storage/v1/b/bkt", strings.NewReader(body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)

			clk.advance(time.Hour)
			e.backend.RunLifecycle()

			remaining := map[string]bool{}
			for _, n := range objectNames(t, b, nil) {
				remaining[n] = true
			}

			for _, n := range []string{"small", "mid", "big"} {
				deleted := false
				for _, w := range tt.want {
					deleted = deleted || w == n
				}

				assert.Equal(t, deleted, !remaining[n], n)
			}
		})
	}
}

func TestListIncludeFoldersAsPrefixes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		query    string
		wantCode int
	}{
		{name: "slash delimiter", query: "delimiter=/&includeFoldersAsPrefixes=true", wantCode: http.StatusOK},
		{name: "no delimiter", query: "includeFoldersAsPrefixes=true", wantCode: http.StatusBadRequest},
		{name: "other delimiter", query: "delimiter=-&includeFoldersAsPrefixes=true", wantCode: http.StatusBadRequest},
		{name: "flag off", query: "includeFoldersAsPrefixes=false", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newSDK(t)
			e.bucket(t, "bkt", nil)

			req, err := http.NewRequestWithContext(
				t.Context(), http.MethodGet, e.url+"/storage/v1/b/bkt/o?"+tt.query, nil)
			require.NoError(t, err)

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}
