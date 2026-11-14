package azurearm_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestAPIMChildListPaging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		wantNames []string
		wantNext  bool
		wantCode  int
	}{
		{name: "all", wantNames: []string{"a1", "a2", "a3"}},
		{name: "top", query: "&$top=2", wantNames: []string{"a1", "a2"}, wantNext: true},
		{name: "skip", query: "&$skip=2", wantNames: []string{"a3"}},
		{name: "top and skip", query: "&$top=1&$skip=1", wantNames: []string{"a2"}, wantNext: true},
		{name: "skip past end", query: "&$skip=10", wantNames: []string{}},
		{name: "bad top", query: "&$top=0", wantCode: http.StatusBadRequest},
		{name: "bad skip", query: "&$skip=x", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newAPIMStack(t)
			s.devService(t)

			for _, id := range []string{"a1", "a2", "a3"} {
				s.putAPI(t, id, id, "")
			}

			status, body, _ := s.raw(t, http.MethodGet, s.armPath("/apis")+tt.query, "", nil)
			if tt.wantCode != 0 {
				assert.Equal(t, tt.wantCode, status)

				return
			}

			require.Equal(t, http.StatusOK, status, body)

			var got struct {
				NextLink string `json:"nextLink"`
				Value    []struct {
					Name string `json:"name"`
				} `json:"value"`
				Count int `json:"count"`
			}

			require.NoError(t, json.Unmarshal([]byte(body), &got, jsonx.V1()))

			names := make([]string, 0, len(got.Value))
			for _, v := range got.Value {
				names = append(names, v.Name)
			}

			assert.Equal(t, tt.wantNames, names)
			assert.Equal(t, 3, got.Count)
			assert.Equal(t, tt.wantNext, got.NextLink != "")
		})
	}
}
