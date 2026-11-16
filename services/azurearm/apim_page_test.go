package azurearm_test

import (
	"encoding/json/v2"
	"net/http"
	"net/url"
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

func TestAPIMChildListFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    string
		wantNames []string
		wantCode  int
	}{
		{name: "name eq", filter: "name eq 'a2'", wantNames: []string{"a2"}},
		{name: "name ne", filter: "name ne 'a2'", wantNames: []string{"a1", "a3"}},
		{name: "contains", filter: "contains(displayName,'3')", wantNames: []string{"a3"}},
		{name: "startswith", filter: "startswith(path,'a')", wantNames: []string{"a1", "a2", "a3"}},
		{name: "endswith", filter: "endswith(name,'1')", wantNames: []string{"a1"}},
		{name: "substringof", filter: "substringof('2',displayName)", wantNames: []string{"a2"}},
		{name: "and or", filter: "name eq 'a1' or (name eq 'a2' and path eq 'a2')", wantNames: []string{"a1", "a2"}},
		{name: "not", filter: "not (name eq 'a1')", wantNames: []string{"a2", "a3"}},
		{name: "ge", filter: "name ge 'a2'", wantNames: []string{"a2", "a3"}},
		{name: "is current", filter: "isCurrent eq false", wantNames: []string{}},
		{name: "case insensitive", filter: "NAME EQ 'A1'", wantNames: []string{"a1"}},
		{name: "escaped quote", filter: "name eq 'a''b'", wantNames: []string{}},
		{name: "unknown field", filter: "color eq 'red'", wantCode: http.StatusBadRequest},
		{name: "bad operator", filter: "isCurrent gt true", wantCode: http.StatusBadRequest},
		{name: "unterminated", filter: "name eq 'a", wantCode: http.StatusBadRequest},
		{name: "trailing text", filter: "name eq 'a1' extra", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newAPIMStack(t)
			s.devService(t)

			for _, id := range []string{"a1", "a2", "a3"} {
				s.putAPI(t, id, id, "")
			}

			q := "&$filter=" + url.QueryEscape(tt.filter)

			status, body, _ := s.raw(t, http.MethodGet, s.armPath("/apis")+q, "", nil)
			if tt.wantCode != 0 {
				assert.Equal(t, tt.wantCode, status)

				return
			}

			require.Equal(t, http.StatusOK, status, body)

			var got struct {
				Value []struct {
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
			assert.Equal(t, len(tt.wantNames), got.Count)
		})
	}
}
