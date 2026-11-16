package azurearm_test

import (
	"encoding/json/v2"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestAPIMListFilterByType(t *testing.T) {
	t.Parallel()

	tags := map[string]string{
		"a": `{"properties":{"displayName":"Alpha"}}`, "b": `{"properties":{"displayName":"Beta"}}`,
	}
	users := map[string]string{
		"u1": `{"properties":{"email":"a@x.io","firstName":"Ann","lastName":"One","state":"active"}}`,
		"u2": `{"properties":{"email":"b@x.io","firstName":"Bob","lastName":"Two","state":"blocked"}}`,
	}
	loggers := map[string]string{
		"l1": `{"properties":{"loggerType":"applicationInsights","credentials":{"instrumentationKey":"k1"}}}`,
		"l2": `{"properties":{"loggerType":"azureMonitor"}}`,
	}

	tests := []struct {
		name     string
		list     string
		seed     map[string]string
		filter   string
		want     []string
		wantCode int
	}{
		{name: "tag display", list: "tags", seed: tags, filter: "contains(displayName,'lph')", want: []string{"a"}},
		{name: "tag name ge", list: "tags", seed: tags, filter: "name ge 'b'", want: []string{"b"}},
		{name: "user email", list: "users", seed: users, filter: "email eq 'b@x.io'", want: []string{"u2"}},
		{name: "user state", list: "users", seed: users, filter: "state eq 'active'", want: []string{"u1"}},
		{
			name:     "user state gt",
			list:     "users",
			seed:     users,
			filter:   "state gt 'active'",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "user state fn",
			list:     "users",
			seed:     users,
			filter:   "startswith(state,'a')",
			wantCode: http.StatusBadRequest,
		},
		{name: "user first", list: "users", seed: users, filter: "startswith(firstName,'B')", want: []string{"u2"}},
		{
			name:   "logger type",
			list:   "loggers",
			seed:   loggers,
			filter: "loggerType eq 'azureMonitor'",
			want:   []string{"l2"},
		},
		{name: "unknown field", list: "tags", seed: tags, filter: "color eq 'red'", wantCode: http.StatusBadRequest},
		{name: "nested ok", list: "tags", seed: tags, want: []string{"a"},
			filter: strings.Repeat("(", 8) + "name eq 'a'" + strings.Repeat(")", 8)},
		{name: "nested too deep", list: "tags", seed: tags, wantCode: http.StatusBadRequest,
			filter: strings.Repeat("(", 40) + "name eq 'a'" + strings.Repeat(")", 40)},
		{name: "too long", list: "tags", seed: tags, wantCode: http.StatusBadRequest,
			filter: "name eq 'a'" + strings.Repeat(" or name eq 'a'", 1000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newAPIMStack(t)
			s.devService(t)

			for id, body := range tt.seed {
				status, resp, _ := s.raw(t, http.MethodPut, s.armPath("/"+tt.list+"/"+id), body, nil)
				require.Less(t, status, http.StatusBadRequest, resp)
			}

			status, body, _ := s.raw(t, http.MethodGet,
				s.armPath("/"+tt.list)+"&$filter="+url.QueryEscape(tt.filter), "", nil)
			if tt.wantCode != 0 {
				assert.Equal(t, tt.wantCode, status)

				return
			}

			require.Equal(t, http.StatusOK, status, body)

			var got struct {
				Value []struct {
					Name string `json:"name"`
				} `json:"value"`
			}

			require.NoError(t, json.Unmarshal([]byte(body), &got, jsonx.V1()))

			names := make([]string, 0, len(got.Value))
			for _, v := range got.Value {
				names = append(names, v.Name)
			}

			assert.Equal(t, tt.want, names)
		})
	}
}
