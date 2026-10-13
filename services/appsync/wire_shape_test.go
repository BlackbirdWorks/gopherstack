package appsync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestWire_NoInternalAPIID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		path    string
		listKey string
		objKey  string
	}{
		{
			name: "data_source", path: "/datasources", objKey: "dataSource", listKey: "dataSources",
			body: map[string]any{"name": "ds1", "type": "NONE", "tags": map[string]string{"k": "v"}},
		},
		{
			name: "function", path: "/functions", objKey: "functionConfiguration", listKey: "functions",
			body: map[string]any{
				"name":                    "fn1",
				"dataSourceName":          "ds1",
				"functionVersion":         "2018-05-29",
				"requestMappingTemplate":  "{}",
				"responseMappingTemplate": "$util.toJson($ctx.result)",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "ds1", Type: "NONE"})
			require.NoError(t, err)

			base := "/v1/apis/" + api.APIID + tt.path

			if tt.name == "function" {
				rec := doRequest(t, h, http.MethodPost, base, tt.body)
				require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
			} else {
				_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "ds2", Type: "NONE"})
				require.NoError(t, err)
			}

			list := doRequest(t, h, http.MethodGet, base, nil)
			require.Equal(t, http.StatusOK, list.Code)

			var out map[string][]map[string]any
			require.NoError(t, json.Unmarshal(list.Body.Bytes(), &out))
			require.NotEmpty(t, out[tt.listKey])

			for _, item := range out[tt.listKey] {
				assert.NotContains(t, item, "apiId")
				assert.NotContains(t, item, "tags")
			}
		})
	}
}
