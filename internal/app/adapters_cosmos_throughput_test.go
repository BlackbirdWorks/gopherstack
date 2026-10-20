package app

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

const cosmosThroughputAcct = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg/" +
	"providers/Microsoft.DocumentDB/databaseAccounts/acct1"

func armCosmosHandler(t *testing.T) (http.Handler, *cosmosdb.Handler) {
	t.Helper()

	data := cosmosdb.NewHandler(cosmosdb.NewInMemoryBackend())
	backend := azurearm.NewInMemoryBackend()
	registry := azurearm.NewRegistry(backend)
	registry.Register(azurearm.NewCosmosProvider(azurearm.CosmosEndpointConfig{}, &azureCosmosAdapter{handler: data}))

	e := echo.New()
	e.Any("/*", azurearm.NewHandler(backend, registry, nil, azurearm.DefaultSettings()).Handler())

	return e, data
}

func armCosmosDo(t *testing.T, h http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()

	var raw []byte

	if body != nil {
		var err error

		raw, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path+"?api-version=2024-11-15", bytes.NewReader(raw))
	req.Host = "host:10006"
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	var out map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}

	return rec.Code, out
}

func TestCosmosThroughput_ARMSharesDataPlaneOffers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		options   map[string]any
		name      string
		container string
		wantAuto  int
		wantRU    int
	}{
		{name: "database manual", options: map[string]any{"throughput": 500}, wantRU: 500},
		{
			name:    "database autoscale",
			options: map[string]any{"autoscaleSettings": map[string]any{"maxThroughput": 4000}}, wantAuto: 4000,
		},
		{name: "container manual", container: "c1", options: map[string]any{"throughput": 700}, wantRU: 700},
		{
			name: "container autoscale", container: "c1",
			options: map[string]any{"autoscaleSettings": map[string]any{"maxThroughput": 2000}}, wantAuto: 2000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, data := armCosmosHandler(t)

			st, _ := armCosmosDo(t, h, http.MethodPut, cosmosThroughputAcct, map[string]any{
				"location": "westus", "kind": "GlobalDocumentDB", "properties": map[string]any{},
			})
			require.Equal(t, http.StatusOK, st)

			dbOpts := map[string]any{}
			if tt.container == "" {
				dbOpts = tt.options
			}

			st, _ = armCosmosDo(t, h, http.MethodPut, cosmosThroughputAcct+"/sqlDatabases/db1", map[string]any{
				"properties": map[string]any{"resource": map[string]any{"id": "db1"}, "options": dbOpts},
			})
			require.Equal(t, http.StatusOK, st)

			path := cosmosThroughputAcct + "/sqlDatabases/db1"

			if tt.container != "" {
				st, _ = armCosmosDo(t, h, http.MethodPut, path+"/containers/"+tt.container, map[string]any{
					"properties": map[string]any{
						"resource": map[string]any{
							"id":           tt.container,
							"partitionKey": map[string]any{"paths": []string{"/pk"}, "kind": "Hash"},
						},
						"options": tt.options,
					},
				})
				require.Equal(t, http.StatusOK, st)

				path += "/containers/" + tt.container
			}

			info, err := data.Backend.(cosmosdb.ThroughputBackend).GetThroughput("db1", tt.container)
			require.NoError(t, err, "the ARM write lands in the data-plane offer")
			assert.Equal(t, tt.wantRU, info.Manual)
			assert.Equal(t, tt.wantAuto, info.AutoscaleMax)

			_, err = data.Backend.(cosmosdb.ThroughputBackend).SetThroughput(
				"db1", tt.container, cosmosdb.ThroughputSpec{Manual: 1300})
			require.NoError(t, err)

			st, got := armCosmosDo(t, h, http.MethodGet, path+"/throughputSettings/default", nil)
			require.Equal(t, http.StatusOK, st)

			res, _ := got["properties"].(map[string]any)["resource"].(map[string]any)
			assert.InDelta(t, 1300, res["throughput"], 0, "a data-plane replace shows through ARM")
		})
	}
}

func TestCosmosThroughput_ARMReplaceVisibleToOffers(t *testing.T) {
	t.Parallel()

	h, data := armCosmosHandler(t)

	armCosmosDo(t, h, http.MethodPut, cosmosThroughputAcct,
		map[string]any{"location": "westus", "properties": map[string]any{}})
	armCosmosDo(t, h, http.MethodPut, cosmosThroughputAcct+"/sqlDatabases/db1", map[string]any{
		"properties": map[string]any{
			"resource": map[string]any{"id": "db1"}, "options": map[string]any{"throughput": 400},
		},
	})

	st, _ := armCosmosDo(t, h, http.MethodPut, cosmosThroughputAcct+"/sqlDatabases/db1/throughputSettings/default",
		map[string]any{"properties": map[string]any{"resource": map[string]any{"throughput": 900}}})
	require.Equal(t, http.StatusOK, st)

	offers := data.Backend.(cosmosdb.ThroughputBackend).ListThroughputs()
	require.Len(t, offers, 1)
	assert.Equal(t, 900, offers[0].Manual)
}
