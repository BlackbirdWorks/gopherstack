package app

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/search/armsearch"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
	"github.com/blackbirdworks/gopherstack/services/azuresearch"
	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

type searchSDKCred struct{}

func (searchSDKCred) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "emulator", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func TestReserveFixedServicePorts_AzureSearch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		port int
		want bool
	}{
		{name: "default port inside pool", port: azuresearch.DefaultPort, want: true},
		{name: "port outside pool", port: 9995, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.want, tt.port)

			cli := CLI{AzureSearch: azuresearch.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)
			assert.Equal(t, tt.want, alloc.IsAllocated(port))
		})
	}
}

func TestAzureSearchDefaultPortsAgree(t *testing.T) {
	t.Parallel()

	assert.Equal(t, azuresearch.DefaultPort, azurearm.DefaultSearchPort)
}

type searchE2E struct {
	factory *armsearch.ClientFactory
	blobs   *azureblob.InMemoryBackend
	cosmos  *cosmosdb.InMemoryBackend
	search  *azuresearch.Handler
	data    string
	arm     string
}

func newSearchE2E(t *testing.T) *searchE2E {
	t.Helper()

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	t.Cleanup(func() { armH.Shutdown(context.Background()) })

	clock := &struct{ now time.Time }{now: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)}
	searchH := azuresearch.NewHandler(
		azuresearch.NewInMemoryBackend(
			azuresearch.WithClock(func() time.Time { return clock.now }),
		),
		azuresearch.DefaultSettings(),
	)
	blobs := azureblob.NewInMemoryBackend()
	cosmos := cosmosdb.NewInMemoryBackend()

	wireAzureSearch(map[string]service.Registerable{
		"AzureARM":    armH,
		"AzureSearch": searchH,
		"AzureBlob":   azureblob.NewHandler(blobs),
		"CosmosDB":    cosmosdb.NewHandler(cosmos),
	})

	armE := echo.New()
	armE.Any("/*", armH.Handler())

	armSrv := httptest.NewTLSServer(armE)
	t.Cleanup(armSrv.Close)

	dataE := echo.New()
	dataE.Any("/*", searchH.Handler())

	dataSrv := httptest.NewServer(dataE)
	t.Cleanup(dataSrv.Close)

	f, err := armsearch.NewClientFactory("00000000-0000-0000-0000-000000000000", searchSDKCred{}, &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: armSrv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: armSrv.Client(), Retry: policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	return &searchE2E{factory: f, blobs: blobs, cosmos: cosmos, search: searchH, data: dataSrv.URL, arm: armSrv.URL}
}

func (e *searchE2E) call(t *testing.T, method, path, key string, body any) (int, map[string]any) {
	t.Helper()

	var rd io.Reader

	if body != nil {
		raw, err := json.Marshal(body, jsonx.V1())
		require.NoError(t, err)

		rd = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, e.data+path, rd)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Api-Key", key)

	res, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer res.Body.Close()

	raw, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	var out map[string]any

	_ = json.Unmarshal(raw, &out, jsonx.V1())

	return res.StatusCode, out
}

func TestAzureSearchARMKeysUnlockDataPlane(t *testing.T) {
	t.Parallel()

	e := newSearchE2E(t)
	poller, err := e.factory.NewServicesClient().BeginCreateOrUpdate(t.Context(), "rg", "e2esearch", armsearch.Service{
		Location: new("westus"), SKU: &armsearch.SKU{Name: new(armsearch.SKUNameBasic)},
	}, nil, nil)
	require.NoError(t, err)

	svc, err := poller.PollUntilDone(t.Context(), nil)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(*svc.Properties.Endpoint, "https://e2esearch.search."))

	keys, err := e.factory.NewAdminKeysClient().Get(t.Context(), "rg", "e2esearch", nil, nil)
	require.NoError(t, err)

	idx := map[string]any{"name": "things", "fields": []any{
		map[string]any{"name": "id", "type": "Edm.String", "key": true},
		map[string]any{"name": "body", "type": "Edm.String", "searchable": true},
	}}

	status, _ := e.call(t, http.MethodPost, "/indexes?api-version=2024-07-01", *keys.PrimaryKey, idx)
	assert.Equal(t, http.StatusCreated, status, "the ARM-issued admin key authenticates the data plane")

	status, _ = e.call(t, http.MethodPost, "/indexes?api-version=2024-07-01", "not-a-key", idx)
	assert.Equal(t, http.StatusForbidden, status)

	qk, err := e.factory.NewQueryKeysClient().Create(t.Context(), "rg", "e2esearch", "reader", nil, nil)
	require.NoError(t, err)

	status, _ = e.call(t, http.MethodGet, "/indexes('things')/docs?search=*&api-version=2024-07-01", *qk.Key, nil)
	assert.Equal(t, http.StatusOK, status, "an ARM-created query key can query")

	status, _ = e.call(t, http.MethodPost, "/indexes?api-version=2024-07-01", *qk.Key, idx)
	assert.Equal(t, http.StatusForbidden, status, "a query key cannot manage indexes")

	regen, err := e.factory.NewAdminKeysClient().
		Regenerate(t.Context(), "rg", "e2esearch", armsearch.AdminKeyKindPrimary, nil, nil)
	require.NoError(t, err)

	status, _ = e.call(t, http.MethodGet, "/indexes?api-version=2024-07-01", *keys.PrimaryKey, nil)
	assert.Equal(t, http.StatusForbidden, status, "regenerating revokes the old key")

	status, _ = e.call(t, http.MethodGet, "/indexes?api-version=2024-07-01", *regen.PrimaryKey, nil)
	assert.Equal(t, http.StatusOK, status)

	_, err = e.factory.NewServicesClient().Delete(t.Context(), "rg", "e2esearch", nil, nil)
	require.NoError(t, err)

	status, _ = e.call(t, http.MethodGet, "/indexes?api-version=2024-07-01", *regen.PrimaryKey, nil)
	assert.Equal(t, http.StatusForbidden, status, "deleting the ARM resource deletes the data-plane service")
}

func TestAzureSearchIndexerFromInProcessEmulators(t *testing.T) {
	t.Parallel()

	e := newSearchE2E(t)
	poller, err := e.factory.NewServicesClient().BeginCreateOrUpdate(t.Context(), "rg", "idxsvc", armsearch.Service{
		Location: new("westus"), SKU: &armsearch.SKU{Name: new(armsearch.SKUNameBasic)},
	}, nil, nil)
	require.NoError(t, err)

	_, err = poller.PollUntilDone(t.Context(), nil)
	require.NoError(t, err)

	keys, err := e.factory.NewAdminKeysClient().Get(t.Context(), "rg", "idxsvc", nil, nil)
	require.NoError(t, err)

	key := *keys.PrimaryKey

	require.NoError(t, e.blobs.CreateContainer("docs"))
	_, err = e.blobs.PutBlob("docs", "readme.txt", []byte("emulated search indexing"), "text/plain")
	require.NoError(t, err)

	_, err = e.cosmos.CreateDatabase("db")
	require.NoError(t, err)
	_, err = e.cosmos.CreateContainer("db", cosmosdb.ContainerSpec{ID: "items", PartitionKeyPath: "/pk"})
	require.NoError(t, err)
	_, err = e.cosmos.CreateDocument(
		"db",
		"items",
		map[string]any{"id": "c1", "pk": "p", "content": "cosmos record"},
		false,
	)
	require.NoError(t, err)

	for _, step := range []struct {
		body map[string]any
		path string
		want int
	}{
		{path: "/indexes", body: map[string]any{"name": "docs", "fields": []any{
			map[string]any{"name": "id", "type": "Edm.String", "key": true},
			map[string]any{"name": "content", "type": "Edm.String", "searchable": true},
		}}, want: http.StatusCreated},
		{path: "/datasources", body: map[string]any{
			"name": "blobs", "type": "azureblob", "container": map[string]any{"name": "docs"},
			"credentials": map[string]any{"connectionString": "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=a2V5;"}, //nolint:lll // test fixture
		}, want: http.StatusCreated},
		{path: "/datasources", body: map[string]any{
			"name": "cosmos", "type": "cosmosdb", "container": map[string]any{"name": "items"},
			"credentials": map[string]any{"connectionString": "AccountEndpoint=https://x;AccountKey=a2V5;Database=db"},
		}, want: http.StatusCreated},
		{path: "/indexers", body: map[string]any{
			"name": "from-blob", "dataSourceName": "blobs", "targetIndexName": "docs",
			"fieldMappings": []any{map[string]any{
				"sourceFieldName": "metadata_storage_path", "targetFieldName": "id", "mappingFunction": map[string]any{"name": "base64Encode"}, //nolint:lll // test fixture
			}},
		}, want: http.StatusCreated},
		{path: "/indexers", body: map[string]any{"name": "from-cosmos", "dataSourceName": "cosmos", "targetIndexName": "docs"}, want: http.StatusCreated}, //nolint:lll // test fixture
	} {
		status, body := e.call(t, http.MethodPost, step.path+"?api-version=2024-07-01", key, step.body)
		require.Equal(t, step.want, status, "%s: %v", step.path, body)
	}

	for _, name := range []string{"from-blob", "from-cosmos"} {
		status, _ := e.call(t, http.MethodPost, "/indexers('"+name+"')/search.run?api-version=2024-07-01", key, nil)
		require.Equal(t, http.StatusAccepted, status)
	}

	status, body := e.call(
		t,
		http.MethodGet,
		"/indexes('docs')/docs?search=*&$count=true&api-version=2024-07-01",
		key,
		nil,
	)
	require.Equal(t, http.StatusOK, status)
	assert.EqualValues(t, 2, body["@odata.count"], "one document from Blob Storage and one from Cosmos DB")

	status, body = e.call(t, http.MethodGet, "/indexes('docs')/docs?search=emulated&api-version=2024-07-01", key, nil)
	require.Equal(t, http.StatusOK, status)
	assert.Len(t, body["value"], 1)

	status, body = e.call(t, http.MethodGet, "/indexers('from-blob')/search.status?api-version=2024-07-01", key, nil)
	require.Equal(t, http.StatusOK, status)

	last, _ := body["lastResult"].(map[string]any)
	assert.Equal(t, "success", last["status"])
}
