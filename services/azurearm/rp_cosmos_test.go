package azurearm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const (
	cosmosBase   = "/subscriptions/" + azurearm.DefaultSubscriptionID + "/resourceGroups/rg/providers/Microsoft.DocumentDB"
	cosmosAcct   = cosmosBase + "/databaseAccounts/acct1"
	cosmosMaster = "master-key-for-tests"
)

type fakeCosmos struct {
	dbs        map[string]map[string]azurearm.CosmosContainer
	accepted   map[string]bool
	tps        map[string]azurearm.CosmosThroughput
	regions    []azurearm.CosmosRegion
	policies   azurearm.CosmosAccountPolicies
	rid        int
	port       int
	mu         sync.Mutex
	multiWrite bool
	tls        bool
}

func newFakeCosmos() *fakeCosmos {
	return &fakeCosmos{
		dbs:      map[string]map[string]azurearm.CosmosContainer{},
		accepted: map[string]bool{},
		tps:      map[string]azurearm.CosmosThroughput{},
		port:     8081,
	}
}

func (f *fakeCosmos) Endpoint() (int, bool) { return f.port, f.tls }
func (f *fakeCosmos) MasterKey() string     { return cosmosMaster }

func (f *fakeCosmos) AcceptKey(k string) {
	f.mu.Lock()
	f.accepted[k] = true
	f.mu.Unlock()
}

func (f *fakeCosmos) RevokeKey(k string) {
	f.mu.Lock()
	delete(f.accepted, k)
	f.mu.Unlock()
}

func (f *fakeCosmos) SetRegions(r []azurearm.CosmosRegion, multi bool) {
	f.mu.Lock()
	f.regions, f.multiWrite = r, multi
	f.mu.Unlock()
}

func (f *fakeCosmos) isAccepted(k string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.accepted[k]
}

func (f *fakeCosmos) CreateDatabase(id string) (azurearm.CosmosDatabase, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.dbs[id]; ok {
		return azurearm.CosmosDatabase{}, azurearm.ErrCosmosConflict
	}

	f.rid++
	f.dbs[id] = map[string]azurearm.CosmosContainer{}

	return azurearm.CosmosDatabase{ID: id, RID: "rid" + id, Modified: time.Unix(1700000000, 0)}, nil
}

func (f *fakeCosmos) GetDatabase(id string) (azurearm.CosmosDatabase, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.dbs[id]; !ok {
		return azurearm.CosmosDatabase{}, azurearm.ErrCosmosDatabaseNotFound
	}

	return azurearm.CosmosDatabase{ID: id, RID: "rid" + id, Modified: time.Unix(1700000000, 0)}, nil
}

func (f *fakeCosmos) ListDatabases() []azurearm.CosmosDatabase {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]azurearm.CosmosDatabase, 0, len(f.dbs))
	for id := range f.dbs {
		out = append(out, azurearm.CosmosDatabase{ID: id, RID: "rid" + id, Modified: time.Unix(1700000000, 0)})
	}

	return out
}

func (f *fakeCosmos) DeleteDatabase(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.dbs[id]; !ok {
		return azurearm.ErrCosmosDatabaseNotFound
	}

	delete(f.dbs, id)

	for k := range f.tps {
		if k == id || strings.HasPrefix(k, id+"/") {
			delete(f.tps, k)
		}
	}

	return nil
}

func (f *fakeCosmos) GetThroughput(db, container string) (azurearm.CosmosThroughput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	tp, ok := f.tps[strings.TrimSuffix(db+"/"+container, "/")]
	if !ok {
		return azurearm.CosmosThroughput{}, azurearm.ErrCosmosThroughputNotFound
	}

	return tp, nil
}

func (f *fakeCosmos) PutThroughput(db, container string, t azurearm.CosmosThroughput) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.tps[strings.TrimSuffix(db+"/"+container, "/")] = t

	return nil
}

func (f *fakeCosmos) PutContainer(db string, c azurearm.CosmosContainer) (azurearm.CosmosContainer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	d, ok := f.dbs[db]
	if !ok {
		return azurearm.CosmosContainer{}, azurearm.ErrCosmosDatabaseNotFound
	}

	c.RID = "rid" + c.ID
	c.Modified = time.Unix(1700000000, 0)
	d[c.ID] = c

	return c, nil
}

func (f *fakeCosmos) GetContainer(db, id string) (azurearm.CosmosContainer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	d, ok := f.dbs[db]
	if !ok {
		return azurearm.CosmosContainer{}, azurearm.ErrCosmosDatabaseNotFound
	}

	c, ok := d[id]
	if !ok {
		return azurearm.CosmosContainer{}, azurearm.ErrCosmosContainerNotFound
	}

	return c, nil
}

func (f *fakeCosmos) ListContainers(db string) ([]azurearm.CosmosContainer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	d, ok := f.dbs[db]
	if !ok {
		return nil, azurearm.ErrCosmosDatabaseNotFound
	}

	var out []azurearm.CosmosContainer
	for _, c := range d {
		out = append(out, c)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out, nil
}

func (f *fakeCosmos) DeleteContainer(db, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	d, ok := f.dbs[db]
	if !ok {
		return azurearm.ErrCosmosDatabaseNotFound
	}

	if _, ok = d[id]; !ok {
		return azurearm.ErrCosmosContainerNotFound
	}

	delete(d, id)

	return nil
}

func newCosmosHandler() (*azurearm.Handler, *fakeCosmos) {
	fake := newFakeCosmos()

	return azurearm.NewTestHandlerWithCosmos(fake, azurearm.DefaultSettings()), fake
}

func cosmosDo(t *testing.T, h *azurearm.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()

	var raw []byte

	if body != nil {
		var err error

		raw, err = json.Marshal(body)
		require.NoError(t, err)
	}

	return doRequest(t, h, method, path, raw)
}

func putCosmosAccount(t *testing.T, h *azurearm.Handler, props map[string]any) map[string]any {
	t.Helper()

	st, out := cosmosDo(t, h, http.MethodPut, cosmosAcct, map[string]any{
		"location": "westus", "kind": "GlobalDocumentDB", "tags": map[string]any{"a": "b"}, "properties": props,
	})
	require.Equal(t, http.StatusOK, st)

	return out
}

func asMap(t *testing.T, v any) map[string]any {
	t.Helper()

	m, ok := v.(map[string]any)
	require.True(t, ok, "want object, got %T", v)

	return m
}

func TestCosmos_AccountLifecycle(t *testing.T) {
	t.Parallel()

	h, _ := newCosmosHandler()
	out := putCosmosAccount(t, h, map[string]any{
		"databaseAccountOfferType": "Standard",
		"consistencyPolicy":        map[string]any{"defaultConsistencyLevel": "Eventual"},
		"locations":                []any{map[string]any{"locationName": "westus", "failoverPriority": 0}},
		"capabilities":             []any{map[string]any{"name": "EnableTable"}},
	})

	props := asMap(t, out["properties"])
	assert.Equal(t, "Microsoft.DocumentDB/databaseAccounts", out["type"])
	assert.Equal(t, "GlobalDocumentDB", out["kind"])
	assert.Equal(t, "Succeeded", props["provisioningState"])
	assert.Equal(t, "http://host:8081/", props["documentEndpoint"])
	assert.Equal(t, "Standard", props["databaseAccountOfferType"])
	assert.Equal(t, false, props["enableAutomaticFailover"])
	assert.Equal(t, "Enabled", props["publicNetworkAccess"])
	assert.Equal(t, "Eventual", asMap(t, props["consistencyPolicy"])["defaultConsistencyLevel"])
	assert.Equal(t, "Periodic", asMap(t, props["backupPolicy"])["type"])
	assert.Len(t, props["readLocations"], 1)
	assert.Len(t, props["writeLocations"], 1)
	assert.Len(t, props["failoverPolicies"], 1)
	assert.Len(t, props["capabilities"], 1)

	st, got := cosmosDo(t, h, http.MethodGet, cosmosAcct, nil)
	require.Equal(t, http.StatusOK, st)
	assert.Equal(t, out["id"], got["id"])

	st, list := cosmosDo(t, h, http.MethodGet, cosmosBase+"/databaseAccounts", nil)
	require.Equal(t, http.StatusOK, st)
	assert.Len(t, list["value"], 1)

	st, list = cosmosDo(t, h, http.MethodGet,
		"/subscriptions/"+azurearm.DefaultSubscriptionID+"/providers/Microsoft.DocumentDB/databaseAccounts", nil)
	require.Equal(t, http.StatusOK, st)
	assert.Len(t, list["value"], 1)

	st, patched := cosmosDo(t, h, http.MethodPatch, cosmosAcct, map[string]any{
		"tags": map[string]any{"env": "dev"}, "properties": map[string]any{"enableAutomaticFailover": true},
	})
	require.Equal(t, http.StatusOK, st)
	assert.Equal(t, map[string]any{"env": "dev"}, patched["tags"])
	assert.Equal(t, true, asMap(t, patched["properties"])["enableAutomaticFailover"])

	st, _ = cosmosDo(t, h, http.MethodDelete, cosmosAcct, nil)
	assert.Equal(t, http.StatusAccepted, st)

	st, _ = cosmosDo(t, h, http.MethodGet, cosmosAcct, nil)
	assert.Equal(t, http.StatusNotFound, st)

	st, _ = cosmosDo(t, h, http.MethodDelete, cosmosAcct, nil)
	assert.Equal(t, http.StatusNotFound, st)
}

func TestCosmos_DeleteIsAsynchronous(t *testing.T) {
	t.Parallel()

	h, _ := newCosmosHandler()
	putCosmosAccount(t, h, map[string]any{})

	req := httptest.NewRequest(http.MethodDelete, cosmosAcct, nil)
	req.Host = "host:10006"
	rec := httptest.NewRecorder()
	newEchoServer(h).ServeHTTP(rec, req)

	require.Equal(t, http.StatusAccepted, rec.Code)

	op := rec.Header().Get("Azure-Asyncoperation")
	require.Contains(t, op, "/providers/Microsoft.DocumentDB/locations/")
	assert.Equal(t, op, rec.Header().Get("Location"))

	st, body := cosmosDo(t, h, http.MethodGet, strings.TrimPrefix(op, "https://host:10006"), nil)
	require.Equal(t, http.StatusOK, st)
	assert.Equal(t, "Succeeded", body["status"])
}

func TestCosmos_AccountValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		acct   string
		method string
		want   int
	}{
		{name: "uppercase_name", acct: "BadName", method: http.MethodPut, want: http.StatusBadRequest},
		{name: "short_name", acct: "ab", method: http.MethodPut, want: http.StatusBadRequest},
		{name: "get_missing", acct: "nope", method: http.MethodGet, want: http.StatusNotFound},
		{name: "patch_missing", acct: "nope", method: http.MethodPatch, want: http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newCosmosHandler()
			st, _ := cosmosDo(t, h, tc.method, cosmosBase+"/databaseAccounts/"+tc.acct, map[string]any{})
			assert.Equal(t, tc.want, st)
		})
	}
}

func TestCosmos_AccountNameUniqueAcrossGroups(t *testing.T) {
	t.Parallel()

	h, _ := newCosmosHandler()
	putCosmosAccount(t, h, map[string]any{})

	other := "/subscriptions/" + azurearm.DefaultSubscriptionID +
		"/resourceGroups/other/providers/Microsoft.DocumentDB/databaseAccounts/acct1"
	st, _ := cosmosDo(t, h, http.MethodPut, other, map[string]any{})
	assert.Equal(t, http.StatusConflict, st)
}

func TestCosmos_Keys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		check  func(t *testing.T, fake *fakeCosmos, body map[string]any)
		name   string
		method string
		path   string
		want   int
	}{
		{
			name: "list_keys", method: http.MethodPost, path: cosmosAcct + "/listKeys", want: http.StatusOK,
			check: func(t *testing.T, fake *fakeCosmos, body map[string]any) {
				t.Helper()
				assert.Equal(t, cosmosMaster, body["primaryMasterKey"])

				for _, k := range []string{"secondaryMasterKey", "primaryReadonlyMasterKey", "secondaryReadonlyMasterKey"} {
					key, _ := body[k].(string)
					assert.NotEmpty(t, key)
					assert.True(t, fake.isAccepted(key), k)
				}
			},
		},
		{
			name: "readonly_get", method: http.MethodGet, path: cosmosAcct + "/readonlykeys", want: http.StatusOK,
			check: func(t *testing.T, _ *fakeCosmos, body map[string]any) {
				t.Helper()
				assert.NotEmpty(t, body["primaryReadonlyMasterKey"])
				assert.NotContains(t, body, "primaryMasterKey")
			},
		},
		{
			name: "readonly_post", method: http.MethodPost, path: cosmosAcct + "/readonlykeys", want: http.StatusOK,
			check: func(t *testing.T, _ *fakeCosmos, body map[string]any) {
				t.Helper()
				assert.NotEmpty(t, body["secondaryReadonlyMasterKey"])
			},
		},
		{
			name: "connection_strings", method: http.MethodPost, path: cosmosAcct + "/listConnectionStrings",
			want: http.StatusOK,
			check: func(t *testing.T, _ *fakeCosmos, body map[string]any) {
				t.Helper()
				list, _ := body["connectionStrings"].([]any)
				require.Len(t, list, 4)
				first := asMap(t, list[0])
				assert.Equal(t, "Primary", first["keyKind"])
				assert.Equal(
					t,
					"AccountEndpoint=http://host:8081/;AccountKey="+cosmosMaster+";",
					first["connectionString"],
				)
			},
		},
		{
			name: "regenerate_unknown_kind", method: http.MethodPost, path: cosmosAcct + "/regenerateKey",
			body: map[string]any{"keyKind": "Bogus"}, want: http.StatusBadRequest,
		},
		{
			name: "missing_account", method: http.MethodPost, path: cosmosBase + "/databaseAccounts/nope/listKeys",
			want: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, fake := newCosmosHandler()
			putCosmosAccount(t, h, map[string]any{})

			st, body := cosmosDo(t, h, tc.method, tc.path, tc.body)
			require.Equal(t, tc.want, st)

			if tc.check != nil {
				tc.check(t, fake, body)
			}
		})
	}
}

func TestCosmos_RegenerateKey(t *testing.T) {
	t.Parallel()

	h, fake := newCosmosHandler()
	putCosmosAccount(t, h, map[string]any{})

	_, before := cosmosDo(t, h, http.MethodPost, cosmosAcct+"/listKeys", nil)
	oldSecondary, _ := before["secondaryMasterKey"].(string)
	require.True(t, fake.isAccepted(oldSecondary))

	st, _ := cosmosDo(t, h, http.MethodPost, cosmosAcct+"/regenerateKey", map[string]any{"keyKind": "secondary"})
	require.Equal(t, http.StatusOK, st)

	_, after := cosmosDo(t, h, http.MethodPost, cosmosAcct+"/listKeys", nil)
	newSecondary, _ := after["secondaryMasterKey"].(string)

	assert.NotEqual(t, oldSecondary, newSecondary)
	assert.False(t, fake.isAccepted(oldSecondary), "old key revoked")
	assert.True(t, fake.isAccepted(newSecondary), "new key accepted")
	assert.Equal(t, before["primaryMasterKey"], after["primaryMasterKey"])
}

func TestCosmos_SQLDatabasesAndContainers(t *testing.T) {
	t.Parallel()

	h, fake := newCosmosHandler()
	putCosmosAccount(t, h, map[string]any{})

	dbPath := cosmosAcct + "/sqlDatabases/db1"
	st, db := cosmosDo(t, h, http.MethodPut, dbPath, map[string]any{
		"properties": map[string]any{
			"resource": map[string]any{"id": "db1"},
			"options":  map[string]any{"throughput": 400},
		},
	})
	require.Equal(t, http.StatusOK, st)
	assert.Equal(t, "Microsoft.DocumentDB/databaseAccounts/sqlDatabases", db["type"])
	assert.Equal(t, "db1", asMap(t, asMap(t, db["properties"])["resource"])["id"])

	_, err := fake.GetDatabase("db1")
	require.NoError(t, err, "database created on the data plane")

	st, tp := cosmosDo(t, h, http.MethodGet, dbPath+"/throughputSettings/default", nil)
	require.Equal(t, http.StatusOK, st)
	assert.InDelta(t, 400, asMap(t, asMap(t, tp["properties"])["resource"])["throughput"], 0)

	st, _ = cosmosDo(t, h, http.MethodPut, dbPath+"/throughputSettings/default", map[string]any{
		"properties": map[string]any{
			"resource": map[string]any{"autoscaleSettings": map[string]any{"maxThroughput": 4000}},
		},
	})
	require.Equal(t, http.StatusOK, st)

	st, tp = cosmosDo(t, h, http.MethodGet, dbPath+"/throughputSettings/default", nil)
	require.Equal(t, http.StatusOK, st)
	assert.Contains(t, asMap(t, asMap(t, tp["properties"])["resource"]), "autoscaleSettings")

	ctPath := dbPath + "/containers/c1"
	st, ct := cosmosDo(t, h, http.MethodPut, ctPath, map[string]any{
		"properties": map[string]any{"resource": map[string]any{
			"id":           "c1",
			"partitionKey": map[string]any{"paths": []any{"/pk"}, "kind": "Hash"},
			"defaultTtl":   60,
			"uniqueKeyPolicy": map[string]any{
				"uniqueKeys": []any{map[string]any{"paths": []any{"/email"}}},
			},
		}},
	})
	require.Equal(t, http.StatusOK, st)

	res := asMap(t, asMap(t, ct["properties"])["resource"])
	assert.InDelta(t, 60, res["defaultTtl"], 0)
	assert.Equal(t, []any{"/pk"}, asMap(t, res["partitionKey"])["paths"])
	assert.Equal(t, "consistent", asMap(t, res["indexingPolicy"])["indexingMode"])
	assert.Contains(t, asMap(t, res["uniqueKeyPolicy"]), "uniqueKeys")
	assert.Equal(t, "LastWriterWins", asMap(t, res["conflictResolutionPolicy"])["mode"])

	stored, err := fake.GetContainer("db1", "c1")
	require.NoError(t, err)
	assert.Equal(t, []string{"/pk"}, stored.PartitionKeyPaths)

	st, list := cosmosDo(t, h, http.MethodGet, dbPath+"/containers", nil)
	require.Equal(t, http.StatusOK, st)
	assert.Len(t, list["value"], 1)

	st, list = cosmosDo(t, h, http.MethodGet, cosmosAcct+"/sqlDatabases", nil)
	require.Equal(t, http.StatusOK, st)
	assert.Len(t, list["value"], 1)

	st, _ = cosmosDo(t, h, http.MethodDelete, ctPath, nil)
	require.Equal(t, http.StatusAccepted, st)

	_, err = fake.GetContainer("db1", "c1")
	require.ErrorIs(t, err, azurearm.ErrCosmosContainerNotFound)

	st, _ = cosmosDo(t, h, http.MethodDelete, dbPath, nil)
	require.Equal(t, http.StatusAccepted, st)

	st, _ = cosmosDo(t, h, http.MethodGet, dbPath, nil)
	assert.Equal(t, http.StatusNotFound, st)
}

func TestCosmos_DataPlaneDatabaseVisibleThroughARM(t *testing.T) {
	t.Parallel()

	h, fake := newCosmosHandler()
	putCosmosAccount(t, h, map[string]any{})

	_, err := fake.CreateDatabase("native")
	require.NoError(t, err)

	st, db := cosmosDo(t, h, http.MethodGet, cosmosAcct+"/sqlDatabases/native", nil)
	require.Equal(t, http.StatusOK, st)
	assert.Equal(t, "native", db["name"])

	st, list := cosmosDo(t, h, http.MethodGet, cosmosAcct+"/sqlDatabases", nil)
	require.Equal(t, http.StatusOK, st)
	assert.Len(t, list["value"], 1)
}

func TestCosmos_SQLErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		name   string
		method string
		path   string
		want   int
	}{
		{
			name: "no_account", method: http.MethodPut, want: http.StatusNotFound,
			path: cosmosBase + "/databaseAccounts/none/sqlDatabases/db1",
			body: map[string]any{"properties": map[string]any{"resource": map[string]any{"id": "db1"}}},
		},
		{
			name: "id_mismatch", method: http.MethodPut, want: http.StatusBadRequest,
			path: cosmosAcct + "/sqlDatabases/db1",
			body: map[string]any{"properties": map[string]any{"resource": map[string]any{"id": "other"}}},
		},
		{
			name: "bad_throughput", method: http.MethodPut, want: http.StatusBadRequest,
			path: cosmosAcct + "/sqlDatabases/db1",
			body: map[string]any{"properties": map[string]any{
				"resource": map[string]any{"id": "db1"}, "options": map[string]any{"throughput": 150},
			}},
		},
		{
			name: "container_without_database", method: http.MethodPut, want: http.StatusNotFound,
			path: cosmosAcct + "/sqlDatabases/missing/containers/c1",
			body: map[string]any{"properties": map[string]any{"resource": map[string]any{
				"id": "c1", "partitionKey": map[string]any{"paths": []any{"/pk"}},
			}}},
		},
		{
			name: "get_missing_database", method: http.MethodGet, want: http.StatusNotFound,
			path: cosmosAcct + "/sqlDatabases/missing",
		},
		{
			name: "throughput_missing_database", method: http.MethodGet, want: http.StatusNotFound,
			path: cosmosAcct + "/sqlDatabases/missing/throughputSettings/default",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newCosmosHandler()
			putCosmosAccount(t, h, map[string]any{})

			st, _ := cosmosDo(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.want, st)
		})
	}
}

func TestCosmos_ContainerRequiresPartitionKeyAndThroughputOptional(t *testing.T) {
	t.Parallel()

	h, _ := newCosmosHandler()
	putCosmosAccount(t, h, map[string]any{})
	cosmosDo(t, h, http.MethodPut, cosmosAcct+"/sqlDatabases/db1", map[string]any{
		"properties": map[string]any{"resource": map[string]any{"id": "db1"}},
	})

	st, _ := cosmosDo(t, h, http.MethodPut, cosmosAcct+"/sqlDatabases/db1/containers/c1", map[string]any{
		"properties": map[string]any{"resource": map[string]any{"id": "c1"}},
	})
	assert.Equal(t, http.StatusBadRequest, st)

	st, _ = cosmosDo(t, h, http.MethodGet, cosmosAcct+"/sqlDatabases/db1/throughputSettings/default", nil)
	assert.Equal(t, http.StatusNotFound, st, "no throughput configured")
}

func TestCosmos_DeleteAccountRemovesOwnedDatabases(t *testing.T) {
	t.Parallel()

	h, fake := newCosmosHandler()
	putCosmosAccount(t, h, map[string]any{})
	cosmosDo(t, h, http.MethodPut, cosmosAcct+"/sqlDatabases/db1", map[string]any{
		"properties": map[string]any{"resource": map[string]any{"id": "db1"}},
	})
	_, err := fake.CreateDatabase("native")
	require.NoError(t, err)

	_, keys := cosmosDo(t, h, http.MethodPost, cosmosAcct+"/listKeys", nil)
	secondary, _ := keys["secondaryMasterKey"].(string)

	st, _ := cosmosDo(t, h, http.MethodDelete, cosmosAcct, nil)
	require.Equal(t, http.StatusAccepted, st)

	_, err = fake.GetDatabase("db1")
	require.ErrorIs(t, err, azurearm.ErrCosmosDatabaseNotFound)

	_, err = fake.GetDatabase("native")
	require.NoError(t, err, "databases created on the data plane survive")
	assert.False(t, fake.isAccepted(secondary))
}

func TestCosmos_SnapshotRestore(t *testing.T) {
	t.Parallel()

	h, _ := newCosmosHandler()
	putCosmosAccount(t, h, map[string]any{"enableAutomaticFailover": true})
	cosmosDo(t, h, http.MethodPut, cosmosAcct+"/sqlDatabases/db1", map[string]any{
		"properties": map[string]any{
			"resource": map[string]any{"id": "db1"}, "options": map[string]any{"throughput": 500},
		},
	})

	_, keys := cosmosDo(t, h, http.MethodPost, cosmosAcct+"/listKeys", nil)
	snap := h.Snapshot(context.Background())

	h2, fake2 := newCosmosHandler()
	_, err := fake2.CreateDatabase("db1")
	require.NoError(t, err)
	require.NoError(t, fake2.PutThroughput("db1", "", azurearm.CosmosThroughput{Throughput: 500}))
	require.NoError(t, h2.Restore(context.Background(), snap))

	st, got := cosmosDo(t, h2, http.MethodGet, cosmosAcct, nil)
	require.Equal(t, http.StatusOK, st)
	assert.Equal(t, true, asMap(t, got["properties"])["enableAutomaticFailover"])

	_, keys2 := cosmosDo(t, h2, http.MethodPost, cosmosAcct+"/listKeys", nil)
	assert.Equal(t, keys["secondaryMasterKey"], keys2["secondaryMasterKey"])

	secondary, _ := keys2["secondaryMasterKey"].(string)
	assert.True(t, fake2.isAccepted(secondary), "restored keys are re-authorized")

	st, tp := cosmosDo(t, h2, http.MethodGet, cosmosAcct+"/sqlDatabases/db1/throughputSettings/default", nil)
	require.Equal(t, http.StatusOK, st)
	assert.InDelta(t, 500, asMap(t, asMap(t, tp["properties"])["resource"])["throughput"], 0)
}

func TestCosmos_DocumentEndpointOverride(t *testing.T) {
	t.Parallel()

	settings := azurearm.DefaultSettings()
	settings.AdvertiseCosmosDB = "localhost:18081"
	fake := newFakeCosmos()
	fake.tls = true
	h := azurearm.NewTestHandlerWithCosmos(fake, settings)

	out := putCosmosAccount(t, h, map[string]any{})
	assert.Equal(t, "https://localhost:18081/", asMap(t, out["properties"])["documentEndpoint"])
}

func TestCosmos_LegacyThroughputSnapshotMigrates(t *testing.T) {
	t.Parallel()

	h, _ := newCosmosHandler()
	putCosmosAccount(t, h, map[string]any{})

	var env map[string]any
	require.NoError(t, json.Unmarshal(h.Snapshot(context.Background()), &env))
	inject(t, env, map[string]any{"acct1/db1": map[string]any{"autoscaleMax": 4000}})
	raw, err := json.Marshal(env)
	require.NoError(t, err)

	h2, fake2 := newCosmosHandler()
	_, err = fake2.CreateDatabase("db1")
	require.NoError(t, err)
	require.NoError(t, h2.Restore(context.Background(), raw))

	tp, err := fake2.GetThroughput("db1", "")
	require.NoError(t, err, "restore moves the legacy entry onto the data plane")
	assert.Equal(t, 4000, tp.AutoscaleMax)
}

// inject sets the legacy throughput map in whichever object of env carries the cosmos accounts.
func inject(t *testing.T, node any, legacy map[string]any) {
	t.Helper()

	m, ok := node.(map[string]any)
	if !ok {
		return
	}

	if _, has := m["accounts"]; has {
		m["throughput"] = legacy

		return
	}

	for _, v := range m {
		inject(t, v, legacy)
	}
}
