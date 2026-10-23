package azurearm_test

import (
	"errors"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

var errACBad = errors.New("bad store")

type fakeAppConfig struct {
	stores map[string]time.Time
	keys   map[string][]azurearm.AppConfigKey
	kvs    map[string]map[string]azurearm.AppConfigKV
}

func newFakeAppConfig() *fakeAppConfig {
	return &fakeAppConfig{
		stores: map[string]time.Time{}, keys: map[string][]azurearm.AppConfigKey{},
		kvs: map[string]map[string]azurearm.AppConfigKV{},
	}
}

func (f *fakeAppConfig) PutStore(name string) (time.Time, error) {
	if name == "badstore" {
		return time.Time{}, errACBad
	}

	if _, ok := f.stores[name]; !ok {
		f.stores[name] = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
		f.keys[name] = []azurearm.AppConfigKey{
			{ID: "id-rw", Name: "Primary", Value: "c2VjcmV0"},
			{ID: "id-ro", Name: "Primary Read Only", Value: "cm8=", ReadOnly: true},
		}
		f.kvs[name] = map[string]azurearm.AppConfigKV{}
	}

	return f.stores[name], nil
}

func (f *fakeAppConfig) StoreCreated(name string) (time.Time, bool) {
	t, ok := f.stores[name]

	return t, ok
}

func (f *fakeAppConfig) DeleteStore(name string) error {
	delete(f.stores, name)
	delete(f.kvs, name)

	return nil
}

func (f *fakeAppConfig) StoreKeys(name string) ([]azurearm.AppConfigKey, error) {
	return f.keys[name], nil
}

func (f *fakeAppConfig) RegenerateStoreKey(name, keyID string) (azurearm.AppConfigKey, error) {
	for i, k := range f.keys[name] {
		if k.ID == keyID {
			f.keys[name][i].Value = "bmV3"

			return f.keys[name][i], nil
		}
	}

	return azurearm.AppConfigKey{}, errACBad
}

func acKVKey(key, label string) string { return key + "\x00" + label }

func (f *fakeAppConfig) PutKeyValue(store string, kv azurearm.AppConfigKV) (azurearm.AppConfigKV, error) {
	if kv.Key == "locked" {
		return azurearm.AppConfigKV{}, azurearm.ErrAppConfigKeyLocked
	}

	kv.ETag, kv.LastModified = "etag-"+kv.Key, time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	f.kvs[store][acKVKey(kv.Key, kv.Label)] = kv

	return kv, nil
}

func (f *fakeAppConfig) GetKeyValue(store, key, label string) (azurearm.AppConfigKV, bool) {
	kv, ok := f.kvs[store][acKVKey(key, label)]

	return kv, ok
}

func (f *fakeAppConfig) DeleteKeyValue(store, key, label string) error {
	delete(f.kvs[store], acKVKey(key, label))

	return nil
}

func (f *fakeAppConfig) ListKeyValues(store string) ([]azurearm.AppConfigKV, error) {
	out := make([]azurearm.AppConfigKV, 0, len(f.kvs[store]))
	for _, kv := range f.kvs[store] {
		out = append(out, kv)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Key+out[i].Label < out[j].Key+out[j].Label })

	return out, nil
}

func acID(types []string, names ...string) azurearm.ResourceID {
	return azurearm.ResourceID{
		SubscriptionID: "sub", ResourceGroup: "rg", Namespace: "Microsoft.AppConfiguration", Types: types, Names: names,
	}
}

func TestAppConfigProvider_StoreLifecycle(t *testing.T) {
	t.Parallel()

	dp := newFakeAppConfig()
	p := azurearm.NewAppConfigProvider(azurearm.AppConfigEndpointConfig{}, dp)
	ctx := azurearm.WithRequestHost(t.Context(), "myhost")
	store := acID([]string{"configurationStores"}, "mystore")

	body, err := p.Put(ctx, store, map[string]any{
		"location": "westus", "tags": map[string]any{"a": "b"}, "sku": map[string]any{"name": "Standard"},
		"identity":   map[string]any{"type": "SystemAssigned"},
		"properties": map[string]any{"publicNetworkAccess": "Disabled", "createMode": "Default"},
	})
	require.NoError(t, err)

	pr := props(body)
	assert.Equal(t, "https://mystore.azconfig.myhost:10008", pr["endpoint"])
	assert.Equal(t, "Disabled", pr["publicNetworkAccess"], "unmodelled properties round-trip")
	assert.Equal(t, "Succeeded", pr["provisioningState"])
	assert.NotContains(t, pr, "createMode")
	assert.EqualValues(t, 7, pr["softDeleteRetentionInDays"])
	assert.Equal(t, map[string]any{"name": "standard"}, body["sku"])
	assert.Equal(t, "westus", body["location"])
	assert.NotEmpty(t, body["identity"].(map[string]any)["principalId"])

	_, err = p.Put(ctx, acID([]string{"configurationStores"}, "badstore"), map[string]any{})
	require.ErrorIs(t, err, azurearm.ErrInvalidRequestBody)

	keys, err := p.ListKeys(ctx, store)
	require.NoError(t, err)

	vals, _ := keys["value"].([]map[string]any)
	require.Len(t, vals, 2)
	assert.Equal(
		t,
		"Endpoint=https://mystore.azconfig.myhost:10008;Id=id-rw;Secret=c2VjcmV0",
		vals[0]["connectionString"],
	)
	assert.Equal(t, true, vals[1]["readOnly"])

	regen, err := p.RegenerateKey(ctx, store, "id-rw")
	require.NoError(t, err)
	assert.Equal(t, "bmV3", regen["value"])

	_, err = p.RegenerateKey(ctx, store, "")
	require.ErrorIs(t, err, azurearm.ErrInvalidRequestBody)

	_, err = p.ListKeys(ctx, acID([]string{"configurationStores"}, "nope"))
	require.ErrorIs(t, err, azurearm.ErrAppConfigStoreNotFound)

	_, err = p.Put(ctx, acID([]string{"bogus"}, "x"), map[string]any{})
	require.ErrorIs(t, err, azurearm.ErrResourceNotFound)

	list, err := p.List(ctx, acID([]string{"configurationStores"}))
	require.NoError(t, err)
	require.Len(t, list, 1)

	require.NoError(t, p.Delete(ctx, store))
	require.ErrorIs(t, p.Delete(ctx, store), azurearm.ErrAppConfigStoreNotFound)
	assert.Empty(t, dp.stores)
}

func TestAppConfigProvider_SoftDeleteAndNames(t *testing.T) {
	t.Parallel()

	dp := newFakeAppConfig()
	p := azurearm.NewAppConfigProvider(azurearm.AppConfigEndpointConfig{Override: "azconfig.localhost:1"}, dp)
	ctx := t.Context()
	store := acID([]string{"configurationStores"}, "softstore")

	_, err := p.Put(
		ctx,
		store,
		map[string]any{"location": "westus", "properties": map[string]any{"enablePurgeProtection": true}},
	)
	require.NoError(t, err)

	other := store
	other.ResourceGroup = "other"
	_, err = p.Put(ctx, other, map[string]any{})
	require.ErrorIs(t, err, azurearm.ErrAppConfigNameTaken)

	require.NoError(t, p.Delete(ctx, store))

	got, err := p.DeletedStore(ctx, "westus", "softstore")
	require.NoError(t, err)
	assert.Equal(t, "softstore", got["name"])
	assert.Equal(t, store.ARMID(), props(got)["configurationStoreId"])

	_, err = p.Put(ctx, store, map[string]any{"location": "westus"})
	require.ErrorIs(t, err, azurearm.ErrAppConfigSoftDeleted)

	require.ErrorIs(t, p.PurgeDeletedStore(ctx, "westus", "softstore"), azurearm.ErrAppConfigPurgeProtected)

	_, err = p.Put(ctx, store, map[string]any{
		"location": "westus", "properties": map[string]any{"createMode": "Recover"},
	})
	require.NoError(t, err)

	_, err = p.DeletedStore(ctx, "westus", "softstore")
	require.ErrorIs(t, err, azurearm.ErrAppConfigDeletedNotFound)

	tests := []struct {
		name      string
		store     string
		available bool
	}{
		{name: "free name", store: "fresh-store", available: true},
		{name: "in use", store: "softstore"},
		{name: "too short", store: "abc"},
		{name: "bad characters", store: "bad_store"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.available, p.CheckName(ctx, tt.store)["nameAvailable"])
		})
	}
}

func TestAppConfigProvider_ReplicasAndKeyValues(t *testing.T) {
	t.Parallel()

	dp := newFakeAppConfig()
	p := azurearm.NewAppConfigProvider(azurearm.AppConfigEndpointConfig{Override: "azconfig.localhost:1"}, dp)
	ctx := t.Context()
	store := acID([]string{"configurationStores"}, "kvstore")

	_, err := p.Put(ctx, acID([]string{"configurationStores", "keyValues"}, "kvstore", "k"), map[string]any{})
	require.ErrorIs(t, err, azurearm.ErrAppConfigStoreNotFound)

	_, err = p.Put(ctx, store, map[string]any{})
	require.NoError(t, err)

	rep := acID([]string{"configurationStores", "replicas"}, "kvstore", "east")
	body, err := p.Put(ctx, rep, map[string]any{"location": "eastus"})
	require.NoError(t, err)
	assert.Equal(t, "https://kvstore-east.azconfig.localhost:1", props(body)["endpoint"])

	reps, err := p.ListReplicas(ctx, store)
	require.NoError(t, err)
	require.Len(t, reps, 1)

	name := azurearm.EscapeKVName("app/color", "prod")
	assert.Equal(t, "app~2Fcolor$prod", name)

	kvID := acID([]string{"configurationStores", "keyValues"}, "kvstore", name)
	out, err := p.Put(ctx, kvID, map[string]any{"properties": map[string]any{
		"value": "red", "contentType": "text/plain", "tags": map[string]any{"t": "v"},
	}})
	require.NoError(t, err)

	pr := props(out)
	assert.Equal(t, "app/color", pr["key"])
	assert.Equal(t, "prod", pr["label"])
	assert.Equal(t, "red", *(pr["value"].(*string)))
	assert.Equal(t, name, out["name"])

	got, err := p.Get(ctx, kvID)
	require.NoError(t, err)
	assert.Equal(t, "etag-app/color", props(got)["eTag"])

	listed, err := p.ListKeyValues(ctx, store)
	require.NoError(t, err)
	require.Len(t, listed, 1)

	direct, err := p.KeyValue(ctx, store, "app/color", "prod")
	require.NoError(t, err)
	assert.Equal(t, "app/color", direct["key"])

	_, err = p.KeyValue(ctx, store, "nope", "")
	require.ErrorIs(t, err, azurearm.ErrAppConfigKeyValueNotFound)

	_, err = p.Put(ctx, acID([]string{"configurationStores", "keyValues"}, "kvstore", "locked"), map[string]any{})
	require.ErrorIs(t, err, azurearm.ErrAppConfigKeyLocked)

	_, err = p.Put(ctx, acID([]string{"configurationStores", "keyValues"}, "kvstore", "bad~zz"), map[string]any{})
	require.ErrorIs(t, err, azurearm.ErrInvalidRequestBody)

	require.NoError(t, p.Delete(ctx, kvID))
	_, err = p.Get(ctx, kvID)
	require.ErrorIs(t, err, azurearm.ErrAppConfigKeyValueNotFound)

	require.NoError(t, p.Delete(ctx, rep))
	require.ErrorIs(t, p.Delete(ctx, rep), azurearm.ErrAppConfigReplicaNotFound)

	_, err = p.Put(ctx, rep, map[string]any{})
	require.NoError(t, err)
	require.NoError(t, p.Delete(ctx, store))

	_, err = p.Get(ctx, rep)
	require.ErrorIs(t, err, azurearm.ErrAppConfigReplicaNotFound, "deleting the store removes its replicas")
}

func TestKVNameCodec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		key   string
		label string
	}{
		{name: "plain", key: "k"},
		{name: "label", key: "k", label: "l"},
		{name: "slash", key: ".appconfig.featureflag/beta"},
		{name: "separators", key: "a$b~c", label: "x/y"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key, label, err := azurearm.DecodeKVName(azurearm.EscapeKVName(tt.key, tt.label))
			require.NoError(t, err)
			assert.Equal(t, tt.key, key)
			assert.Equal(t, tt.label, label)
		})
	}

	_, _, err := azurearm.DecodeKVName("bad~2")
	require.ErrorIs(t, err, azurearm.ErrInvalidRequestBody)
}

func TestAppConfigProvider_PersistenceAndGroupDelete(t *testing.T) {
	t.Parallel()

	dp := newFakeAppConfig()
	p := azurearm.NewAppConfigProvider(azurearm.AppConfigEndpointConfig{}, dp)
	ctx := t.Context()
	store := acID([]string{"configurationStores"}, "persist1")

	_, err := p.Put(ctx, store, map[string]any{"tags": map[string]any{"a": "b"}, "sku": map[string]any{"name": "free"}})
	require.NoError(t, err)
	_, err = p.Put(ctx, acID([]string{"configurationStores", "replicas"}, "persist1", "r"), map[string]any{})
	require.NoError(t, err)
	require.NoError(t, p.Delete(ctx, acID([]string{"configurationStores", "replicas"}, "persist1", "r")))

	_, err = p.Put(ctx, acID([]string{"configurationStores"}, "gone-soon"), map[string]any{"location": "westus"})
	require.NoError(t, err)
	require.NoError(t, p.Delete(ctx, acID([]string{"configurationStores"}, "gone-soon")))

	p2 := azurearm.NewAppConfigProvider(azurearm.AppConfigEndpointConfig{}, dp)
	azurearm.AppConfigRestoreJSON(t, p2, azurearm.AppConfigSnapshotJSON(p))

	got, err := p2.Get(ctx, store)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "b"}, got["tags"])
	assert.EqualValues(t, 0, props(got)["softDeleteRetentionInDays"], "free SKU keeps no soft-delete retention")

	_, err = p2.DeletedStore(ctx, "westus", "gone-soon")
	require.NoError(t, err)

	p2.DeleteResourcesInGroup(ctx, "RG")
	assert.Empty(t, dp.stores)

	p.Reset()
	_, err = p.Get(ctx, store)
	require.ErrorIs(t, err, azurearm.ErrAppConfigStoreNotFound)
}

const acDeletedPath = "/subscriptions/sub/providers/Microsoft.AppConfiguration/locations/local/" +
	"deletedConfigurationStores/mystore"

const (
	acTestBase = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.AppConfiguration/configurationStores/mystore"
	acAPI      = "?api-version=2024-05-01"
)

func newAppConfigHandler(t *testing.T) *azurearm.Handler {
	t.Helper()

	h := azurearm.NewTestHandlerWithAppConfig(newFakeAppConfig(), azurearm.DefaultSettings())

	st, _ := doRequest(t, h, http.MethodPut, "/subscriptions/sub/resourceGroups/rg", []byte(`{"location":"local"}`))
	require.Equal(t, http.StatusCreated, st)

	st, body := doRequest(
		t,
		h,
		http.MethodPut,
		acTestBase+acAPI,
		[]byte(`{"location":"local","sku":{"name":"standard"}}`),
	)
	require.Equal(t, http.StatusOK, st, "%v", body)

	return h
}

func TestAppConfigHandlerRoutes(t *testing.T) {
	t.Parallel()

	kvName := "app~2Fcolor$prod"
	tests := []struct {
		want   any
		name   string
		method string
		path   string
		body   string
		field  string
		status int
	}{
		{name: "list keys", method: http.MethodPost, path: acTestBase + "/listKeys", status: 200},
		{
			name: "regenerate", method: http.MethodPost, path: acTestBase + "/regenerateKey", body: `{"id":"id-rw"}`,
			field: "value", want: "bmV3", status: 200,
		},
		{
			name: "regenerate unknown id", method: http.MethodPost, path: acTestBase + "/regenerateKey",
			body: `{"id":"zzz"}`, status: 400,
		},
		{
			name: "put key value", method: http.MethodPut, path: acTestBase + "/keyValues/" + kvName,
			body: `{"properties":{"value":"red"}}`, field: "name", want: kvName, status: 200,
		},
		{name: "get missing key value", method: http.MethodGet, path: acTestBase + "/keyValues/nope", status: 404},
		{name: "list key values", method: http.MethodGet, path: acTestBase + "/keyValues", status: 200},
		{name: "list replicas", method: http.MethodGet, path: acTestBase + "/replicas", status: 200},
		{
			name: "list key value action", method: http.MethodPost, path: acTestBase + "/listKeyValue",
			body: `{"key":"nope"}`, status: 404,
		},
		{
			name: "check name", method: http.MethodPost,
			path:  "/subscriptions/sub/providers/Microsoft.AppConfiguration/checkNameAvailability",
			body:  `{"name":"mystore","type":"Microsoft.AppConfiguration/configurationStores"}`,
			field: "nameAvailable", want: false, status: 200,
		},
		{
			name:   "deleted missing",
			method: http.MethodGet,
			path:   "/subscriptions/sub/providers/Microsoft.AppConfiguration/locations/local/deletedConfigurationStores/mystore",
			status: 404,
		},
		{
			name: "list deleted", method: http.MethodGet,
			path:   "/subscriptions/sub/providers/Microsoft.AppConfiguration/deletedConfigurationStores",
			status: 200,
		},
		{
			name:   "purge missing",
			method: http.MethodPost,
			path:   acDeletedPath + "/purge",
			status: 404,
		},
		{name: "missing store keys", method: http.MethodPost, path: acTestBase + "x/listKeys", status: 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAppConfigHandler(t)

			status, out := doRequest(t, h, tt.method, tt.path+acAPI, []byte(tt.body))
			require.Equal(t, tt.status, status, "%v", out)

			if tt.field != "" {
				assert.Equal(t, tt.want, out[tt.field])
			}
		})
	}
}

func TestAppConfigHandlerDeleteThenSoftDeleteFlow(t *testing.T) {
	t.Parallel()

	h := newAppConfigHandler(t)
	deleted := acDeletedPath

	st, _ := doRequest(t, h, http.MethodDelete, acTestBase+acAPI, nil)
	assert.Equal(t, http.StatusOK, st)

	st, _ = doRequest(t, h, http.MethodGet, acTestBase+acAPI, nil)
	assert.Equal(t, http.StatusNotFound, st)

	st, body := doRequest(t, h, http.MethodGet, deleted+acAPI, nil)
	require.Equal(t, http.StatusOK, st)
	assert.Equal(t, "mystore", body["name"])

	st, _ = doRequest(t, h, http.MethodPost, deleted+"/purge"+acAPI, nil)
	assert.Equal(t, http.StatusOK, st)

	st, _ = doRequest(t, h, http.MethodGet, deleted+acAPI, nil)
	assert.Equal(t, http.StatusNotFound, st)
}
