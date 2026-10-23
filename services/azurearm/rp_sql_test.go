package azurearm_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const (
	sqlBase = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg1/providers/Microsoft.Sql/servers/"
	sqlAPI  = "?api-version=2023-08-01-preview"
	sqlPass = "Passw0rd!x"
)

var errSQLBoom = errors.New("boom")

type fakeSQL struct {
	servers map[string]azurearm.SQLSpec
	dbs     map[string]map[string]string
	fail    string
	mu      sync.Mutex
}

func newFakeSQL() *fakeSQL {
	return &fakeSQL{servers: map[string]azurearm.SQLSpec{}, dbs: map[string]map[string]string{}}
}

func (f *fakeSQL) EnsureServer(id string, spec azurearm.SQLSpec) (azurearm.SQLEndpoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.fail == "server" {
		return azurearm.SQLEndpoint{}, errSQLBoom
	}

	if spec.AdminPassword == "" {
		spec.AdminPassword = f.servers[id].AdminPassword
	}

	f.servers[id] = spec
	if f.dbs[id] == nil {
		f.dbs[id] = map[string]string{}
	}

	return azurearm.SQLEndpoint{Host: "127.0.0.1", Port: 14330}, nil
}

func (f *fakeSQL) Endpoint(id string) (azurearm.SQLEndpoint, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.servers[id]; !ok {
		return azurearm.SQLEndpoint{}, false
	}

	return azurearm.SQLEndpoint{Host: "127.0.0.1", Port: 14330}, true
}

func (f *fakeSQL) DeleteServer(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.servers, id)
	delete(f.dbs, id)

	return nil
}

func (f *fakeSQL) EnsureDatabase(serverID, name, collation string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.fail == "database" {
		return errSQLBoom
	}

	f.dbs[serverID][name] = collation

	return nil
}

func (f *fakeSQL) DropDatabase(serverID, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.dbs[serverID], name)

	return nil
}

type sqlEnv struct {
	h  *azurearm.Handler
	p  *azurearm.SQLProvider
	dp *fakeSQL
}

func newSQLEnv() *sqlEnv {
	dp := newFakeSQL()
	h, p := azurearm.NewTestHandlerWithSQL(dp)

	return &sqlEnv{h: h, p: p, dp: dp}
}

func (e *sqlEnv) do(t *testing.T, method, path string, body any) (int, map[string]any, http.Header) {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	if body == nil {
		raw = nil
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(raw))
	req.Host = "host:10006"
	rec := httptest.NewRecorder()
	newEchoServer(e.h).ServeHTTP(rec, req)

	var out map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}

	return rec.Code, out, rec.Header()
}

func sqlServerBody(login, password string, extra map[string]any) map[string]any {
	props := map[string]any{"administratorLogin": login, "administratorLoginPassword": password}
	maps.Copy(props, extra)

	return map[string]any{"location": "local", "properties": props}
}

func (e *sqlEnv) createServer(t *testing.T, name string) {
	t.Helper()

	status, body, _ := e.do(t, http.MethodPut, sqlBase+name+sqlAPI, sqlServerBody("sqladmin", sqlPass, nil))
	require.Equal(t, http.StatusCreated, status, body)
}

func TestSQLServerLifecycle(t *testing.T) {
	t.Parallel()

	e := newSQLEnv()
	status, body, hdr := e.do(t, http.MethodPut, sqlBase+"srv1"+sqlAPI, sqlServerBody("sqladmin", sqlPass, nil))
	require.Equal(t, http.StatusCreated, status, body)

	props := propsOf(body)
	assert.Equal(t, "Microsoft.Sql/servers", body["type"])
	assert.Equal(t, "v12.0", body["kind"])
	assert.Equal(t, "12.0", props["version"])
	assert.Equal(t, "Ready", props["state"])
	assert.Equal(t, "srv1.database.windows.net", props["fullyQualifiedDomainName"])
	assert.Equal(t, "sqladmin", props["administratorLogin"])
	assert.Equal(t, "127.0.0.1:14330", props["gopherstackEndpoint"])
	assert.Equal(t, "1.2", props["minimalTlsVersion"])
	assert.Equal(t, "Enabled", props["publicNetworkAccess"])
	assert.NotContains(t, props, "administratorLoginPassword")

	opURL, err := url.Parse(hdr.Get("Azure-Asyncoperation"))
	require.NoError(t, err)
	assert.Contains(t, opURL.Path, "/providers/Microsoft.Sql/locations/local/serverAzureAsyncOperation/")

	st, op, _ := e.do(t, http.MethodGet, opURL.RequestURI(), nil)
	require.Equal(t, http.StatusOK, st)
	assert.Equal(t, "Succeeded", op["status"])

	status, body, _ = e.do(t, http.MethodPut, sqlBase+"srv1"+sqlAPI, sqlServerBody("sqladmin", sqlPass, nil))
	assert.Equal(t, http.StatusOK, status, body)

	status, _, _ = e.do(t, http.MethodGet, sqlBase+"srv1"+sqlAPI, nil)
	assert.Equal(t, http.StatusOK, status)

	status, _, hdr = e.do(t, http.MethodDelete, sqlBase+"srv1"+sqlAPI, nil)
	assert.Equal(t, http.StatusAccepted, status)
	assert.NotEmpty(t, hdr.Get("Azure-Asyncoperation"))

	status, body, _ = e.do(t, http.MethodGet, sqlBase+"srv1"+sqlAPI, nil)
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "ResourceNotFound", errCode(body))
	assert.Empty(t, e.dp.servers)

	status, _, _ = e.do(t, http.MethodDelete, sqlBase+"srv1"+sqlAPI, nil)
	assert.Equal(t, http.StatusNoContent, status, "deleting a missing server is a no-op")
}

func errCode(body map[string]any) string {
	e, _ := body["error"].(map[string]any)
	c, _ := e["code"].(string)

	return c
}

func TestSQLServerValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		path     string
		wantCode string
		status   int
	}{
		{
			name: "weak password", path: "s1", body: sqlServerBody("sqladmin", "password", nil),
			status: http.StatusBadRequest, wantCode: "PasswordNotComplex",
		},
		{
			name: "short password", path: "s1", body: sqlServerBody("sqladmin", "Ab1!", nil),
			status: http.StatusBadRequest, wantCode: "PasswordNotComplex",
		},
		{
			name: "password contains login", path: "s1", body: sqlServerBody("sqladmin", "Xsqladmin1!", nil),
			status: http.StatusBadRequest, wantCode: "PasswordNotComplex",
		},
		{
			name: "missing password", path: "s1",
			body:   map[string]any{"location": "local", "properties": map[string]any{"administratorLogin": "sqladmin"}},
			status: http.StatusBadRequest, wantCode: "InvalidParameterValue",
		},
		{
			name: "reserved login", path: "s1", body: sqlServerBody("admin", sqlPass, nil),
			status: http.StatusBadRequest, wantCode: "InvalidParameterValue",
		},
		{
			name: "uppercase name", path: "Srv", body: sqlServerBody("sqladmin", sqlPass, nil),
			status: http.StatusBadRequest, wantCode: "InvalidParameterValue",
		},
		{
			name:     "bad tls",
			path:     "s1",
			body:     sqlServerBody("sqladmin", sqlPass, map[string]any{"minimalTlsVersion": "0.9"}),
			status:   http.StatusBadRequest,
			wantCode: "InvalidParameterValue",
		},
		{
			name: "bad version", path: "s1", body: sqlServerBody("sqladmin", sqlPass, map[string]any{"version": "2.0"}),
			status: http.StatusBadRequest, wantCode: "InvalidParameterValue",
		},
		{
			name: "entra only server needs no login", path: "s1",
			body:   map[string]any{"location": "local", "properties": map[string]any{"minimalTlsVersion": "1.0"}},
			status: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newSQLEnv()
			status, body, _ := e.do(t, http.MethodPut, sqlBase+tt.path+sqlAPI, tt.body)
			assert.Equal(t, tt.status, status, body)

			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, errCode(body))
				assert.Empty(t, e.dp.servers, "a refused request never reaches the engine")
			}
		})
	}
}

func TestSQLNameTakenAcrossGroupsAndEngineFailure(t *testing.T) {
	t.Parallel()

	e := newSQLEnv()
	e.createServer(t, "shared")

	other := strings.Replace(sqlBase, "rg1", "rg2", 1)
	status, body, _ := e.do(t, http.MethodPut, other+"shared"+sqlAPI, sqlServerBody("sqladmin", sqlPass, nil))
	assert.Equal(t, http.StatusConflict, status, body)

	e.dp.fail = "server"
	status, body, _ = e.do(t, http.MethodPut, sqlBase+"fresh"+sqlAPI, sqlServerBody("sqladmin", sqlPass, nil))
	assert.Equal(t, http.StatusServiceUnavailable, status, body)

	status, _, _ = e.do(t, http.MethodGet, sqlBase+"fresh"+sqlAPI, nil)
	assert.Equal(t, http.StatusNotFound, status, "a server whose engine failed is not recorded")
}

func TestSQLServerPatchAndList(t *testing.T) {
	t.Parallel()

	e := newSQLEnv()
	e.createServer(t, "a")
	e.createServer(t, "b")

	status, body, _ := e.do(t, http.MethodPatch, sqlBase+"a"+sqlAPI, map[string]any{
		"tags": map[string]any{"env": "t"},
		"properties": map[string]any{
			"administratorLoginPassword": "N3wPassw0rd!", "publicNetworkAccess": "Disabled",
		},
	})
	require.Equal(t, http.StatusOK, status, body)
	assert.Equal(t, "Disabled", propsOf(body)["publicNetworkAccess"])
	assert.Equal(t, "sqladmin", propsOf(body)["administratorLogin"], "PATCH keeps what it does not set")
	assert.Equal(t, "N3wPassw0rd!", e.dp.servers[strings.ToLower(sqlBase)+"a"].AdminPassword)

	status, _, _ = e.do(t, http.MethodPatch, sqlBase+"missing"+sqlAPI, map[string]any{})
	assert.Equal(t, http.StatusNotFound, status)

	status, list, _ := e.do(t, http.MethodGet, strings.TrimSuffix(sqlBase, "/")+sqlAPI, nil)
	require.Equal(t, http.StatusOK, status)

	values, _ := list["value"].([]any)
	require.Len(t, values, 2)
	assert.Equal(t, "a", values[0].(map[string]any)["name"])
}

func (e *sqlEnv) putDB(t *testing.T, server, name string, body map[string]any) (int, map[string]any) {
	t.Helper()

	status, out, _ := e.do(t, http.MethodPut, sqlBase+server+"/databases/"+name+sqlAPI, body)

	return status, out
}

func TestSQLDatabaseLifecycle(t *testing.T) {
	t.Parallel()

	e := newSQLEnv()
	e.createServer(t, "srv")

	status, body := e.putDB(t, "srv", "app", map[string]any{
		"location": "local",
		"sku":      map[string]any{"name": "GP_S_Gen5_2", "tier": "GeneralPurpose"},
		"properties": map[string]any{
			"collation": "Latin1_General_CI_AS", "maxSizeBytes": 1073741824, "zoneRedundant": true,
		},
	})
	require.Equal(t, http.StatusCreated, status, body)

	props := propsOf(body)
	assert.Equal(t, "Microsoft.Sql/servers/databases", body["type"])
	assert.Equal(t, "Latin1_General_CI_AS", props["collation"])
	assert.EqualValues(t, 1073741824, props["maxSizeBytes"])
	assert.Equal(t, "Online", props["status"])
	assert.Equal(t, "GP_S_Gen5_2", props["currentServiceObjectiveName"])
	assert.Equal(t, true, props["zoneRedundant"], "unmodelled properties round-trip")
	assert.NotEmpty(t, props["databaseId"])
	assert.Equal(t, "GP_S_Gen5_2", body["sku"].(map[string]any)["name"])

	key := strings.ToLower(sqlBase) + "srv"
	assert.Equal(t, map[string]string{"app": "Latin1_General_CI_AS"}, e.dp.dbs[key])

	status, again := e.putDB(t, "srv", "app", map[string]any{"location": "local", "properties": map[string]any{
		"collation": "Latin1_General_CI_AS",
	}})
	require.Equal(t, http.StatusOK, status, again)
	assert.Equal(t, props["databaseId"], propsOf(again)["databaseId"], "an update keeps the database ID")

	status, body = e.putDB(t, "srv", "app", map[string]any{"properties": map[string]any{"collation": "Other_CI_AS"}})
	assert.Equal(t, http.StatusBadRequest, status, body)

	status, list, _ := e.do(t, http.MethodGet, sqlBase+"srv/databases"+sqlAPI, nil)
	require.Equal(t, http.StatusOK, status)
	assert.Len(t, list["value"], 1)

	status, _, _ = e.do(t, http.MethodDelete, sqlBase+"srv/databases/app"+sqlAPI, nil)
	assert.Equal(t, http.StatusAccepted, status)
	assert.Empty(t, e.dp.dbs[key])

	status, _, _ = e.do(t, http.MethodDelete, sqlBase+"srv/databases/app"+sqlAPI, nil)
	assert.Equal(t, http.StatusNoContent, status)
}

func TestSQLDatabaseValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		server string
		db     string
		status int
	}{
		{name: "no server", server: "nope", db: "d", status: http.StatusNotFound, body: map[string]any{}},
		{name: "reserved name", server: "srv", db: "master", status: http.StatusBadRequest, body: map[string]any{}},
		{name: "bad character", server: "srv", db: "a%25b", status: http.StatusBadRequest, body: map[string]any{}},
		{
			name: "restore mode", server: "srv", db: "d", status: http.StatusBadRequest,
			body: map[string]any{"properties": map[string]any{"createMode": "Restore"}},
		},
		{
			name: "missing pool", server: "srv", db: "d", status: http.StatusNotFound,
			body: map[string]any{"properties": map[string]any{"elasticPoolId": sqlBase + "srv/elasticPools/none"}},
		},
		{
			name:   "default sku",
			server: "srv",
			db:     "d",
			status: http.StatusCreated,
			body:   map[string]any{"location": "local"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newSQLEnv()
			e.createServer(t, "srv")

			status, body := e.putDB(t, tt.server, tt.db, tt.body)
			assert.Equal(t, tt.status, status, body)
		})
	}
}

func TestSQLDatabaseEngineFailure(t *testing.T) {
	t.Parallel()

	e := newSQLEnv()
	e.createServer(t, "srv")
	e.dp.fail = "database"

	status, body := e.putDB(t, "srv", "app", map[string]any{"location": "local"})
	assert.Equal(t, http.StatusServiceUnavailable, status, body)

	status, _, _ = e.do(t, http.MethodGet, sqlBase+"srv/databases/app"+sqlAPI, nil)
	assert.Equal(t, http.StatusNotFound, status)
}

func TestSQLChildren(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		path       string
		wantType   string
		wantStatus int
	}{
		{
			name: "firewall rule", path: "srv/firewallRules/office", wantStatus: http.StatusOK,
			wantType: "Microsoft.Sql/servers/firewallRules",
			body: map[string]any{
				"properties": map[string]any{"startIpAddress": "10.0.0.1", "endIpAddress": "10.0.0.9"},
			},
		},
		{
			name: "azure services rule", path: "srv/firewallRules/AllowAllWindowsAzureIps", wantStatus: http.StatusOK,
			wantType: "Microsoft.Sql/servers/firewallRules",
			body: map[string]any{
				"properties": map[string]any{"startIpAddress": "0.0.0.0", "endIpAddress": "0.0.0.0"},
			},
		},
		{
			name: "azure services rule must be zero", path: "srv/firewallRules/AllowAllWindowsAzureIps",
			wantStatus: http.StatusBadRequest,
			body: map[string]any{
				"properties": map[string]any{"startIpAddress": "1.0.0.0", "endIpAddress": "1.0.0.1"},
			},
		},
		{
			name: "reversed range", path: "srv/firewallRules/bad", wantStatus: http.StatusBadRequest,
			body: map[string]any{
				"properties": map[string]any{"startIpAddress": "10.0.0.9", "endIpAddress": "10.0.0.1"},
			},
		},
		{
			name: "entra administrator", path: "srv/administrators/ActiveDirectory", wantStatus: http.StatusOK,
			wantType: "Microsoft.Sql/servers/administrators",
			body: map[string]any{"properties": map[string]any{
				"administratorType": "ActiveDirectory", "login": "dba@example.com",
				"sid": "11111111-1111-1111-1111-111111111111", "tenantId": "22222222-2222-2222-2222-222222222222",
			}},
		},
		{
			name: "administrator needs a guid", path: "srv/administrators/ActiveDirectory",
			wantStatus: http.StatusBadRequest,
			body:       map[string]any{"properties": map[string]any{"login": "dba", "sid": "x"}},
		},
		{
			name: "administrator name", path: "srv/administrators/other", wantStatus: http.StatusBadRequest,
			body: map[string]any{"properties": map[string]any{}},
		},
		{
			name: "elastic pool", path: "srv/elasticPools/pool1", wantStatus: http.StatusCreated,
			wantType: "Microsoft.Sql/servers/elasticPools",
			body: map[string]any{
				"location": "local", "sku": map[string]any{"name": "GP_Gen5", "tier": "GeneralPurpose", "capacity": 2},
			},
		},
		{
			name: "audit settings", path: "srv/auditingSettings/default", wantStatus: http.StatusOK,
			wantType: "Microsoft.Sql/servers/auditingSettings",
			body:     map[string]any{"properties": map[string]any{"state": "Enabled", "retentionDays": 30}},
		},
		{
			name: "setting name", path: "srv/auditingSettings/other", wantStatus: http.StatusBadRequest,
			body: map[string]any{"properties": map[string]any{}},
		},
		{
			name: "tde", path: "srv/databases/app/transparentDataEncryption/current", wantStatus: http.StatusOK,
			wantType: "Microsoft.Sql/servers/databases/transparentDataEncryption",
			body:     map[string]any{"properties": map[string]any{"state": "Disabled"}},
		},
		{
			name: "tde state", path: "srv/databases/app/transparentDataEncryption/current",
			wantStatus: http.StatusBadRequest, body: map[string]any{"properties": map[string]any{"state": "Maybe"}},
		},
		{
			name: "unknown child type", path: "srv/bogus/x", wantStatus: http.StatusNotFound,
			body: map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newSQLEnv()
			e.createServer(t, "srv")
			status, _ := e.putDB(t, "srv", "app", map[string]any{"location": "local"})
			require.Equal(t, http.StatusCreated, status)

			status, body, _ := e.do(t, http.MethodPut, sqlBase+tt.path+sqlAPI, tt.body)
			require.Equal(t, tt.wantStatus, status, body)

			if tt.wantType == "" {
				return
			}

			assert.Equal(t, tt.wantType, body["type"])

			status, got, _ := e.do(t, http.MethodGet, sqlBase+tt.path+sqlAPI, nil)
			require.Equal(t, http.StatusOK, status)
			assert.Equal(t, body["properties"], got["properties"])
		})
	}
}

func TestSQLSettingDefaultsAndDelete(t *testing.T) {
	t.Parallel()

	e := newSQLEnv()
	e.createServer(t, "srv")

	status, _ := e.putDB(t, "srv", "app", map[string]any{"location": "local"})
	require.Equal(t, http.StatusCreated, status)

	tde := sqlBase + "srv/databases/app/transparentDataEncryption/current" + sqlAPI
	_, body, _ := e.do(t, http.MethodGet, tde, nil)
	assert.Equal(t, "Enabled", propsOf(body)["state"], "a new database has TDE on")

	_, _, _ = e.do(t, http.MethodPut, tde, map[string]any{"properties": map[string]any{"state": "Disabled"}})
	_, body, _ = e.do(t, http.MethodGet, tde, nil)
	assert.Equal(t, "Disabled", propsOf(body)["state"])

	_, _, _ = e.do(t, http.MethodDelete, tde, nil)
	_, body, _ = e.do(t, http.MethodGet, tde, nil)
	assert.Equal(t, "Enabled", propsOf(body)["state"], "delete restores the default")

	status, list, _ := e.do(t, http.MethodGet, sqlBase+"srv/databases/app/transparentDataEncryption"+sqlAPI, nil)
	require.Equal(t, http.StatusOK, status)
	assert.Len(t, list["value"], 1)

	status, _, _ = e.do(t, http.MethodGet, sqlBase+"srv/databases/none/transparentDataEncryption/current"+sqlAPI, nil)
	assert.Equal(t, http.StatusNotFound, status)
}

func TestSQLChildListsAndPoolInUse(t *testing.T) {
	t.Parallel()

	e := newSQLEnv()
	e.createServer(t, "srv")

	_, _, _ = e.do(t, http.MethodPut, sqlBase+"srv/elasticPools/p1"+sqlAPI, map[string]any{"location": "local"})
	_, _, _ = e.do(t, http.MethodPut, sqlBase+"srv/firewallRules/r1"+sqlAPI,
		map[string]any{"properties": map[string]any{"startIpAddress": "1.1.1.1", "endIpAddress": "1.1.1.1"}})

	status, body := e.putDB(t, "srv", "pooled", map[string]any{
		"location":   "local",
		"properties": map[string]any{"elasticPoolId": sqlBase + "srv/elasticPools/p1"},
	})
	require.Equal(t, http.StatusCreated, status, body)

	for _, typ := range []string{"elasticPools", "firewallRules", "databases", "administrators", "auditingSettings"} {
		st, list, _ := e.do(t, http.MethodGet, sqlBase+"srv/"+typ+sqlAPI, nil)
		require.Equal(t, http.StatusOK, st, typ)
		assert.NotNil(t, list["value"], typ)
	}

	status, _, _ = e.do(t, http.MethodDelete, sqlBase+"srv/elasticPools/p1"+sqlAPI, nil)
	assert.Equal(t, http.StatusConflict, status, "a pool with databases cannot be deleted")

	_, _, _ = e.do(t, http.MethodDelete, sqlBase+"srv/databases/pooled"+sqlAPI, nil)
	status, _, _ = e.do(t, http.MethodDelete, sqlBase+"srv/elasticPools/p1"+sqlAPI, nil)
	assert.Equal(t, http.StatusAccepted, status)

	status, _, _ = e.do(t, http.MethodGet, sqlBase+"missing/databases"+sqlAPI, nil)
	assert.Equal(t, http.StatusNotFound, status)
}

func TestSQLServerDeleteCascadesAndResourceGroup(t *testing.T) {
	t.Parallel()

	e := newSQLEnv()
	e.createServer(t, "srv")

	status, _ := e.putDB(t, "srv", "app", map[string]any{"location": "local"})
	require.Equal(t, http.StatusCreated, status)

	e.p.DeleteResourcesInGroup(t.Context(), "RG1")
	assert.Empty(t, e.dp.servers, "deleting the resource group stops the engine")

	status, _, _ = e.do(t, http.MethodGet, sqlBase+"srv/databases/app"+sqlAPI, nil)
	assert.Equal(t, http.StatusNotFound, status)
}

func TestSQLOperationUnknown(t *testing.T) {
	t.Parallel()

	e := newSQLEnv()
	path := "/subscriptions/00000000-0000-0000-0000-000000000000/providers/Microsoft.Sql/locations/local/"
	status, _, _ := e.do(t, http.MethodGet, path+"databaseAzureAsyncOperation/nope"+sqlAPI, nil)
	assert.Equal(t, http.StatusNotFound, status)
}

func TestSQLSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newSQLEnv()
	e.createServer(t, "srv")

	status, _ := e.putDB(t, "srv", "app", map[string]any{"location": "local"})
	require.Equal(t, http.StatusCreated, status)

	_, _, _ = e.do(t, http.MethodPut, sqlBase+"srv/firewallRules/r1"+sqlAPI,
		map[string]any{"properties": map[string]any{"startIpAddress": "1.1.1.1", "endIpAddress": "1.1.1.2"}})

	other := newSQLEnv()
	azurearm.SQLRestoreJSON(t, other.p, azurearm.SQLSnapshotJSON(e.p))

	status, body, _ := other.do(t, http.MethodGet, sqlBase+"srv/databases/app"+sqlAPI, nil)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "app", body["name"])

	status, body, _ = other.do(t, http.MethodGet, sqlBase+"srv/firewallRules/r1"+sqlAPI, nil)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "1.1.1.2", propsOf(body)["endIpAddress"])

	azurearm.SQLRestoreJSON(t, other.p, []byte(`{}`))
	status, _, _ = other.do(t, http.MethodGet, sqlBase+"srv"+sqlAPI, nil)
	assert.Equal(t, http.StatusNotFound, status)
}

func TestSQLProviderRegistration(t *testing.T) {
	t.Parallel()

	_, p := azurearm.NewTestHandlerWithSQL(nil)
	assert.Equal(t, "Microsoft.Sql", p.Namespace())
	assert.NotEmpty(t, p.ResourceTypes())

	_, err := p.ListKeys(t.Context(), azurearm.ResourceID{})
	require.ErrorIs(t, err, azurearm.ErrResourceNotFound)
}
