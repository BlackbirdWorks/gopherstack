package dashboard_test

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/ocidatabase"
)

const (
	dbComp       = "ocid1.compartment.oc1..dbcomp"
	dbBase       = "/dashboard/api/v1/oci/database"
	dbAPI        = dbBase + "/api"
	dbAdminPass  = "Sup3rSecretPw#Value"
	dbWalletPass = "Sup3rSecretWallet#1"
)

type dbFixture struct {
	h   *dashboard.DashboardHandler
	ops *ocidatabase.Handler
}

func newDBDashboard(t *testing.T) dbFixture {
	t.Helper()

	be := ocidatabase.NewInMemoryBackend()
	be.SetTransition(0)
	t.Cleanup(be.Close)

	ops := ocidatabase.NewHandler(be)

	return dbFixture{h: dashboard.NewHandler(dashboard.Config{OCIDatabaseOps: ops}), ops: ops}
}

func (f dbFixture) direct(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	e.Any("/*", f.ops.Trusted())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	e.ServeHTTP(rec, req)

	return rec
}

func (f dbFixture) proxy(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	return lbDo(t, f.h, method, path, body)
}

func (f dbFixture) settle(t *testing.T, id, state string) {
	t.Helper()

	require.Eventually(t, func() bool {
		f.ops.Backend.Sweep()

		rec := f.direct(t, http.MethodGet, "/20160918/autonomousDatabases/"+id, "")

		var got struct {
			State string `json:"lifecycleState"`
		}

		return json.Unmarshal(rec.Body.Bytes(), &got) == nil && got.State == state
	}, 10*time.Second, 5*time.Millisecond)
}

func (f dbFixture) create(t *testing.T, extra string) string {
	t.Helper()

	rec := f.proxy(t, http.MethodPost, dbAPI+"/autonomousDatabases",
		`{"compartmentId":"`+dbComp+`","displayName":"orders","dbName":"ORDERS","dbWorkload":"OLTP",
		 "adminPassword":"`+dbAdminPass+`","computeModel":"ECPU","computeCount":2,"dataStorageSizeInTBs":1`+extra+`}`)
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

	var db struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &db))
	f.settle(t, db.ID, "AVAILABLE")

	return db.ID
}

func TestOCIDatabaseProxyLifecycle(t *testing.T) {
	t.Parallel()

	f := newDBDashboard(t)
	id := f.create(t, "")
	item := dbAPI + "/autonomousDatabases/" + id

	list := f.proxy(t, http.MethodGet, dbAPI+"/autonomousDatabases?compartmentId="+dbComp, "")
	require.Equal(t, http.StatusOK, list.Code, list.Body.String())
	assert.Contains(t, list.Body.String(), `"dbName":"ORDERS"`)

	for _, step := range []struct{ action, state string }{
		{"stop", "STOPPED"}, {"start", "AVAILABLE"}, {"restart", "AVAILABLE"},
	} {
		got := f.proxy(t, http.MethodPost, item+"/actions/"+step.action, "")
		require.Less(t, got.Code, http.StatusMultipleChoices, "%s: %s", step.action, got.Body.String())
		f.settle(t, id, step.state)
	}

	upd := f.proxy(t, http.MethodPut, item,
		`{"displayName":"orders-renamed","whitelistedIps":["203.0.113.0/24"],"isAccessControlEnabled":true,
		 "computeCount":4,"isFreeTier":true,"adminPassword":"Different#Passw0rd1"}`)
	require.Less(t, upd.Code, http.StatusMultipleChoices, upd.Body.String())
	f.settle(t, id, "AVAILABLE")

	got := f.proxy(t, http.MethodGet, item, "")
	require.Equal(t, http.StatusOK, got.Code)
	assert.Contains(t, got.Body.String(), `"displayName":"orders-renamed"`)
	assert.Contains(t, got.Body.String(), `"203.0.113.0/24"`)
	assert.Contains(t, got.Body.String(), `"computeCount":4`)
	assert.Contains(t, got.Body.String(), `"isFreeTier":false`, "fields outside the allow-list are dropped")

	const longTerm = `,"isLongTermBackup":true,"retentionPeriodInDays":365}`

	bk := f.proxy(t, http.MethodPost, dbAPI+"/autonomousDatabaseBackups",
		`{"autonomousDatabaseId":"`+id+`","displayName":"manual-1"`+longTerm)
	require.Equal(t, http.StatusAccepted, bk.Code, bk.Body.String())
	assert.Contains(t, bk.Body.String(), `"displayName":"manual-1"`)
	assert.NotContains(t, bk.Body.String(), "LONGTERM")

	query := "?compartmentId=" + dbComp + "&autonomousDatabaseId=" + id
	bks := f.proxy(t, http.MethodGet, dbAPI+"/autonomousDatabaseBackups"+query, "")
	require.Equal(t, http.StatusOK, bks.Code, bks.Body.String())
	assert.Contains(t, bks.Body.String(), `"manual-1"`)

	del := f.proxy(t, http.MethodDelete, item, "")
	assert.Less(t, del.Code, http.StatusMultipleChoices, del.Body.String())
}

func TestOCIDatabaseProxyPasswordsNeverEchoed(t *testing.T) {
	t.Parallel()

	f := newDBDashboard(t)
	id := f.create(t, "")
	item := dbAPI + "/autonomousDatabases/" + id

	paths := []struct{ method, path, body string }{
		{http.MethodGet, item, ""},
		{http.MethodGet, dbAPI + "/autonomousDatabases?compartmentId=" + dbComp, ""},
		{http.MethodPost, item + "/actions/stop", ""},
	}

	for _, p := range paths {
		got := f.proxy(t, p.method, p.path, p.body)
		assert.NotContains(t, got.Body.String(), dbAdminPass, "%s %s", p.method, p.path)
		assert.NotContains(t, got.Body.String(), "adminPassword", "%s %s", p.method, p.path)
	}

	create := f.proxy(t, http.MethodPost, dbAPI+"/autonomousDatabases",
		`{"compartmentId":"`+dbComp+`","dbName":"SECOND","dbWorkload":"DW","adminPassword":"`+dbAdminPass+`",
		 "computeCount":2,"dataStorageSizeInTBs":1}`)
	require.Equal(t, http.StatusAccepted, create.Code, create.Body.String())
	assert.NotContains(t, create.Body.String(), dbAdminPass)

	rejected := f.proxy(t, http.MethodPost, dbAPI+"/autonomousDatabases",
		`{"compartmentId":"`+dbComp+`","dbName":"THIRD","dbWorkload":"OLTP","adminPassword":"weak"}`)
	assert.GreaterOrEqual(t, rejected.Code, http.StatusBadRequest)
	assert.NotContains(t, rejected.Body.String(), "weak\"")
}

func TestOCIDatabaseProxyCreateRebuild(t *testing.T) {
	t.Parallel()

	f := newDBDashboard(t)
	id := f.create(t, `,"freeformTags":{"owner":"mallory"},"isDedicated":true,"isDevTier":true,
	 "secretId":"ocid1.secret","source":"BACKUP_FROM_ID","whitelistedIps":["1.2.3.4"],"subnetId":"ocid1.subnet.x"`)

	got := f.direct(t, http.MethodGet, "/20160918/autonomousDatabases/"+id, "")
	require.Equal(t, http.StatusOK, got.Code)
	assert.NotContains(t, got.Body.String(), "mallory")
	assert.NotContains(t, got.Body.String(), "1.2.3.4")
	assert.NotContains(t, got.Body.String(), "ocid1.subnet.x")
}

func TestOCIDatabaseProxyWalletAttachment(t *testing.T) {
	t.Parallel()

	f := newDBDashboard(t)
	id := f.create(t, "")

	got := f.proxy(t, http.MethodPost, dbAPI+"/autonomousDatabases/"+id+"/wallet",
		`{"password":"`+dbWalletPass+`","generateType":"SINGLE","isRegional":true}`)
	require.Equal(t, http.StatusOK, got.Code, got.Body.String())
	assert.Equal(t, "application/zip", got.Header().Get("Content-Type"))
	assert.Equal(t, `attachment; filename="Wallet_ORDERS.zip"`, got.Header().Get("Content-Disposition"))
	assert.Equal(t, "nosniff", got.Header().Get("X-Content-Type-Options"))
	assert.Equal(t, "no-store", got.Header().Get("Cache-Control"))
	assert.Contains(t, got.Header().Get("Content-Security-Policy"), "sandbox")

	zr, err := zip.NewReader(bytes.NewReader(got.Body.Bytes()), int64(got.Body.Len()))
	require.NoError(t, err)

	names := make([]string, 0, len(zr.File))

	for _, zf := range zr.File {
		names = append(names, zf.Name)
	}

	assert.Contains(t, names, "tnsnames.ora")
	assert.Contains(t, names, "ewallet.p12")
	assert.NotContains(t, got.Header().Get("Content-Disposition"), dbWalletPass)
}

func TestOCIDatabaseProxyWalletFailures(t *testing.T) {
	t.Parallel()

	f := newDBDashboard(t)
	id := f.create(t, "")
	wallet := dbAPI + "/autonomousDatabases/" + id + "/wallet"

	weak := f.proxy(t, http.MethodPost, wallet, `{"password":"short1!"}`)
	assert.Equal(t, http.StatusBadRequest, weak.Code, weak.Body.String())
	assert.NotContains(t, weak.Body.String(), "short1!")
	assert.Empty(t, weak.Header().Get("Content-Disposition"))

	for _, body := range []string{`{}`, `{"password":""}`, `{`, `{"password":"` + strings.Repeat("a1!", 100) + `"}`} {
		got := f.proxy(t, http.MethodPost, wallet, body)
		assert.Equal(t, http.StatusBadRequest, got.Code, body)
	}

	missing := f.proxy(t, http.MethodPost, dbAPI+"/autonomousDatabases/ocid1.nope/wallet",
		`{"password":"`+dbWalletPass+`"}`)
	assert.Equal(t, http.StatusNotFound, missing.Code, missing.Body.String())
	assert.NotContains(t, missing.Body.String(), dbWalletPass)

	assert.Equal(t, http.StatusBadRequest, f.proxy(t, http.MethodGet, wallet, "").Code)
	assert.Equal(t, http.StatusBadRequest, f.proxy(t, http.MethodPut, wallet, `{"shouldRotate":true}`).Code)
}

func TestOCIDatabaseProxyRejectsOutsideAllowList(t *testing.T) {
	t.Parallel()

	f := newDBDashboard(t)
	item := dbAPI + "/autonomousDatabases/ocid1.db"
	valid := `{"compartmentId":"` + dbComp + `","dbName":"X","dbWorkload":"OLTP","adminPassword":"` + dbAdminPass + `"}`

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"restore", http.MethodPost, item + "/actions/restore", `{"latest":true}`},
		{"change compartment", http.MethodPost, item + "/actions/changeCompartment", `{"compartmentId":"x"}`},
		{"generate wallet action", http.MethodPost, item + "/actions/generateWallet", `{"password":"abcdefg1!"}`},
		{"unknown action", http.MethodPost, item + "/actions/terminate", ""},
		{"rotate wallet", http.MethodPut, item + "/wallet", `{"shouldRotate":true}`},
		{"delete backup", http.MethodDelete, dbAPI + "/autonomousDatabaseBackups/b1", ""},
		{"get backup", http.MethodGet, dbAPI + "/autonomousDatabaseBackups/b1", ""},
		{"work requests", http.MethodGet, dbAPI + "/workRequests", ""},
		{"patch database", http.MethodPatch, item, `{}`},
		{"unknown collection", http.MethodGet, dbAPI + "/cloudExadataInfrastructures", ""},
		{"traversal", http.MethodGet, dbAPI + "/autonomousDatabases/%2e%2e", ""},
		{"encoded slash", http.MethodGet, dbAPI + "/autonomousDatabases/a%2Fb", ""},
		{"admin path", http.MethodPost, dbAPI + "/_gopherstack/keys", `{}`},
		{"too deep", http.MethodGet, item + "/a/b/c", ""},
		{"bad db name", http.MethodPost, dbAPI + "/autonomousDatabases", strings.Replace(valid, `"X"`, `"1 bad"`, 1)},
		{"bad workload", http.MethodPost, dbAPI + "/autonomousDatabases", strings.Replace(valid, `"OLTP"`, `"EXA"`, 1)},
		{
			"no password", http.MethodPost, dbAPI + "/autonomousDatabases",
			`{"compartmentId":"` + dbComp + `","dbName":"X","dbWorkload":"OLTP"}`,
		},
		{"empty update", http.MethodPut, item, `{"freeformTags":{"a":"b"}}`},
		{"bad acl", http.MethodPut, item, `{"whitelistedIps":["a b"]}`},
		{"empty display name", http.MethodPut, item, `{"displayName":""}`},
		{"backup without db", http.MethodPost, dbAPI + "/autonomousDatabaseBackups", `{"displayName":"x"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := f.proxy(t, tt.method, tt.path, tt.body)
			assert.Contains(t, []int{http.StatusBadRequest, http.StatusMethodNotAllowed}, got.Code, got.Body.String())
		})
	}
}

func TestOCIDatabaseProxyReadRoutes(t *testing.T) {
	t.Parallel()

	f := newDBDashboard(t)
	f.create(t, "")

	comps := f.proxy(t, http.MethodGet, dbBase+"/compartments", "")
	require.Equal(t, http.StatusOK, comps.Code)
	assert.Contains(t, comps.Body.String(), dbComp)

	engine := f.proxy(t, http.MethodGet, dbBase+"/engine", "")
	require.Equal(t, http.StatusOK, engine.Code)
	assert.Contains(t, engine.Body.String(), `"engine":"stub"`)
}

func TestOCIDatabaseProxyUnavailableWithoutService(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	for _, p := range []struct{ method, path string }{
		{http.MethodGet, dbBase + "/compartments"},
		{http.MethodGet, dbBase + "/engine"},
		{http.MethodGet, dbAPI + "/autonomousDatabases?compartmentId=" + dbComp},
		{http.MethodPost, dbAPI + "/autonomousDatabases/x/wallet"},
	} {
		got := lbDo(t, h, p.method, p.path, `{}`)
		assert.Equal(t, http.StatusServiceUnavailable, got.Code, "%s %s", p.method, p.path)
	}
}
