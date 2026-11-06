package dashboard_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/azureappconfig"
)

const (
	acPxBase = "/dashboard/api/v1/azure/appconfig/stores/store1"
	acFlag   = `{"id":"beta","description":"Beta UI","enabled":false,"conditions":{"client_filters":[{"name":"Microsoft.Percentage","parameters":{"Value":50}}]}}`
)

func newAppConfigEnv(t *testing.T) (*dashboard.DashboardHandler, string) {
	t.Helper()

	b := azureappconfig.NewInMemoryBackend()
	_, err := b.PutStore("store1")
	require.NoError(t, err)

	set := func(key, label, value string, ct *string) {
		_, setErr := b.SetKeyValue("store1", key, label, azureappconfig.KVInput{Value: &value, ContentType: ct}, azureappconfig.Precondition{})
		require.NoError(t, setErr)
	}

	ff := "application/vnd.microsoft.appconfig.ff+json;charset=utf-8"
	set("app/key", "", "v1", nil)
	set("app/key", "", "v2", nil)
	set(".appconfig.featureflag/beta", "prod", acFlag, &ff)
	set(".appconfig.featureflag/gamma", "", `{"id":"gamma","enabled":true}`, &ff)

	_, err = b.CreateSnapshot("store1", "snap1", azureappconfig.SnapshotSpec{
		Filters: []azureappconfig.Filter{{Key: "app/*"}}, Composition: "key",
	})
	require.NoError(t, err)

	keys, err := b.AccessKeys("store1")
	require.NoError(t, err)

	return dashboard.NewHandler(dashboard.Config{AzureAppConfigOps: azureappconfig.NewHandler(b, azureappconfig.Settings{})}), keys[0].ID
}

func TestAzureAppConfigExtraReads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		wantBody []string
		wantCode int
	}{
		{name: "snapshots", path: acPxBase + "/snapshots", wantCode: 200, wantBody: []string{`"snap1"`, `"itemCount":1`}},
		{name: "snapshot items", path: acPxBase + "/snapshots/snap1/kv", wantCode: 200, wantBody: []string{`"app/key"`, `"v2"`}},
		{name: "missing snapshot", path: acPxBase + "/snapshots/nope/kv", wantCode: 404, wantBody: []string{"NOT_FOUND"}},
		{name: "revisions newest first", path: acPxBase + "/revisions?key=app/key", wantCode: 200, wantBody: []string{`"v2"`, `"v1"`}},
		{name: "features", path: acPxBase + "/features", wantCode: 200, wantBody: []string{`"id":"beta"`, `"description":"Beta UI"`, `"filterCount":1`, `"id":"gamma"`}},
		{name: "missing store", path: "/dashboard/api/v1/azure/appconfig/stores/nope/features", wantCode: 404, wantBody: []string{"NOT_FOUND"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newAppConfigEnv(t)
			rec := doProxy(t, h, http.MethodGet, tt.path, "")

			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			for _, w := range tt.wantBody {
				assert.Contains(t, rec.Body.String(), w)
			}
		})
	}
}

func TestAzureAppConfigRevisionOrder(t *testing.T) {
	t.Parallel()

	h, _ := newAppConfigEnv(t)
	rec := doProxy(t, h, http.MethodGet, acPxBase+"/revisions?key=app/key", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var body struct {
		Items []struct {
			Value string `json:"value"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Items, 2)
	assert.Equal(t, "v2", body.Items[0].Value)
}

func TestAzureAppConfigToggleKeepsDocument(t *testing.T) {
	t.Parallel()

	h, _ := newAppConfigEnv(t)

	rec := doProxy(t, h, http.MethodPost, acPxBase+"/features/toggle", `{"id":"beta","label":"prod","enabled":true,"value":"IGNORED","key":"app/key"}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = doProxy(t, h, http.MethodGet, acPxBase+"/features", "")
	assert.Contains(t, rec.Body.String(), `"enabled":true`)
	assert.Contains(t, rec.Body.String(), `"description":"Beta UI"`)
	assert.Contains(t, rec.Body.String(), `"filterCount":1`)

	rec = doProxy(t, h, http.MethodGet, acPxBase+"/kv?key=app/key", "")
	assert.NotContains(t, rec.Body.String(), "IGNORED")

	rec = doProxy(t, h, http.MethodGet, acPxBase+"/kv?key=.appconfig.featureflag/beta", "")
	assert.Contains(t, rec.Body.String(), "Microsoft.Percentage")
	assert.NotContains(t, rec.Body.String(), "IGNORED")
}

func TestAzureAppConfigToggleRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantBody string
		wantCode int
	}{
		{name: "missing id", body: `{"enabled":true}`, wantCode: 400, wantBody: "invalid feature flag id"},
		{name: "unknown flag", body: `{"id":"ghost","enabled":true}`, wantCode: 404, wantBody: "NOT_FOUND"},
		{name: "wrong label", body: `{"id":"beta","label":"dev","enabled":true}`, wantCode: 404, wantBody: "NOT_FOUND"},
		{name: "bad json", body: `{`, wantCode: 400, wantBody: "invalid request body"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newAppConfigEnv(t)
			rec := doProxy(t, h, http.MethodPost, acPxBase+"/features/toggle", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestAzureAppConfigKeysMaskedAndReveal(t *testing.T) {
	t.Parallel()

	h, id := newAppConfigEnv(t)

	rec := doProxy(t, h, http.MethodGet, acPxBase+"/keys", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), id)
	assert.NotContains(t, rec.Body.String(), "secret")

	rec = doProxy(t, h, http.MethodPost, acPxBase+"/keys/reveal", `{"id":"`+id+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	assert.Contains(t, rec.Body.String(), `"secret":"`)

	rec = doProxy(t, h, http.MethodPost, acPxBase+"/keys/reveal", `{"id":"nope"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = doProxy(t, h, http.MethodGet, acPxBase+"/keys/reveal", "")
	assert.NotContains(t, rec.Body.String(), `"secret"`)
}

func TestAzureAppConfigExtraAbsent(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	for _, p := range []string{"/snapshots", "/features", "/keys", "/revisions"} {
		rec := doProxy(t, h, http.MethodGet, acPxBase+p, "")
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code, p)
	}
}
