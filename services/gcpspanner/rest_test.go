package gcpspanner_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func restDo(t *testing.T, base, method, path, body string) (int, map[string]any) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, base+path, strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	out := map[string]any{}
	if len(raw) > 0 {
		require.NoError(t, json.Unmarshal(raw, &out), string(raw))
	}

	return resp.StatusCode, out
}

func TestREST_InstanceAndDatabaseLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	base := "http://" + e.handler.Addr()
	inst := "/v1/" + parent + "/instances"

	steps := []struct {
		method string
		path   string
		body   string
		check  func(t *testing.T, out map[string]any)
		name   string
		status int
	}{
		{
			"POST",
			inst,
			`{"instanceId":"rest1","instance":{"config":"` + configRef + `","displayName":"rest inst","nodeCount":1}}`,
			func(t *testing.T, out map[string]any) {
				t.Helper()

				name, _ := out["name"].(string)
				assert.Contains(t, name, "/instances/rest1/operations/")

				require.Eventually(t, func() bool {
					_, op := restDo(t, base, "GET", "/v1/"+name, "")

					return op["done"] == true
				}, 10*time.Second, 2*time.Millisecond)
			},
			"create instance",
			http.StatusOK,
		},
		{
			"GET", inst + "/rest1", "",
			func(t *testing.T, out map[string]any) {
				t.Helper()

				assert.Equal(t, "READY", out["state"])
				assert.EqualValues(t, 1000, out["processingUnits"])
			},
			"get instance", http.StatusOK,
		},
		{
			"GET", inst + "/rest1/operations", "",
			func(t *testing.T, out map[string]any) {
				t.Helper()
				assert.NotEmpty(t, out["operations"])
			},
			"list instance operations", http.StatusOK,
		},
		{
			"POST",
			inst + "/rest1/databases",
			`{"createStatement":"CREATE DATABASE ` + "`rdb`" + `",` +
				`"extraStatements":["CREATE TABLE t (id INT64) PRIMARY KEY (id)"]}`,
			func(t *testing.T, out map[string]any) {
				t.Helper()

				assert.Contains(t, out["name"], "/databases/rdb/operations/")
				assert.Equal(t, true, out["done"])
			},
			"create database",
			http.StatusOK,
		},
		{
			"GET", inst + "/rest1/databases/rdb/ddl", "",
			func(t *testing.T, out map[string]any) {
				t.Helper()
				assert.Len(t, out["statements"], 1)
			},
			"get ddl", http.StatusOK,
		},
		{
			"GET", inst + "/rest1/databases/missing", "",
			func(t *testing.T, out map[string]any) {
				t.Helper()
				assert.NotEmpty(t, out["error"])
			},
			"missing database", http.StatusNotFound,
		},
		{"DELETE", inst + "/rest1/databases/rdb", "", nil, "drop database", http.StatusOK},
		{"DELETE", inst + "/rest1", "", nil, "delete instance", http.StatusOK},
	}

	for _, s := range steps {
		code, out := restDo(t, base, s.method, s.path, s.body)
		assert.Equal(t, s.status, code, s.name)

		if s.check != nil {
			s.check(t, out)
		}
	}
}
