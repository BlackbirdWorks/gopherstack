package azurefunctions_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

func TestNetwork_ContainerEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setting string
		want    string
	}{
		{
			"blob endpoint",
			"DefaultEndpointsProtocol=http;AccountName=a;AccountKey=k;BlobEndpoint=http://127.0.0.1:10000/a;",
			"DefaultEndpointsProtocol=http;AccountName=a;AccountKey=k;BlobEndpoint=http://172.17.9.1:10000/a;",
		},
		{"localhost url", "http://localhost:5000/x", "http://172.17.9.1:5000/x"},
		{
			"service bus",
			"Endpoint=sb://localhost;SharedAccessKeyName=k",
			"Endpoint=sb://172.17.9.1;SharedAccessKeyName=k",
		},
		{"other host untouched", "http://example.com/localhost", "http://example.com/localhost"},
		{"plain value", "hello", "hello"},
		{
			"development storage",
			"UseDevelopmentStorage=true",
			"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
				"AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
				"BlobEndpoint=http://172.17.9.1:10000/devstoreaccount1;" +
				"QueueEndpoint=http://172.17.9.1:10001/devstoreaccount1;" +
				"TableEndpoint=http://172.17.9.1:10002/devstoreaccount1;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			e.backend.SetNetwork(
				azurefunctions.Network{Host: "172.17.9.1", BlobPort: 10000, QueuePort: 10001, TablePort: 10002},
			)
			e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
			require.NoError(t, e.backend.SetSettings("app", map[string]string{"CONN": tt.setting}))
			_, err := e.backend.Deploy("app", helloZip(t), "t", "m")
			require.NoError(t, err)

			resp, _ := e.do(t, http.MethodGet, "app."+testDomain, "/api/greet", nil, nil)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			specs := e.rt.specs()
			require.Len(t, specs, 1)
			assert.Equal(t, tt.want, envOf(specs[0])["CONN"])

			stored, err := e.backend.Settings("app")
			require.NoError(t, err)
			assert.Equal(t, tt.setting, stored["CONN"], "ARM still sees the setting as written")
		})
	}
}

func TestRunFromPackage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		wantNames []string
		wantErr   bool
	}{
		{"url package wins over deployed zip", "/pkg.zip", []string{"hello", "tick"}, false},
		{"missing package fails", "/missing.zip", nil, true},
		{"one uses the deployed zip", "", []string{"deployed"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pkg := helloZip(t)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/pkg.zip" {
					http.NotFound(w, r)

					return
				}

				_, _ = w.Write(pkg)
			}))
			t.Cleanup(srv.Close)

			e := newEnv(t, false)
			e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})

			value := "1"
			if tt.path != "" {
				value = srv.URL + tt.path
			}

			require.NoError(t, e.backend.SetSettings("app", map[string]string{"WEBSITE_RUN_FROM_PACKAGE": value}))

			_, err := e.backend.Deploy("app", zipOf(t, map[string]string{
				"host.json": `{"version":"2.0"}`, "deployed/function.json": `{"bindings":[]}`,
			}), "t", "m")
			require.NoError(t, err)

			fns, err := e.backend.Functions(t.Context(), "app")
			if tt.wantErr {
				require.Error(t, err)
				assert.NotContains(t, err.Error(), srv.URL, "the URL (it may carry a SAS token) stays out of errors")

				return
			}

			require.NoError(t, err)

			names := make([]string, 0, len(fns))
			for _, f := range fns {
				names = append(names, f.Name)
			}

			assert.Equal(t, tt.wantNames, names)
		})
	}
}
