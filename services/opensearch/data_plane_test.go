package opensearch_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/endpointstrategy"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

type strategyConfig struct{ cfg endpointstrategy.Config }

func (s strategyConfig) EndpointStrategies() endpointstrategy.Config { return s.cfg }

type dataPlaneEnv struct {
	client *opensearchsdk.Client
	url    string
}

func newDataPlaneEnv(t *testing.T, cfg endpointstrategy.Config) *dataPlaneEnv {
	t.Helper()

	cfg.OpenSearch = "path"

	reg, err := (&opensearch.Provider{}).Init(&service.AppContext{Config: strategyConfig{cfg: cfg}})
	require.NoError(t, err)

	registry := service.NewRegistry()
	require.NoError(t, registry.Register(reg))

	e := echo.New()
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	awsCfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	return &dataPlaneEnv{
		url: srv.URL,
		client: opensearchsdk.NewFromConfig(awsCfg, func(o *opensearchsdk.Options) {
			o.BaseEndpoint = aws.String(srv.URL)
		}),
	}
}

func (e *dataPlaneEnv) do(t *testing.T, method, path, body, user, pass string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, e.url+path, strings.NewReader(body))
	require.NoError(t, err)

	if user != "" || pass != "" {
		req.SetBasicAuth(user, pass)
	}

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(b)
}

func (e *dataPlaneEnv) createDomain(t *testing.T, name string, aso *types.AdvancedSecurityOptionsInput) {
	t.Helper()

	_, err := e.client.CreateDomain(t.Context(), &opensearchsdk.CreateDomainInput{
		DomainName: aws.String(name), AdvancedSecurityOptions: aso,
	})
	require.NoError(t, err)

	code, _ := e.do(t, http.MethodPost, "/2021-01-01/opensearch/domain/"+name+"/index/idx", "{}", "", "")
	require.Equal(t, http.StatusOK, code)
}

func fgac() *types.AdvancedSecurityOptionsInput {
	return &types.AdvancedSecurityOptionsInput{
		Enabled:                     aws.Bool(true),
		InternalUserDatabaseEnabled: aws.Bool(true),
		MasterUserOptions: &types.MasterUserOptions{
			MasterUserName:     aws.String("admin"),
			MasterUserPassword: aws.String("Sup3r-Secret!"),
		},
	}
}

func TestDataPlane_BasicAuthEnforcement(t *testing.T) {
	t.Parallel()

	env := newDataPlaneEnv(t, endpointstrategy.Config{})
	env.createDomain(t, "secured", fgac())
	env.createDomain(t, "open", nil)

	tests := []struct {
		name     string
		domain   string
		user     string
		pass     string
		wantCode int
	}{
		{name: "no credentials", domain: "secured", wantCode: http.StatusUnauthorized},
		{name: "wrong password", domain: "secured", user: "admin", pass: "nope", wantCode: http.StatusUnauthorized},
		{name: "wrong user", domain: "secured", user: "root", pass: "Sup3r-Secret!", wantCode: http.StatusUnauthorized},
		{name: "master user", domain: "secured", user: "admin", pass: "Sup3r-Secret!", wantCode: http.StatusOK},
		{name: "domain without fgac", domain: "open", wantCode: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			code, _ := env.do(t, http.MethodGet, "/opensearch/us-east-1/"+tc.domain+"/idx/_count", "", tc.user, tc.pass)
			assert.Equal(t, tc.wantCode, code)
		})
	}

	t.Run("control plane is not gated", func(t *testing.T) {
		t.Parallel()

		out, err := env.client.DescribeDomain(t.Context(), &opensearchsdk.DescribeDomainInput{
			DomainName: aws.String("secured"),
		})
		require.NoError(t, err)
		require.NotNil(t, out.DomainStatus.AdvancedSecurityOptions)
		assert.True(t, aws.ToBool(out.DomainStatus.AdvancedSecurityOptions.InternalUserDatabaseEnabled))
	})

	t.Run("update without master user keeps credentials", func(t *testing.T) {
		t.Parallel()

		env.createDomain(t, "kept", fgac())

		_, err := env.client.UpdateDomainConfig(t.Context(), &opensearchsdk.UpdateDomainConfigInput{
			DomainName: aws.String("kept"),
			AdvancedSecurityOptions: &types.AdvancedSecurityOptionsInput{
				Enabled: aws.Bool(true), InternalUserDatabaseEnabled: aws.Bool(true),
			},
		})
		require.NoError(t, err)

		code, _ := env.do(t, http.MethodGet, "/opensearch/us-east-1/kept/idx/_count", "", "", "")
		assert.Equal(t, http.StatusUnauthorized, code)

		code, _ = env.do(t, http.MethodGet, "/opensearch/us-east-1/kept/idx/_count", "", "admin", "Sup3r-Secret!")
		assert.Equal(t, http.StatusOK, code)
	})
}

func TestDataPlane_SharedCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		multiCluster string
		wantSeenByB  int
	}{
		{name: "default isolates domains", multiCluster: "", wantSeenByB: http.StatusNotFound},
		{name: "multi cluster true isolates", multiCluster: "true", wantSeenByB: http.StatusNotFound},
		{name: "multi cluster false shares", multiCluster: "false", wantSeenByB: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newDataPlaneEnv(t, endpointstrategy.Config{OpenSearchMultiCluster: tc.multiCluster})

			_, err := env.client.CreateDomain(
				t.Context(),
				&opensearchsdk.CreateDomainInput{DomainName: aws.String("dom-a")},
			)
			require.NoError(t, err)
			_, err = env.client.CreateDomain(
				t.Context(),
				&opensearchsdk.CreateDomainInput{DomainName: aws.String("dom-b")},
			)
			require.NoError(t, err)

			code, _ := env.do(t, http.MethodPost, "/2021-01-01/opensearch/domain/dom-a/index/shared", "{}", "", "")
			require.Equal(t, http.StatusOK, code)

			code, _ = env.do(t, http.MethodPost, "/opensearch/us-east-1/dom-a/shared/_doc/1", `{"k":"v"}`, "", "")
			require.Equal(t, http.StatusOK, code)

			code, body := env.do(t, http.MethodGet, "/opensearch/us-east-1/dom-b/shared/_count", "", "", "")
			assert.Equal(t, tc.wantSeenByB, code)

			if tc.wantSeenByB == http.StatusOK {
				assert.Contains(t, body, `"count":1`)
			}
		})
	}
}

func TestDataPlane_CustomBackend(t *testing.T) {
	t.Parallel()

	type seen struct{ method, path, query, host, auth string }

	got := make(chan seen, 8)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- seen{r.Method, r.URL.Path, r.URL.RawQuery, r.Host, r.Header.Get("Authorization")}
		_, _ = w.Write([]byte(`{"from":"upstream"}`))
	}))
	t.Cleanup(upstream.Close)

	env := newDataPlaneEnv(t, endpointstrategy.Config{OpenSearchCustomBackend: upstream.URL + "/base"})
	env.createDomain(t, "proxied", fgac())

	const base = "/opensearch/us-east-1/proxied"

	tests := []struct {
		name     string
		method   string
		path     string
		wantPath string
	}{
		{
			name:     "search",
			method:   http.MethodPost,
			path:     "/opensearch/us-east-1/proxied/idx/_search?q=1",
			wantPath: "/base/idx/_search",
		},
		{
			name:     "cluster health",
			method:   http.MethodGet,
			path:     base + "/_cluster/health",
			wantPath: "/base/_cluster/health",
		},
		{name: "traversal stays under base", method: http.MethodGet, path: base + "/idx/../../x", wantPath: "/base/x"},
	}

	for _, tc := range tests {
		code, body := env.do(t, tc.method, tc.path, "", "admin", "pw")
		require.Equal(t, http.StatusOK, code, tc.name)
		assert.JSONEq(t, `{"from":"upstream"}`, body, tc.name)

		var s seen
		select {
		case s = <-got:
		case <-time.After(10 * time.Second):
			require.FailNow(t, "request never reached the custom backend", tc.name)
		}

		assert.Equal(t, tc.method, s.method, tc.name)
		assert.Equal(t, tc.wantPath, s.path, tc.name)
		assert.Equal(t, strings.TrimPrefix(upstream.URL, "http://"), s.host, tc.name)
		assert.NotEmpty(t, s.auth, "credentials are the backend's to judge")
	}

	code, _ := env.do(t, http.MethodGet, "/opensearch/us-east-1/missing/idx/_search", "", "", "")
	assert.NotEqual(t, http.StatusOK, code, "unknown domains are not proxied")
	assert.Empty(t, got)
}

func TestDataPlane_InvalidBackendURLRejected(t *testing.T) {
	t.Parallel()

	for _, v := range []string{"ftp://x", "not a url", "http://"} {
		require.Error(t, endpointstrategy.Config{OpenSearchCustomBackend: v}.Validate(), v)
	}

	require.NoError(t, endpointstrategy.Config{OpenSearchCustomBackend: "http://localhost:9200"}.Validate())
	require.Error(t, endpointstrategy.Config{OpenSearchMultiCluster: "maybe"}.Validate())
}
