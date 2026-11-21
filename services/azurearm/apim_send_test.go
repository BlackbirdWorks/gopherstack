package azurearm_test

import (
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordedReq struct {
	header http.Header
	method string
	path   string
	body   string
}

func newRecordingBackend(t *testing.T, reply string) (*httptest.Server, chan recordedReq) {
	t.Helper()

	ch := make(chan recordedReq, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		ch <- recordedReq{header: r.Header.Clone(), method: r.Method, path: r.URL.Path, body: string(b)}

		w.Header().Set("X-Reply", "yes")
		_, _ = io.WriteString(w, reply)
	}))
	t.Cleanup(srv.Close)

	return srv, ch
}

func TestAPIMGateway_SendRequest(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	aux, got := newRecordingBackend(t, "token-ok")

	e.policy(t, "/apis/echo", policyDoc(`
		<send-request mode="new" response-variable-name="r" timeout="5" ignore-error="false">
			<set-url>@(&quot;`+aux.URL+`/introspect/&quot; + context.Request.MatchedParameters[&quot;id&quot;])</set-url>
			<set-method>POST</set-method>
			<set-header name="X-Token" exists-action="override">
				<value>@(&quot;t-&quot; + context.Request.Headers.GetValueOrDefault(&quot;X-In&quot;, &quot;&quot;))</value>
			</set-header>
			<set-body>
				@(&quot;token=&quot; + context.Request.Headers.GetValueOrDefault(&quot;X-In&quot;, &quot;&quot;))
			</set-body>
		</send-request>`+
		headerPolicy("X-Status", `@(((IResponse)context.Variables["r"]).StatusCode)`)+
		headerPolicy("X-Body", `@(((IResponse)context.Variables["r"]).Body.As<string>())`)+
		headerPolicy("X-Reply", `@(((IResponse)context.Variables["r"]).Headers.GetValueOrDefault("X-Reply", "no"))`),
		"", ""), http.StatusCreated)

	status, body, _ := e.call(t, "GET", "/echo/items/42", "", map[string]string{"X-In": "abc"})
	require.Equal(t, http.StatusOK, status, body)

	r := <-got
	assert.Equal(t, "POST", r.method)
	assert.Equal(t, "/introspect/42", r.path)
	assert.Equal(t, "t-abc", r.header.Get("X-Token"))
	assert.Equal(t, "token=abc", r.body)

	h := decodeEcho(t, body).Header
	assert.Equal(t, []string{"200"}, h["X-Status"])
	assert.Equal(t, []string{"token-ok"}, h["X-Body"])
	assert.Equal(t, []string{"yes"}, h["X-Reply"])
}

func TestAPIMGateway_SendRequestCopy(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	aux, got := newRecordingBackend(t, "ok")

	e.policy(t, "/apis/echo", policyDoc(`
		<send-request mode="copy" response-variable-name="r"><set-url>`+aux.URL+`/mirror</set-url></send-request>`,
		"", ""), http.StatusCreated)

	status, body, _ := e.call(t, "POST", "/echo/items", "payload", map[string]string{"X-Copy": "1"})
	require.Equal(t, http.StatusOK, status, body)

	r := <-got
	assert.Equal(t, "POST", r.method)
	assert.Equal(t, "/mirror", r.path)
	assert.Equal(t, "1", r.header.Get("X-Copy"))
	assert.Equal(t, "payload", r.body)
	assert.Equal(t, "payload", decodeEcho(t, body).Body, "the original request still reaches its backend with its body")
}

func TestAPIMGateway_SendOneWayRequest(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	aux, got := newRecordingBackend(t, "ignored")

	e.policy(t, "/apis/echo", policyDoc(`
		<send-one-way-request mode="new"><set-url>`+aux.URL+`/audit</set-url><set-method>PUT</set-method>
			<set-header name="X-Audit" exists-action="override"><value>yes</value></set-header>
			<set-body>@(context.Request.Method)</set-body></send-one-way-request>`, "", ""), http.StatusCreated)

	status, body, _ := e.call(t, "GET", "/echo/items/1", "", nil)
	require.Equal(t, http.StatusOK, status, body)

	r := <-got
	assert.Equal(t, "PUT", r.method)
	assert.Equal(t, "/audit", r.path)
	assert.Equal(t, "yes", r.header.Get("X-Audit"))
	assert.Equal(t, "GET", r.body)
}

func TestAPIMGateway_SendRequestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // test-only struct; layout not performance-critical
		name       string
		ignore     string
		wantStatus int
		wantHeader string
	}{
		{"ignore-error true", "true", http.StatusOK, "True"},
		{"ignore-error false", "false", http.StatusBadGateway, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newGWEnv(t)
			e.policy(t, "/apis/echo", `<policies><inbound><base />
				<send-request mode="new" response-variable-name="r" ignore-error="`+tc.ignore+`">
					<set-url>http://203.0.113.9:81/x</set-url><set-method>GET</set-method></send-request>`+
				headerPolicy("X-Null", `@(context.Variables.GetValueOrDefault<string>("r") == null)`)+`</inbound>
				<backend><base /></backend><outbound><base /></outbound>
				<on-error><set-header name="X-Source" exists-action="override">
					<value>@(context.LastError.Source)</value></set-header></on-error>
				</policies>`, http.StatusCreated)

			status, body, hdr := e.call(t, "GET", "/echo/items/1", "", nil)
			assert.Equal(t, tc.wantStatus, status, body)

			if tc.ignore == "true" {
				assert.Equal(t, []string{tc.wantHeader}, decodeEcho(t, body).Header["X-Null"])

				return
			}

			assert.Equal(t, "send-request", hdr.Get("X-Source"))
		})
	}
}

func TestAPIMGateway_AuthenticationBasic(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.policy(
		t,
		"/apis/echo",
		policyDoc(`<authentication-basic username="svc" password="p@ss" />`, "", ""),
		http.StatusCreated,
	)

	status, body, _ := e.call(t, "GET", "/echo/items/1", "", map[string]string{"Authorization": "Bearer client"})
	require.Equal(t, http.StatusOK, status, body)

	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("svc:p@ss"))
	assert.Equal(t, []string{want}, decodeEcho(t, body).Header["Authorization"])
}

func TestAPIMSend_Validation(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)

	tests := []struct{ name, stmt, want string }{
		{
			"no url",
			`<send-request response-variable-name="r"><set-method>GET</set-method></send-request>`,
			"element is required unless mode is copy",
		},
		{
			"no method",
			`<send-request response-variable-name="r"><set-url>http://a/</set-url></send-request>`,
			"element is required unless mode is copy",
		},
		{
			"bad url",
			`<send-request response-variable-name="r"><set-url>ftp://a/</set-url><set-method>GET</set-method></send-request>`,
			"absolute http or https",
		},
		{
			"bad method",
			`<send-request response-variable-name="r"><set-url>http://a/</set-url><set-method>FETCH</set-method></send-request>`,
			"set-method",
		},
		{
			"no variable",
			`<send-request><set-url>http://a/</set-url><set-method>GET</set-method></send-request>`,
			"'response-variable-name' is required",
		},
		{"bad mode", `<send-one-way-request mode="clone" />`, "'mode'"},
		{"basic without password", `<authentication-basic username="u" />`, "'password' is required"},
		{"basic in outbound", "", "not allowed in the outbound"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doc := policyDoc(tc.stmt, "", "")
			if tc.stmt == "" {
				doc = policyDoc("", "", `<authentication-basic username="u" password="p" />`)
			}

			body := e.policy(t, "/apis/echo", doc, http.StatusBadRequest)
			assert.Contains(t, body, tc.want)
		})
	}
}
