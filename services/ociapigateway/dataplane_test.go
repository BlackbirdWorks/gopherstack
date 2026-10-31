package ociapigateway_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ociapigateway "github.com/blackbirdworks/gopherstack/services/ociapigateway"
)

func stock(path, methods, body string) string {
	return `{"path":"` + path + `","methods":[` + methods + `],` +
		`"backend":{"type":"STOCK_RESPONSE_BACKEND","status":200,"body":"` + body + `"}}`
}

func TestRoutePrecedence(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "prec")
	e.mkDeployment(t, *gw.Id, "/api", `{"routes":[`+
		stock("/users/me", `"GET"`, "literal")+`,`+
		stock("/users/{id}", `"GET"`, "param")+`,`+
		stock("/users/{id}", `"DELETE"`, "param-delete")+`,`+
		stock("/users/{id}/posts/{post}", `"ANY"`, "two-params")+`,`+
		stock("/files/{rest*}", `"GET"`, "wild")+`,`+
		stock("/files/readme", `"GET"`, "exact-file")+`,`+
		stock("/", `"GET"`, "root")+`,`+
		stock("/any", `"ANY"`, "any")+`]}`)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		allow  string
		status int
	}{
		{"literal beats param", "GET", "/api/users/me", "literal", "", 200},
		{"param", "GET", "/api/users/42", "param", "", 200},
		{"param other method", "DELETE", "/api/users/42", "param-delete", "", 200},
		{"method not allowed", "POST", "/api/users/42", "", "DELETE, GET", 405},
		{"two params", "PUT", "/api/users/1/posts/2", "two-params", "", 200},
		{"wildcard multi segment", "GET", "/api/files/a/b/c", "wild", "", 200},
		{"wildcard zero segments", "GET", "/api/files", "wild", "", 200},
		{"exact beats wildcard", "GET", "/api/files/readme", "exact-file", "", 200},
		{"root", "GET", "/api", "root", "", 200},
		{"root slash", "GET", "/api/", "root", "", 200},
		{"any method", "PATCH", "/api/any", "any", "", 200},
		{"no route", "GET", "/api/nothing/here", "", "", 404},
		{"outside prefix", "GET", "/other/users/me", "", "", 404},
		{"prefix is segment bounded", "GET", "/apiary/users/me", "", "", 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := e.call(t, *gw.Id, tt.method, tt.path, nil, "")
			assert.Equal(t, tt.status, r.status)

			if tt.status == 200 {
				assert.Equal(t, tt.body, r.body)
			}

			if tt.allow != "" {
				assert.Equal(t, tt.allow, r.header.Get("Allow"))
			}
		})
	}
}

func TestHostHeaderRouting(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "host")
	e.mkDeployment(t, *gw.Id, "/v1", stockSpec(200, "by-host"))

	r := e.callURL(t, e.base+"/v1/hi", *gw.Hostname, http.MethodGet, nil, "")
	assert.Equal(t, 200, r.status)
	assert.Equal(t, "by-host", r.body)

	r = e.callURL(
		t,
		e.base+"/v1/hi",
		"unknown123.apigateway.us-ashburn-1.oci.customer-oci.com",
		http.MethodGet,
		nil,
		"",
	)
	assert.Equal(t, 404, r.status)

	r = e.call(t, "ocid1.apigateway.oc1..missing", http.MethodGet, "/v1/hi", nil, "")
	assert.Equal(t, 404, r.status)
}

func TestInactiveGatewayDoesNotServe(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	id := *e.createGateway(t, "booting").Id

	assert.Equal(t, 404, e.call(t, id, http.MethodGet, "/v1/hi", nil, "").status)
}

func TestStockResponseHeaders(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "stock")
	e.mkDeployment(t, *gw.Id, "/s", `{"routes":[{"path":"/echo/{name}","methods":["GET"],"backend":{
		"type":"STOCK_RESPONSE_BACKEND","status":202,"body":"hello ${request.path[name]}",
		"headers":[{"name":"X-Stock","value":"yes"}]}}]}`)

	r := e.call(t, *gw.Id, http.MethodGet, "/s/echo/bob", nil, "")
	assert.Equal(t, 202, r.status)
	assert.Equal(t, "hello bob", r.body)
	assert.Equal(t, "yes", r.header.Get("X-Stock"))
}

func TestHTTPBackend(t *testing.T) {
	t.Parallel()

	var (
		mu   sync.Mutex
		seen []string
	)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.RequestURI()+" body="+string(body)+" h="+r.Header.Get("X-Client"))
		mu.Unlock()

		if r.URL.Path == "/redir" {
			http.Redirect(w, r, "/elsewhere", http.StatusFound)

			return
		}

		w.Header().Set("X-Backend", "1")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("from-backend"))
	}))
	t.Cleanup(backend.Close)

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "http")
	e.mkDeployment(t, *gw.Id, "/p", `{"routes":[
		{"path":"/svc/{rest*}","methods":["ANY"],
		 "backend":{"type":"HTTP_BACKEND","url":"`+backend.URL+`/base/${request.path[rest]}"}},
		{"path":"/redir","methods":["GET"],"backend":{"type":"HTTP_BACKEND","url":"`+backend.URL+`/redir"}},
		{"path":"/slow","methods":["GET"],
		 "backend":{"type":"HTTP_BACKEND","url":"http://192.0.2.10:9/x","isSslVerifyDisabled":true}}]}`)

	r := e.call(t, *gw.Id, http.MethodPost, "/p/svc/a/b?x=1&y=two", map[string]string{"X-Client": "c1"}, "payload")
	assert.Equal(t, http.StatusTeapot, r.status)
	assert.Equal(t, "from-backend", r.body)
	assert.Equal(t, "1", r.header.Get("X-Backend"))

	mu.Lock()
	assert.Contains(t, seen, "POST /base/a/b?x=1&y=two body=payload h=c1")
	mu.Unlock()

	red := e.call(t, *gw.Id, http.MethodGet, "/p/redir", nil, "")
	assert.Equal(t, http.StatusFound, red.status, "redirects are passed to the client, never followed")
	assert.Equal(t, "/elsewhere", red.header.Get("Location"))

	blocked := e.call(t, *gw.Id, http.MethodGet, "/p/slow", nil, "")
	assert.Equal(t, http.StatusBadGateway, blocked.status, "non-loopback destinations are refused by default")
}

var errBoom = errors.New("boom")

type fakeInvoker struct {
	fn   func(id string, in ociapigateway.FunctionRequest) (ociapigateway.FunctionResponse, error)
	logs []string
	mu   sync.Mutex
}

func (f *fakeInvoker) InvokeFunction(
	_ context.Context, id string, in ociapigateway.FunctionRequest,
) (ociapigateway.FunctionResponse, error) {
	f.mu.Lock()
	f.logs = append(f.logs, id+" "+in.Method+" "+string(in.Body))
	f.mu.Unlock()

	return f.fn(id, in)
}

func TestFunctionsBackend(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	inv := &fakeInvoker{fn: func(id string, in ociapigateway.FunctionRequest) (ociapigateway.FunctionResponse, error) {
		if id == "ocid1.fnfunc.oc1.iad.broken" {
			return ociapigateway.FunctionResponse{}, errBoom
		}

		return ociapigateway.FunctionResponse{
			Status: 201, Header: http.Header{"X-Fn": {"1"}}, Body: []byte("fn:" + string(in.Body)),
		}, nil
	}}
	e.backend.SetFunctionInvoker(inv)

	gw := e.mkGateway(t, "fn")
	e.mkDeployment(t, *gw.Id, "/f", `{"routes":[
		{"path":"/run","methods":["POST"],"backend":{"type":"ORACLE_FUNCTIONS_BACKEND","functionId":"`+testFunction+`"}},
		{"path":"/bad","methods":["POST"],
		 "backend":{"type":"ORACLE_FUNCTIONS_BACKEND","functionId":"ocid1.fnfunc.oc1.iad.broken"}}]}`)

	r := e.call(t, *gw.Id, http.MethodPost, "/f/run", nil, "ping")
	assert.Equal(t, 201, r.status)
	assert.Equal(t, "fn:ping", r.body)
	assert.Equal(t, "1", r.header.Get("X-Fn"))

	assert.Equal(t, http.StatusBadGateway, e.call(t, *gw.Id, http.MethodPost, "/f/bad", nil, "").status)
}

func TestFunctionsBackendUnwired(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "nofn")
	e.mkDeployment(
		t,
		*gw.Id,
		"/f",
		`{"routes":[{"path":"/run","backend":{"type":"ORACLE_FUNCTIONS_BACKEND","functionId":"`+testFunction+`"}}]}`,
	)

	assert.Equal(t, http.StatusBadGateway, e.call(t, *gw.Id, http.MethodGet, "/f/run", nil, "").status)
}

func TestCORS(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "cors")
	e.mkDeployment(t, *gw.Id, "/c", `{
		"requestPolicies":{"cors":{"allowedOrigins":["https://app.example.test"],"allowedMethods":["GET","POST"],
			"allowedHeaders":["X-Custom"],"exposedHeaders":["X-Out"],"isAllowCredentialsEnabled":true,"maxAgeInSeconds":600}},
		"routes":[`+stock("/r", `"GET"`, "ok")+`,
		{"path":"/open","methods":["GET"],"backend":{"type":"STOCK_RESPONSE_BACKEND","status":200},
		 "requestPolicies":{"cors":{"allowedOrigins":["*"]}}}]}`)

	pre := e.call(t, *gw.Id, http.MethodOptions, "/c/r", map[string]string{
		"Origin": "https://app.example.test", "Access-Control-Request-Method": "POST",
	}, "")
	assert.Equal(t, 200, pre.status)
	assert.Equal(t, "https://app.example.test", pre.header.Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "GET, POST", pre.header.Get("Access-Control-Allow-Methods"))
	assert.Equal(t, "X-Custom", pre.header.Get("Access-Control-Allow-Headers"))
	assert.Equal(t, "600", pre.header.Get("Access-Control-Max-Age"))
	assert.Equal(t, "true", pre.header.Get("Access-Control-Allow-Credentials"))

	deniedPre := e.call(t, *gw.Id, http.MethodOptions, "/c/r", map[string]string{
		"Origin": "https://evil.example.test", "Access-Control-Request-Method": "GET",
	}, "")
	assert.Empty(t, deniedPre.header.Get("Access-Control-Allow-Origin"))

	actual := e.call(t, *gw.Id, http.MethodGet, "/c/r", map[string]string{"Origin": "https://app.example.test"}, "")
	assert.Equal(t, "https://app.example.test", actual.header.Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "X-Out", actual.header.Get("Access-Control-Expose-Headers"))

	other := e.call(t, *gw.Id, http.MethodGet, "/c/r", map[string]string{"Origin": "https://evil.example.test"}, "")
	assert.Empty(t, other.header.Get("Access-Control-Allow-Origin"))

	open := e.call(
		t,
		*gw.Id,
		http.MethodGet,
		"/c/open",
		map[string]string{"Origin": "https://anything.example.test"},
		"",
	)
	assert.Equal(t, "*", open.header.Get("Access-Control-Allow-Origin"), "route policy overrides the deployment policy")
}

func TestRateLimiting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
	}{
		{"total", "TOTAL"},
		{"client ip", "CLIENT_IP"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			gw := e.mkGateway(t, "rl")
			e.mkDeployment(t, *gw.Id, "/r", `{"requestPolicies":{"rateLimiting":{"rateKey":"`+tt.key+
				`","rateInRequestsPerSecond":2}},"routes":[`+stock("/x", `"GET"`, "ok")+`]}`)

			codes := make([]int, 0, 3)
			for range 3 {
				codes = append(codes, e.call(t, *gw.Id, http.MethodGet, "/r/x", nil, "").status)
			}

			assert.Equal(t, []int{200, 200, 429}, codes)

			limited := e.call(t, *gw.Id, http.MethodGet, "/r/x", nil, "")
			assert.Equal(t, 429, limited.status)
			assert.Equal(t, "1", limited.header.Get("Retry-After"))

			e.clock.Advance(time1s)
			assert.Equal(
				t,
				200,
				e.call(t, *gw.Id, http.MethodGet, "/r/x", nil, "").status,
				"tokens refill on the clock",
			)
		})
	}
}

func TestTransformations(t *testing.T) {
	t.Parallel()

	var got struct {
		h   http.Header
		uri string
		sync.Mutex
	}

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Lock()
		got.uri, got.h = r.URL.RequestURI(), r.Header.Clone()
		got.Unlock()

		w.Header().Set("X-Internal", "secret")
		w.Header().Set("X-Keep", "k")
	}))
	t.Cleanup(backend.Close)

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "tx")
	e.mkDeployment(t, *gw.Id, "/t", `{"routes":[{"path":"/go/{id}","methods":["GET"],
		"backend":{"type":"HTTP_BACKEND","url":"`+backend.URL+`/svc"},
		"requestPolicies":{
			"headerTransformations":{
				"filterHeaders":{"type":"BLOCK","items":[{"name":"X-Drop"}]},
				"renameHeaders":{"items":[{"from":"X-Old","to":"X-New"}]},
				"setHeaders":{"items":[
					{"name":"X-Id","values":["${request.path[id]}"]},
					{"name":"X-Fixed","values":["first"],"ifExists":"SKIP"},
					{"name":"X-Multi","values":["b"],"ifExists":"APPEND"},
					{"name":"X-Over","values":["new"]}]}},
			"queryParameterTransformations":{
				"filterQueryParameters":{"type":"BLOCK","items":[{"name":"secret"}]},
				"renameQueryParameters":{"items":[{"from":"q","to":"query"}]},
				"setQueryParameters":{"items":[{"name":"src","values":["gw"]}]}}},
		"responsePolicies":{"headerTransformations":{
			"filterHeaders":{"type":"BLOCK","items":[{"name":"X-Internal"}]},
			"setHeaders":{"items":[{"name":"X-Gw","values":["1"]}]}}}}]}`)

	r := e.call(t, *gw.Id, http.MethodGet, "/t/go/77?q=v&secret=s", map[string]string{
		"X-Drop": "d", "X-Old": "o", "X-Fixed": "keep", "X-Multi": "a", "X-Over": "old",
	}, "")
	assert.Equal(t, 200, r.status)
	assert.Empty(t, r.header.Get("X-Internal"))
	assert.Equal(t, "k", r.header.Get("X-Keep"))
	assert.Equal(t, "1", r.header.Get("X-Gw"))

	got.Lock()
	defer got.Unlock()

	assert.Empty(t, got.h.Get("X-Drop"))
	assert.Empty(t, got.h.Get("X-Old"))
	assert.Equal(t, "o", got.h.Get("X-New"))
	assert.Equal(t, "77", got.h.Get("X-Id"))
	assert.Equal(t, "keep", got.h.Get("X-Fixed"))
	assert.Equal(t, []string{"a", "b"}, got.h.Values("X-Multi"))
	assert.Equal(t, "new", got.h.Get("X-Over"))
	assert.Contains(t, got.uri, "query=v")
	assert.Contains(t, got.uri, "src=gw")
	assert.NotContains(t, got.uri, "secret")
}

func TestDynamicRouting(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "dyn")
	e.mkDeployment(t, *gw.Id, "/d", `{"routes":[{"path":"/x","methods":["GET"],"backend":{
		"type":"DYNAMIC_ROUTING_BACKEND","selectionSource":{"type":"SINGLE","selector":"request.headers[X-Tier]"},
		"routingBackends":[
			{"key":{"type":"ANY_OF","name":"gold","values":["gold","platinum"]},
			 "backend":{"type":"STOCK_RESPONSE_BACKEND","status":200,"body":"gold"}},
			{"key":{"type":"WILDCARD","name":"beta","expression":"beta-*"},
			 "backend":{"type":"STOCK_RESPONSE_BACKEND","status":200,"body":"beta"}},
			{"key":{"type":"WILDCARD","name":"any","expression":"+"},
			 "backend":{"type":"STOCK_RESPONSE_BACKEND","status":200,"body":"nonempty"}},
			{"key":{"type":"ANY_OF","name":"default","isDefault":true},
			 "backend":{"type":"STOCK_RESPONSE_BACKEND","status":200,"body":"default"}}]}}]}`)

	tests := []struct {
		name, tier, want string
	}{
		{"exact", "gold", "gold"},
		{"exact second", "platinum", "gold"},
		{"wildcard prefix", "beta-7", "beta"},
		{"default", "", "default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			hdr := map[string]string{}
			if tt.tier != "" {
				hdr["X-Tier"] = tt.tier
			}

			assert.Equal(t, tt.want, e.call(t, *gw.Id, http.MethodGet, "/d/x", hdr, "").body)
		})
	}
}

func TestSpecRoundTrip(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "rt")
	id := e.mkDeployment(t, *gw.Id, "/rt", stockSpec(200, "x"))

	status, body, _ := rawDo(t, http.MethodGet, e.base+"/20190501/deployments/"+id, "")
	require.Equal(t, 200, status)

	var out struct {
		Specification struct {
			Routes []struct {
				Backend struct {
					Type string `json:"type"`
				} `json:"backend"`
			} `json:"routes"`
		} `json:"specification"`
	}

	require.NoError(t, json.Unmarshal([]byte(body), &out))
	assert.Equal(t, "STOCK_RESPONSE_BACKEND", out.Specification.Routes[0].Backend.Type)
	assert.NotContains(t, body, "StubResponse")
}
