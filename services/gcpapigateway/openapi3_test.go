package gcpapigateway_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/apigateway/apiv1/apigatewaypb"
)

const spec3Header = "openapi: 3.0.4\ninfo:\n  title: t\n  version: \"1\"\n"

const quota3 = "  metrics:\n    reads:\n      displayName: Reads\n  quota:\n    limits:\n      reads-limit:\n" +
	"        metric: reads\n        values: 2\n"

func mgmt3(address, extra string) string {
	return "x-google-api-management:\n  backends:\n    be:\n      address: " + address + "\n" + extra
}

func spec3(mgmt, top, paths string) string {
	return spec3Header + mgmt + top + "paths:\n" + paths
}

func TestOpenAPI3Grammar(t *testing.T) {
	t.Parallel()

	const be = "https://b.example"

	hello := "  /hello:\n" + op("get", "")
	named := "  /hello:\n" + op("get", "      x-google-backend: be\n")

	tests := []struct {
		name string
		doc  string
		want string
	}{
		{name: "top level backend", doc: spec3(mgmt3(be, ""), "x-google-backend: be\n", hello)},
		{name: "operation backend only", doc: spec3(mgmt3(be, ""), "", named)},
		{
			name: "metrics and quota",
			doc:  spec3(mgmt3(be, quota3), "x-google-backend: be\nx-google-quota:\n  reads: 1\n", hello),
		},
		{
			name: "no management block",
			doc:  spec3("", "x-google-backend: be\n", hello),
			want: "x-google-api-management is required",
		},
		{
			name: "no backends",
			doc:  spec3("x-google-api-management:\n  metrics: {}\n", "x-google-backend: be\n", hello),
			want: "x-google-api-management.backends is required",
		},
		{
			name: "no effective backend",
			doc:  spec3(mgmt3(be, ""), "", hello),
			want: "Operation has no x-google-backend",
		},
		{
			name: "undefined backend name",
			doc:  spec3(mgmt3(be, ""), "x-google-backend: nope\n", hello),
			want: `Backend "nope" is not defined`,
		},
		{
			name: "backend object is not the 3.x form",
			doc:  spec3(mgmt3(be, ""), "x-google-backend:\n  address: https://b.example\n", hello),
			want: "must name a backend",
		},
		{
			name: "quota refers to undefined metric",
			doc: spec3(
				mgmt3(be, ""),
				"x-google-quota:\n  ghosts: 1\n",
				"  /hello:\n"+op("get", "      x-google-backend: be\n"),
			),
			want: "undefined metric",
		},
		{
			name: "quota limit value",
			doc: spec3(mgmt3(be, strings.Replace(quota3, "values: 2", "values: -1", 1)),
				"x-google-backend: be\n", hello),
			want: "non-negative integer",
		},
		{
			name: "issuer scheme without x-google-auth",
			doc: spec3(
				mgmt3(be, ""),
				"x-google-backend: be\ncomponents:\n  securitySchemes:\n    j:\n      type: oauth2\n"+
					"      x-google-issuer: iss\nsecurity:\n  - j: []\n",
				hello,
			),
			want: "unsupported",
		},
		{
			name: "jwt location needs one of header and query",
			doc: spec3(
				mgmt3(be, ""),
				"x-google-backend: be\ncomponents:\n  securitySchemes:\n    j:\n      type: oauth2\n"+
					"      x-google-auth:\n        issuer: iss\n        jwksUri: https://k.example/jwks\n"+
					"        jwtLocations:\n          - valuePrefix: p\n",
				hello,
			),
			want: "Exactly one of header and query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createAPI(t, apiID)

			cfgOp, err := e.client.CreateApiConfig(t.Context(), configRequest("c", tt.doc))
			require.NoError(t, err)

			cfg, err := cfgOp.Wait(t.Context())
			if tt.want == "" {
				require.NoError(t, err)
				assert.Equal(t, apigatewaypb.ApiConfig_ACTIVE, cfg.GetState())

				return
			}

			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestOpenAPI3DataPlane(t *testing.T) {
	t.Parallel()

	be := echoBackend(t)
	hello := "  /hello/{name}:\n" + op("get", "")

	e := newEnv(t)
	gw := e.deploy(t, spec3(
		mgmt3(be.URL+"/fn", "      pathTranslation: CONSTANT_ADDRESS\n"),
		"x-google-backend: be\n", hello))

	got := e.call(t, gw, http.MethodGet, "/hello/world", nil)
	require.Equal(t, http.StatusOK, got.status, got.raw)
	assert.Equal(t, "/fn", got.body["path"], "pathTranslation is the 3.x spelling")
	assert.Equal(t, "name=world", got.body["query"])
}

func TestOpenAPI3JWTLocations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		hdr  map[string]string
		path string
		want int
	}{
		{name: "custom header with prefix", hdr: map[string]string{"X-Token": "tk "}, path: "/hello", want: 200},
		{name: "custom query", path: "/hello?jwt=", want: 200},
		{
			name: "default bearer is replaced",
			hdr:  map[string]string{"Authorization": "Bearer "},
			path: "/hello",
			want: 401,
		},
		{name: "wrong prefix", hdr: map[string]string{"X-Token": "other "}, path: "/hello", want: 401},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSigner(t)
			be := echoBackend(t)
			e := newEnv(t)
			now := e.clock.Now()

			doc := spec3(mgmt3(be.URL, ""), "x-google-backend: be\ncomponents:\n  securitySchemes:\n    j:\n"+
				"      type: oauth2\n      x-google-auth:\n        issuer: "+issuer+"\n        jwksUri: "+s.jwks.URL+"\n"+
				"        audiences:\n          - aud-1\n        jwtLocations:\n          - header: X-Token\n"+
				"            valuePrefix: \"tk \"\n          - query: jwt\nsecurity:\n  - j: []\n", "  /hello:\n"+op("get", ""))
			gw := e.deploy(t, doc)

			tok := s.rsaToken(t, jwt.MapClaims{"iss": issuer, "aud": "aud-1", "exp": now.Add(time.Hour).Unix()})

			hdr := map[string]string{}
			for k, v := range tt.hdr {
				hdr[k] = v + tok
			}

			got := e.call(t, gw, http.MethodGet, tt.path+tokenIf(tt.path, tok), hdr)
			assert.Equal(t, tt.want, got.status, got.raw)

			if tt.want == http.StatusUnauthorized {
				assert.EqualValues(t, "Jwt is missing", got.body["message"])
			}
		})
	}
}

func tokenIf(path, tok string) string {
	if strings.HasSuffix(path, "=") {
		return tok
	}

	return ""
}

func TestOpenAPI3Quota(t *testing.T) {
	t.Parallel()

	be := echoBackend(t)
	e := newEnv(t)
	gw := e.deploy(t, spec3(mgmt3(be.URL, quota3), "x-google-backend: be\nx-google-quota:\n  reads: 1\n",
		"  /top:\n"+op("get", "")+"  /dear:\n"+op("get", "      x-google-quota:\n        reads: 2\n")))

	codes := func(target string, n int) []int {
		out := make([]int, 0, n)
		for range n {
			out = append(out, e.call(t, gw, http.MethodGet, target, nil).status)
		}

		return out
	}

	assert.Equal(t, []int{200, 200, 429}, codes("/top", 3), "the top-level cost of 1 against a limit of 2")

	e.clock.Advance(time.Minute)
	assert.Equal(t, []int{200, 429}, codes("/dear", 2), "an operation cost replaces the top-level one")
}

const quota2 = "x-google-management:\n  metrics:\n    - name: reads\n      displayName: Reads\n" +
	"      valueType: INT64\n" +
	"      metricKind: DELTA\n  quota:\n    limits:\n      - name: reads-limit\n        metric: reads\n" +
	"        unit: \"1/min/{project}\"\n        values:\n          STANDARD: %d\n"

func TestQuotaFollowsLatestConfig(t *testing.T) {
	t.Parallel()

	be := echoBackend(t)
	paths := "  /read:\n" + op("get", "      x-google-quota:\n        metricCosts:\n          reads: 1\n")
	e := newEnv(t)
	e.createAPI(t, apiID)

	first := e.createConfig(t, "cfg-1", opSpec(be.URL, strings.Replace(quota2, "%d", "1", 1), paths))
	gw := e.createGateway(t, "my-gw", first.GetName())

	statuses := func(n int) []int {
		out := make([]int, 0, n)
		for range n {
			out = append(out, e.call(t, gw, http.MethodGet, "/read", nil).status)
		}

		return out
	}

	assert.Equal(t, []int{200, 429}, statuses(2), "the gateway's own config limits while it is the latest")

	e.clock.Advance(time.Minute)
	e.createConfig(t, "cfg-2", opSpec(be.URL, strings.Replace(quota2, "%d", "3", 1), paths))
	assert.Equal(t, []int{200, 200, 200, 429}, statuses(4), "the newest config's limit governs the whole API")

	e.clock.Advance(time.Minute)
	e.createConfig(t, "cfg-3", opSpec(be.URL, "", "  /other:\n"+op("get", "")))

	got := e.call(t, gw, http.MethodGet, "/read", nil)
	assert.Equal(t, http.StatusInternalServerError, got.status, got.raw)
	assert.EqualValues(t, "Failed to call Service Control Quota.", got.body["message"])
}
