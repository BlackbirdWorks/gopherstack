package azurearm_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/apimanagement/armapimanagement/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIMPolicy_Validation(t *testing.T) {
	t.Parallel()

	long := "<policies><inbound>" + strings.Repeat("<base />", 70000) + "</inbound></policies>"

	tests := []struct {
		name    string
		doc     string
		format  string
		wantErr string
	}{
		{name: "minimal", doc: "<policies><inbound /><backend /><outbound /><on-error /></policies>"},
		{
			name: "every supported element",
			doc: policyDoc(
				`<set-header name="a" exists-action="skip"><value>1</value></set-header>
				<set-query-parameter name="q"><value>1</value></set-query-parameter>
				<rewrite-uri template="/x/{id}" copy-unmatched-params="false" />
				`+checkHeader("h", "401", "no", "true", "v")+`
				<rate-limit calls="5" renewal-period="10" />
				<cors allow-credentials="false"><allowed-origins><origin>*</origin></allowed-origins>
				<allowed-methods preflight-result-max-age="60"><method>*</method></allowed-methods>
				<allowed-headers><header>*</header></allowed-headers></cors>
				<return-response><set-status code="200" reason="OK" />
				<set-header name="x"><value>y</value></set-header><set-body>hi</set-body></return-response>`,
				`<forward-request timeout="30" follow-redirects="false" />`,
				`<set-header name="o" exists-action="delete" />`,
			),
		},
		{name: "not well formed", doc: "<policies><inbound></policies>", wantErr: "not well-formed"},
		{name: "empty", doc: "   ", wantErr: "empty"},
		{name: "wrong root", doc: "<foo />", wantErr: "policies"},
		{name: "two roots", doc: "<policies /><policies />", wantErr: "single root"},
		{name: "unknown section", doc: "<policies><sideways /></policies>", wantErr: "'sideways'"},
		{
			name: "unsupported policy", doc: policyDoc(`<validate-jwt header-name="Authorization" />`, "", ""),
			wantErr: "Error in element 'validate-jwt' on line 1",
		},
		{
			name:    "unsupported nested",
			doc:     policyDoc(`<rate-limit calls="1" renewal-period="1"><api name="a" /></rate-limit>`, "", ""),
			wantErr: "'api'",
		},
		{
			name:    "unknown attribute",
			doc:     policyDoc(`<set-header name="a" bogus="1"><value>1</value></set-header>`, "", ""),
			wantErr: "attribute 'bogus'",
		},
		{
			name:    "missing attribute",
			doc:     policyDoc(`<set-header><value>1</value></set-header>`, "", ""),
			wantErr: "'name' is required",
		},
		{name: "missing value", doc: policyDoc(`<set-header name="a" />`, "", ""), wantErr: "value"},
		{
			name:    "bad exists action",
			doc:     policyDoc(`<set-header name="a" exists-action="merge"><value>1</value></set-header>`, "", ""),
			wantErr: "exists-action",
		},
		{
			name:    "expression attribute",
			doc:     policyDoc(`<set-header name="a"><value>@(context.Request.Url)</value></set-header>`, "", ""),
			wantErr: "expressions",
		},
		{name: "rate zero", doc: policyDoc(`<rate-limit calls="0" renewal-period="10" />`, "", ""), wantErr: "'calls'"},
		{
			name:    "rate period over 300",
			doc:     policyDoc(`<rate-limit calls="1" renewal-period="301" />`, "", ""),
			wantErr: "'renewal-period'",
		},
		{name: "rate text", doc: policyDoc(`<rate-limit calls="x" renewal-period="10" />`, "", ""), wantErr: "'calls'"},
		{
			name:    "forward in inbound",
			doc:     policyDoc(`<forward-request />`, "", ""),
			wantErr: "not allowed in the inbound",
		},
		{
			name:    "rewrite in outbound",
			doc:     policyDoc("", "", `<rewrite-uri template="/x" />`),
			wantErr: "not allowed in the outbound",
		},
		{
			name:    "follow redirects",
			doc:     policyDoc("", `<forward-request follow-redirects="true" />`, ""),
			wantErr: "follow-redirects",
		},
		{
			name:    "backend both",
			doc:     policyDoc(`<set-backend-service base-url="http://a" backend-id="b" />`, "", ""),
			wantErr: "Exactly one",
		},
		{
			name:    "backend missing",
			doc:     policyDoc(`<set-backend-service backend-id="ghost" />`, "", ""),
			wantErr: "'ghost'",
		},
		{
			name:    "backend bad url",
			doc:     policyDoc(`<set-backend-service base-url="ftp://a" />`, "", ""),
			wantErr: "base-url",
		},
		{
			name:    "unknown named value",
			doc:     policyDoc(`<set-header name="a"><value>{{nope}}</value></set-header>`, "", ""),
			wantErr: "'nope'",
		},
		{name: "doctype", doc: `<!DOCTYPE policies [<!ENTITY x "y">]><policies />`, wantErr: "DOCTYPE"},
		{name: "link format", doc: "<policies />", format: "xml-link", wantErr: "link"},
		{name: "too large", doc: long, wantErr: "characters long"},
		{
			name:    "bad status code",
			doc:     policyDoc(`<return-response><set-status code="99" reason="x" /></return-response>`, "", ""),
			wantErr: "'code'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newAPIMStack(t)
			s.devService(t)
			s.putAPI(t, "echo", "echo", "")

			format := tc.format
			if format == "" {
				format = "xml"
			}

			b, err := json.Marshal(map[string]any{"properties": map[string]any{"format": format, "value": tc.doc}})
			require.NoError(t, err)

			status, body, _ := s.raw(t, http.MethodPut, s.armPath("/apis/echo/policies/policy"), string(b), nil)

			if tc.wantErr == "" {
				assert.Equal(t, http.StatusCreated, status, body)

				return
			}

			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Contains(t, body, "ValidationError")
			assert.Contains(t, body, tc.wantErr)
		})
	}
}

func TestAPIMPolicy_CRUDAtEveryScope(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	ctx := t.Context()
	doc := "<policies><inbound><base /></inbound></policies>"

	contract := armapimanagement.PolicyContract{Properties: &armapimanagement.PolicyContractProperties{
		Format: new(armapimanagement.PolicyContentFormatXML), Value: &doc,
	}}

	tests := []struct {
		put    func() error
		get    func() (string, error)
		delete func() error
		list   func() (int, error)
		name   string
	}{
		{
			name: "service",
			put: func() error {
				_, err := e.f.NewPolicyClient().
					CreateOrUpdate(ctx, apimRG, apimSvc, armapimanagement.PolicyIDNamePolicy, contract, nil)

				return err
			},
			get: func() (string, error) {
				r, err := e.f.NewPolicyClient().Get(ctx, apimRG, apimSvc, armapimanagement.PolicyIDNamePolicy, nil)
				if err != nil {
					return "", err
				}

				return *r.Properties.Value, nil
			},
			delete: func() error {
				_, err := e.f.NewPolicyClient().
					Delete(ctx, apimRG, apimSvc, armapimanagement.PolicyIDNamePolicy, "*", nil)

				return err
			},
			list: func() (int, error) {
				r, err := e.f.NewPolicyClient().ListByService(ctx, apimRG, apimSvc, nil)

				return len(r.Value), err
			},
		},
		{
			name: "api",
			put: func() error {
				_, err := e.f.NewAPIPolicyClient().
					CreateOrUpdate(ctx, apimRG, apimSvc, "echo", armapimanagement.PolicyIDNamePolicy, contract, nil)

				return err
			},
			get: func() (string, error) {
				r, err := e.f.NewAPIPolicyClient().
					Get(ctx, apimRG, apimSvc, "echo", armapimanagement.PolicyIDNamePolicy, nil)
				if err != nil {
					return "", err
				}

				return *r.Properties.Value, nil
			},
			delete: func() error {
				_, err := e.f.NewAPIPolicyClient().
					Delete(ctx, apimRG, apimSvc, "echo", armapimanagement.PolicyIDNamePolicy, "*", nil)

				return err
			},
			list: func() (int, error) {
				r, err := e.f.NewAPIPolicyClient().ListByAPI(ctx, apimRG, apimSvc, "echo", nil)

				return len(r.Value), err
			},
		},
		{
			name: "operation",
			put: func() error {
				_, err := e.f.NewAPIOperationPolicyClient().CreateOrUpdate(ctx, apimRG, apimSvc, "echo", "static",
					armapimanagement.PolicyIDNamePolicy, contract, nil)

				return err
			},
			get: func() (string, error) {
				r, err := e.f.NewAPIOperationPolicyClient().Get(ctx, apimRG, apimSvc, "echo", "static",
					armapimanagement.PolicyIDNamePolicy, nil)
				if err != nil {
					return "", err
				}

				return *r.Properties.Value, nil
			},
			delete: func() error {
				_, err := e.f.NewAPIOperationPolicyClient().Delete(ctx, apimRG, apimSvc, "echo", "static",
					armapimanagement.PolicyIDNamePolicy, "*", nil)

				return err
			},
			list: func() (int, error) {
				r, err := e.f.NewAPIOperationPolicyClient().ListByOperation(ctx, apimRG, apimSvc, "echo", "static", nil)

				return len(r.Value), err
			},
		},
	}

	for _, tc := range tests {
		_, err := tc.get()
		require.Error(t, err, tc.name)
		assert.Equal(t, "ResourceNotFound", sdkErrCode(err), tc.name)

		n, err := tc.list()
		require.NoError(t, err, tc.name)
		assert.Zero(t, n, tc.name)

		require.NoError(t, tc.put(), tc.name)

		got, err := tc.get()
		require.NoError(t, err, tc.name)
		assert.Equal(t, doc, got, tc.name)

		n, err = tc.list()
		require.NoError(t, err, tc.name)
		assert.Equal(t, 1, n, tc.name)

		require.NoError(t, tc.delete(), tc.name)
		require.NoError(t, tc.delete(), tc.name)

		_, err = tc.get()
		require.Error(t, err, tc.name)
	}

	_, err := e.f.NewAPIPolicyClient().Get(ctx, apimRG, apimSvc, "ghost", armapimanagement.PolicyIDNamePolicy, nil)
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	status, _, _ := e.raw(
		t,
		http.MethodPut,
		e.armPath("/policies/other"),
		`{"properties":{"value":"<policies />"}}`,
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, status)

	require.NoError(t, tests[1].put())
	_, err = e.f.NewAPIClient().Delete(ctx, apimRG, apimSvc, "echo", "*", nil)
	require.NoError(t, err)

	e.putAPI(t, "echo", "echo", e.backend.URL)

	_, err = e.f.NewAPIPolicyClient().Get(ctx, apimRG, apimSvc, "echo", armapimanagement.PolicyIDNamePolicy, nil)
	require.Error(t, err, "policy must not survive its API")
}

func (e *gwEnv) mockOperation(t *testing.T) {
	t.Helper()

	_, err := e.f.NewAPIOperationClient().CreateOrUpdate(t.Context(), apimRG, apimSvc, "echo", "static",
		armapimanagement.OperationContract{Properties: &armapimanagement.OperationContractProperties{
			DisplayName: new("static"), Method: new("GET"), URLTemplate: new("/static"),
			Responses: []*armapimanagement.ResponseContract{{
				StatusCode: new(int32(200)),
				Representations: []*armapimanagement.RepresentationContract{{
					ContentType: new("application/json"),
					Examples: map[string]*armapimanagement.ParameterExampleContract{
						"default": {Value: map[string]any{"mocked": true}},
					},
				}},
			}},
		}}, nil)
	require.NoError(t, err)
}

func TestAPIMGateway_Policies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check   func(t *testing.T, e *gwEnv, status int, body string, hdr http.Header)
		setup   func(t *testing.T, e *gwEnv)
		hdr     map[string]string
		name    string
		method  string
		path    string
		inbound string
		backend string
		outbnd  string
	}{
		{
			name: "set-header override", method: "GET", path: "/echo/items/1",
			inbound: `<set-header name="X-Added" exists-action="override"><value>v1</value></set-header>`,
			check: func(t *testing.T, _ *gwEnv, status int, body string, _ http.Header) {
				t.Helper()

				assert.Equal(t, 200, status)
				assert.Equal(t, []string{"v1"}, decodeEcho(t, body).Header["X-Added"])
			},
		},
		{
			name:    "set-header skip keeps client value",
			method:  "GET",
			path:    "/echo/items/1",
			hdr:     map[string]string{"X-A": "orig"},
			inbound: `<set-header name="X-A" exists-action="skip"><value>new</value></set-header>`,
			check: func(t *testing.T, _ *gwEnv, _ int, body string, _ http.Header) {
				t.Helper()

				assert.Equal(t, []string{"orig"}, decodeEcho(t, body).Header["X-A"])
			},
		},
		{
			name: "set-header append", method: "GET", path: "/echo/items/1", hdr: map[string]string{"X-A": "orig"},
			inbound: `<set-header name="X-A" exists-action="append"><value>new</value></set-header>`,
			check: func(t *testing.T, _ *gwEnv, _ int, body string, _ http.Header) {
				t.Helper()

				assert.Equal(t, []string{"orig", "new"}, decodeEcho(t, body).Header["X-A"])
			},
		},
		{
			name: "set-header delete", method: "GET", path: "/echo/items/1", hdr: map[string]string{"X-A": "orig"},
			inbound: `<set-header name="X-A" exists-action="delete" />`,
			check: func(t *testing.T, _ *gwEnv, _ int, body string, _ http.Header) {
				t.Helper()

				assert.NotContains(t, decodeEcho(t, body).Header, "X-A")
			},
		},
		{
			name: "set-query-parameter", method: "GET", path: "/echo/items/1?a=1&keep=k",
			inbound: `<set-query-parameter name="a" exists-action="override"><value>9</value></set-query-parameter>
				<set-query-parameter name="b"><value>2</value></set-query-parameter>`,
			check: func(t *testing.T, _ *gwEnv, _ int, body string, _ http.Header) {
				t.Helper()

				assert.Equal(t, "a=9&b=2&keep=k", decodeEcho(t, body).Query)
			},
		},
		{
			name: "rewrite-uri", method: "GET", path: "/echo/items/5?keep=1",
			inbound: `<rewrite-uri template="/rewritten/{id}/x" />`,
			check: func(t *testing.T, _ *gwEnv, _ int, body string, _ http.Header) {
				t.Helper()

				got := decodeEcho(t, body)
				assert.Equal(t, "/base/rewritten/5/x", got.Path)
				assert.Equal(t, "keep=1", got.Query)
			},
		},
		{
			name: "rewrite-uri drops unmatched params", method: "GET", path: "/echo/items/5?keep=1",
			inbound: `<rewrite-uri template="/r/{id}?fixed=yes" copy-unmatched-params="false" />`,
			check: func(t *testing.T, _ *gwEnv, _ int, body string, _ http.Header) {
				t.Helper()

				got := decodeEcho(t, body)
				assert.Equal(t, "/base/r/5", got.Path)
				assert.Equal(t, "fixed=yes", got.Query)
			},
		},
		{
			name: "return-response", method: "GET", path: "/echo/items/1",
			inbound: `<return-response><set-status code="418" reason="teapot" />
				<set-header name="X-Custom" exists-action="override"><value>c</value></set-header>
				<set-body>short and stout</set-body></return-response>`,
			check: func(t *testing.T, _ *gwEnv, status int, body string, hdr http.Header) {
				t.Helper()

				assert.Equal(t, 418, status)
				assert.Equal(t, "short and stout", body)
				assert.Equal(t, "c", hdr.Get("X-Custom"))
				assert.Empty(t, hdr.Get("X-Backend"))
			},
		},
		{
			name: "mock-response", method: "GET", path: "/echo/static",
			setup: func(t *testing.T, e *gwEnv) {
				t.Helper()
				e.mockOperation(t)
			},
			inbound: `<mock-response status-code="200" content-type="application/json" />`,
			check: func(t *testing.T, _ *gwEnv, status int, body string, hdr http.Header) {
				t.Helper()

				assert.Equal(t, 200, status)
				assert.JSONEq(t, `{"mocked":true}`, body)
				assert.Equal(t, "application/json", hdr.Get("Content-Type"))
				assert.Empty(t, hdr.Get("X-Backend"))
			},
		},
		{
			name: "mock-response without sample", method: "GET", path: "/echo/static",
			inbound: `<mock-response status-code="204" />`,
			check: func(t *testing.T, _ *gwEnv, status int, body string, _ http.Header) {
				t.Helper()

				assert.Equal(t, 204, status)
				assert.Empty(t, body)
			},
		},
		{
			name: "outbound set-header", method: "GET", path: "/echo/items/1",
			outbnd: `<set-header name="X-Out" exists-action="override"><value>o</value></set-header>
				<set-header name="X-Backend" exists-action="delete" />`,
			check: func(t *testing.T, _ *gwEnv, status int, _ string, hdr http.Header) {
				t.Helper()

				assert.Equal(t, 200, status)
				assert.Equal(t, "o", hdr.Get("X-Out"))
				assert.Empty(t, hdr.Get("X-Backend"))
			},
		},
		{
			name:    "check-header pass",
			method:  "GET",
			path:    "/echo/items/1",
			hdr:     map[string]string{"X-Token": "ABC"},
			inbound: checkHeader("X-Token", "403", "forbidden", "true", "abc"),
			check: func(t *testing.T, _ *gwEnv, status int, _ string, _ http.Header) {
				t.Helper()

				assert.Equal(t, 200, status)
			},
		},
		{
			name:    "check-header case sensitive fail",
			method:  "GET",
			path:    "/echo/items/1",
			hdr:     map[string]string{"X-Token": "ABC"},
			inbound: checkHeader("X-Token", "403", "forbidden", "false", "abc"),
			check: func(t *testing.T, _ *gwEnv, status int, body string, _ http.Header) {
				t.Helper()

				assert.Equal(t, 403, status)
				assert.Equal(t, "forbidden", body)
			},
		},
		{
			name:    "check-header missing",
			method:  "GET",
			path:    "/echo/items/1",
			inbound: checkHeader("X-Token", "401", "need token", "true", "abc"),
			check: func(t *testing.T, _ *gwEnv, status int, body string, _ http.Header) {
				t.Helper()

				assert.Equal(t, 401, status)
				assert.Equal(t, "need token", body)
			},
		},
		{
			name: "set-backend-service base-url", method: "GET", path: "/echo/items/7",
			inbound: `<set-backend-service base-url="BACKEND/alt" />`,
			check: func(t *testing.T, _ *gwEnv, _ int, body string, _ http.Header) {
				t.Helper()

				assert.Equal(t, "/alt/items/7", decodeEcho(t, body).Path)
			},
		},
		{
			name: "set-backend-service entity with credentials", method: "GET", path: "/echo/items/7",
			setup: func(t *testing.T, e *gwEnv) {
				t.Helper()

				_, err := e.f.NewBackendClient().
					CreateOrUpdate(t.Context(), apimRG, apimSvc, "be1", armapimanagement.BackendContract{
						Properties: &armapimanagement.BackendContractProperties{
							URL: new(
								e.backend.URL + "/viabackend",
							),
							Protocol: new(armapimanagement.BackendProtocolHTTP),
							Credentials: &armapimanagement.BackendCredentialsContract{
								Authorization: &armapimanagement.BackendAuthorizationHeaderCredentials{
									Scheme: new("Bearer"), Parameter: new("tok"),
								},
								Header: map[string][]*string{"X-Be-Key": {new("bk")}},
								Query:  map[string][]*string{"code": {new("qc")}},
							},
						},
					}, nil)
				require.NoError(t, err)
			},
			inbound: `<set-backend-service backend-id="be1" />`,
			check: func(t *testing.T, _ *gwEnv, _ int, body string, _ http.Header) {
				t.Helper()

				got := decodeEcho(t, body)
				assert.Equal(t, "/viabackend/items/7", got.Path)
				assert.Equal(t, []string{"Bearer tok"}, got.Header["Authorization"])
				assert.Equal(t, []string{"bk"}, got.Header["X-Be-Key"])
				assert.Equal(t, "code=qc", got.Query)
			},
		},
		{
			name: "named value substitution", method: "GET", path: "/echo/items/1",
			setup: func(t *testing.T, e *gwEnv) {
				t.Helper()

				p, err := e.f.NewNamedValueClient().
					BeginCreateOrUpdate(t.Context(), apimRG, apimSvc, "nv", armapimanagement.NamedValueCreateContract{
						Properties: &armapimanagement.NamedValueCreateContractProperties{
							DisplayName: new("my-secret"), Value: new("shh"), Secret: new(true),
						},
					}, nil)
				require.NoError(t, err)

				_, err = p.PollUntilDone(t.Context(), &runtime.PollUntilDoneOptions{Frequency: 5})
				require.NoError(t, err)
			},
			inbound: `<set-header name="X-Named"><value>pre-{{my-secret}}-post</value></set-header>`,
			check: func(t *testing.T, _ *gwEnv, _ int, body string, _ http.Header) {
				t.Helper()

				assert.Equal(t, []string{"pre-shh-post"}, decodeEcho(t, body).Header["X-Named"])
			},
		},
		{
			name:   "cors preflight allowed",
			method: "OPTIONS",
			path:   "/echo/items/1",
			hdr: map[string]string{
				"Ocp-Apim-Subscription-Key":      "",
				"Origin":                         "https://app.example",
				"Access-Control-Request-Method":  "GET",
				"Access-Control-Request-Headers": "x-custom",
			},
			inbound: `<cors allow-credentials="false"><allowed-origins><origin>https://app.example</origin></allowed-origins>
				<allowed-methods preflight-result-max-age="120"><method>GET</method><method>POST</method></allowed-methods>
				<allowed-headers><header>*</header></allowed-headers></cors>`,
			check: func(t *testing.T, _ *gwEnv, status int, _ string, hdr http.Header) {
				t.Helper()

				assert.Equal(t, 200, status)
				assert.Equal(t, "https://app.example", hdr.Get("Access-Control-Allow-Origin"))
				assert.Equal(t, "GET, POST", hdr.Get("Access-Control-Allow-Methods"))
				assert.Equal(t, "x-custom", hdr.Get("Access-Control-Allow-Headers"))
				assert.Equal(t, "120", hdr.Get("Access-Control-Max-Age"))
			},
		},
		{
			name: "cors preflight origin not allowed", method: "OPTIONS", path: "/echo/items/1",
			hdr: map[string]string{
				"Ocp-Apim-Subscription-Key":     "",
				"Origin":                        "https://evil.example",
				"Access-Control-Request-Method": "GET",
			},
			inbound: `<cors><allowed-origins><origin>https://app.example</origin></allowed-origins>
				<allowed-headers><header>*</header></allowed-headers></cors>`,
			check: func(t *testing.T, _ *gwEnv, status int, _ string, hdr http.Header) {
				t.Helper()

				assert.Equal(t, 200, status)
				assert.Empty(t, hdr.Get("Access-Control-Allow-Origin"))
			},
		},
		{
			name:   "cors actual request",
			method: "GET",
			path:   "/echo/items/1",
			hdr:    map[string]string{"Origin": "https://any.example"},
			inbound: `<cors><allowed-origins><origin>*</origin></allowed-origins>
				<allowed-headers><header>*</header></allowed-headers>
				<expose-headers><header>X-Backend</header></expose-headers></cors>`,
			check: func(t *testing.T, _ *gwEnv, status int, _ string, hdr http.Header) {
				t.Helper()

				assert.Equal(t, 200, status)
				assert.Equal(t, "*", hdr.Get("Access-Control-Allow-Origin"))
				assert.Equal(t, "X-Backend", hdr.Get("Access-Control-Expose-Headers"))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newGWEnv(t)

			if tc.setup != nil {
				tc.setup(t, e)
			}

			backend := tc.backend
			if backend == "" {
				backend = "<base />"
			}

			doc := strings.ReplaceAll(policyDoc(tc.inbound, backend, tc.outbnd), "BACKEND", e.backend.URL)
			e.policy(t, "/apis/echo", doc, http.StatusCreated)

			status, body, hdr := e.call(t, tc.method, tc.path, "", tc.hdr)
			tc.check(t, e, status, body, hdr)
		})
	}
}

func TestAPIMGateway_RateLimit(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.policy(
		t,
		"/apis/echo",
		policyDoc(`<rate-limit calls="2" renewal-period="60" remaining-calls-header-name="X-Left" />`, "", ""),
		http.StatusCreated,
	)

	status, _, hdr := e.call(t, "GET", "/echo/items/1", "", nil)
	assert.Equal(t, 200, status)
	assert.Equal(t, "1", hdr.Get("X-Left"))

	status, _, _ = e.call(t, "GET", "/echo/items/1", "", nil)
	assert.Equal(t, 200, status)

	e.clock.Advance(20 * time.Second)

	status, body, hdr := e.call(t, "GET", "/echo/items/1", "", nil)
	assert.Equal(t, http.StatusTooManyRequests, status)
	assert.Contains(t, body, `"message":"Rate limit is exceeded"`)
	assert.Equal(t, "40", hdr.Get("Retry-After"))

	_, err := e.f.NewSubscriptionClient().CreateOrUpdate(t.Context(), apimRG, apimSvc, "k2",
		armapimanagement.SubscriptionCreateParameters{
			Properties: &armapimanagement.SubscriptionCreateParameterProperties{
				DisplayName: new("k2"), Scope: new("/apis"), State: new(armapimanagement.SubscriptionStateActive),
			},
		}, nil)
	require.NoError(t, err)

	sec, err := e.f.NewSubscriptionClient().ListSecrets(t.Context(), apimRG, apimSvc, "k2", nil)
	require.NoError(t, err)

	status, _, _ = e.call(
		t,
		"GET",
		"/echo/items/1",
		"",
		map[string]string{"Ocp-Apim-Subscription-Key": *sec.PrimaryKey},
	)
	assert.Equal(t, 200, status, "limits are per subscription")

	e.clock.Advance(41 * time.Second)

	status, _, _ = e.call(t, "GET", "/echo/items/1", "", nil)
	assert.Equal(t, 200, status, "window renewed")
}

func TestAPIMGateway_PolicyInheritance(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)

	svcDoc := policyDoc(`<set-header name="X-Svc"><value>s</value></set-header>`, "", "")
	b, err := json.Marshal(map[string]any{"properties": map[string]any{"format": "xml", "value": svcDoc}})
	require.NoError(t, err)

	status, body, _ := e.raw(t, http.MethodPut, e.armPath("/policies/policy"), string(b), nil)
	require.Equal(t, http.StatusCreated, status, body)

	headers := func(path string) map[string][]string {
		_, echoed, _ := e.call(t, "GET", path, "", nil)

		return decodeEcho(t, echoed).Header
	}

	assert.Equal(t, []string{"s"}, headers("/echo/items/1")["X-Svc"])

	e.policy(
		t,
		"/apis/echo",
		policyDoc(`<set-header name="X-Api"><value>a</value></set-header>`, "", ""),
		http.StatusCreated,
	)

	got := headers("/echo/items/1")
	assert.Equal(t, []string{"s"}, got["X-Svc"])
	assert.Equal(t, []string{"a"}, got["X-Api"])

	e.policy(t, "/apis/echo", "<policies><inbound><set-header name=\"X-Api\"><value>a</value></set-header></inbound>"+
		"<backend><base /></backend><outbound><base /></outbound></policies>", http.StatusOK)

	got = headers("/echo/items/1")
	assert.NotContains(t, got, "X-Svc", "an inbound section without <base /> drops the parent's statements")
	assert.Equal(t, []string{"a"}, got["X-Api"])

	e.policy(
		t,
		"/apis/echo/operations/get-item",
		policyDoc(`<set-header name="X-Op"><value>o</value></set-header>`, "", ""),
		http.StatusCreated,
	)

	got = headers("/echo/items/1")
	assert.Equal(t, []string{"o"}, got["X-Op"])
	assert.Equal(t, []string{"a"}, got["X-Api"])

	got = headers("/echo/static")
	assert.NotContains(t, got, "X-Op")
}

func checkHeader(name, code, msg, ignoreCase, value string) string {
	return `<check-header name="` + name + `" failed-check-httpcode="` + code + `" failed-check-error-message="` + msg +
		`" ignore-case="` + ignoreCase + `"><value>` + value + `</value></check-header>`
}
