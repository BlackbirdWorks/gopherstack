package azurearm_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/apimanagement/armapimanagement/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func serviceBody(sku string, capacity int) string {
	return `{"location":"westus","sku":{"name":"` + sku + `","capacity":` + strconv.Itoa(capacity) + `},` +
		`"properties":{"publisherEmail":"ops@example.com","publisherName":"Ops"}}`
}

func decodeProps(t *testing.T, body string) map[string]any {
	t.Helper()

	var out struct {
		Properties map[string]any `json:"properties"`
	}

	require.NoError(t, json.Unmarshal([]byte(body), &out), body)

	return out.Properties
}

// Bodies follow the examples of azure-rest-api-specs apimdeployment.json (ApiManagementCreateService,
// ApiManagementCreateServiceHavingMsi): Created heading for Activating, then URLs only once Succeeded, none of the
// portal URLs for Consumption.
func TestAPIMService_ProvisioningBodies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sku          string
		platform     string
		capacity     int
		wantPortals  bool
		wantRegional bool
	}{
		{name: "developer", sku: "Developer", capacity: 1, platform: "stv2", wantPortals: true, wantRegional: true},
		{name: "premium", sku: "Premium", capacity: 2, platform: "stv2", wantPortals: true, wantRegional: true},
		{name: "consumption", sku: "Consumption", capacity: 0, platform: "mtv1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newAPIMStack(t)

			status, body, _ := s.raw(t, http.MethodPut, s.armPath(""), serviceBody(tc.sku, tc.capacity), nil)
			require.Equal(t, http.StatusCreated, status, body)

			created := decodeProps(t, body)
			assert.Equal(t, "Created", created["provisioningState"])
			assert.Equal(t, "Activating", created["targetProvisioningState"])
			assert.Equal(t, "https://gw1.azure-api.net", created["gatewayUrl"])
			assert.Equal(t, tc.platform, created["platformVersion"])

			for _, k := range []string{"portalUrl", "developerPortalUrl", "managementApiUrl", "scmUrl", "gatewayRegionalUrl"} {
				assert.NotContains(t, created, k)
			}

			s.clock.Advance(time.Hour)

			status, body, _ = s.raw(t, http.MethodGet, s.armPath(""), "", nil)
			require.Equal(t, http.StatusOK, status, body)

			done := decodeProps(t, body)
			assert.Equal(t, "Succeeded", done["provisioningState"])
			assert.Empty(t, done["targetProvisioningState"])
			assert.Equal(t, "https://gw1.azure-api.net", done["gatewayUrl"])

			for _, k := range []string{"portalUrl", "developerPortalUrl", "managementApiUrl", "scmUrl"} {
				if tc.wantPortals {
					assert.NotEmpty(t, done[k], k)
				} else {
					assert.NotContains(t, done, k)
				}
			}

			if tc.wantRegional {
				assert.Equal(t, "https://gw1-westus-01.regional.azure-api.net", done["gatewayRegionalUrl"])
			} else {
				assert.NotContains(t, done, "gatewayRegionalUrl")
			}
		})
	}
}

// Limits: Learn "API Management limits" (policy document size 512 KiB, 16 KiB on Consumption).
func TestAPIMLimits_PolicySize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		sku    string
		bases  int
		cap    int
		status int
	}{
		{name: "consumption within", sku: "Consumption", bases: 1000, status: http.StatusCreated},
		{name: "consumption over 16 KiB", sku: "Consumption", bases: 2100, status: http.StatusBadRequest},
		{name: "developer 160 KB", sku: "Developer", bases: 20000, cap: 1, status: http.StatusCreated},
		{name: "developer over 512 KiB", sku: "Developer", bases: 66000, cap: 1, status: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newAPIMStack(t)
			s.createService(t, apimSvc, armapimanagement.SKUType(tc.sku), int32(tc.cap))
			s.putAPI(t, "echo", "echo", "")

			doc := "<policies><inbound>" + strings.Repeat("<base />", tc.bases) + "</inbound></policies>"
			b, err := json.Marshal(map[string]any{"properties": map[string]any{"format": "xml", "value": doc}})
			require.NoError(t, err)

			status, body, _ := s.raw(t, http.MethodPut, s.armPath("/apis/echo/policies/policy"), string(b), nil)
			assert.Equal(t, tc.status, status, body[:min(len(body), 300)])
		})
	}
}

func TestAPIMLimits_ProductsOnConsumption(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)
	s.createService(t, apimSvc, armapimanagement.SKUTypeConsumption, 0)

	for i := range 100 {
		status, body, _ := s.raw(t, http.MethodPut, s.armPath("/products/p"+strconv.Itoa(i)),
			`{"properties":{"displayName":"P"}}`, nil)
		require.Equal(t, http.StatusCreated, status, body)
	}

	status, body, _ := s.raw(t, http.MethodPut, s.armPath("/products/over"), `{"properties":{"displayName":"P"}}`, nil)
	assert.Equal(t, http.StatusConflict, status, body)

	status, body, _ = s.raw(t, http.MethodPut, s.armPath("/products/p1"), `{"properties":{"displayName":"P2"}}`, nil)
	assert.Equal(t, http.StatusOK, status, body)
}

// Spec: operationId and backendId are limited to 80 characters, the other identifiers to 256; backend title and
// description have minLength 1.
func TestAPIMEntities_SpecConstraints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		body   string
		status int
	}{
		{
			name: "backend id 80", path: "/backends/" + strings.Repeat("b", 80),
			body: `{"properties":{"url":"http://a","protocol":"http"}}`, status: http.StatusCreated,
		},
		{
			name: "backend id 81", path: "/backends/" + strings.Repeat("b", 81),
			body: `{"properties":{"url":"http://a","protocol":"http"}}`, status: http.StatusBadRequest,
		},
		{
			name: "backend empty title", path: "/backends/b",
			body: `{"properties":{"url":"http://a","protocol":"http","title":""}}`, status: http.StatusBadRequest,
		},
		{
			name: "backend empty description", path: "/backends/b",
			body: `{"properties":{"url":"http://a","protocol":"http","description":""}}`, status: http.StatusBadRequest,
		},
		{
			name: "operation id 81", path: "/apis/echo/operations/" + strings.Repeat("o", 81),
			body: `{"properties":{"displayName":"o","method":"GET","urlTemplate":"/o"}}`, status: http.StatusBadRequest,
		},
		{
			name: "operation id 80", path: "/apis/echo/operations/" + strings.Repeat("o", 80),
			body: `{"properties":{"displayName":"o","method":"GET","urlTemplate":"/o"}}`, status: http.StatusCreated,
		},
		{
			name: "product id 81", path: "/products/" + strings.Repeat("p", 81),
			body: `{"properties":{"displayName":"p"}}`, status: http.StatusCreated,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newAPIMStack(t)
			s.devService(t)
			s.putAPI(t, "echo", "echo", "")

			status, body, _ := s.raw(t, http.MethodPut, s.armPath(tc.path), tc.body, nil)
			assert.Equal(t, tc.status, status, body)
		})
	}
}

// Learn manage-mcp-servers-rest-api: "412 Precondition Failed - If-Match is required for deletions"; the
// specification marks If-Match required on every child delete.
func TestAPIMEntities_DeleteRequiresIfMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		body    string
		ifMatch string
		status  int
	}{
		{name: "product none", path: "/products/p", body: `{"properties":{"displayName":"P"}}`, status: 412},
		{
			name: "product star", path: "/products/p", body: `{"properties":{"displayName":"P"}}`, ifMatch: "*",
			status: 200,
		},
		{
			name: "backend none", path: "/backends/b", body: `{"properties":{"url":"http://a","protocol":"http"}}`,
			status: 412,
		},
		{
			name: "named value none", path: "/namedValues/n", body: `{"properties":{"displayName":"n","value":"v"}}`,
			status: 412,
		},
		{
			name: "api policy none", path: "/apis/echo/policies/policy",
			body: `{"properties":{"format":"xml","value":"<policies><inbound /></policies>"}}`, status: 412,
		},
		{name: "missing entity", path: "/products/ghost", status: 204},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newAPIMStack(t)
			s.devService(t)
			s.putAPI(t, "echo", "echo", "")

			if tc.body != "" {
				status, body, _ := s.raw(t, http.MethodPut, s.armPath(tc.path), tc.body, nil)
				require.Equal(t, http.StatusCreated, status, body)
			}

			hdr := map[string]string{}
			if tc.ifMatch != "" {
				hdr["If-Match"] = tc.ifMatch
			}

			status, body, _ := s.raw(t, http.MethodDelete, s.armPath(tc.path), "", hdr)
			assert.Equal(t, tc.status, status, body)

			if tc.status == http.StatusPreconditionFailed {
				assert.Contains(t, body, "PreconditionFailed")
			}
		})
	}
}

func TestAPIMEntities_SecretActionsCarryETag(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)
	s.devService(t)

	status, body, _ := s.raw(t, http.MethodPut, s.armPath("/namedValues/n"),
		`{"properties":{"displayName":"n","value":"v","secret":true}}`, nil)
	require.Equal(t, http.StatusCreated, status, body)

	status, body, _ = s.raw(t, http.MethodPut, s.armPath("/subscriptions/s"),
		`{"properties":{"displayName":"s","scope":"/apis","state":"active"}}`, nil)
	require.Equal(t, http.StatusCreated, status, body)

	for _, path := range []string{"/namedValues/n/listValue", "/subscriptions/s/listSecrets"} {
		code, out, hdr := s.raw(t, http.MethodPost, s.armPath(path), "", nil)
		assert.Equal(t, http.StatusOK, code, out)
		assert.NotEmpty(t, hdr.Get("ETag"), path)
	}
}

// Learn api-management-subscriptions: the key is passed to the backend by default and an invalid key falls back to
// an open product; api-management-error-handling-policies: predefined messages.
func TestAPIMGateway_KeyHandling(t *testing.T) {
	t.Parallel()

	const (
		missing = "Access denied due to missing subscription key. " +
			"Make sure to include subscription key when making requests to this API."
		invalid = "Access denied due to invalid subscription key. " +
			"Make sure to provide a valid key for an active subscription."
	)

	tests := []struct {
		name      string
		path      string
		key       string
		openAPI   string
		wantMsg   string
		status    int
		wantKey   bool
		useCustom bool
	}{
		{name: "valid key forwarded", path: "/echo/items/1", key: "VALID", status: 200, wantKey: true},
		{name: "missing", path: "/echo/items/1", status: 401, wantMsg: missing},
		{name: "invalid", path: "/echo/items/1", key: "nope", status: 401, wantMsg: invalid},
		{name: "invalid key with open product", path: "/echo/items/1", key: "nope", openAPI: "echo", status: 200},
		{name: "no key with open product", path: "/echo/items/1", openAPI: "echo", status: 200},
		{
			name: "custom header forwarded", path: "/echo/items/1", key: "VALID", status: 200, wantKey: true,
			useCustom: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newGWEnv(t)

			if tc.openAPI != "" {
				_, err := e.f.NewProductClient().
					CreateOrUpdate(t.Context(), apimRG, apimSvc, "open", armapimanagement.ProductContract{
						Properties: &armapimanagement.ProductContractProperties{
							DisplayName: new("Open"), SubscriptionRequired: new(false),
						},
					}, nil)
				require.NoError(t, err)

				_, err = e.f.NewProductAPIClient().CreateOrUpdate(t.Context(), apimRG, apimSvc, "open", tc.openAPI, nil)
				require.NoError(t, err)
			}

			name := "Ocp-Apim-Subscription-Key"

			if tc.useCustom {
				name = "X-Api-Key"

				_, err := e.f.NewAPIClient().
					Update(t.Context(), apimRG, apimSvc, "echo", "*", armapimanagement.APIUpdateContract{
						Properties: &armapimanagement.APIContractUpdateProperties{
							SubscriptionKeyParameterNames: &armapimanagement.SubscriptionKeyParameterNamesContract{
								Header: new(name), Query: new("apikey"),
							},
						},
					}, nil)
				require.NoError(t, err)
			}

			hdr := map[string]string{"Ocp-Apim-Subscription-Key": ""}

			switch tc.key {
			case "":
			case "VALID":
				hdr[name] = e.key
			default:
				hdr[name] = tc.key
			}

			status, body, _ := e.call(t, "GET", tc.path, "", hdr)
			require.Equal(t, tc.status, status, body)

			if tc.wantMsg != "" {
				assert.JSONEq(t, `{"statusCode":401,"message":"`+tc.wantMsg+`"}`, body)
			}

			if tc.wantKey {
				assert.Equal(t, []string{e.key}, decodeEcho(t, body).Header[http.CanonicalHeaderKey(name)])
			}
		})
	}
}

func TestAPIMGateway_KeyRemovedByPolicy(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.policy(t, "/apis/echo",
		policyDoc(`<set-header name="Ocp-Apim-Subscription-Key" exists-action="delete" />
			<set-query-parameter name="subscription-key" exists-action="delete" />`, "", ""),
		http.StatusCreated)

	status, body, _ := e.call(t, "GET", "/echo/items/1?subscription-key="+e.key, "", nil)
	require.Equal(t, http.StatusOK, status, body)
	assert.False(t, decodeEcho(t, body).keyForwarded(e.key))
	assert.NotContains(t, decodeEcho(t, body).Header, "Ocp-Apim-Subscription-Key")
}

func TestAPIMGateway_RateLimitSlidingWindow(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.policy(t, "/apis/echo",
		policyDoc(`<rate-limit calls="2" renewal-period="60" retry-after-header-name="X-Retry"
			remaining-calls-header-name="X-Left" total-calls-header-name="X-Total" />`, "", ""),
		http.StatusCreated)

	call := func() (int, http.Header) {
		status, _, hdr := e.call(t, "GET", "/echo/items/1", "", nil)

		return status, hdr
	}

	status, hdr := call()
	assert.Equal(t, 200, status)
	assert.Equal(t, "1", hdr.Get("X-Left"))
	assert.Equal(t, "2", hdr.Get("X-Total"))

	e.clock.Advance(30 * time.Second)

	status, _ = call()
	assert.Equal(t, 200, status)

	status, hdr = call()
	assert.Equal(t, http.StatusTooManyRequests, status)
	assert.Equal(t, "30", hdr.Get("X-Retry"))
	assert.Empty(t, hdr.Get("Retry-After"), "retry-after-header-name replaces Retry-After")
	assert.Equal(t, "0", hdr.Get("X-Left"))

	e.clock.Advance(30 * time.Second)

	status, _ = call()
	assert.Equal(t, 200, status, "only the first call left the sliding window")

	status, hdr = call()
	assert.Equal(t, http.StatusTooManyRequests, status, "a fixed window would have renewed both calls")
	assert.Equal(t, "30", hdr.Get("X-Retry"))
}

func TestAPIMGateway_ForwardTimeoutMillis(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)

	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))

	t.Cleanup(func() {
		close(release)
		slow.Close()
	})

	e.putAPI(t, "slow", "slow", slow.URL)
	e.putOp(t, "slow", "root", "get", "/")
	e.policy(t, "/apis/slow", policyDoc("", `<forward-request timeout-ms="50" />`, ""), http.StatusCreated)

	status, body, _ := e.call(t, "GET", "/slow", "", nil)
	assert.Equal(t, http.StatusBadGateway, status, body)
}

func TestAPIMGateway_CORSDefaultMethods(t *testing.T) {
	t.Parallel()

	e := newGWEnv(t)
	e.policy(t, "/apis/echo", policyDoc(`<cors><allowed-origins><origin>https://app.example</origin></allowed-origins>
		<allowed-headers><header>*</header></allowed-headers></cors>`, "", ""), http.StatusCreated)

	status, _, hdr := e.call(t, "OPTIONS", "/echo/items/1", "", map[string]string{
		"Ocp-Apim-Subscription-Key": "", "Origin": "https://app.example", "Access-Control-Request-Method": "GET",
	})
	assert.Equal(t, 200, status)
	assert.Equal(t, "GET, POST", hdr.Get("Access-Control-Allow-Methods"))
}

// Policy reference pages on learn.microsoft.com/azure/api-management (one per element).
func TestAPIMPolicy_ReferenceRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		doc     string
		scope   string
		wantErr string
	}{
		{
			name: "rate-limit at service scope", scope: "",
			doc:     policyDoc(`<rate-limit calls="1" renewal-period="1" />`, "", ""),
			wantErr: "product, API and operation",
		},
		{
			name: "rate-limit at api scope", scope: "/apis/echo",
			doc: policyDoc(`<rate-limit calls="1" renewal-period="1" />`, "", ""),
		},
		{
			name: "check-header without message", scope: "/apis/echo",
			doc: policyDoc(
				`<check-header name="h" failed-check-httpcode="401" ignore-case="true"><value>v</value></check-header>`,
				"", ""),
			wantErr: "'failed-check-error-message' is required",
		},
		{
			name: "check-header without ignore-case", scope: "/apis/echo",
			doc: policyDoc(
				`<check-header name="h" failed-check-httpcode="401" failed-check-error-message="m"><value>v</value></check-header>`,
				"",
				"",
			),
			wantErr: "'ignore-case' is required",
		},
		{
			name: "check-header without values", scope: "/apis/echo",
			doc: policyDoc(
				`<check-header name="h" failed-check-httpcode="401" failed-check-error-message="m" ignore-case="false" />`,
				"",
				"",
			),
		},
		{
			name: "set-status without reason", scope: "/apis/echo",
			doc:     policyDoc(`<return-response><set-status code="200" /></return-response>`, "", ""),
			wantErr: "'reason' is required",
		},
		{
			name: "forward timeout zero", scope: "/apis/echo",
			doc: policyDoc("", `<forward-request timeout="0" />`, ""),
		},
		{
			name: "forward timeout 600", scope: "/apis/echo",
			doc: policyDoc("", `<forward-request timeout="600" />`, ""),
		},
		{
			name: "forward timeout-ms", scope: "/apis/echo",
			doc: policyDoc("", `<forward-request timeout-ms="250" />`, ""),
		},
		{
			name: "forward both timeouts", scope: "/apis/echo",
			doc:     policyDoc("", `<forward-request timeout="1" timeout-ms="1" />`, ""),
			wantErr: "either timeout or timeout-ms",
		},
		{
			name: "mock-response in outbound", scope: "/apis/echo",
			doc: policyDoc("", "", `<mock-response status-code="200" />`),
		},
		{
			name: "statement in on-error", scope: "/apis/echo",
			doc: "<policies><inbound><base /></inbound><backend><base /></backend><outbound><base /></outbound>" +
				`<on-error><set-header name="x"><value>y</value></set-header></on-error></policies>`,
			wantErr: "on-error",
		},
		{
			name: "cors without origins", scope: "/apis/echo",
			doc:     policyDoc(`<cors><allowed-headers><header>*</header></allowed-headers></cors>`, "", ""),
			wantErr: "allowed-origins",
		},
		{
			name: "cors without headers", scope: "/apis/echo",
			doc:     policyDoc(`<cors><allowed-origins><origin>*</origin></allowed-origins></cors>`, "", ""),
			wantErr: "allowed-headers",
		},
		{
			name: "cors bad origin", scope: "/apis/echo",
			doc: policyDoc(`<cors><allowed-origins><origin>app.example</origin></allowed-origins>
				<allowed-headers><header>*</header></allowed-headers></cors>`, "", ""),
			wantErr: "scheme and a host",
		},
		{
			name: "cors empty methods", scope: "/apis/echo",
			doc: policyDoc(`<cors><allowed-origins><origin>*</origin></allowed-origins><allowed-methods />
				<allowed-headers><header>*</header></allowed-headers></cors>`, "", ""),
			wantErr: "element is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newAPIMStack(t)
			s.devService(t)
			s.putAPI(t, "echo", "echo", "")

			b, err := json.Marshal(map[string]any{"properties": map[string]any{"format": "xml", "value": tc.doc}})
			require.NoError(t, err)

			status, body, _ := s.raw(t, http.MethodPut, s.armPath(tc.scope+"/policies/policy"), string(b), nil)

			if tc.wantErr == "" {
				assert.Equal(t, http.StatusCreated, status, body)

				return
			}

			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Contains(t, body, tc.wantErr)
		})
	}
}
