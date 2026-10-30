package ociapigateway_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/apigateway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGatewayLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true, register: true})

	created := e.createGateway(t, "gw1")
	require.NotNil(t, created.OpcWorkRequestId)
	assert.Equal(t, apigateway.GatewayLifecycleStateCreating, created.LifecycleState)
	assert.Regexp(t, `^[0-9a-f]{26}\.apigateway\.us-ashburn-1\.oci\.customer-oci\.com$`, *created.Hostname)

	wr, err := e.wr.GetWorkRequest(
		t.Context(),
		apigateway.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Equal(t, apigateway.WorkRequestStatusInProgress, wr.Status)
	assert.Equal(t, apigateway.WorkRequestOperationTypeCreateGateway, wr.OperationType)

	e.settle()

	wr, err = e.wr.GetWorkRequest(
		t.Context(),
		apigateway.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Equal(t, apigateway.WorkRequestStatusSucceeded, wr.Status)

	got, err := e.gw.GetGateway(t.Context(), apigateway.GetGatewayRequest{GatewayId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, apigateway.GatewayLifecycleStateActive, got.LifecycleState)
	require.NotNil(t, got.Etag)

	upd, err := e.gw.UpdateGateway(t.Context(), apigateway.UpdateGatewayRequest{
		GatewayId: created.Id, IfMatch: got.Etag,
		DisplayName: new("renamed"), FreeformTags: map[string]string{"k": "v"},
	})
	require.NoError(t, err)
	require.NotNil(t, upd.OpcWorkRequestId)

	_, err = e.gw.UpdateGateway(t.Context(), apigateway.UpdateGatewayRequest{
		GatewayId: created.Id, DisplayName: new("again"),
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "UPDATING gateway rejects updates")

	e.settle()

	got, err = e.gw.GetGateway(t.Context(), apigateway.GetGatewayRequest{GatewayId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *got.DisplayName)
	assert.Equal(t, "v", got.FreeformTags["k"])

	_, err = e.gw.UpdateGateway(t.Context(), apigateway.UpdateGatewayRequest{
		GatewayId: created.Id, IfMatch: new("stale"),
		DisplayName: new("x"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	list, err := e.gw.ListGateways(t.Context(), apigateway.ListGatewaysRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	_, err = e.gw.DeleteGateway(t.Context(), apigateway.DeleteGatewayRequest{GatewayId: created.Id})
	require.NoError(t, err)

	e.settle()

	list, err = e.gw.ListGateways(t.Context(), apigateway.ListGatewaysRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	assert.Empty(t, list.Items)

	deleted, err := e.gw.ListGateways(t.Context(), apigateway.ListGatewaysRequest{
		CompartmentId: new(testCompartment), LifecycleState: apigateway.GatewayLifecycleStateDeleted,
	})
	require.NoError(t, err)
	assert.Len(t, deleted.Items, 1)

	wrs, err := e.wr.ListWorkRequests(t.Context(), apigateway.ListWorkRequestsRequest{
		CompartmentId: new(testCompartment), ResourceId: created.Id,
	})
	require.NoError(t, err)
	assert.Len(t, wrs.Items, 3)
}

func TestCreateGatewayValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.backend.SetSubnetChecker(func(id string) bool { return id == testSubnet })
	e.backend.SetCompartmentChecker(func(id string) bool { return id == testCompartment })

	tests := []struct {
		mutate func(*apigateway.CreateGatewayDetails)
		name   string
		status int
	}{
		{
			func(d *apigateway.CreateGatewayDetails) { d.SubnetId = new("ocid1.subnet.oc1..nope") },
			"unknown subnet",
			404,
		},
		{func(d *apigateway.CreateGatewayDetails) { d.CompartmentId = new(otherCompart) }, "unknown compartment", 404},
		{func(d *apigateway.CreateGatewayDetails) { d.CertificateId = new("ocid1.cert") }, "unknown certificate", 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := apigateway.CreateGatewayDetails{
				CompartmentId: new(testCompartment), EndpointType: apigateway.GatewayEndpointTypePrivate,
				SubnetId: new(testSubnet),
			}
			tt.mutate(&d)

			_, err := e.gw.CreateGateway(t.Context(), apigateway.CreateGatewayRequest{CreateGatewayDetails: d})
			assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	t.Run("duplicate name", func(t *testing.T) {
		t.Parallel()

		e.createGateway(t, "dup")

		_, err := e.gw.CreateGateway(t.Context(), apigateway.CreateGatewayRequest{
			CompartmentId: new(testCompartment), EndpointType: apigateway.GatewayEndpointTypePublic,
			SubnetId: new(testSubnet), DisplayName: new("DUP"),
		})
		assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
	})
}

func TestChangeGatewayCompartment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "mv")

	resp, err := e.gw.ChangeGatewayCompartment(t.Context(), apigateway.ChangeGatewayCompartmentRequest{
		GatewayId:     gw.Id,
		CompartmentId: new(otherCompart),
	})
	require.NoError(t, err)
	require.NotNil(t, resp.OpcWorkRequestId)

	got, err := e.gw.GetGateway(t.Context(), apigateway.GetGatewayRequest{GatewayId: gw.Id})
	require.NoError(t, err)
	assert.Equal(t, otherCompart, *got.CompartmentId)
}

func TestDeleteGatewayWithDeployment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "busy")
	dep := e.mkDeployment(t, *gw.Id, "/v1", stockSpec(200, "ok"))

	_, err := e.gw.DeleteGateway(t.Context(), apigateway.DeleteGatewayRequest{GatewayId: gw.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.dep.DeleteDeployment(t.Context(), apigateway.DeleteDeploymentRequest{DeploymentId: &dep})
	require.NoError(t, err)

	e.settle()

	_, err = e.gw.DeleteGateway(t.Context(), apigateway.DeleteGatewayRequest{GatewayId: gw.Id})
	require.NoError(t, err)
}

func TestAuthEnforcement(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true, register: true})
	e.createGateway(t, "signed")

	status, body, _ := rawDo(t, http.MethodGet, e.base+"/20190501/gateways?compartmentId="+testCompartment, "")
	assert.Equal(t, http.StatusUnauthorized, status)
	assert.Contains(t, body, "NotAuthenticated")
}

func TestUnknownRoute(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	status, body, hdr := rawDo(t, http.MethodGet, e.base+"/20190501/nothing", "")
	assert.Equal(t, http.StatusNotFound, status)
	assert.Contains(t, body, `"code":"NotFound"`)
	assert.NotEmpty(t, hdr.Get("Opc-Request-Id"))
}

func TestDeploymentValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "val")
	url := e.base + "/20190501/deployments"

	mk := func(prefix, spec string) string {
		return `{"gatewayId":"` + *gw.Id + `","compartmentId":"` + testCompartment + `","pathPrefix":"` + prefix +
			`","specification":` + spec + `}`
	}
	route := func(path, backend string) string {
		return `{"routes":[{"path":"` + path + `","methods":["GET"],"backend":` + backend + `}]}`
	}
	stock := `{"type":"STOCK_RESPONSE_BACKEND","status":200}`

	tests := []struct {
		name   string
		body   string
		status int
	}{
		{"prefix no slash", mk("v1", route("/a", stock)), 400},
		{"prefix trailing slash", mk("/v1/", route("/a", stock)), 400},
		{"prefix parameter", mk("/{x}", route("/a", stock)), 400},
		{"adjacent slashes", mk("/a//b", route("/a", stock)), 400},
		{"route no slash", mk("/v1", route("a", stock)), 400},
		{"wildcard not last", mk("/v1", route("/a/{x*}/b", stock)), 400},
		{"bad param name", mk("/v1", route("/a/{x-y}", stock)), 400},
		{"bad method", mk("/v1", `{"routes":[{"path":"/a","methods":["FETCH"],"backend":`+stock+`}]}`), 400},
		{"stock bad status", mk("/v1", route("/a", `{"type":"STOCK_RESPONSE_BACKEND","status":99}`)), 400},
		{"http bad url", mk("/v1", route("/a", `{"type":"HTTP_BACKEND","url":"ftp://x"}`)), 400},
		{"function bad id", mk("/v1", route("/a", `{"type":"ORACLE_FUNCTIONS_BACKEND","functionId":"x"}`)), 400},
		{"unknown backend", mk("/v1", route("/a", `{"type":"NOPE"}`)), 400},
		{"oauth2 backend unsupported", mk("/v1", route("/a", `{"type":"OAUTH2_LOGIN_BACKEND"}`)), 400},
		{"missing backend", mk("/v1", `{"routes":[{"path":"/a"}]}`), 400},
		{"duplicate routes", mk("/v1", `{"routes":[{"path":"/a/{x}","methods":["GET"],"backend":`+stock+
			`},{"path":"/a/{y}","methods":["GET"],"backend":`+stock+`}]}`), 400},
		{
			"usage plans unsupported",
			mk("/v1", `{"requestPolicies":{"usagePlans":{"tokenLocations":["request.headers[t]"]}},"routes":[]}`),
			400,
		},
		{
			"rate limit zero",
			mk(
				"/v1",
				`{"requestPolicies":{"rateLimiting":{"rateKey":"TOTAL","rateInRequestsPerSecond":0}},"routes":[]}`,
			),
			400,
		},
		{"cors no origins", mk("/v1", `{"requestPolicies":{"cors":{"allowedOrigins":[]}},"routes":[]}`), 400},
		{
			"jwt without keys",
			mk(
				"/v1",
				`{"requestPolicies":{"authentication":{"type":"JWT_AUTHENTICATION",`+
					`"issuers":["i"],"audiences":["a"]}},"routes":[]}`,
			),
			400,
		},
		{
			"remote discovery unsupported",
			mk(
				"/v1",
				`{"requestPolicies":{"authentication":{"type":"TOKEN_AUTHENTICATION","tokenHeader":"Authorization",`+
					`"tokenAuthScheme":"Bearer","validationPolicy":{"type":"REMOTE_DISCOVERY"}}},"routes":[]}`,
			),
			400,
		},
		{"anonymous route needs anonymous access", mk("/v1", `{"routes":[{"path":"/a","backend":`+stock+
			`,"requestPolicies":{"authorization":{"type":"ANONYMOUS"}}}]}`), 400},
		{
			"unknown gateway",
			strings.Replace(mk("/v1", route("/a", stock)), *gw.Id, "ocid1.apigateway.oc1..none", 1),
			404,
		},
		{
			"missing spec",
			`{"gatewayId":"` + *gw.Id + `","compartmentId":"` + testCompartment + `","pathPrefix":"/v1"}`,
			400,
		},
		{"ok", mk("/ok", route("/a/{x}", stock)), 202},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body, _ := rawDo(t, http.MethodPost, url, tt.body)
			assert.Equal(t, tt.status, status, body)
		})
	}
}

func TestDeploymentCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true, register: true})
	gw := e.mkGateway(t, "crud")

	resp, err := e.createDeployment(t, *gw.Id, "/v1", specFromJSON(t, stockSpec(200, "one")))
	require.NoError(t, err)
	assert.Equal(t, apigateway.DeploymentLifecycleStateCreating, resp.LifecycleState)
	assert.Equal(t, "https://"+*gw.Hostname+"/v1", *resp.Endpoint)

	_, err = e.createDeployment(t, *gw.Id, "/v1", specFromJSON(t, stockSpec(200, "dup")))
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.createDeployment(t, *gw.Id, "/", specFromJSON(t, stockSpec(200, "root")))
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "root prefix excludes other deployments")

	e.settle()

	got, err := e.dep.GetDeployment(t.Context(), apigateway.GetDeploymentRequest{DeploymentId: resp.Id})
	require.NoError(t, err)
	require.Len(t, got.Specification.Routes, 1)
	assert.Equal(t, "/hi", *got.Specification.Routes[0].Path)

	_, err = e.dep.UpdateDeployment(t.Context(), apigateway.UpdateDeploymentRequest{
		DeploymentId: resp.Id, IfMatch: got.Etag,
		DisplayName: new("renamed"), Specification: specFromJSON(t, stockSpec(201, "two")),
	})
	require.NoError(t, err)

	e.settle()

	assert.Equal(t, 201, e.call(t, *gw.Id, http.MethodGet, "/v1/hi", nil, "").status)

	list, err := e.dep.ListDeployments(t.Context(), apigateway.ListDeploymentsRequest{
		CompartmentId: new(testCompartment), GatewayId: gw.Id,
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "renamed", *list.Items[0].DisplayName)

	_, err = e.dep.ChangeDeploymentCompartment(t.Context(), apigateway.ChangeDeploymentCompartmentRequest{
		DeploymentId:  resp.Id,
		CompartmentId: new(otherCompart),
	})
	require.NoError(t, err)

	_, err = e.dep.DeleteDeployment(t.Context(), apigateway.DeleteDeploymentRequest{DeploymentId: resp.Id})
	require.NoError(t, err)

	e.settle()

	assert.Equal(t, http.StatusNotFound, e.call(t, *gw.Id, http.MethodGet, "/v1/hi", nil, "").status)
}

func TestCreateGatewayRawValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	url := e.base + "/20190501/gateways"

	tests := []struct {
		name, body string
	}{
		{
			"bad endpoint type",
			`{"compartmentId":"` + testCompartment + `","endpointType":"WEIRD","subnetId":"` + testSubnet + `"}`,
		},
		{
			"bad ip mode",
			`{"compartmentId":"` + testCompartment + `","endpointType":"PUBLIC","subnetId":"` + testSubnet + `","ipMode":"X"}`,
		},
		{"missing subnet", `{"compartmentId":"` + testCompartment + `","endpointType":"PUBLIC"}`},
		{
			"response cache unsupported",
			`{"compartmentId":"` + testCompartment + `","endpointType":"PUBLIC","subnetId":"` + testSubnet +
				`","responseCacheDetails":{"type":"EXTERNAL_RESP_CACHE"}}`,
		},
		{"not json", `{`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body, _ := rawDo(t, http.MethodPost, url, tt.body)
			assert.Equal(t, http.StatusBadRequest, status, body)
		})
	}
}
