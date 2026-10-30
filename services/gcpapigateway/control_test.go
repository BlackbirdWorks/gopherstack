package gcpapigateway_test

import (
	"testing"
	"time"

	"cloud.google.com/go/apigateway/apiv1/apigatewaypb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/cloud/location"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/services/gcpapigateway"
)

func helloSpec(backend string) string {
	return opSpec(backend, "", "  /hello:\n    get:\n"+okResponses)
}

func TestAPICRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	api := e.createAPI(t, apiID)

	assert.Equal(t, parent+"/apis/"+apiID, api.GetName())
	assert.Equal(t, apigatewaypb.Api_ACTIVE, api.GetState())
	assert.Regexp(t, `^my-api-[a-z2-7]{18}\.apigateway\.test-project\.cloud\.goog$`, api.GetManagedService())

	got, err := e.client.GetApi(t.Context(), &apigatewaypb.GetApiRequest{Name: api.GetName()})
	require.NoError(t, err)
	assert.Equal(t, api.GetManagedService(), got.GetManagedService())

	op, err := e.client.UpdateApi(t.Context(), &apigatewaypb.UpdateApiRequest{
		Api: &apigatewaypb.Api{
			Name:        api.GetName(),
			DisplayName: "renamed",
			Labels:      map[string]string{"env": "dev"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "labels"}},
	})
	require.NoError(t, err)

	updated, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "renamed", updated.GetDisplayName())
	assert.Equal(t, map[string]string{"env": "dev"}, updated.GetLabels())

	e.createAPI(t, "second")

	it := e.client.ListApis(t.Context(), &apigatewaypb.ListApisRequest{Parent: parent, PageSize: 1})

	var names []string

	for {
		a, nerr := it.Next()
		if nerr != nil {
			break
		}

		names = append(names, a.GetName())
	}

	assert.Equal(t, []string{parent + "/apis/" + apiID, parent + "/apis/second"}, names)

	del, err := e.client.DeleteApi(t.Context(), &apigatewaypb.DeleteApiRequest{Name: api.GetName()})
	require.NoError(t, err)
	require.NoError(t, del.Wait(t.Context()))

	_, err = e.client.GetApi(t.Context(), &apigatewaypb.GetApiRequest{Name: api.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestAPIErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req  *apigatewaypb.CreateApiRequest
		name string
		want codes.Code
	}{
		{&apigatewaypb.CreateApiRequest{Parent: parent, ApiId: "Bad_ID"}, "bad id", codes.InvalidArgument},
		{&apigatewaypb.CreateApiRequest{Parent: parent}, "missing id", codes.InvalidArgument},
		{
			&apigatewaypb.CreateApiRequest{Parent: "projects/p/locations/us-central1", ApiId: "x"},
			"not global",
			codes.InvalidArgument,
		},
		{&apigatewaypb.CreateApiRequest{Parent: "nope", ApiId: "x"}, "bad parent", codes.InvalidArgument},
		{&apigatewaypb.CreateApiRequest{Parent: parent, ApiId: "dup"}, "duplicate", codes.AlreadyExists},
		{&apigatewaypb.CreateApiRequest{
			Parent: parent, ApiId: "lab", Api: &apigatewaypb.Api{Labels: map[string]string{"Bad": "x"}},
		}, "bad label", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createAPI(t, "dup")

			_, err := e.client.CreateApi(t.Context(), tt.req)
			assert.Equal(t, tt.want, status.Code(err), "%v", err)
		})
	}
}

func TestConfigLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createAPI(t, apiID)

	cfg := e.createConfig(t, "cfg-1", helloSpec("https://backend.example"))
	assert.Equal(t, apigatewaypb.ApiConfig_ACTIVE, cfg.GetState())
	assert.Equal(t, parent+"/apis/"+apiID+"/configs/cfg-1", cfg.GetName())
	assert.NotEmpty(t, cfg.GetServiceConfigId())

	basic, err := e.client.GetApiConfig(t.Context(), &apigatewaypb.GetApiConfigRequest{Name: cfg.GetName()})
	require.NoError(t, err)
	assert.Empty(t, basic.GetOpenapiDocuments())

	full, err := e.client.GetApiConfig(t.Context(), &apigatewaypb.GetApiConfigRequest{
		Name: cfg.GetName(), View: apigatewaypb.GetApiConfigRequest_FULL,
	})
	require.NoError(t, err)
	require.Len(t, full.GetOpenapiDocuments(), 1)
	assert.Contains(t, string(full.GetOpenapiDocuments()[0].GetDocument().GetContents()), "swagger")

	op, err := e.client.UpdateApiConfig(t.Context(), &apigatewaypb.UpdateApiConfigRequest{
		ApiConfig:  &apigatewaypb.ApiConfig{Name: cfg.GetName(), DisplayName: "v1"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	})
	require.NoError(t, err)

	upd, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "v1", upd.GetDisplayName())

	_, err = e.client.UpdateApiConfig(t.Context(), &apigatewaypb.UpdateApiConfigRequest{
		ApiConfig:  &apigatewaypb.ApiConfig{Name: cfg.GetName()},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"openapi_documents"}},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	gw := e.createGateway(t, "gw", cfg.GetName())
	assert.Equal(t, apigatewaypb.Gateway_ACTIVE, gw.GetState())

	_, err = e.client.DeleteApiConfig(t.Context(), &apigatewaypb.DeleteApiConfigRequest{Name: cfg.GetName()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "config used by gateway")

	_, err = e.client.DeleteApi(t.Context(), &apigatewaypb.DeleteApiRequest{Name: parent + "/apis/" + apiID})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "api still has configs")

	dg, err := e.client.DeleteGateway(t.Context(), &apigatewaypb.DeleteGatewayRequest{Name: gw.GetName()})
	require.NoError(t, err)
	require.NoError(t, dg.Wait(t.Context()))

	dc, err := e.client.DeleteApiConfig(t.Context(), &apigatewaypb.DeleteApiConfigRequest{Name: cfg.GetName()})
	require.NoError(t, err)
	require.NoError(t, dc.Wait(t.Context()))

	da, err := e.client.DeleteApi(t.Context(), &apigatewaypb.DeleteApiRequest{Name: parent + "/apis/" + apiID})
	require.NoError(t, err)
	require.NoError(t, da.Wait(t.Context()))
}

func TestConfigInputErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut  func(*apigatewaypb.CreateApiConfigRequest)
		name string
		want codes.Code
	}{
		{
			func(r *apigatewaypb.CreateApiConfigRequest) { r.ApiConfig.OpenapiDocuments = nil },
			"no document",
			codes.InvalidArgument,
		},
		{func(r *apigatewaypb.CreateApiConfigRequest) {
			doc := r.GetApiConfig().GetOpenapiDocuments()[0]
			r.ApiConfig.OpenapiDocuments = []*apigatewaypb.ApiConfig_OpenApiDocument{doc, doc}
		}, "two documents", codes.InvalidArgument},
		{
			func(r *apigatewaypb.CreateApiConfigRequest) { r.ApiConfig.GatewayServiceAccount = "nope" },
			"bad service account",
			codes.InvalidArgument,
		},
		{func(r *apigatewaypb.CreateApiConfigRequest) {
			r.ApiConfig.GrpcServices = []*apigatewaypb.ApiConfig_GrpcServiceDefinition{{}}
		}, "grpc", codes.InvalidArgument},
		{
			func(r *apigatewaypb.CreateApiConfigRequest) { r.Parent = parent + "/apis/ghost" },
			"missing api",
			codes.NotFound,
		},
		{func(r *apigatewaypb.CreateApiConfigRequest) { r.ApiConfigId = "" }, "no id", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createAPI(t, apiID)

			req := configRequest("cfg", helloSpec("https://backend.example"))
			tt.mut(req)

			_, err := e.client.CreateApiConfig(t.Context(), req)
			assert.Equal(t, tt.want, status.Code(err), "%v", err)
		})
	}

	t.Run("service account email accepted", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		e.createAPI(t, apiID)

		req := configRequest("cfg", helloSpec("https://backend.example"))
		req.ApiConfig.GatewayServiceAccount = "gw@" + project + ".iam.gserviceaccount.com"

		op, err := e.client.CreateApiConfig(t.Context(), req)
		require.NoError(t, err)

		cfg, err := op.Wait(t.Context())
		require.NoError(t, err)
		assert.Equal(t, req.GetApiConfig().GetGatewayServiceAccount(), cfg.GetGatewayServiceAccount())
	})
}

const (
	backendAddr = "https://backend.example"
	getA        = "  /a:\n    get:\n" + okResponses
	metricM     = "x-google-management:\n  metrics:\n    - name: m\n      valueType: INT64\n"
)

func jwtDef(name, issuerURL, jwks string) string {
	d := "  " + name + ":\n    type: oauth2\n    x-google-issuer: " + issuerURL + "\n"
	if jwks != "" {
		d += "    x-google-jwks_uri: " + jwks + "\n"
	}

	return d
}

func quotaLimit(unit string) string {
	return "  quota:\n    limits:\n      - name: l\n        metric: m\n        unit: \"" + unit +
		"\"\n        values:\n          STANDARD: 1\n"
}

func TestOpenAPIValidation(t *testing.T) {
	t.Parallel()

	const (
		secHdr = "securityDefinitions:\n"
		keyDef = "  api_key:\n    type: apiKey\n    in: query\n"
	)

	tests := []struct {
		name string
		doc  string
		want string
	}{
		{"not yaml", "{{{", "not valid YAML"},
		{"wrong version", "swagger: \"1.2\"\npaths: {}\n", "Unsupported Swagger version"},
		{"no version", "paths: {}\n", "Unsupported document"},
		{"no paths", opSpec(backendAddr, "", "  {}\n"), "no paths"},
		{"no backend", opSpec("", "", getA), "no x-google-backend"},
		{"backend no scheme", opSpec("backend.example", "", getA), "http or https"},
		{"bad translation", opSpec(backendAddr+"\n  path_translation: SIDEWAYS", "", getA), "path_translation"},
		{"deadline too big", opSpec(backendAddr+"\n  deadline: 9999", "", getA), "deadline"},
		{
			"audience and disable auth",
			opSpec(backendAddr+"\n  jwt_audience: x\n  disable_auth: true", "", getA),
			"Only one of",
		},
		{"missing responses", opSpec(backendAddr, "", "  /a:\n    get: {}\n"), "responses"},
		{
			"undefined security",
			opSpec(backendAddr, "", "  /a:\n    get:\n      security:\n        - nope: []\n"+okResponses),
			"not defined",
		},
		{"jwt without jwks", opSpec(backendAddr, secHdr+jwtDef("j", "i", ""), getA), "x-google-jwks_uri"},
		{
			"duplicate issuer",
			opSpec(backendAddr, secHdr+jwtDef("a", "i", "https://x/k")+jwtDef("b", "i", "https://x/k"), getA),
			"already used",
		},
		{"api key without name", opSpec(backendAddr, secHdr+keyDef, getA), "needs a name"},
		{
			"ambiguous templates",
			opSpec(backendAddr, "", "  /a/{x}:\n    get:\n"+okResponses+"  /a/{y}:\n    get:\n"+okResponses),
			"ambiguous",
		},
		{"partial template", opSpec(backendAddr, "", "  /a/pre{x}:\n    get:\n"+okResponses), "whole segment"},
		{
			"quota undefined metric",
			opSpec(
				backendAddr,
				"",
				"  /a:\n    get:\n      x-google-quota:\n        metricCosts:\n          m: 1\n"+okResponses,
			),
			"undefined metric",
		},
		{
			"quota bad unit",
			opSpec(backendAddr, metricM+"      metricKind: DELTA\n"+quotaLimit("1/s/{project}"), getA),
			"unit",
		},
		{"metric kind", opSpec(backendAddr, metricM+"      metricKind: GAUGE\n", getA), "DELTA"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createAPI(t, apiID)

			op, err := e.client.CreateApiConfig(t.Context(), configRequest("bad", tt.doc))
			require.NoError(t, err)

			_, err = op.Wait(t.Context())
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, err.Error(), tt.want)

			meta, merr := op.Metadata()
			require.NoError(t, merr)
			assert.NotEmpty(t, meta.GetDiagnostics())

			cfg, gerr := e.client.GetApiConfig(t.Context(), &apigatewaypb.GetApiConfigRequest{
				Name: parent + "/apis/" + apiID + "/configs/bad",
			})
			require.NoError(t, gerr)
			assert.Equal(t, apigatewaypb.ApiConfig_FAILED, cfg.GetState())

			_, gerr = e.client.CreateGateway(t.Context(), gatewayRequest("gw", gwParen, cfg.GetName()))
			assert.Equal(t, codes.FailedPrecondition, status.Code(gerr), "a failed config cannot be served")
		})
	}
}

func TestGatewayLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createAPI(t, apiID)

	cfg := e.createConfig(t, "cfg-1", helloSpec("https://b.example"))
	cfg2 := e.createConfig(t, "cfg-2", helloSpec("https://b.example"))
	gw := e.createGateway(t, "my-gw", cfg.GetName())

	assert.Equal(t, gwParen+"/gateways/my-gw", gw.GetName())
	assert.Regexp(t, `^my-gw-[a-z2-7]{18}\.uc\.gateway\.dev$`, gw.GetDefaultHostname())
	assert.Equal(t, apigatewaypb.Gateway_EFFECTIVE_STREAMING_MODE_DISABLED, gw.GetEffectiveStreamingMode())

	op, err := e.client.UpdateGateway(t.Context(), &apigatewaypb.UpdateGatewayRequest{
		Gateway:    &apigatewaypb.Gateway{Name: gw.GetName(), ApiConfig: cfg2.GetName()},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"api_config"}},
	})
	require.NoError(t, err)

	upd, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, cfg2.GetName(), upd.GetApiConfig())
	assert.Equal(t, gw.GetDefaultHostname(), upd.GetDefaultHostname())

	it := e.client.ListGateways(
		t.Context(),
		&apigatewaypb.ListGatewaysRequest{Parent: "projects/" + project + "/locations/-"},
	)
	g, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, gw.GetName(), g.GetName())

	tests := []struct {
		req  *apigatewaypb.CreateGatewayRequest
		name string
		want codes.Code
	}{
		{gatewayRequest("x", parent, cfg.GetName()), "global location", codes.InvalidArgument},
		{gatewayRequest("x", gwParen, ""), "no config", codes.InvalidArgument},
		{gatewayRequest("x", gwParen, parent+"/apis/a/configs/ghost"), "missing config", codes.NotFound},
		{gatewayRequest("my-gw", gwParen, cfg.GetName()), "duplicate", codes.AlreadyExists},
		{gatewayRequest("X", gwParen, cfg.GetName()), "bad id", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, cerr := e.client.CreateGateway(t.Context(), tt.req)
			assert.Equal(t, tt.want, status.Code(cerr), "%v", cerr)
		})
	}
}

func TestOperationLifecycleOnClock(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.handler.Backend.SetTiming(gcpapigateway.Timing{
		Create: 10 * time.Second, Update: 5 * time.Second, Delete: 3 * time.Second,
	})

	op, err := e.client.CreateApi(t.Context(), &apigatewaypb.CreateApiRequest{Parent: parent, ApiId: apiID})
	require.NoError(t, err)
	assert.False(t, op.Done())

	name := parent + "/apis/" + apiID
	api, err := e.client.GetApi(t.Context(), &apigatewaypb.GetApiRequest{Name: name})
	require.NoError(t, err)
	assert.Equal(t, apigatewaypb.Api_CREATING, api.GetState())

	e.clock.Advance(10 * time.Second)

	api, err = e.client.GetApi(t.Context(), &apigatewaypb.GetApiRequest{Name: name})
	require.NoError(t, err)
	assert.Equal(t, apigatewaypb.Api_ACTIVE, api.GetState())

	done, err := op.Poll(t.Context())
	require.NoError(t, err)
	assert.Equal(t, name, done.GetName())

	del, err := e.client.DeleteApi(t.Context(), &apigatewaypb.DeleteApiRequest{Name: name})
	require.NoError(t, err)

	api, err = e.client.GetApi(t.Context(), &apigatewaypb.GetApiRequest{Name: name})
	require.NoError(t, err)
	assert.Equal(t, apigatewaypb.Api_DELETING, api.GetState())

	e.clock.Advance(3 * time.Second)

	_, err = e.client.GetApi(t.Context(), &apigatewaypb.GetApiRequest{Name: name})
	assert.Equal(t, codes.NotFound, status.Code(err))
	require.NoError(t, del.Wait(t.Context()))
}

func TestOperationsAPI(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.handler.Backend.SetTiming(gcpapigateway.Timing{Create: time.Minute})

	op, err := e.client.CreateApi(t.Context(), &apigatewaypb.CreateApiRequest{Parent: parent, ApiId: apiID})
	require.NoError(t, err)

	lro := e.client.LROClient
	opName := op.Name()
	assert.Contains(t, opName, "projects/"+project+"/locations/global/operations/operation-")

	got, err := lro.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: opName})
	require.NoError(t, err)
	assert.False(t, got.GetDone())

	list, err := lro.ListOperations(t.Context(), &longrunningpb.ListOperationsRequest{Name: parent}).Next()
	require.NoError(t, err)
	assert.Equal(t, opName, list.GetName())

	_, err = lro.WaitOperation(
		t.Context(),
		&longrunningpb.WaitOperationRequest{Name: "projects/p/locations/global/operations/ghost"},
	)
	assert.Equal(t, codes.NotFound, status.Code(err))

	running := lro.DeleteOperation(t.Context(), &longrunningpb.DeleteOperationRequest{Name: opName})
	assert.Equal(t, codes.FailedPrecondition, status.Code(running), "running operation")

	require.NoError(t, lro.CancelOperation(t.Context(), &longrunningpb.CancelOperationRequest{Name: opName}))

	cancelled, err := lro.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: opName})
	require.NoError(t, err)
	assert.True(t, cancelled.GetDone())
	assert.Equal(t, int32(codes.Canceled), cancelled.GetError().GetCode())

	_, err = e.client.GetApi(t.Context(), &apigatewaypb.GetApiRequest{Name: parent + "/apis/" + apiID})
	assert.Equal(t, codes.NotFound, status.Code(err), "cancelled create is rolled back")

	require.NoError(t, lro.DeleteOperation(t.Context(), &longrunningpb.DeleteOperationRequest{Name: opName}))
	assert.Equal(
		t,
		codes.NotFound,
		status.Code(lro.DeleteOperation(t.Context(), &longrunningpb.DeleteOperationRequest{Name: opName})),
	)
}

func TestIAMAndLocations(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	gw := e.deploy(t, helloSpec("https://b.example"))
	apiName := parent + "/apis/" + apiID
	iam := iampb.NewIAMPolicyClient(e.conn)
	loc := location.NewLocationsClient(e.conn)

	for _, res := range []string{apiName, apiName + "/configs/cfg-1", gw.GetName()} {
		t.Run(res, func(t *testing.T) {
			t.Parallel()

			pol, err := iam.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: res})
			require.NoError(t, err)
			assert.Empty(t, pol.GetBindings())

			set, err := iam.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
				Resource: res, Policy: &iampb.Policy{Bindings: []*iampb.Binding{
					{Role: "roles/apigateway.viewer", Members: []string{"user:a@example.com"}},
				}},
			})
			require.NoError(t, err)
			require.Len(t, set.GetBindings(), 1)

			again, err := iam.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: res})
			require.NoError(t, err)
			assert.Equal(t, "roles/apigateway.viewer", again.GetBindings()[0].GetRole())

			perms, err := iam.TestIamPermissions(t.Context(), &iampb.TestIamPermissionsRequest{
				Resource: res, Permissions: []string{"apigateway.gateways.get", "storage.objects.get"},
			})
			require.NoError(t, err)
			assert.Equal(t, []string{"apigateway.gateways.get"}, perms.GetPermissions())
		})
	}

	_, err := iam.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: apiName + "x"})
	require.Error(t, err)

	locs, err := loc.ListLocations(t.Context(), &location.ListLocationsRequest{Name: "projects/" + project})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(locs.GetLocations()), 2)
}
