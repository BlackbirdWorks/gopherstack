package azurearm_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/apimanagement/armapimanagement/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *apimStack) putAPI(t *testing.T, id, path, serviceURL string) {
	t.Helper()

	props := &armapimanagement.APICreateOrUpdateProperties{
		DisplayName: new(
			id,
		),
		Path:      new(path),
		Protocols: []*armapimanagement.Protocol{new(armapimanagement.ProtocolHTTPS)},
	}
	if serviceURL != "" {
		props.ServiceURL = &serviceURL
	}

	p, err := s.f.NewAPIClient().BeginCreateOrUpdate(t.Context(), apimRG, apimSvc, id,
		armapimanagement.APICreateOrUpdateParameter{Properties: props}, nil)
	require.NoError(t, err)

	_, err = p.PollUntilDone(t.Context(), &runtime.PollUntilDoneOptions{Frequency: 5})
	require.NoError(t, err)
}

func (s *apimStack) putOp(t *testing.T, api, id, method, tpl string) {
	t.Helper()

	_, err := s.f.NewAPIOperationClient().CreateOrUpdate(t.Context(), apimRG, apimSvc, api, id,
		armapimanagement.OperationContract{Properties: &armapimanagement.OperationContractProperties{
			DisplayName: new(id), Method: new(method), URLTemplate: new(tpl),
		}}, nil)
	require.NoError(t, err)
}

func TestAPIMAPI_CRUDAndValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		id       string
		props    armapimanagement.APICreateOrUpdateProperties
		wantCode string
	}{
		{
			name: "ok", id: "a1",
			props: armapimanagement.APICreateOrUpdateProperties{DisplayName: new("A1"), Path: new("a1")},
		},
		{
			name: "missing display name", id: "a2", wantCode: "ValidationError",
			props: armapimanagement.APICreateOrUpdateProperties{Path: new("a2")},
		},
		{
			name: "missing path", id: "a3", wantCode: "ValidationError",
			props: armapimanagement.APICreateOrUpdateProperties{DisplayName: new("A3")},
		},
		{
			name: "bad service url", id: "a4", wantCode: "ValidationError",
			props: armapimanagement.APICreateOrUpdateProperties{
				DisplayName: new("A4"), Path: new("a4"), ServiceURL: new("not a url"),
			},
		},
		{
			name: "bad id", id: "bad*id", wantCode: "ValidationError",
			props: armapimanagement.APICreateOrUpdateProperties{DisplayName: new("A5"), Path: new("a5")},
		},
		{
			name: "path in use", id: "dup", wantCode: "ValidationError",
			props: armapimanagement.APICreateOrUpdateProperties{DisplayName: new("Dup"), Path: new("shared")},
		},
		{
			name: "bad protocol", id: "a6", wantCode: "ValidationError",
			props: armapimanagement.APICreateOrUpdateProperties{
				DisplayName: new(
					"A6",
				),
				Path:      new("a6"),
				Protocols: []*armapimanagement.Protocol{new(armapimanagement.Protocol("ftp"))},
			},
		},
	}

	s := newAPIMStack(t)
	s.devService(t)
	s.putAPI(t, "base", "shared", "")

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p, err := s.f.NewAPIClient().BeginCreateOrUpdate(t.Context(), apimRG, apimSvc, tc.id,
				armapimanagement.APICreateOrUpdateParameter{Properties: &tc.props}, nil)
			if tc.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tc.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			_, err = p.PollUntilDone(t.Context(), &runtime.PollUntilDoneOptions{Frequency: 5})
			require.NoError(t, err)

			got, err := s.f.NewAPIClient().Get(t.Context(), apimRG, apimSvc, tc.id, nil)
			require.NoError(t, err)
			assert.Equal(t, "1", *got.Properties.APIRevision)
			assert.True(t, *got.Properties.SubscriptionRequired)
			assert.Equal(t, "Ocp-Apim-Subscription-Key", *got.Properties.SubscriptionKeyParameterNames.Header)
			assert.Equal(t, "subscription-key", *got.Properties.SubscriptionKeyParameterNames.Query)
			assert.Equal(t, []*armapimanagement.Protocol{new(armapimanagement.ProtocolHTTPS)}, got.Properties.Protocols)
		})
	}
}

func TestAPIMAPI_UpdateDeleteCascade(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)
	s.devService(t)
	s.putAPI(t, "api1", "api1", "http://127.0.0.1:1")
	s.putOp(t, "api1", "op1", "get", "/items/{id}")

	ctx := t.Context()

	upd, err := s.f.NewAPIClient().Update(ctx, apimRG, apimSvc, "api1", "*", armapimanagement.APIUpdateContract{
		Properties: &armapimanagement.APIContractUpdateProperties{Description: new("changed")},
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, "changed", *upd.Properties.Description)
	assert.Equal(t, "api1", *upd.Properties.Path)

	_, err = s.f.NewAPIClient().Update(ctx, apimRG, apimSvc, "api1", `"stale"`, armapimanagement.APIUpdateContract{
		Properties: &armapimanagement.APIContractUpdateProperties{Description: new("x")},
	}, nil)
	require.Error(t, err)
	assert.Equal(t, "PreconditionFailed", sdkErrCode(err))

	_, err = s.f.NewAPIClient().Delete(ctx, apimRG, apimSvc, "api1", "*", nil)
	require.NoError(t, err)

	_, err = s.f.NewAPIOperationClient().Get(ctx, apimRG, apimSvc, "api1", "op1", nil)
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	status, _, _ := s.raw(t, http.MethodGet, s.armPath("/apis/api1"), "", nil)
	assert.Equal(t, http.StatusNotFound, status)
}

func TestAPIMOperation_CRUDAndValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		id       string
		method   string
		tpl      string
		display  string
		wantCode string
		params   []*armapimanagement.ParameterContract
		wantN    int
	}{
		{name: "ok derives params", id: "o1", method: "get", tpl: "/c/{cid}/o/{oid}", display: "o1", wantN: 2},
		{name: "no display", id: "o2", method: "get", tpl: "/x", wantCode: "ValidationError"},
		{name: "no template", id: "o3", method: "get", display: "o3", wantCode: "ValidationError"},
		{
			name:     "duplicate",
			id:       "o4",
			method:   "GET",
			tpl:      "/seed/{p}/x/{q}",
			display:  "o4",
			wantCode: "ValidationError",
		},
		{
			name: "declared not in template", id: "o5", method: "get", tpl: "/y", display: "o5",
			params:   []*armapimanagement.ParameterContract{{Name: new("zz"), Type: new("string")}},
			wantCode: "ValidationError",
		},
		{
			name: "declared kept", id: "o6", method: "post", tpl: "/z/{k}", display: "o6", wantN: 1,
			params: []*armapimanagement.ParameterContract{{Name: new("k"), Type: new("int"), Required: new(true)}},
		},
	}

	s := newAPIMStack(t)
	s.devService(t)
	s.putAPI(t, "api1", "api1", "")
	s.putOp(t, "api1", "seed", "get", "/seed/{a}/x/{b}")

	t.Cleanup(func() {
		n := 0

		pager := s.f.NewAPIOperationClient().NewListByAPIPager(apimRG, apimSvc, "api1", nil)
		for pager.More() {
			page, err := pager.NextPage(context.Background())
			require.NoError(t, err)

			n += len(page.Value)
		}

		assert.Equal(t, 3, n)
	})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s.opCase(t, tc.id, tc.method, tc.tpl, tc.display, tc.params, tc.wantCode, tc.wantN)
		})
	}
}

func (s *apimStack) opCase(
	t *testing.T,
	id, method, tpl, display string,
	params []*armapimanagement.ParameterContract,
	wantCode string,
	wantN int,
) {
	t.Helper()

	props := &armapimanagement.OperationContractProperties{Method: &method, TemplateParameters: params}
	if tpl != "" {
		props.URLTemplate = &tpl
	}

	if display != "" {
		props.DisplayName = &display
	}

	_, err := s.f.NewAPIOperationClient().CreateOrUpdate(t.Context(), apimRG, apimSvc, "api1", id,
		armapimanagement.OperationContract{Properties: props}, nil)
	if wantCode != "" {
		require.Error(t, err)
		assert.Equal(t, wantCode, sdkErrCode(err))

		return
	}

	require.NoError(t, err)

	got, err := s.f.NewAPIOperationClient().Get(t.Context(), apimRG, apimSvc, "api1", id, nil)
	require.NoError(t, err)
	assert.Len(t, got.Properties.TemplateParameters, wantN)
	assert.Equal(t, strings.ToUpper(method), *got.Properties.Method)
}

const apimOpenAPI = `{
  "openapi": "3.0.1",
  "info": {"title": "Petstore", "description": "pets", "version": "1"},
  "servers": [{"url": "http://127.0.0.1:9/v1"}],
  "paths": {
    "/pets": {
      "get": {"operationId": "listPets", "summary": "List pets", "responses": {
        "200": {"description": "ok", "content": {"application/json": {"example": [{"id": 1}]}}}}},
      "post": {"summary": "Create pet"}
    },
    "/pets/{petId}": {
      "get": {"operationId": "getPet", "responses": {"404": {"description": "missing"}}},
      "delete": {"operationId": "list Pets"}
    }
  }
}`

func TestAPIMAPI_ImportOpenAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		format   armapimanagement.ContentFormat
		value    string
		wantCode string
		wantOps  int
	}{
		{name: "openapi json", format: armapimanagement.ContentFormatOpenapiJSON, value: apimOpenAPI, wantOps: 4},
		{
			name: "yaml refused", format: armapimanagement.ContentFormatOpenapi, value: "openapi: 3.0.0\n",
			wantCode: "ValidationError",
		},
		{
			name: "link refused", format: armapimanagement.ContentFormatOpenapiJSONLink, value: "http://127.0.0.1/x",
			wantCode: "ValidationError",
		},
		{
			name: "not a spec", format: armapimanagement.ContentFormatOpenapiJSON, value: `{"foo":1}`,
			wantCode: "ValidationError",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newAPIMStack(t)
			s.devService(t)

			p, err := s.f.NewAPIClient().BeginCreateOrUpdate(t.Context(), apimRG, apimSvc, "pets",
				armapimanagement.APICreateOrUpdateParameter{Properties: &armapimanagement.APICreateOrUpdateProperties{
					Path: new("pets"), Format: &tc.format, Value: &tc.value,
				}}, nil)
			if tc.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tc.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			_, err = p.PollUntilDone(t.Context(), &runtime.PollUntilDoneOptions{Frequency: 5})
			require.NoError(t, err)

			api, err := s.f.NewAPIClient().Get(t.Context(), apimRG, apimSvc, "pets", nil)
			require.NoError(t, err)
			assert.Equal(t, "Petstore", *api.Properties.DisplayName)
			assert.Equal(t, "http://127.0.0.1:9/v1", *api.Properties.ServiceURL)

			got, err := s.f.NewAPIOperationClient().Get(t.Context(), apimRG, apimSvc, "pets", "getPet", nil)
			require.NoError(t, err)
			assert.Equal(t, "/pets/{petId}", *got.Properties.URLTemplate)
			assert.Len(t, got.Properties.TemplateParameters, 1)

			ops := 0

			pager := s.f.NewAPIOperationClient().NewListByAPIPager(apimRG, apimSvc, "pets", nil)
			for pager.More() {
				page, perr := pager.NextPage(t.Context())
				require.NoError(t, perr)

				ops += len(page.Value)
			}

			assert.Equal(t, tc.wantOps, ops)
		})
	}
}

func TestAPIMProduct_LinkAndDelete(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)
	s.devService(t)
	s.putAPI(t, "api1", "api1", "")

	ctx := t.Context()
	prods := s.f.NewProductClient()

	tests := []struct {
		name     string
		id       string
		display  string
		state    armapimanagement.ProductState
		wantCode string
	}{
		{name: "ok", id: "p1", display: "P1", state: armapimanagement.ProductStatePublished},
		{name: "default state", id: "p2", display: "P2"},
		{name: "no display", id: "p3", wantCode: "ValidationError"},
		{name: "bad state", id: "p4", display: "P4", state: "weird", wantCode: "ValidationError"},
	}

	for _, tc := range tests {
		props := &armapimanagement.ProductContractProperties{}
		if tc.display != "" {
			props.DisplayName = &tc.display
		}

		if tc.state != "" {
			props.State = &tc.state
		}

		got, err := prods.CreateOrUpdate(
			ctx,
			apimRG,
			apimSvc,
			tc.id,
			armapimanagement.ProductContract{Properties: props},
			nil,
		)
		if tc.wantCode != "" {
			require.Error(t, err, tc.name)
			assert.Equal(t, tc.wantCode, sdkErrCode(err), tc.name)

			continue
		}

		require.NoError(t, err, tc.name)
		assert.True(t, *got.Properties.SubscriptionRequired, tc.name)

		wantState := tc.state
		if wantState == "" {
			wantState = armapimanagement.ProductStateNotPublished
		}

		assert.Equal(t, wantState, *got.Properties.State, tc.name)
	}

	_, err := s.f.NewProductAPIClient().CreateOrUpdate(ctx, apimRG, apimSvc, "p1", "api1", nil)
	require.NoError(t, err)

	_, err = s.f.NewProductAPIClient().CheckEntityExists(ctx, apimRG, apimSvc, "p1", "api1", nil)
	require.NoError(t, err)

	_, err = s.f.NewProductAPIClient().CreateOrUpdate(ctx, apimRG, apimSvc, "p1", "ghost", nil)
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	linked := 0

	pager := s.f.NewProductAPIClient().NewListByProductPager(apimRG, apimSvc, "p1", nil)
	for pager.More() {
		page, perr := pager.NextPage(ctx)
		require.NoError(t, perr)

		linked += len(page.Value)
	}

	assert.Equal(t, 1, linked)

	_, err = s.f.NewSubscriptionClient().CreateOrUpdate(ctx, apimRG, apimSvc, "sub-p1",
		armapimanagement.SubscriptionCreateParameters{
			Properties: &armapimanagement.SubscriptionCreateParameterProperties{
				DisplayName: new(
					"sp1",
				),
				Scope: new("/products/p1"),
				State: new(armapimanagement.SubscriptionStateActive),
			},
		}, nil)
	require.NoError(t, err)

	_, err = prods.Delete(ctx, apimRG, apimSvc, "p1", "*", nil)
	require.Error(t, err)
	assert.Equal(t, "Conflict", sdkErrCode(err))

	_, err = prods.Delete(ctx, apimRG, apimSvc, "p1", "*", &armapimanagement.ProductClientDeleteOptions{
		DeleteSubscriptions: new(true),
	})
	require.NoError(t, err)

	_, err = s.f.NewSubscriptionClient().Get(ctx, apimRG, apimSvc, "sub-p1", nil)
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	_, err = s.f.NewProductAPIClient().CheckEntityExists(ctx, apimRG, apimSvc, "p1", "api1", nil)
	require.Error(t, err)
}

func TestAPIMSubscription_SecretsAndKeys(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)
	s.devService(t)
	s.putAPI(t, "api1", "api1", "")

	ctx := t.Context()
	subs := s.f.NewSubscriptionClient()

	tests := []struct {
		name     string
		id       string
		scope    string
		display  string
		wantCode string
		wantScp  string
	}{
		{name: "api scope", id: "s1", scope: "/apis/api1", display: "S1", wantScp: "/apis/api1"},
		{name: "all apis", id: "s2", scope: "/apis", display: "S2", wantScp: "/apis"},
		{name: "full scope", id: "s3", display: "S3", wantScp: "/apis/api1"},
		{name: "missing api", id: "s4", scope: "/apis/ghost", display: "S4", wantCode: "ValidationError"},
		{name: "bad scope", id: "s5", scope: "/users/x", display: "S5", wantCode: "ValidationError"},
		{name: "no scope", id: "s6", display: "S6", wantCode: "ValidationError"},
		{name: "no display", id: "s7", scope: "/apis", wantCode: "ValidationError"},
	}

	for _, tc := range tests {
		scope := tc.scope
		if tc.name == "full scope" {
			scope = "/subscriptions/" + aciSub + "/resourceGroups/" + apimRG +
				"/providers/Microsoft.ApiManagement/service/" + apimSvc + "/apis/API1"
		}

		props := &armapimanagement.SubscriptionCreateParameterProperties{
			State: new(armapimanagement.SubscriptionStateActive),
		}
		if scope != "" {
			props.Scope = &scope
		}

		if tc.display != "" {
			props.DisplayName = &tc.display
		}

		got, err := subs.CreateOrUpdate(
			ctx,
			apimRG,
			apimSvc,
			tc.id,
			armapimanagement.SubscriptionCreateParameters{Properties: props},
			nil,
		)
		if tc.wantCode != "" {
			require.Error(t, err, tc.name)
			assert.Equal(t, tc.wantCode, sdkErrCode(err), tc.name)

			continue
		}

		require.NoError(t, err, tc.name)
		assert.Nil(t, got.Properties.PrimaryKey, tc.name)
		assert.Nil(t, got.Properties.SecondaryKey, tc.name)
		assert.Equal(t, "active", string(*got.Properties.State), tc.name)
		assert.Contains(t, *got.Properties.Scope, "/service/"+apimSvc+tc.wantScp, tc.name)
	}

	g, err := subs.Get(ctx, apimRG, apimSvc, "s1", nil)
	require.NoError(t, err)
	assert.Nil(t, g.Properties.PrimaryKey)

	sec, err := subs.ListSecrets(ctx, apimRG, apimSvc, "s1", nil)
	require.NoError(t, err)
	require.NotEmpty(t, *sec.PrimaryKey)
	require.NotEmpty(t, *sec.SecondaryKey)
	assert.NotEqual(t, *sec.PrimaryKey, *sec.SecondaryKey)

	_, err = subs.RegeneratePrimaryKey(ctx, apimRG, apimSvc, "s1", nil)
	require.NoError(t, err)

	after, err := subs.ListSecrets(ctx, apimRG, apimSvc, "s1", nil)
	require.NoError(t, err)
	assert.NotEqual(t, *sec.PrimaryKey, *after.PrimaryKey)
	assert.Equal(t, *sec.SecondaryKey, *after.SecondaryKey)

	_, err = subs.RegenerateSecondaryKey(ctx, apimRG, apimSvc, "s1", nil)
	require.NoError(t, err)

	again, err := subs.ListSecrets(ctx, apimRG, apimSvc, "s1", nil)
	require.NoError(t, err)
	assert.Equal(t, *after.PrimaryKey, *again.PrimaryKey)
	assert.NotEqual(t, *after.SecondaryKey, *again.SecondaryKey)

	upd, err := subs.Update(ctx, apimRG, apimSvc, "s1", "*", armapimanagement.SubscriptionUpdateParameters{
		Properties: &armapimanagement.SubscriptionUpdateParameterProperties{
			State: new(armapimanagement.SubscriptionStateSuspended),
		},
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, armapimanagement.SubscriptionStateSuspended, *upd.Properties.State)

	keep, err := subs.ListSecrets(ctx, apimRG, apimSvc, "s1", nil)
	require.NoError(t, err)
	assert.Equal(t, *again.PrimaryKey, *keep.PrimaryKey)

	pager := subs.NewListPager(apimRG, apimSvc, nil)
	n := 0

	for pager.More() {
		page, perr := pager.NextPage(ctx)
		require.NoError(t, perr)

		for _, v := range page.Value {
			assert.Nil(t, v.Properties.PrimaryKey)

			n++
		}
	}

	assert.Equal(t, 3, n)
}

func TestAPIMNamedValue_SecretWriteOnly(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)
	s.devService(t)

	ctx := t.Context()
	nv := s.f.NewNamedValueClient()

	tests := []struct {
		name     string
		id       string
		display  string
		value    string
		wantCode string
		secret   bool
	}{
		{name: "plain", id: "n1", display: "plain-v", value: "hello"},
		{name: "secret", id: "n2", display: "secret.v", value: "s3cr3t", secret: true},
		{name: "bad display", id: "n3", display: "bad name!", value: "x", wantCode: "ValidationError"},
		{name: "duplicate display", id: "n4", display: "plain-v", value: "x", wantCode: "ValidationError"},
		{name: "empty value", id: "n5", display: "e", value: "  ", wantCode: "ValidationError"},
		{name: "expression", id: "n6", display: "ex", value: "@(1+1)", wantCode: "ValidationError"},
	}

	for _, tc := range tests {
		p, err := nv.BeginCreateOrUpdate(ctx, apimRG, apimSvc, tc.id, armapimanagement.NamedValueCreateContract{
			Properties: &armapimanagement.NamedValueCreateContractProperties{
				DisplayName: &tc.display, Value: &tc.value, Secret: &tc.secret,
			},
		}, nil)
		if tc.wantCode != "" {
			require.Error(t, err, tc.name)
			assert.Equal(t, tc.wantCode, sdkErrCode(err), tc.name)

			continue
		}

		require.NoError(t, err, tc.name)

		_, err = p.PollUntilDone(ctx, &runtime.PollUntilDoneOptions{Frequency: 5})
		require.NoError(t, err, tc.name)

		got, err := nv.Get(ctx, apimRG, apimSvc, tc.id, nil)
		require.NoError(t, err, tc.name)

		if tc.secret {
			assert.Nil(t, got.Properties.Value, tc.name)
		} else {
			assert.Equal(t, tc.value, *got.Properties.Value, tc.name)
		}

		lv, err := nv.ListValue(ctx, apimRG, apimSvc, tc.id, nil)
		require.NoError(t, err, tc.name)
		assert.Equal(t, tc.value, *lv.Value, tc.name)
	}

	status, body, _ := s.raw(t, http.MethodGet, s.armPath("/namedValues"), "", nil)
	assert.Equal(t, http.StatusOK, status)
	assert.NotContains(t, body, "s3cr3t")

	_, err := nv.Get(ctx, apimRG, apimSvc, "ghost", nil)
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
}

func TestAPIMBackend_CredentialsRedacted(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)
	s.devService(t)

	ctx := t.Context()
	be := s.f.NewBackendClient()

	tests := []struct {
		name     string
		url      string
		protocol armapimanagement.BackendProtocol
		wantCode string
	}{
		{name: "ok", url: "http://127.0.0.1:8080", protocol: armapimanagement.BackendProtocolHTTP},
		{name: "relative url", url: "/x", protocol: armapimanagement.BackendProtocolHTTP, wantCode: "ValidationError"},
		{name: "bad protocol", url: "http://127.0.0.1", protocol: "ftp", wantCode: "ValidationError"},
	}

	for _, tc := range tests {
		_, err := be.CreateOrUpdate(ctx, apimRG, apimSvc, "b1", armapimanagement.BackendContract{
			Properties: &armapimanagement.BackendContractProperties{
				URL: &tc.url, Protocol: &tc.protocol,
				Credentials: &armapimanagement.BackendCredentialsContract{
					Authorization: &armapimanagement.BackendAuthorizationHeaderCredentials{
						Scheme: new("Bearer"), Parameter: new("tok-secret"),
					},
					Header: map[string][]*string{"X-Key": {new("hdr-secret")}},
				},
			},
		}, nil)
		if tc.wantCode != "" {
			require.Error(t, err, tc.name)
			assert.Equal(t, tc.wantCode, sdkErrCode(err), tc.name)

			continue
		}

		require.NoError(t, err, tc.name)
	}

	status, body, _ := s.raw(t, http.MethodGet, s.armPath("/backends/b1"), "", nil)
	assert.Equal(t, http.StatusOK, status)
	assert.NotContains(t, body, "tok-secret")
	assert.NotContains(t, body, "hdr-secret")
	assert.Contains(t, body, "X-Key")

	_, err := be.Delete(ctx, apimRG, apimSvc, "b1", "*", nil)
	require.NoError(t, err)

	_, err = be.Get(ctx, apimRG, apimSvc, "b1", nil)
	require.Error(t, err)
}
