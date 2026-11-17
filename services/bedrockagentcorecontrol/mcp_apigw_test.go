package bedrockagentcorecontrol_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockagentcorecontrol"
)

var errNoSuchAPI = errors.New("no such api")

type fakeRestAPIs struct {
	fakeInvoker
	calls []bedrockagentcorecontrol.RestAPIRequest
	mu    sync.Mutex
}

func (f *fakeRestAPIs) RestAPIExport(_, apiID, stage string) (map[string]any, error) {
	if apiID != "abc123" || stage != "prod" {
		return nil, errNoSuchAPI
	}

	op := func(id, desc string) map[string]any {
		return map[string]any{"operationId": id, "description": desc, "parameters": []any{
			map[string]any{"name": "petId", "in": "path", "required": true, "schema": map[string]any{"type": "string"}},
		}}
	}

	return map[string]any{
		"openapi": "3.0.1",
		"paths": map[string]any{
			"/pets":         map[string]any{"get": op("listPets", "List pets"), "post": op("addPet", "Add a pet")},
			"/pets/{petId}": map[string]any{"get": op("getPet", "Get a pet")},
			"/orders":       map[string]any{"get": op("listOrders", "List orders")},
		},
	}, nil
}

func (f *fakeRestAPIs) InvokeRestAPI(
	_ context.Context, _ string, req bedrockagentcorecontrol.RestAPIRequest,
) (bedrockagentcorecontrol.RestAPIResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, req)

	return bedrockagentcorecontrol.RestAPIResponse{Status: http.StatusOK, Body: []byte(`{"pet":"rex"}`)}, nil
}

func apiGatewayTarget(
	filters []types.ApiGatewayToolFilter, overrides []types.ApiGatewayToolOverride,
) types.TargetConfiguration {
	return &types.TargetConfigurationMemberMcp{Value: &types.McpTargetConfigurationMemberApiGateway{
		Value: types.ApiGatewayTargetConfiguration{
			RestApiId: aws.String("abc123"), Stage: aws.String("prod"),
			ApiGatewayToolConfiguration: &types.ApiGatewayToolConfiguration{
				ToolFilters: filters, ToolOverrides: overrides,
			},
		},
	}}
}

func TestGatewayMCPAPIGatewayTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filters   []types.ApiGatewayToolFilter
		overrides []types.ApiGatewayToolOverride
		want      []string
	}{
		{
			name: "wildcard filter",
			filters: []types.ApiGatewayToolFilter{
				{FilterPath: aws.String("/pets/*"), Methods: []types.RestApiMethod{types.RestApiMethodGet}},
			},
			want: []string{"api___getPet", "api___listPets"},
		},
		{
			name: "override renames",
			filters: []types.ApiGatewayToolFilter{
				{FilterPath: aws.String("/orders"), Methods: []types.RestApiMethod{types.RestApiMethodGet}},
			},
			overrides: []types.ApiGatewayToolOverride{{
				Path: aws.String("/pets"), Method: types.RestApiMethodPost, Name: aws.String("adopt"),
				Description: aws.String("Adopt a pet"),
			}},
			want: []string{"api___adopt", "api___listOrders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := newMCPFixture(t)
			base := fakeRefs{functions: map[string]bool{lambdaARN: true}}
			host := &fakeRestAPIs{fakeInvoker: fakeInvoker{fakeRefs: base, calls: m.calls}}
			m.backend.SetReferences(host)

			gid := aws.ToString(createGateway(t, m.client, "Apigw-Gw").GatewayId)
			m.createTarget(t, gid, "api", apiGatewayTarget(tt.filters, tt.overrides))

			cs, err := connectMCP(t, m.mcpURL(gid), m.token(t, nil))
			require.NoError(t, err)

			t.Cleanup(func() { _ = cs.Close() })

			list, err := cs.ListTools(t.Context(), nil)
			require.NoError(t, err)

			got := make([]string, 0, len(list.Tools))
			for _, tool := range list.Tools {
				got = append(got, tool.Name)
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestGatewayMCPAPIGatewayCall(t *testing.T) {
	t.Parallel()

	m := newMCPFixture(t)
	base := fakeRefs{functions: map[string]bool{lambdaARN: true}}
	host := &fakeRestAPIs{fakeInvoker: fakeInvoker{fakeRefs: base, calls: m.calls}}
	m.backend.SetReferences(host)

	gid := aws.ToString(createGateway(t, m.client, "Apigw-Call").GatewayId)
	m.createTarget(t, gid, "api", apiGatewayTarget(
		[]types.ApiGatewayToolFilter{{
			FilterPath: aws.String("/pets/{petId}"), Methods: []types.RestApiMethod{types.RestApiMethodGet},
		}},
		nil))

	status, raw := m.rpc(t, gid, nil,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"api___getPet","arguments":{"petId":"7"}}}`)
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, raw, `rex`)

	host.mu.Lock()
	defer host.mu.Unlock()

	require.Len(t, host.calls, 1)
	assert.Equal(t, "/pets/7", host.calls[0].Path)
	assert.Equal(t, "GET", host.calls[0].Method)
	assert.Equal(t, "abc123", host.calls[0].APIID)
	assert.True(t, strings.EqualFold("prod", host.calls[0].Stage))
}

func TestCreateAPIGatewayTargetValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		apiID   string
		want    string
		filters int
	}{
		{name: "missing api", apiID: "nope", filters: 1, want: "ResourceNotFoundException"},
		{name: "no filters", apiID: "abc123", filters: 0, want: "ValidationException"},
		{name: "ok", apiID: "abc123", filters: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := newMCPFixture(t)
			base := fakeRefs{functions: map[string]bool{lambdaARN: true}}
			m.backend.SetReferences(&fakeRestAPIs{fakeInvoker: fakeInvoker{fakeRefs: base, calls: m.calls}})

			gid := aws.ToString(createGateway(t, m.client, "Apigw-Val").GatewayId)

			var filters []types.ApiGatewayToolFilter
			filters = make([]types.ApiGatewayToolFilter, 0)
			for range tt.filters {
				filters = append(filters, types.ApiGatewayToolFilter{
					FilterPath: aws.String("/pets"), Methods: []types.RestApiMethod{types.RestApiMethodGet},
				})
			}

			cfg := apiGatewayTarget(filters, nil)
			cfg.(*types.TargetConfigurationMemberMcp).Value.(*types.McpTargetConfigurationMemberApiGateway).
				Value.RestApiId = aws.String(tt.apiID)

			_, err := m.client.CreateGatewayTarget(t.Context(), &sdk.CreateGatewayTargetInput{
				GatewayIdentifier: aws.String(gid), Name: aws.String("api"), TargetConfiguration: cfg,
				CredentialProviderConfigurations: []types.CredentialProviderConfiguration{
					{CredentialProviderType: types.CredentialProviderTypeGatewayIamRole},
				},
			})
			assert.Equal(t, tt.want, apiCode(err))
		})
	}
}

type lockProbeAPIs struct {
	*fakeRestAPIs
	backend *bedrockagentcorecontrol.InMemoryBackend
	held    *atomic.Bool
}

func (p lockProbeAPIs) RestAPIExport(region, apiID, stage string) (map[string]any, error) {
	if p.backend.WriteLocked() {
		p.held.Store(true)
	}

	return p.fakeRestAPIs.RestAPIExport(region, apiID, stage)
}

func TestAPIGatewayTargetExportRunsOutsideBackendLock(t *testing.T) {
	t.Parallel()

	m := newMCPFixture(t)
	held := &atomic.Bool{}
	base := fakeRefs{functions: map[string]bool{lambdaARN: true}}
	host := lockProbeAPIs{
		fakeRestAPIs: &fakeRestAPIs{fakeInvoker: fakeInvoker{fakeRefs: base, calls: m.calls}},
		backend:      m.backend, held: held,
	}
	m.backend.SetReferences(host)

	gw := createGateway(t, m.client, "Lock-Gw")
	filters := []types.ApiGatewayToolFilter{
		{FilterPath: aws.String("/pets"), Methods: []types.RestApiMethod{types.RestApiMethodGet}},
	}

	tid := m.createTarget(t, aws.ToString(gw.GatewayId), "api", apiGatewayTarget(filters, nil))

	_, err := m.client.UpdateGatewayTarget(t.Context(), &sdk.UpdateGatewayTargetInput{
		GatewayIdentifier: gw.GatewayId, TargetId: aws.String(tid), Name: aws.String("api"),
		TargetConfiguration: apiGatewayTarget(filters, nil),
		CredentialProviderConfigurations: []types.CredentialProviderConfiguration{
			{CredentialProviderType: types.CredentialProviderTypeGatewayIamRole},
		},
	})
	require.NoError(t, err)
	assert.False(t, held.Load(), "the REST API export must not run under the backend lock")

	_, err = m.client.CreateGatewayTarget(t.Context(), &sdk.CreateGatewayTargetInput{
		GatewayIdentifier: gw.GatewayId, Name: aws.String("missing"),
		TargetConfiguration: &types.TargetConfigurationMemberMcp{Value: &types.McpTargetConfigurationMemberApiGateway{
			Value: types.ApiGatewayTargetConfiguration{
				RestApiId: aws.String("nope"), Stage: aws.String("prod"),
				ApiGatewayToolConfiguration: &types.ApiGatewayToolConfiguration{ToolFilters: filters},
			},
		}},
		CredentialProviderConfigurations: []types.CredentialProviderConfiguration{
			{CredentialProviderType: types.CredentialProviderTypeGatewayIamRole},
		},
	})
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

type hookAPIs struct {
	*fakeRestAPIs
	hook func()
	gone atomic.Bool
}

func (h *hookAPIs) RestAPIExport(region, apiID, stage string) (map[string]any, error) {
	if h.gone.Load() {
		return nil, errNoSuchAPI
	}

	out, err := h.fakeRestAPIs.RestAPIExport(region, apiID, stage)
	if h.hook != nil {
		h.hook()
	}

	return out, err
}

func TestAPIGatewayTargetWriteRevalidatesAfterExport(t *testing.T) {
	t.Parallel()

	filters := []types.ApiGatewayToolFilter{
		{FilterPath: aws.String("/pets"), Methods: []types.RestApiMethod{types.RestApiMethodGet}},
	}
	creds := []types.CredentialProviderConfiguration{
		{CredentialProviderType: types.CredentialProviderTypeGatewayIamRole},
	}

	t.Run("replay survives a deleted stage", func(t *testing.T) {
		t.Parallel()

		m := newMCPFixture(t)
		base := fakeRefs{functions: map[string]bool{lambdaARN: true}}
		host := &hookAPIs{fakeRestAPIs: &fakeRestAPIs{fakeInvoker: fakeInvoker{fakeRefs: base, calls: m.calls}}}
		m.backend.SetReferences(host)

		gw := createGateway(t, m.client, "Replay-Gw")
		in := &sdk.CreateGatewayTargetInput{
			GatewayIdentifier: gw.GatewayId, Name: aws.String("api"),
			ClientToken:         aws.String("replay-token-0001-replay-token-0001"),
			TargetConfiguration: apiGatewayTarget(filters, nil), CredentialProviderConfigurations: creds,
		}

		first, err := m.client.CreateGatewayTarget(t.Context(), in)
		require.NoError(t, err)

		host.gone.Store(true)

		again, err := m.client.CreateGatewayTarget(t.Context(), in)
		require.NoError(t, err, "an idempotent replay returns the original result without re-checking the stage")
		assert.Equal(t, aws.ToString(first.TargetId), aws.ToString(again.TargetId))
	})

	t.Run("gateway deleted during export", func(t *testing.T) {
		t.Parallel()

		m := newMCPFixture(t)
		base := fakeRefs{functions: map[string]bool{lambdaARN: true}}
		host := &hookAPIs{fakeRestAPIs: &fakeRestAPIs{fakeInvoker: fakeInvoker{fakeRefs: base, calls: m.calls}}}
		m.backend.SetReferences(host)

		gw := createGateway(t, m.client, "Gone-Gw")
		host.hook = func() {
			_, _ = m.client.DeleteGateway(
				context.WithoutCancel(t.Context()), &sdk.DeleteGatewayInput{GatewayIdentifier: gw.GatewayId},
			)
		}

		_, err := m.client.CreateGatewayTarget(t.Context(), &sdk.CreateGatewayTargetInput{
			GatewayIdentifier: gw.GatewayId, Name: aws.String("api"),
			TargetConfiguration: apiGatewayTarget(filters, nil), CredentialProviderConfigurations: creds,
		})
		require.Error(t, err)
		assert.Equal(t, "ResourceNotFoundException", apiCode(err))
	})
}
