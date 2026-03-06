package chaos_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockChaosService is a service.Registerable that also implements service.ChaosProvider.
type mockChaosService struct {
	name       string
	chaosName  string
	operations []string
	regions    []string
}

func (m *mockChaosService) Name() string                            { return m.name }
func (m *mockChaosService) Handler() echo.HandlerFunc               { return nil }
func (m *mockChaosService) RouteMatcher() service.Matcher           { return nil }
func (m *mockChaosService) GetSupportedOperations() []string        { return m.operations }
func (m *mockChaosService) ExtractOperation(_ *echo.Context) string { return "" }
func (m *mockChaosService) ExtractResource(_ *echo.Context) string  { return "" }
func (m *mockChaosService) MatchPriority() int                      { return 0 }
func (m *mockChaosService) ChaosServiceName() string                { return m.chaosName }
func (m *mockChaosService) ChaosOperations() []string               { return m.operations }
func (m *mockChaosService) ChaosRegions() []string                  { return m.regions }

// plainService implements service.Registerable but NOT service.ChaosProvider.
type plainService struct{ name string }

func (p *plainService) Name() string                            { return p.name }
func (p *plainService) Handler() echo.HandlerFunc               { return nil }
func (p *plainService) RouteMatcher() service.Matcher           { return nil }
func (p *plainService) GetSupportedOperations() []string        { return nil }
func (p *plainService) ExtractOperation(_ *echo.Context) string { return "" }
func (p *plainService) ExtractResource(_ *echo.Context) string  { return "" }
func (p *plainService) MatchPriority() int                      { return 0 }

// callGroup executes a request against a set of routes registered on a group.
func callGroup(t *testing.T, e *echo.Echo, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	return rec
}

func buildChaosAPI(t *testing.T, store *chaos.FaultStore, reg *service.Registry) *echo.Echo {
	t.Helper()
	e := echo.New()
	group := e.Group("/_gopherstack/chaos")
	chaos.RegisterRoutes(group, store, reg)

	return e
}

func TestChaosHandler_Faults_GetEmpty(t *testing.T) {
	t.Parallel()

	store := chaos.NewFaultStore()
	reg := service.NewRegistry()
	e := buildChaosAPI(t, store, reg)

	rec := callGroup(t, e, http.MethodGet, "/_gopherstack/chaos/faults", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var rules []chaos.FaultRule
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rules))
	assert.Empty(t, rules)
}

func TestChaosHandler_Faults_PostReplacesRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		initial    []chaos.FaultRule
		body       []chaos.FaultRule
		wantRules  []chaos.FaultRule
		wantStatus int
	}{
		{
			name: "post empty array clears rules",
			initial: []chaos.FaultRule{
				{Service: "s3"},
			},
			body:       []chaos.FaultRule{},
			wantRules:  []chaos.FaultRule{},
			wantStatus: http.StatusOK,
		},
		{
			name: "post replaces existing rules",
			initial: []chaos.FaultRule{
				{Service: "s3"},
			},
			body: []chaos.FaultRule{
				{Service: "dynamodb"},
			},
			wantRules:  []chaos.FaultRule{{Service: "dynamodb"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "post multiple rules",
			body: []chaos.FaultRule{
				{Service: "s3"},
				{Service: "dynamodb", Operation: "PutItem", Probability: 0.5},
			},
			wantRules: []chaos.FaultRule{
				{Service: "s3"},
				{Service: "dynamodb", Operation: "PutItem", Probability: 0.5},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()

			if len(tt.initial) > 0 {
				store.SetRules(tt.initial)
			}

			reg := service.NewRegistry()
			e := buildChaosAPI(t, store, reg)

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := callGroup(t, e, http.MethodPost, "/_gopherstack/chaos/faults", bodyBytes)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var rules []chaos.FaultRule
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rules))
			assert.Equal(t, tt.wantRules, rules)
		})
	}
}

func TestChaosHandler_Faults_PatchAppendsRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		initial    []chaos.FaultRule
		patch      []chaos.FaultRule
		wantCount  int
		wantStatus int
	}{
		{
			name:       "patch empty store appends",
			patch:      []chaos.FaultRule{{Service: "s3"}},
			wantCount:  1,
			wantStatus: http.StatusOK,
		},
		{
			name: "patch existing store appends",
			initial: []chaos.FaultRule{
				{Service: "s3"},
			},
			patch:      []chaos.FaultRule{{Service: "dynamodb"}},
			wantCount:  2,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()
			store.SetRules(tt.initial)

			reg := service.NewRegistry()
			e := buildChaosAPI(t, store, reg)

			bodyBytes, err := json.Marshal(tt.patch)
			require.NoError(t, err)

			rec := callGroup(t, e, http.MethodPatch, "/_gopherstack/chaos/faults", bodyBytes)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var rules []chaos.FaultRule
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rules))
			assert.Len(t, rules, tt.wantCount)
		})
	}
}

func TestChaosHandler_Faults_DeleteRemovesRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		initial    []chaos.FaultRule
		delete     []chaos.FaultRule
		wantCount  int
		wantStatus int
	}{
		{
			name: "delete matching rule",
			initial: []chaos.FaultRule{
				{Service: "s3"},
				{Service: "dynamodb"},
			},
			delete:     []chaos.FaultRule{{Service: "s3"}},
			wantCount:  1,
			wantStatus: http.StatusOK,
		},
		{
			name: "delete non-existent rule leaves store unchanged",
			initial: []chaos.FaultRule{
				{Service: "s3"},
			},
			delete:     []chaos.FaultRule{{Service: "lambda"}},
			wantCount:  1,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()
			store.SetRules(tt.initial)

			reg := service.NewRegistry()
			e := buildChaosAPI(t, store, reg)

			bodyBytes, err := json.Marshal(tt.delete)
			require.NoError(t, err)

			rec := callGroup(t, e, http.MethodDelete, "/_gopherstack/chaos/faults", bodyBytes)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var rules []chaos.FaultRule
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rules))
			assert.Len(t, rules, tt.wantCount)
		})
	}
}

func TestChaosHandler_Effects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		initial     *chaos.NetworkEffects
		postBody    *chaos.NetworkEffects
		wantEffects chaos.NetworkEffects
		wantStatus  int
	}{
		{
			name:        "get effects returns zero effects by default",
			wantEffects: chaos.NetworkEffects{},
			wantStatus:  http.StatusOK,
		},
		{
			name:        "post effects updates configuration",
			postBody:    &chaos.NetworkEffects{Latency: 200},
			wantEffects: chaos.NetworkEffects{Latency: 200},
			wantStatus:  http.StatusOK,
		},
		{
			name: "post effects with latency range",
			postBody: &chaos.NetworkEffects{
				LatencyRange: &chaos.LatencyRange{Min: 100, Max: 500},
			},
			wantEffects: chaos.NetworkEffects{
				LatencyRange: &chaos.LatencyRange{Min: 100, Max: 500},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()
			if tt.initial != nil {
				store.SetEffects(*tt.initial)
			}

			reg := service.NewRegistry()
			e := buildChaosAPI(t, store, reg)

			if tt.postBody != nil {
				bodyBytes, err := json.Marshal(tt.postBody)
				require.NoError(t, err)

				postRec := callGroup(t, e, http.MethodPost, "/_gopherstack/chaos/effects", bodyBytes)
				require.Equal(t, tt.wantStatus, postRec.Code)
			}

			getRec := callGroup(t, e, http.MethodGet, "/_gopherstack/chaos/effects", nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var effects chaos.NetworkEffects
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &effects))
			assert.Equal(t, tt.wantEffects, effects)
		})
	}
}

func TestChaosHandler_Targets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		services     []service.Registerable
		wantNames    []string
		wantNotNames []string
	}{
		{
			name:      "empty registry returns empty services list",
			services:  nil,
			wantNames: nil,
		},
		{
			name: "services implementing ChaosProvider are included",
			services: []service.Registerable{
				&mockChaosService{
					name:       "DynamoDB",
					chaosName:  "dynamodb",
					operations: []string{"GetItem", "PutItem"},
					regions:    []string{"us-east-1"},
				},
			},
			wantNames: []string{"dynamodb"},
		},
		{
			name: "services not implementing ChaosProvider are excluded",
			services: []service.Registerable{
				&plainService{name: "PlainSvc"},
			},
			wantNotNames: []string{"PlainSvc", "plainsvc"},
		},
		{
			name: "mixed services: only ChaosProvider services included",
			services: []service.Registerable{
				&mockChaosService{
					name:       "S3",
					chaosName:  "s3",
					operations: []string{"PutObject"},
					regions:    []string{"us-east-1"},
				},
				&plainService{name: "IAM"},
			},
			wantNames:    []string{"s3"},
			wantNotNames: []string{"iam", "IAM"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()
			reg := service.NewRegistry()

			for _, svc := range tt.services {
				require.NoError(t, reg.Register(svc))
			}

			e := buildChaosAPI(t, store, reg)
			rec := callGroup(t, e, http.MethodGet, "/_gopherstack/chaos/targets", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			type targetsResp struct {
				Services []struct {
					Name string `json:"name"`
				} `json:"services"`
			}

			var resp targetsResp
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			names := make([]string, 0, len(resp.Services))
			for _, s := range resp.Services {
				names = append(names, s.Name)
			}

			for _, want := range tt.wantNames {
				assert.Contains(t, names, want)
			}

			for _, notWant := range tt.wantNotNames {
				assert.NotContains(t, names, notWant)
			}
		})
	}
}

func TestChaosHandler_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "POST /faults with invalid JSON returns 400",
			method:     http.MethodPost,
			path:       "/_gopherstack/chaos/faults",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "PATCH /faults with invalid JSON returns 400",
			method:     http.MethodPatch,
			path:       "/_gopherstack/chaos/faults",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "DELETE /faults with invalid JSON returns 400",
			method:     http.MethodDelete,
			path:       "/_gopherstack/chaos/faults",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "POST /effects with invalid JSON returns 400",
			method:     http.MethodPost,
			path:       "/_gopherstack/chaos/effects",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()
			reg := service.NewRegistry()
			e := buildChaosAPI(t, store, reg)

			rec := callGroup(t, e, tt.method, tt.path, []byte("not-json!!!"))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
