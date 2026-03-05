package route53resolver_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/route53resolver"
)

func newTestHandler(t *testing.T) *route53resolver.Handler {
	t.Helper()

	return route53resolver.NewHandler(route53resolver.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(t *testing.T, h *route53resolver.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Route53Resolver."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// doInvalidJSONRequest sends a request with invalid JSON body to test parse errors.
func doInvalidJSONRequest(t *testing.T, h *route53resolver.Handler, action string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Route53Resolver."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandlerName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "Route53Resolver", h.Name())
}

func TestHandlerGetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	tests := []struct {
		name   string
		wantOp string
	}{
		{name: "CreateResolverEndpoint", wantOp: "CreateResolverEndpoint"},
		{name: "DeleteResolverEndpoint", wantOp: "DeleteResolverEndpoint"},
		{name: "ListResolverEndpoints", wantOp: "ListResolverEndpoints"},
		{name: "GetResolverEndpoint", wantOp: "GetResolverEndpoint"},
		{name: "CreateResolverRule", wantOp: "CreateResolverRule"},
		{name: "DeleteResolverRule", wantOp: "DeleteResolverRule"},
		{name: "ListResolverRules", wantOp: "ListResolverRules"},
		{name: "GetResolverRule", wantOp: "GetResolverRule"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Contains(t, ops, tt.wantOp)
		})
	}
}

func TestHandlerMatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestHandlerRouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "Match",
			target:    "Route53Resolver.CreateResolverEndpoint",
			wantMatch: true,
		},
		{
			name:      "NoMatch",
			target:    "Firehose_20150804.CreateDeliveryStream",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

func TestHandlerExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		wantOp string
	}{
		{
			name:   "WithTarget",
			target: "Route53Resolver.CreateResolverEndpoint",
			wantOp: "CreateResolverEndpoint",
		},
		{
			name:   "NoTarget",
			target: "",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandlerExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"Name":"my-endpoint"}`))
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Equal(t, "my-endpoint", h.ExtractResource(c))
}

func TestCreateResolverEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "my-endpoint",
		"Direction": "INBOUND",
		"IpAddresses": []map[string]string{
			{"SubnetId": "subnet-abc", "Ip": "10.0.0.1"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ep, ok := resp["ResolverEndpoint"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, ep["Arn"], "arn:aws:route53resolver:")
	assert.Equal(t, "my-endpoint", ep["Name"])
	assert.Equal(t, "INBOUND", ep["Direction"])
	assert.Equal(t, "OPERATIONAL", ep["Status"])
}

func TestGetResolverEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "ep1",
		"Direction": "OUTBOUND",
	})
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	ep := createResp["ResolverEndpoint"].(map[string]any)
	id := ep["Id"].(string)

	rec := doRequest(t, h, "GetResolverEndpoint", map[string]any{
		"ResolverEndpointId": id,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	got := resp["ResolverEndpoint"].(map[string]any)
	assert.Equal(t, id, got["Id"])
}

func TestListResolverEndpoints(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateResolverEndpoint", map[string]any{"Name": "ep1", "Direction": "INBOUND"})
	doRequest(t, h, "CreateResolverEndpoint", map[string]any{"Name": "ep2", "Direction": "OUTBOUND"})

	rec := doRequest(t, h, "ListResolverEndpoints", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	endpoints, ok := resp["ResolverEndpoints"].([]any)
	require.True(t, ok)
	assert.Len(t, endpoints, 2)
}

func TestDeleteResolverEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "ep-to-delete",
		"Direction": "INBOUND",
	})
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	ep := createResp["ResolverEndpoint"].(map[string]any)
	id := ep["Id"].(string)

	rec := doRequest(t, h, "DeleteResolverEndpoint", map[string]any{
		"ResolverEndpointId": id,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deleted
	getRec := doRequest(t, h, "GetResolverEndpoint", map[string]any{"ResolverEndpointId": id})
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

func TestCreateResolverRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateResolverRule", map[string]any{
		"Name":       "my-rule",
		"DomainName": "example.com",
		"RuleType":   "FORWARD",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	rule, ok := resp["ResolverRule"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, rule["Arn"], "arn:aws:route53resolver:")
	assert.Equal(t, "my-rule", rule["Name"])
	assert.Equal(t, "example.com", rule["DomainName"])
	assert.Equal(t, "FORWARD", rule["RuleType"])
	assert.Equal(t, "COMPLETE", rule["Status"])
}

func TestListResolverRules(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateResolverRule", map[string]any{"Name": "r1", "DomainName": "a.com", "RuleType": "FORWARD"})
	doRequest(t, h, "CreateResolverRule", map[string]any{"Name": "r2", "DomainName": "b.com", "RuleType": "SYSTEM"})

	rec := doRequest(t, h, "ListResolverRules", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	rules, ok := resp["ResolverRules"].([]any)
	require.True(t, ok)
	assert.Len(t, rules, 2)
}

func TestDeleteResolverRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateResolverRule", map[string]any{
		"Name":       "rule-to-delete",
		"DomainName": "test.com",
		"RuleType":   "FORWARD",
	})
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	rule := createResp["ResolverRule"].(map[string]any)
	id := rule["Id"].(string)

	rec := doRequest(t, h, "DeleteResolverRule", map[string]any{
		"ResolverRuleId": id,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestGetResolverRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateResolverRule", map[string]any{
		"Name":       "get-rule",
		"DomainName": "get.example.com",
		"RuleType":   "FORWARD",
	})
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	rule := createResp["ResolverRule"].(map[string]any)
	id := rule["Id"].(string)

	rec := doRequest(t, h, "GetResolverRule", map[string]any{
		"ResolverRuleId": id,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	got, ok := resp["ResolverRule"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, id, got["Id"])
	assert.Equal(t, "get-rule", got["Name"])
}

func TestHandlerRequestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "GetResolverEndpoint_NotFound",
			action:   "GetResolverEndpoint",
			body:     map[string]any{"ResolverEndpointId": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteResolverEndpoint_NotFound",
			action:   "DeleteResolverEndpoint",
			body:     map[string]any{"ResolverEndpointId": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteResolverRule_NotFound",
			action:   "DeleteResolverRule",
			body:     map[string]any{"ResolverRuleId": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "GetResolverRule_NotFound",
			action:   "GetResolverRule",
			body:     map[string]any{"ResolverRuleId": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "UnknownAction",
			action:   "UnknownAction",
			body:     nil,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandlerInvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		wantCode int
	}{
		{name: "CreateResolverEndpoint", action: "CreateResolverEndpoint", wantCode: http.StatusBadRequest},
		{name: "DeleteResolverEndpoint", action: "DeleteResolverEndpoint", wantCode: http.StatusBadRequest},
		{name: "GetResolverEndpoint", action: "GetResolverEndpoint", wantCode: http.StatusBadRequest},
		{name: "CreateResolverRule", action: "CreateResolverRule", wantCode: http.StatusBadRequest},
		{name: "GetResolverRule", action: "GetResolverRule", wantCode: http.StatusBadRequest},
		{name: "DeleteResolverRule", action: "DeleteResolverRule", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doInvalidJSONRequest(t, h, tt.action)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestProviderName(t *testing.T) {
	t.Parallel()

	p := &route53resolver.Provider{}
	assert.Equal(t, "Route53Resolver", p.Name())
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &route53resolver.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "Route53Resolver", svc.Name())
}
