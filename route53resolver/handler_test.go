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

	return route53resolver.NewHandler(route53resolver.NewInMemoryBackend("000000000000", "us-east-1"), slog.Default())
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

func TestRoute53Resolver_Handler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "Route53Resolver", h.Name())
}

func TestRoute53Resolver_Handler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateResolverEndpoint")
	assert.Contains(t, ops, "DeleteResolverEndpoint")
	assert.Contains(t, ops, "ListResolverEndpoints")
	assert.Contains(t, ops, "GetResolverEndpoint")
	assert.Contains(t, ops, "CreateResolverRule")
	assert.Contains(t, ops, "DeleteResolverRule")
	assert.Contains(t, ops, "ListResolverRules")
}

func TestRoute53Resolver_Handler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestRoute53Resolver_Handler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "Route53Resolver.CreateResolverEndpoint")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.True(t, matcher(c))
}

func TestRoute53Resolver_Handler_RouteMatcher_NoMatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "Firehose_20150804.CreateDeliveryStream")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.False(t, matcher(c))
}

func TestRoute53Resolver_Handler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "Route53Resolver.CreateResolverEndpoint")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "CreateResolverEndpoint", h.ExtractOperation(c))

	// No target → "Unknown"
	req2 := httptest.NewRequest(http.MethodPost, "/", nil)
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.Equal(t, "Unknown", h.ExtractOperation(c2))
}

func TestRoute53Resolver_Handler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"Name":"my-endpoint"}`))
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "my-endpoint", h.ExtractResource(c))
}

func TestRoute53Resolver_Handler_CreateResolverEndpoint(t *testing.T) {
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

func TestRoute53Resolver_Handler_GetResolverEndpoint(t *testing.T) {
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

func TestRoute53Resolver_Handler_GetResolverEndpoint_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "GetResolverEndpoint", map[string]any{
		"ResolverEndpointId": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRoute53Resolver_Handler_ListResolverEndpoints(t *testing.T) {
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

func TestRoute53Resolver_Handler_DeleteResolverEndpoint(t *testing.T) {
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

func TestRoute53Resolver_Handler_DeleteResolverEndpoint_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "DeleteResolverEndpoint", map[string]any{
		"ResolverEndpointId": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRoute53Resolver_Handler_CreateResolverRule(t *testing.T) {
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

func TestRoute53Resolver_Handler_ListResolverRules(t *testing.T) {
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

func TestRoute53Resolver_Handler_DeleteResolverRule(t *testing.T) {
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

func TestRoute53Resolver_Handler_DeleteResolverRule_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "DeleteResolverRule", map[string]any{
		"ResolverRuleId": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRoute53Resolver_Handler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "UnknownAction", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRoute53Resolver_Provider(t *testing.T) {
	t.Parallel()

	p := &route53resolver.Provider{}
	assert.Equal(t, "Route53Resolver", p.Name())
}

func TestRoute53Resolver_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &route53resolver.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "Route53Resolver", svc.Name())
}
