package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func route53resolverPost(t *testing.T, action string, body any) *http.Response {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Route53Resolver."+action)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func route53resolverReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_Route53Resolver_CreateEndpoint(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := route53resolverPost(t, "CreateResolverEndpoint", map[string]any{
		"Name":             "integ-endpoint",
		"Direction":        "INBOUND",
		"SecurityGroupIds": []string{"sg-12345"},
		"IpAddresses": []map[string]string{
			{"SubnetId": "subnet-1", "Ip": "10.0.0.10"},
		},
	})
	body := route53resolverReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ResolverEndpoint")
}

func TestIntegration_Route53Resolver_ListEndpoints(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	route53resolverPost(t, "CreateResolverEndpoint", map[string]any{
		"Name":             "list-endpoint-test",
		"Direction":        "OUTBOUND",
		"SecurityGroupIds": []string{"sg-abc"},
		"IpAddresses":      []map[string]string{},
	})

	resp := route53resolverPost(t, "ListResolverEndpoints", map[string]any{})
	body := route53resolverReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ResolverEndpoints")
}

func TestIntegration_Route53Resolver_GetEndpoint(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	createResp := route53resolverPost(t, "CreateResolverEndpoint", map[string]any{
		"Name":             "get-endpoint-test",
		"Direction":        "INBOUND",
		"SecurityGroupIds": []string{},
		"IpAddresses":      []map[string]string{},
	})

	var createBody map[string]any
	createData, _ := io.ReadAll(createResp.Body)
	createResp.Body.Close()
	require.NoError(t, json.Unmarshal(createData, &createBody))

	ep, _ := createBody["ResolverEndpoint"].(map[string]any)
	id, _ := ep["Id"].(string)
	require.NotEmpty(t, id)

	resp := route53resolverPost(t, "GetResolverEndpoint", map[string]any{
		"ResolverEndpointId": id,
	})
	body := route53resolverReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, id)
}

func TestIntegration_Route53Resolver_DeleteEndpoint(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	createResp := route53resolverPost(t, "CreateResolverEndpoint", map[string]any{
		"Name":             "delete-endpoint-test",
		"Direction":        "OUTBOUND",
		"SecurityGroupIds": []string{},
		"IpAddresses":      []map[string]string{},
	})

	var createBody map[string]any
	createData, _ := io.ReadAll(createResp.Body)
	createResp.Body.Close()
	require.NoError(t, json.Unmarshal(createData, &createBody))

	ep, _ := createBody["ResolverEndpoint"].(map[string]any)
	id, _ := ep["Id"].(string)
	require.NotEmpty(t, id)

	resp := route53resolverPost(t, "DeleteResolverEndpoint", map[string]any{
		"ResolverEndpointId": id,
	})
	body := route53resolverReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
}

func TestIntegration_Route53Resolver_CreateRule(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := route53resolverPost(t, "CreateResolverRule", map[string]any{
		"Name":       "integ-rule",
		"DomainName": "example.internal",
		"RuleType":   "FORWARD",
	})
	body := route53resolverReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ResolverRule")
}

func TestIntegration_Route53Resolver_ListRules(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	route53resolverPost(t, "CreateResolverRule", map[string]any{
		"Name":       "list-rule-test",
		"DomainName": "internal.example.com",
		"RuleType":   "FORWARD",
	})

	resp := route53resolverPost(t, "ListResolverRules", map[string]any{})
	body := route53resolverReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ResolverRules")
}

func TestIntegration_Route53Resolver_DeleteRule(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	createResp := route53resolverPost(t, "CreateResolverRule", map[string]any{
		"Name":       "delete-rule-test",
		"DomainName": "delete.internal",
		"RuleType":   "FORWARD",
	})

	var createBody map[string]any
	createData, _ := io.ReadAll(createResp.Body)
	createResp.Body.Close()
	require.NoError(t, json.Unmarshal(createData, &createBody))

	rule, _ := createBody["ResolverRule"].(map[string]any)
	id, _ := rule["Id"].(string)
	require.NotEmpty(t, id)

	resp := route53resolverPost(t, "DeleteResolverRule", map[string]any{
		"ResolverRuleId": id,
	})
	body := route53resolverReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
}

func TestIntegration_Route53Resolver_GetRule(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	createResp := route53resolverPost(t, "CreateResolverRule", map[string]any{
		"Name":       "get-rule-test",
		"DomainName": "get.internal",
		"RuleType":   "FORWARD",
	})

	var createBody map[string]any
	createData, _ := io.ReadAll(createResp.Body)
	createResp.Body.Close()
	require.NoError(t, json.Unmarshal(createData, &createBody))

	rule, _ := createBody["ResolverRule"].(map[string]any)
	id, _ := rule["Id"].(string)
	require.NotEmpty(t, id)

	resp := route53resolverPost(t, "GetResolverRule", map[string]any{
		"ResolverRuleId": id,
	})
	body := route53resolverReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "get-rule-test")
}
