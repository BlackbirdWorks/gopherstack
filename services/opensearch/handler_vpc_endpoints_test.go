package opensearch_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthorizeVpcEndpointAccess_ServicePrincipal(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("vpc-domain", "")

	principal, err := b.AuthorizeVpcEndpointAccess("vpc-domain", "", "vpc-endpoint.opensearch.amazonaws.com")
	require.NoError(t, err)
	assert.Equal(t, "AWS_SERVICE", principal.PrincipalType)
	assert.Equal(t, "vpc-endpoint.opensearch.amazonaws.com", principal.Principal)
}

func TestAuthorizeVpcEndpointAccess_AccountPrincipal(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("vpc-domain", "")

	principal, err := b.AuthorizeVpcEndpointAccess("vpc-domain", "111122223333", "")
	require.NoError(t, err)
	assert.Equal(t, "AWS_ACCOUNT", principal.PrincipalType)
	assert.Equal(t, "111122223333", principal.Principal)
}

func TestVpcEndpointAccess_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		revokeAccount string
		accounts      []string
		wantCount     int
	}{
		{
			name:      "authorize_and_list",
			accounts:  []string{"111122223333"},
			wantCount: 1,
		},
		{
			name:          "authorize_then_revoke",
			accounts:      []string{"111122223333", "444455556666"},
			revokeAccount: "111122223333",
			wantCount:     1,
		},
		{
			name:      "multiple_authorize",
			accounts:  []string{"111122223333", "444455556666", "777788889999"},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
			b.AddDomainInternal("vpc-domain", "")

			for _, account := range tt.accounts {
				_, err := b.AuthorizeVpcEndpointAccess("vpc-domain", account, "")
				require.NoError(t, err)
			}

			if tt.revokeAccount != "" {
				err := b.RevokeVpcEndpointAccess("vpc-domain", tt.revokeAccount)
				require.NoError(t, err)
			}

			principals, err := b.ListVpcEndpointAccess("vpc-domain")
			require.NoError(t, err)
			assert.Len(t, principals, tt.wantCount)
		})
	}
}

func TestVpcEndpoints_CreateAndList(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	domARN := createDomainAndGetARN(t, h, "vpc-ep-domain")

	// Create a VPC endpoint.
	cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/vpcEndpoints",
		map[string]any{"DomainArn": domARN, "VpcOptions": map[string]any{"SubnetIds": []string{"subnet-1"}}})
	defer cr.Body.Close()
	require.Equal(t, http.StatusOK, cr.StatusCode)

	var cOut map[string]any
	require.NoError(t, json.NewDecoder(cr.Body).Decode(&cOut))
	ep := cOut["VpcEndpoint"].(map[string]any)
	assert.NotEmpty(t, ep["VpcEndpointId"])
	assert.Equal(t, "ACTIVE", ep["Status"])

	// VpcOptions on the response is the response-shape VPCDerivedInfo, which
	// (unlike the request-shape VPCOptions) also carries server-derived
	// AvailabilityZones/VPCId.
	vpcOptions := ep["VpcOptions"].(map[string]any)
	assert.NotEmpty(t, vpcOptions["VPCId"])
	assert.NotEmpty(t, vpcOptions["AvailabilityZones"])

	// List endpoints — must contain the created one.
	lr := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/vpcEndpoints", nil)
	defer lr.Body.Close()
	require.Equal(t, http.StatusOK, lr.StatusCode)

	var lOut map[string]any
	require.NoError(t, json.NewDecoder(lr.Body).Decode(&lOut))
	eps, ok := lOut["VpcEndpoints"].([]any)
	require.True(t, ok)
	assert.Len(t, eps, 1)
}

// TestVpcEndpoints_ListOmitsGetOnlyFields verifies gopherstack-uult: both
// ListVpcEndpoints and ListVpcEndpointsForDomain must emit only
// types.VpcEndpointSummary's members (DomainArn, Status, VpcEndpointId,
// VpcEndpointOwner) -- opensearch@v1.75.4 types/types.go:3483-3498. Endpoint
// and VpcOptions are DescribeVpcEndpoints-only, and the internal StatusUntil
// clock field must never reach the wire. An SDK client would silently drop
// the extra keys, so this asserts on the raw body.
func TestVpcEndpoints_ListOmitsGetOnlyFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    func(domARN, domName string) string
		key     string
		domName string
	}{
		{
			name:    "list_vpc_endpoints",
			path:    func(_, _ string) string { return "/2021-01-01/opensearch/vpcEndpoints" },
			key:     "VpcEndpoints",
			domName: "vpc-list-a",
		},
		{
			name: "list_vpc_endpoints_for_domain",
			path: func(_, domName string) string {
				return "/2021-01-01/opensearch/domain/" + domName + "/vpcEndpoints"
			},
			key:     "VpcEndpointSummaryList",
			domName: "vpc-list-b",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			domARN := createDomainAndGetARN(t, h, tt.domName)

			cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/vpcEndpoints",
				map[string]any{"DomainArn": domARN, "VpcOptions": map[string]any{"SubnetIds": []string{"subnet-1"}}})
			require.Equal(t, http.StatusOK, cr.StatusCode)
			cr.Body.Close()

			lr := doRequest(t, h, http.MethodGet, tt.path(domARN, tt.domName), nil)
			defer lr.Body.Close()
			require.Equal(t, http.StatusOK, lr.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(lr.Body).Decode(&out))
			items, ok := out[tt.key].([]any)
			require.True(t, ok)
			require.Len(t, items, 1)

			item := items[0].(map[string]any)
			keys := make([]string, 0, len(item))
			for k := range item {
				keys = append(keys, k)
			}
			assert.ElementsMatch(t,
				[]string{"DomainArn", "Status", "VpcEndpointId", "VpcEndpointOwner"},
				keys,
			)
			assert.NotContains(t, item, "Endpoint")
			assert.NotContains(t, item, "VpcOptions")
			assert.NotContains(t, item, "statusUntil")
		})
	}
}

func TestVpcEndpoints_DescribeByIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	domARN := createDomainAndGetARN(t, h, "vpc-desc-domain")

	cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/vpcEndpoints",
		map[string]any{"DomainArn": domARN, "VpcOptions": map[string]any{}})
	var cOut map[string]any
	require.NoError(t, json.NewDecoder(cr.Body).Decode(&cOut))
	cr.Body.Close()
	epID := cOut["VpcEndpoint"].(map[string]any)["VpcEndpointId"].(string)

	// Describe by explicit ID.
	dr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/vpcEndpoints/describe",
		map[string]any{"VpcEndpointIds": []string{epID}})
	defer dr.Body.Close()
	require.Equal(t, http.StatusOK, dr.StatusCode)

	var dOut map[string]any
	require.NoError(t, json.NewDecoder(dr.Body).Decode(&dOut))
	described, ok := dOut["VpcEndpoints"].([]any)
	require.True(t, ok)
	assert.Len(t, described, 1)
	assert.Equal(t, epID, described[0].(map[string]any)["VpcEndpointId"])
}

func TestVpcEndpoints_UpdateAndDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	domARN := createDomainAndGetARN(t, h, "vpc-ud-domain")

	cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/vpcEndpoints",
		map[string]any{"DomainArn": domARN, "VpcOptions": map[string]any{}})
	var cOut map[string]any
	require.NoError(t, json.NewDecoder(cr.Body).Decode(&cOut))
	cr.Body.Close()
	epID := cOut["VpcEndpoint"].(map[string]any)["VpcEndpointId"].(string)

	// Update the endpoint.
	ur := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/vpcEndpoints/update",
		map[string]any{"VpcEndpointId": epID, "VpcOptions": map[string]any{"SubnetIds": []string{"subnet-updated"}}})
	defer ur.Body.Close()
	require.Equal(t, http.StatusOK, ur.StatusCode)

	// Delete the endpoint.
	del := doRequest(t, h, http.MethodDelete, "/2021-01-01/opensearch/vpcEndpoints/"+epID, nil)
	defer del.Body.Close()
	require.Equal(t, http.StatusOK, del.StatusCode)

	var dOut map[string]any
	require.NoError(t, json.NewDecoder(del.Body).Decode(&dOut))
	summary := dOut["VpcEndpointSummary"].(map[string]any)
	assert.Equal(t, epID, summary["VpcEndpointId"])
	assert.Equal(t, domARN, summary["DomainArn"])
	assert.NotEmpty(t, summary["VpcEndpointOwner"])

	// List should now be empty.
	lr := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/vpcEndpoints", nil)
	defer lr.Body.Close()
	var lOut map[string]any
	require.NoError(t, json.NewDecoder(lr.Body).Decode(&lOut))
	eps := lOut["VpcEndpoints"].([]any)
	assert.Empty(t, eps)
}

func TestVpcEndpoints_CreateMissingRequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "missing_domain_arn",
			body: map[string]any{"VpcOptions": map[string]any{"SubnetIds": []string{"subnet-1"}}},
		},
		{
			name: "missing_vpc_options",
			body: map[string]any{"DomainArn": "arn:aws:es:us-east-1:123456789012:domain/x"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/vpcEndpoints", tt.body)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	}
}

func TestVpcEndpoints_DescribeUnknownIDReturnsEndpointNotFoundError(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	dr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/vpcEndpoints/describe",
		map[string]any{"VpcEndpointIds": []string{"vpce-nonexistent"}})
	defer dr.Body.Close()
	require.Equal(t, http.StatusOK, dr.StatusCode)

	var dOut map[string]any
	require.NoError(t, json.NewDecoder(dr.Body).Decode(&dOut))
	errs, ok := dOut["VpcEndpointErrors"].([]any)
	require.True(t, ok)
	require.Len(t, errs, 1)
	assert.Equal(t, "ENDPOINT_NOT_FOUND", errs[0].(map[string]any)["ErrorCode"])
}

func TestOpenSearchHandler_AuthorizeVpcEndpointAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *opensearch.Handler)
		name         string
		domainName   string
		account      string
		service      string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "success_account",
			domainName: "vpc-domain",
			account:    "111122223333",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "vpc-domain"})
				r.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"111122223333", "AWS_ACCOUNT"},
		},
		{
			name:       "success_service",
			domainName: "svc-domain",
			service:    "delivery.logs.amazonaws.com",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "svc-domain"})
				r.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"delivery.logs.amazonaws.com", "AWS_SERVICE"},
		},
		{
			name:       "domain_not_found",
			domainName: "nonexistent",
			account:    "111122223333",
			wantCode:   http.StatusNotFound,
		},
		{
			name:       "invalid_json",
			domainName: "any-domain",
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			path := "/2021-01-01/opensearch/domain/" + tt.domainName + "/authorizeVpcEndpointAccess"

			if tt.name == "invalid_json" {
				req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("bad-json"))
				req.Header.Set("Content-Type", "application/json")
				rw := httptest.NewRecorder()
				h.ServeHTTP(rw, req)
				resp := rw.Result()
				defer resp.Body.Close()
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

				return
			}

			body := map[string]any{
				"Account": tt.account,
				"Service": tt.service,
			}
			resp := doRequest(t, h, http.MethodPost, path, body)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if len(tt.wantContains) > 0 {
				bodyBytes, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				for _, s := range tt.wantContains {
					assert.Contains(t, string(bodyBytes), s)
				}
			}
		})
	}
}
