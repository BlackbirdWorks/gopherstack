package apprunner_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCustomDomainAssociateDescribeDisassociate(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "associate returns domain",
			action: "AssociateCustomDomain",
			body: map[string]any{
				"ServiceArn": svcArn,
				"DomainName": "example.com",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				cd := resp["CustomDomain"].(map[string]any)
				assert.Equal(t, "example.com", cd["DomainName"])
				assert.Equal(t, "ACTIVE", cd["Status"])
				assert.Equal(t, true, cd["EnableWWWSubdomain"])
			},
		},
		{
			name:     "associate missing ServiceArn returns 400",
			action:   "AssociateCustomDomain",
			body:     map[string]any{"DomainName": "x.com"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "associate missing DomainName returns 400",
			action:   "AssociateCustomDomain",
			body:     map[string]any{"ServiceArn": svcArn},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "associate unknown service returns 400",
			action: "AssociateCustomDomain",
			body: map[string]any{
				"ServiceArn": "arn:aws:apprunner:us-east-1:000000000000:service/notexist",
				"DomainName": "x.com",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestCustomDomainDescribeAndDisassociate(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	doRequest(t, h, "AssociateCustomDomain", map[string]any{"ServiceArn": svcArn, "DomainName": "example.com"})
	doRequest(t, h, "AssociateCustomDomain", map[string]any{"ServiceArn": svcArn, "DomainName": "sub.example.com"})

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "describe returns 2 domains",
			action:   "DescribeCustomDomains",
			body:     map[string]any{"ServiceArn": svcArn},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				domains := resp["CustomDomains"].([]any)
				assert.Len(t, domains, 2)
				assert.NotEmpty(t, resp["ServiceArn"])
				assert.NotEmpty(t, resp["DNSTarget"])
			},
		},
		{
			name:     "describe missing ServiceArn returns 400",
			action:   "DescribeCustomDomains",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "describe unknown service returns 400",
			action:   "DescribeCustomDomains",
			body:     map[string]any{"ServiceArn": "arn:aws:apprunner:us-east-1:000000000000:service/notexist"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "disassociate removes domain",
			action:   "DisassociateCustomDomain",
			body:     map[string]any{"ServiceArn": svcArn, "DomainName": "example.com"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				cd := resp["CustomDomain"].(map[string]any)
				assert.Equal(t, "example.com", cd["DomainName"])
			},
		},
		{
			name:     "disassociate missing ServiceArn returns 400",
			action:   "DisassociateCustomDomain",
			body:     map[string]any{"DomainName": "x.com"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "disassociate missing DomainName returns 400",
			action:   "DisassociateCustomDomain",
			body:     map[string]any{"ServiceArn": svcArn},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAssociateCustomDomainDNSTargetIsServiceURL verifies that
// AssociateCustomDomain returns DNSTarget equal to the service URL (e.g.
// <id>.<region>.awsapprunner.com), not the custom domain being associated.
func TestAssociateCustomDomainDNSTargetIsServiceURL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	rec := doRequest(t, h, "AssociateCustomDomain", map[string]any{
		"ServiceArn": svcArn,
		"DomainName": "myapp.example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	dnsTarget, ok := resp["DNSTarget"].(string)
	require.True(t, ok, "AssociateCustomDomain response must include DNSTarget string")
	assert.True(t, strings.HasSuffix(dnsTarget, ".awsapprunner.com"),
		"DNSTarget must be the service URL ending in .awsapprunner.com, got: %s", dnsTarget)
	assert.NotEqual(t, "myapp.example.com", dnsTarget,
		"DNSTarget must not be the custom domain name")
}

// TestDisassociateCustomDomainDNSTargetIsServiceURL verifies that
// DisassociateCustomDomain returns DNSTarget equal to the service URL, not the
// custom domain being removed.
func TestDisassociateCustomDomainDNSTargetIsServiceURL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	doRequest(t, h, "AssociateCustomDomain", map[string]any{
		"ServiceArn": svcArn,
		"DomainName": "myapp.example.com",
	})

	rec := doRequest(t, h, "DisassociateCustomDomain", map[string]any{
		"ServiceArn": svcArn,
		"DomainName": "myapp.example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	dnsTarget, ok := resp["DNSTarget"].(string)
	require.True(t, ok, "DisassociateCustomDomain response must include DNSTarget string")
	assert.True(t, strings.HasSuffix(dnsTarget, ".awsapprunner.com"),
		"DNSTarget must be the service URL ending in .awsapprunner.com, got: %s", dnsTarget)
	assert.NotEqual(t, "myapp.example.com", dnsTarget,
		"DNSTarget must not be the custom domain name")
}
