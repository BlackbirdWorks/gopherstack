package apprunner_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_ServiceOutputInstanceConfiguration verifies DescribeService returns
// InstanceConfiguration with Cpu and Memory populated from backend storage.
func TestParity_ServiceOutputInstanceConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateService", map[string]any{
		"ServiceName": "ic-svc",
		"InstanceConfiguration": map[string]any{
			"Cpu":    "2 vCPU",
			"Memory": "4 GB",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	svcArn := createResp["Service"].(map[string]any)["ServiceArn"].(string)

	rec = doRequest(t, h, "DescribeService", map[string]any{"ServiceArn": svcArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	svc := descResp["Service"].(map[string]any)

	ic, ok := svc["InstanceConfiguration"].(map[string]any)
	require.True(t, ok, "DescribeService must include InstanceConfiguration")
	assert.Equal(t, "2 vCPU", ic["Cpu"])
	assert.Equal(t, "4 GB", ic["Memory"])
}

// TestParity_ServiceOutputSourceConfiguration verifies DescribeService returns
// SourceConfiguration with ImageRepository.ImageIdentifier populated.
func TestParity_ServiceOutputSourceConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateService", map[string]any{
		"ServiceName": "sc-svc",
		"SourceConfiguration": map[string]any{
			"ImageRepository": map[string]any{
				"ImageIdentifier":     "public.ecr.aws/nginx/nginx:latest",
				"ImageRepositoryType": "ECR_PUBLIC",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	svcArn := createResp["Service"].(map[string]any)["ServiceArn"].(string)

	rec = doRequest(t, h, "DescribeService", map[string]any{"ServiceArn": svcArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	svc := descResp["Service"].(map[string]any)

	sc, ok := svc["SourceConfiguration"].(map[string]any)
	require.True(t, ok, "DescribeService must include SourceConfiguration")

	ir, ok := sc["ImageRepository"].(map[string]any)
	require.True(t, ok, "SourceConfiguration must include ImageRepository")
	assert.Equal(t, "public.ecr.aws/nginx/nginx:latest", ir["ImageIdentifier"])
}

// TestParity_AssociateCustomDomainDNSTargetIsServiceURL verifies that
// AssociateCustomDomain returns DNSTarget equal to the service URL (e.g.
// <id>.<region>.awsapprunner.com), not the custom domain being associated.
func TestParity_AssociateCustomDomainDNSTargetIsServiceURL(t *testing.T) {
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

// TestParity_DisassociateCustomDomainDNSTargetIsServiceURL verifies that
// DisassociateCustomDomain returns DNSTarget equal to the service URL, not the
// custom domain being removed.
func TestParity_DisassociateCustomDomainDNSTargetIsServiceURL(t *testing.T) {
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

// TestParity_DefaultInstanceConfigurationPresent verifies that services created
// without explicit InstanceConfiguration still have Cpu and Memory in the
// DescribeService response (defaults applied by backend).
func TestParity_DefaultInstanceConfigurationPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateService", map[string]any{
		"ServiceName": "default-ic-svc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	svcArn := createResp["Service"].(map[string]any)["ServiceArn"].(string)

	rec = doRequest(t, h, "DescribeService", map[string]any{"ServiceArn": svcArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	svc := descResp["Service"].(map[string]any)

	ic, ok := svc["InstanceConfiguration"].(map[string]any)
	require.True(t, ok, "DescribeService must include InstanceConfiguration even with defaults")
	assert.NotEmpty(t, ic["Cpu"], "default Cpu must be non-empty")
	assert.NotEmpty(t, ic["Memory"], "default Memory must be non-empty")
}
