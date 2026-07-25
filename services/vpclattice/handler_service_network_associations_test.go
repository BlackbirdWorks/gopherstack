package vpclattice_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSNSA_CRUD tests service network service associations.
func TestSNSA_CRUD(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// create prerequisite resources
	recSN := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "net1"})
	require.Equal(t, http.StatusCreated, recSN.Code)
	snID, _ := parseBody(t, recSN)["id"].(string)

	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc1"})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svcID, _ := parseBody(t, recSvc)["id"].(string)

	// create association
	rec := doRequest(t, h, http.MethodPost, "/servicenetworkserviceassociations", map[string]any{
		"serviceNetworkIdentifier": snID,
		"serviceIdentifier":        svcID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	assoc := parseBody(t, rec)
	assocID, _ := assoc["id"].(string)
	require.NotEmpty(t, assocID)
	assert.Equal(t, "ACTIVE", assoc["status"])

	// get
	rec = doRequest(t, h, http.MethodGet, "/servicenetworkserviceassociations/"+assocID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// list
	rec = doRequest(t, h, http.MethodGet, "/servicenetworkserviceassociations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)

	// duplicate association returns conflict
	rec = doRequest(t, h, http.MethodPost, "/servicenetworkserviceassociations", map[string]any{
		"serviceNetworkIdentifier": snID,
		"serviceIdentifier":        svcID,
	})
	assert.Equal(t, http.StatusConflict, rec.Code)

	// delete returns 202 per AWS spec
	rec = doRequest(t, h, http.MethodDelete, "/servicenetworkserviceassociations/"+assocID, nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)
}

// TestSNVA_CRUD tests service network VPC associations.
func TestSNVA_CRUD(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// create service network
	recSN := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "net2"})
	require.Equal(t, http.StatusCreated, recSN.Code)
	snID, _ := parseBody(t, recSN)["id"].(string)

	// create
	rec := doRequest(t, h, http.MethodPost, "/servicenetworkvpcassociations", map[string]any{
		"serviceNetworkIdentifier": snID,
		"vpcIdentifier":            "vpc-1234567890",
		"securityGroupIds":         []string{"sg-abcdef01"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	assoc := parseBody(t, rec)
	assocID, _ := assoc["id"].(string)
	assert.NotEmpty(t, assocID)
	assert.Equal(t, "ACTIVE", assoc["status"])

	// get
	rec = doRequest(t, h, http.MethodGet, "/servicenetworkvpcassociations/"+assocID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// update security groups
	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/servicenetworkvpcassociations/"+assocID,
		map[string]any{
			"securityGroupIds": []string{"sg-new"},
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// list
	rec = doRequest(t, h, http.MethodGet, "/servicenetworkvpcassociations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)

	// delete returns 202 per AWS spec
	rec = doRequest(t, h, http.MethodDelete, "/servicenetworkvpcassociations/"+assocID, nil)
	assert.Equal(t, http.StatusAccepted, rec.Code)
}

// TestSNSAIncludesCustomDomainNameAndDNSEntry verifies that
// ServiceNetworkServiceAssociation responses include "customDomainName" and
// "dnsEntry" when the underlying service has them set, matching the real
// CreateServiceNetworkServiceAssociationOutput/
// GetServiceNetworkServiceAssociationOutput shapes. The emulator previously
// captured these fields internally but never serialized them.
func TestSNSAIncludesCustomDomainNameAndDNSEntry(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{
		"name":             "svc-snsa-dns",
		"customDomainName": "example.com",
	})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svc := parseBody(t, recSvc)
	svcID, _ := svc["id"].(string)

	recSN := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "sn-snsa-dns"})
	require.Equal(t, http.StatusCreated, recSN.Code)
	snID, _ := parseBody(t, recSN)["id"].(string)

	rec := doRequest(t, h, http.MethodPost, "/servicenetworkserviceassociations", map[string]any{
		"serviceNetworkIdentifier": snID,
		"serviceIdentifier":        svcID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	assoc := parseBody(t, rec)
	assert.Equal(t, "example.com", assoc["customDomainName"])
	dnsEntry, ok := assoc["dnsEntry"].(map[string]any)
	require.True(t, ok, "dnsEntry must be present on CreateServiceNetworkServiceAssociation response")
	assert.NotEmpty(t, dnsEntry["domainName"])
	assert.NotEmpty(
		t,
		dnsEntry["hostedZoneId"],
		"dnsEntry.hostedZoneId must be populated, matching real AWS's DnsEntry shape",
	)

	assocID, _ := assoc["id"].(string)
	getRec := doRequest(t, h, http.MethodGet, "/servicenetworkserviceassociations/"+assocID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)
	got := parseBody(t, getRec)
	assert.Equal(t, "example.com", got["customDomainName"])
	gotDNSEntry, ok := got["dnsEntry"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, gotDNSEntry["hostedZoneId"])

	listRec := doRequest(t, h, http.MethodGet, "/servicenetworkserviceassociations", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	items, _ := parseBody(t, listRec)["items"].([]any)
	require.Len(t, items, 1)
	summary, _ := items[0].(map[string]any)
	assert.Equal(t, "example.com", summary["customDomainName"])
	assert.NotEmpty(t, summary["dnsEntry"])
}

// TestServiceIncludesHostedZoneIDInDNSEntry verifies that Service responses'
// dnsEntry includes "hostedZoneId" alongside "domainName", matching real
// AWS's DnsEntry shape (domainName + hostedZoneId). The emulator previously
// only populated domainName.
func TestServiceIncludesHostedZoneIDInDNSEntry(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-hosted-zone"})
	require.Equal(t, http.StatusCreated, rec.Code)
	svc := parseBody(t, rec)

	dnsEntry, ok := svc["dnsEntry"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, dnsEntry["domainName"])
	assert.NotEmpty(t, dnsEntry["hostedZoneId"])

	svcID, _ := svc["id"].(string)
	listRec := doRequest(t, h, http.MethodGet, "/services", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	items, _ := parseBody(t, listRec)["items"].([]any)
	require.Len(t, items, 1)
	summary, _ := items[0].(map[string]any)
	require.Equal(t, svcID, summary["id"])
	summaryDNSEntry, ok := summary["dnsEntry"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, summaryDNSEntry["hostedZoneId"])
}

// TestSNVAIncludesPrivateDNSEnabled verifies that a privateDnsEnabled flag
// set on CreateServiceNetworkVpcAssociation round-trips through
// Create/Get/List responses, matching real AWS's
// CreateServiceNetworkVpcAssociationOutput/
// GetServiceNetworkVpcAssociationOutput/
// ServiceNetworkVpcAssociationSummary shapes (all three carry
// privateDnsEnabled).
func TestSNVAIncludesPrivateDNSEnabled(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recSN := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "sn-private-dns"})
	require.Equal(t, http.StatusCreated, recSN.Code)
	snID, _ := parseBody(t, recSN)["id"].(string)

	rec := doRequest(t, h, http.MethodPost, "/servicenetworkvpcassociations", map[string]any{
		"serviceNetworkIdentifier": snID,
		"vpcIdentifier":            "vpc-private-dns",
		"privateDnsEnabled":        true,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	assoc := parseBody(t, rec)
	assert.Equal(t, true, assoc["privateDnsEnabled"])

	assocID, _ := assoc["id"].(string)
	getRec := doRequest(t, h, http.MethodGet, "/servicenetworkvpcassociations/"+assocID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.Equal(t, true, parseBody(t, getRec)["privateDnsEnabled"])

	listRec := doRequest(t, h, http.MethodGet, "/servicenetworkvpcassociations", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	items, _ := parseBody(t, listRec)["items"].([]any)
	require.Len(t, items, 1)
	summary, _ := items[0].(map[string]any)
	assert.Equal(t, true, summary["privateDnsEnabled"])
}
