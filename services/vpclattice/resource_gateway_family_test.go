package vpclattice_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

// TestNewResourceFamiliesSurviveSnapshotRestore verifies the four new
// store.Table-backed resource families (resourceGateways,
// resourceConfigurations, snras, domainVerifications) round-trip through
// Snapshot -> Restore, since they're registered additively via the same
// generic registry every other table already uses (gopherstack-lx2k).
func TestNewResourceFamiliesSurviveSnapshotRestore(t *testing.T) {
	t.Parallel()

	b := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	ctx := t.Context()

	gw, err := b.CreateResourceGateway(ctx, "gw-persist", "vpc-1", "", "", 0, nil, nil, nil)
	require.NoError(t, err)

	def := &vpclattice.ResourceConfigurationDefinition{
		Kind:     "arnResource",
		ArnValue: "arn:aws:rds:us-east-1:000000000000:db:mydb",
	}

	rc, err := b.CreateResourceConfiguration(ctx, "rc-persist", "ARN", "", "", "", false, nil, def, nil)
	require.NoError(t, err)

	sn, err := b.CreateServiceNetwork(ctx, "sn-persist", "", nil)
	require.NoError(t, err)

	snra, err := b.CreateServiceNetworkResourceAssociation(ctx, sn.ID, rc.ID, false, nil)
	require.NoError(t, err)

	dv, err := b.StartDomainVerification(ctx, "persist.example.com", nil)
	require.NoError(t, err)

	snap := b.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(ctx, snap))

	gotGW, err := fresh.GetResourceGateway(gw.ID)
	require.NoError(t, err)
	assert.Equal(t, gw.ARN, gotGW.ARN)

	gotRC, err := fresh.GetResourceConfiguration(rc.ID)
	require.NoError(t, err)
	assert.Equal(t, rc.ARN, gotRC.ARN)

	gotSNRA, err := fresh.GetServiceNetworkResourceAssociation(snra.ID)
	require.NoError(t, err)
	assert.Equal(t, snra.ARN, gotSNRA.ARN)

	gotDV, err := fresh.GetDomainVerification(dv.ID)
	require.NoError(t, err)
	assert.Equal(t, dv.ARN, gotDV.ARN)
}

// TestResourceGatewayCRUD exercises the full ResourceGateway lifecycle
// (gopherstack-lx2k).
func TestResourceGatewayCRUD(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/resourcegateways", map[string]any{
		"name":             "gw1",
		"vpcIdentifier":    "vpc-1",
		"securityGroupIds": []string{"sg-1"},
		"subnetIds":        []string{"subnet-1", "subnet-2"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := parseBody(t, rec)
	assert.Equal(t, "ACTIVE", created["status"])
	gwID, _ := created["id"].(string)
	require.NotEmpty(t, gwID)
	assert.Regexp(t, `^arn:aws:vpc-lattice:us-east-1:000000000000:resourcegateway/rgw-`, created["arn"])

	rec = doRequest(t, h, http.MethodGet, "/resourcegateways/"+gwID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	got := parseBody(t, rec)
	assert.Equal(t, "vpc-1", got["vpcId"])
	assert.Equal(t, []any{"subnet-1", "subnet-2"}, got["subnetIds"])

	rec = doRequest(t, h, http.MethodPatch, "/resourcegateways/"+gwID, map[string]any{
		"securityGroupIds": []string{"sg-2", "sg-3"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	updated := parseBody(t, rec)
	assert.Equal(t, []any{"sg-2", "sg-3"}, updated["securityGroupIds"])

	rec = doRequest(t, h, http.MethodGet, "/resourcegateways", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)

	rec = doRequest(t, h, http.MethodDelete, "/resourcegateways/"+gwID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/resourcegateways/"+gwID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestResourceGatewayDeleteBlockedByResourceConfiguration verifies real
// AWS's DeleteResourceGateway ConflictException when a resource
// configuration still references the gateway.
func TestResourceGatewayDeleteBlockedByResourceConfiguration(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	gwRec := doRequest(t, h, http.MethodPost, "/resourcegateways", map[string]any{"name": "gw-blocked"})
	require.Equal(t, http.StatusCreated, gwRec.Code)
	gwID, _ := parseBody(t, gwRec)["id"].(string)

	rcRec := doRequest(t, h, http.MethodPost, "/resourceconfigurations", map[string]any{
		"name":                      "rc-blocked",
		"type":                      "ARN",
		"resourceGatewayIdentifier": gwID,
		"resourceConfigurationDefinition": map[string]any{
			"arnResource": map[string]any{"arn": "arn:aws:rds:us-east-1:000000000000:db:mydb"},
		},
	})
	require.Equal(t, http.StatusCreated, rcRec.Code)

	rec := doRequest(t, h, http.MethodDelete, "/resourcegateways/"+gwID, nil)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

// TestResourceConfigurationCRUD exercises the full ResourceConfiguration
// lifecycle, including the arnResource/dnsResource/ipResource definition
// union round-tripping (gopherstack-lx2k).
func TestResourceConfigurationCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		definition map[string]any
		name       string
	}{
		{
			name: "arn resource",
			definition: map[string]any{
				"arnResource": map[string]any{"arn": "arn:aws:rds:us-east-1:000000000000:db:mydb"},
			},
		},
		{
			name: "dns resource",
			definition: map[string]any{
				"dnsResource": map[string]any{"domainName": "example.com", "ipAddressType": "IPV4"},
			},
		},
		{
			name: "ip resource",
			definition: map[string]any{
				"ipResource": map[string]any{"ipAddress": "10.0.0.5"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/resourceconfigurations", map[string]any{
				"name":                            "rc-" + tt.name[:3],
				"type":                            "ARN",
				"resourceConfigurationDefinition": tt.definition,
			})
			require.Equal(t, http.StatusCreated, rec.Code)
			created := parseBody(t, rec)
			assert.Equal(t, "ACTIVE", created["status"])
			assert.Equal(t, tt.definition, created["resourceConfigurationDefinition"])

			id, _ := created["id"].(string)
			require.NotEmpty(t, id)

			rec = doRequest(t, h, http.MethodGet, "/resourceconfigurations/"+id, nil)
			require.Equal(t, http.StatusOK, rec.Code)
			got := parseBody(t, rec)
			assert.Equal(t, tt.definition, got["resourceConfigurationDefinition"])

			rec = doRequest(t, h, http.MethodDelete, "/resourceconfigurations/"+id, nil)
			assert.Equal(t, http.StatusNoContent, rec.Code)
		})
	}
}

// TestResourceConfigurationChildInheritsGatewayFromGroup verifies that a
// CHILD resource configuration inherits its ResourceGatewayId from its
// GROUP parent, matching CreateResourceConfigurationInput's documented
// behavior.
func TestResourceConfigurationChildInheritsGatewayFromGroup(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	gwRec := doRequest(t, h, http.MethodPost, "/resourcegateways", map[string]any{"name": "gw-parent"})
	require.Equal(t, http.StatusCreated, gwRec.Code)
	gwID, _ := parseBody(t, gwRec)["id"].(string)

	groupRec := doRequest(t, h, http.MethodPost, "/resourceconfigurations", map[string]any{
		"name":                      "rc-group",
		"type":                      "GROUP",
		"resourceGatewayIdentifier": gwID,
	})
	require.Equal(t, http.StatusCreated, groupRec.Code)
	groupID, _ := parseBody(t, groupRec)["id"].(string)

	childRec := doRequest(t, h, http.MethodPost, "/resourceconfigurations", map[string]any{
		"name":                                 "rc-child",
		"type":                                 "CHILD",
		"resourceConfigurationGroupIdentifier": groupID,
	})
	require.Equal(t, http.StatusCreated, childRec.Code)
	child := parseBody(t, childRec)
	assert.Equal(t, gwID, child["resourceGatewayId"])
	assert.Equal(t, groupID, child["resourceConfigurationGroupId"])
}

// TestServiceNetworkResourceAssociationCRUD exercises the full
// ServiceNetworkResourceAssociation lifecycle (gopherstack-lx2k).
func TestServiceNetworkResourceAssociationCRUD(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	snRec := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "sn-snra"})
	require.Equal(t, http.StatusCreated, snRec.Code)
	snID, _ := parseBody(t, snRec)["id"].(string)

	rcRec := doRequest(t, h, http.MethodPost, "/resourceconfigurations", map[string]any{
		"name": "rc-snra",
		"type": "ARN",
		"resourceConfigurationDefinition": map[string]any{
			"arnResource": map[string]any{"arn": "arn:aws:rds:us-east-1:000000000000:db:mydb"},
		},
	})
	require.Equal(t, http.StatusCreated, rcRec.Code)
	rcID, _ := parseBody(t, rcRec)["id"].(string)

	rec := doRequest(t, h, http.MethodPost, "/servicenetworkresourceassociations", map[string]any{
		"serviceNetworkIdentifier":        snID,
		"resourceConfigurationIdentifier": rcID,
		"privateDnsEnabled":               true,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := parseBody(t, rec)
	assert.Equal(t, "ACTIVE", created["status"])
	assert.Equal(t, true, created["privateDnsEnabled"])
	assocID, _ := created["id"].(string)
	require.NotEmpty(t, assocID)

	// deleting the resource configuration while an association exists must
	// be rejected (matches the DeleteResourceConfiguration cascade rules
	// established for Service/ServiceNetwork).
	rec = doRequest(t, h, http.MethodDelete, "/resourceconfigurations/"+rcID, nil)
	assert.Equal(t, http.StatusConflict, rec.Code)

	// deleting the service network while an SNRA references it must also be
	// rejected.
	rec = doRequest(t, h, http.MethodDelete, "/servicenetworks/"+snID, nil)
	assert.Equal(t, http.StatusConflict, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/servicenetworkresourceassociations/"+assocID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/servicenetworkresourceassociations", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)

	rec = doRequest(t, h, http.MethodDelete, "/servicenetworkresourceassociations/"+assocID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/servicenetworkresourceassociations/"+assocID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestDomainVerificationLifecycle exercises Start/Get/List/Delete and
// verifies that a domain verification never spontaneously reaches VERIFIED
// -- this backend has no DNS to observe, so it must honestly stay PENDING
// (see storedDomainVerification's doc comment).
func TestDomainVerificationLifecycle(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/domainverifications", map[string]any{
		"domainName": "example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	created := parseBody(t, rec)
	assert.Equal(t, "PENDING", created["status"])
	assert.Equal(t, "example.com", created["domainName"])
	id, _ := created["id"].(string)
	require.NotEmpty(t, id)

	rec = doRequest(t, h, http.MethodGet, "/domainverifications/"+id, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	got := parseBody(t, rec)
	assert.Equal(t, "PENDING", got["status"], "no DNS observation path exists, so status can never advance")

	rec = doRequest(t, h, http.MethodPost, "/domainverifications", map[string]any{
		"domainName": "example.com",
	})
	assert.Equal(t, http.StatusConflict, rec.Code, "duplicate domain verification must be rejected")

	rec = doRequest(t, h, http.MethodGet, "/domainverifications", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Len(t, items, 1)

	rec = doRequest(t, h, http.MethodDelete, "/domainverifications/"+id, nil)
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/domainverifications/"+id, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestResourceEndpointAssociationFamiliesAlwaysEmpty documents that
// ResourceEndpointAssociation/ServiceNetworkVpcEndpointAssociation lists are
// always empty in this backend: both are populated in real AWS exclusively
// by EC2 CreateVpcEndpoint, which this backend doesn't model (gap, see
// PARITY.md).
func TestResourceEndpointAssociationFamiliesAlwaysEmpty(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/resourceendpointassociations", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	list := parseBody(t, rec)
	items, _ := list["items"].([]any)
	assert.Empty(t, items)

	rec = doRequest(t, h, http.MethodDelete, "/resourceendpointassociations/reassoc-notexist", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/servicenetworkvpcendpointassociations", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	list = parseBody(t, rec)
	items, _ = list["items"].([]any)
	assert.Empty(t, items)
}

// TestPolicyOrphanFixedByARNNormalization is a regression test for the
// gopherstack-lx2k finding: PutAuthPolicy/PutResourcePolicy previously keyed
// their map by whatever raw identifier form the caller used (ID or ARN),
// while DeleteService/DeleteServiceNetwork's cascade delete always removes
// by ARN. A Put using the short ID left the entry permanently orphaned once
// the parent resource was deleted. Verified here via Snapshot: after
// deleting the parent, both maps must be empty, not carrying a stale
// short-ID-keyed entry forever.
func TestPolicyOrphanFixedByARNNormalization(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	svcRec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-orphan"})
	require.Equal(t, http.StatusCreated, svcRec.Code)
	svcID, _ := parseBody(t, svcRec)["id"].(string)

	putRec := doRequest(t, h, http.MethodPut, "/authpolicy/"+svcID,
		map[string]any{"policy": `{"Version":"2012-10-17"}`})
	require.Equal(t, http.StatusOK, putRec.Code)

	putRPRec := doRequest(t, h, http.MethodPut, "/resourcepolicy/"+svcID,
		map[string]any{"policy": `{"Version":"2012-10-17"}`})
	require.Equal(t, http.StatusOK, putRPRec.Code)

	delRec := doRequest(t, h, http.MethodDelete, "/services/"+svcID, nil)
	require.Equal(t, http.StatusOK, delRec.Code)

	getRec := doRequest(t, h, http.MethodGet, "/authpolicy/"+svcID, nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code, "auth policy must not survive its parent service's deletion")

	getRPRec := doRequest(t, h, http.MethodGet, "/resourcepolicy/"+svcID, nil)
	assert.Equal(
		t, http.StatusNotFound, getRPRec.Code, "resource policy must not survive its parent service's deletion",
	)
}

// TestServiceNetworkVpcAssociationDnsOptionsRoundTrips verifies
// CreateServiceNetworkVpcAssociation's dnsOptions field, previously accepted
// on the wire and silently dropped, now round-trips through Get.
func TestServiceNetworkVpcAssociationDnsOptionsRoundTrips(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	snRec := doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "sn-dns"})
	require.Equal(t, http.StatusCreated, snRec.Code)
	snID, _ := parseBody(t, snRec)["id"].(string)

	rec := doRequest(t, h, http.MethodPost, "/servicenetworkvpcassociations", map[string]any{
		"serviceNetworkIdentifier": snID,
		"vpcIdentifier":            "vpc-dns",
		"dnsOptions": map[string]any{
			"privateDnsPreference":       "SPECIFIED_DOMAINS_ONLY",
			"privateDnsSpecifiedDomains": []string{"a.example.com", "b.example.com"},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	created := parseBody(t, rec)
	dnsOpts, ok := created["dnsOptions"].(map[string]any)
	require.True(t, ok, "dnsOptions must round-trip in the create response, not be silently dropped")
	assert.Equal(t, "SPECIFIED_DOMAINS_ONLY", dnsOpts["privateDnsPreference"])
	assert.Equal(t, []any{"a.example.com", "b.example.com"}, dnsOpts["privateDnsSpecifiedDomains"])

	id, _ := created["id"].(string)
	rec = doRequest(t, h, http.MethodGet, "/servicenetworkvpcassociations/"+id, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	got := parseBody(t, rec)
	dnsOpts, ok = got["dnsOptions"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "SPECIFIED_DOMAINS_ONLY", dnsOpts["privateDnsPreference"])
}
