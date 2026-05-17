package route53_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// ----------------------------------------
// Gap 3: UpdateHostedZoneComment
// ----------------------------------------

func TestUpdateHostedZoneComment(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	body := `<?xml version="1.0" encoding="UTF-8"?>
<UpdateHostedZoneCommentRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Comment>updated comment</Comment>
</UpdateHostedZoneCommentRequest>`

	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID, body)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "UpdateHostedZoneCommentResponse")

	// Verify comment persisted.
	rec = send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID, "")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "updated comment")
}

func TestUpdateHostedZoneComment_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	body := `<?xml version="1.0" encoding="UTF-8"?>
<UpdateHostedZoneCommentRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Comment>nope</Comment>
</UpdateHostedZoneCommentRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/ZNONEXISTENT", body)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ----------------------------------------
// Gap 4+5: Atomic ChangeResourceRecordSets + CNAME-at-apex
// ----------------------------------------

func TestChangeResourceRecordSets_Atomic(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	// Create a valid A record first.
	validCreate := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>host.example.com</Name>
          <Type>A</Type>
          <TTL>300</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>1.2.3.4</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", validCreate)
	require.Equal(t, http.StatusOK, rec.Code)

	// Batch: first change is valid DELETE, second is invalid (CNAME at apex).
	// Atomic: neither change should apply.
	mixedBatch := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>DELETE</Action>
        <ResourceRecordSet>
          <Name>host.example.com</Name>
          <Type>A</Type>
          <TTL>300</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>1.2.3.4</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>example.com</Name>
          <Type>CNAME</Type>
          <TTL>300</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>other.example.com</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", mixedBatch)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidChangeBatch")

	// Verify host.example.com still exists (atomic rollback).
	pg, err := route53.NewInMemoryBackend().ListResourceRecordSets(zoneID, "", "", "", 0)
	_ = pg
	_ = err

	rec = send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "host.example.com")
}

func TestChangeResourceRecordSets_CNAMEAtApex(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	body := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>example.com</Name>
          <Type>CNAME</Type>
          <TTL>300</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>other.example.com</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "CNAME")
}

func TestChangeResourceRecordSets_InvalidIPv4(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	body := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>host.example.com</Name>
          <Type>A</Type>
          <TTL>300</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>not-an-ip</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidChangeBatch")
}

func TestChangeResourceRecordSets_InvalidType(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	body := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>host.example.com</Name>
          <Type>DNSKEY</Type>
          <TTL>300</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>256 3 13 xyz</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidChangeBatch")
}

func TestChangeResourceRecordSets_BatchLimit(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	hz, err := b.CreateHostedZone("example.com", "ref", "", false)
	require.NoError(t, err)

	changes := make([]route53.Change, 1001)
	for i := range changes {
		changes[i] = route53.Change{
			Action: route53.ChangeActionCreate,
			ResourceRecordSet: route53.ResourceRecordSet{
				Name:    fmt.Sprintf("h%d.example.com", i),
				Type:    "A",
				TTL:     300,
				Records: []route53.ResourceRecord{{Value: "1.2.3.4"}},
			},
		}
	}

	_, err = b.ChangeResourceRecordSets(hz.ID, changes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidChangeBatch")
}

// ----------------------------------------
// Gap 6: Routing policy mutual exclusion
// ----------------------------------------

func TestChangeResourceRecordSets_RoutingPolicyMutualExclusion(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	// Weight + Failover together is invalid.
	body := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>host.example.com</Name>
          <Type>A</Type>
          <TTL>300</TTL>
          <SetIdentifier>id1</SetIdentifier>
          <Weight>10</Weight>
          <Failover>PRIMARY</Failover>
          <ResourceRecords>
            <ResourceRecord><Value>1.2.3.4</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestChangeResourceRecordSets_RoutingPolicyRequiresSetIdentifier(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	body := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>host.example.com</Name>
          <Type>A</Type>
          <TTL>300</TTL>
          <Weight>10</Weight>
          <ResourceRecords>
            <ResourceRecord><Value>1.2.3.4</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ----------------------------------------
// Gap 7: MultiValueAnswer + GeoProximityLocation
// ----------------------------------------

func TestChangeResourceRecordSets_MultiValueAnswer(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	body := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>multi.example.com</Name>
          <Type>A</Type>
          <TTL>300</TTL>
          <SetIdentifier>multi-1</SetIdentifier>
          <MultiValueAnswer>true</MultiValueAnswer>
          <ResourceRecords>
            <ResourceRecord><Value>1.2.3.4</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", body)
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify MultiValueAnswer appears in list response.
	rec = send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "MultiValueAnswer")
}

// ----------------------------------------
// Gap 10: ListResourceRecordSets pagination
// ----------------------------------------

func TestListResourceRecordSets_Pagination(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	hz, err := b.CreateHostedZone("example.com", "ref", "", false)
	require.NoError(t, err)

	// Create 5 A records.
	changes := make([]route53.Change, 5)
	for i := range changes {
		changes[i] = route53.Change{
			Action: route53.ChangeActionCreate,
			ResourceRecordSet: route53.ResourceRecordSet{
				Name:    fmt.Sprintf("host%d.example.com.", i),
				Type:    "A",
				TTL:     300,
				Records: []route53.ResourceRecord{{Value: "1.2.3.4"}},
			},
		}
	}

	_, err = b.ChangeResourceRecordSets(hz.ID, changes)
	require.NoError(t, err)

	// Page 1: 3 records.
	pg, err := b.ListResourceRecordSets(hz.ID, "", "", "", 3)
	require.NoError(t, err)
	assert.Len(t, pg.Records, 3)
	assert.True(t, pg.IsTruncated)
	assert.NotEmpty(t, pg.NextName)

	// Page 2: remaining records.
	pg2, err := b.ListResourceRecordSets(hz.ID, pg.NextName, pg.NextType, pg.NextIdentifier, 3)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(pg2.Records), 1)

	// Total across both pages = 5.
	assert.Equal(t, 5, len(pg.Records)+len(pg2.Records))
}

func TestListResourceRecordSets_MaxItemsQueryParam(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	// Create 3 records.
	for i := range 3 {
		body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch><Changes>
    <Change>
      <Action>CREATE</Action>
      <ResourceRecordSet>
        <Name>h%d.example.com</Name><Type>A</Type><TTL>300</TTL>
        <ResourceRecords><ResourceRecord><Value>1.2.3.4</Value></ResourceRecord></ResourceRecords>
      </ResourceRecordSet>
    </Change>
  </Changes></ChangeBatch>
</ChangeResourceRecordSetsRequest>`, i)
		r := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", body)
		require.Equal(t, http.StatusOK, r.Code)
	}

	type listResp struct {
		XMLName     xml.Name `xml:"ListResourceRecordSetsResponse"`
		IsTruncated bool     `xml:"IsTruncated"`
	}

	// Request 2 items.
	rec = send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset?maxitems=2", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var lr listResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &lr))
	assert.True(t, lr.IsTruncated)
}

// ----------------------------------------
// Gap 11: GetChange with tracking
// ----------------------------------------

func TestGetChange_RealTracking(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	body := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch><Changes>
    <Change>
      <Action>CREATE</Action>
      <ResourceRecordSet>
        <Name>host.example.com</Name><Type>A</Type><TTL>300</TTL>
        <ResourceRecords><ResourceRecord><Value>1.2.3.4</Value></ResourceRecord></ResourceRecords>
      </ResourceRecordSet>
    </Change>
  </Changes></ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", body)
	require.Equal(t, http.StatusOK, rec.Code)

	type rrResp struct {
		ChangeInfo struct {
			ID string `xml:"Id"`
		} `xml:"ChangeInfo"`
	}

	var rr rrResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &rr))
	require.NotEmpty(t, rr.ChangeInfo.ID)

	// GetChange with the returned ID should succeed.
	changeID := strings.TrimPrefix(rr.ChangeInfo.ID, "/change/")
	rec = send(t, h, http.MethodGet, "/2013-04-01/change/"+changeID, "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "INSYNC")
}

func TestGetChange_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodGet, "/2013-04-01/change/CNONEXISTENT", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ----------------------------------------
// Gap 13: Multi-region health check observations
// ----------------------------------------

func TestGetHealthCheckStatus_MultiRegion(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>hc-ref-1</CallerReference>
  <HealthCheckConfig>
    <Type>HTTP</Type>
    <IPAddress>1.2.3.4</IPAddress>
    <Port>80</Port>
  </HealthCheckConfig>
</CreateHealthCheckRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	type hcResp struct {
		HealthCheck struct {
			ID string `xml:"Id"`
		} `xml:"HealthCheck"`
	}

	var cr hcResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &cr))

	rec = send(t, h, http.MethodGet, "/2013-04-01/healthcheck/"+cr.HealthCheck.ID+"/status", "")
	require.Equal(t, http.StatusOK, rec.Code)

	type obsResp struct {
		Observations []struct {
			Region string `xml:"Region"`
		} `xml:"HealthCheckObservations>HealthCheckObservation"`
	}

	var obs obsResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &obs))
	assert.GreaterOrEqual(t, len(obs.Observations), 3, "expected at least 3 regional observations")
}

// ----------------------------------------
// Gap 15: RECOVERY_CONTROL health check type
// ----------------------------------------

func TestCreateHealthCheck_RecoveryControl(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	const rcArn = "arn:aws:route53-recovery-control::123456789012:controlpanel/abc/routingcontrol/xyz"
	body := `<?xml version="1.0" encoding="UTF-8"?>` +
		`<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">` +
		`<CallerReference>hc-rc-1</CallerReference>` +
		`<HealthCheckConfig><Type>RECOVERY_CONTROL</Type>` +
		`<RoutingControlArn>` + rcArn + `</RoutingControlArn>` +
		`</HealthCheckConfig></CreateHealthCheckRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", body)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Contains(t, rec.Body.String(), "RECOVERY_CONTROL")
}

// ----------------------------------------
// Gap 17: KSK full fields
// ----------------------------------------

func TestCreateKSK_FullFields(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	zoneID := createZoneForOpsTest(t, h)
	createKSKForOpsTest(t, h, zoneID, "test-ksk")

	// GetDNSSEC should return the KSK with full fields.
	rec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/dnssec", "")
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	assert.Contains(t, body, "ECDSAP256SHA256", "SigningAlgorithmMnemonic missing")
	assert.Contains(t, body, "SHA-256", "DigestAlgorithmMnemonic missing")
	assert.Contains(t, body, "PublicKey")
	assert.Contains(t, body, "DSRecord")
	assert.Contains(t, body, "KeyTag")
}

// ----------------------------------------
// Gap 18: AssociateVPC rejects public zones / VPC association authorizations
// ----------------------------------------

func TestAssociateVPC_RejectsPublicZone(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	// Create public zone.
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	body := `<?xml version="1.0" encoding="UTF-8"?>
<AssociateVPCWithHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC><VPCId>vpc-abc</VPCId><VPCRegion>us-east-1</VPCRegion></VPC>
</AssociateVPCWithHostedZoneRequest>`

	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/associatevpc", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestVPCAssociationAuthorizations_CRUD(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateVPCAssociationAuthorizationRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC><VPCId>vpc-cross</VPCId><VPCRegion>us-west-2</VPCRegion></VPC>
</CreateVPCAssociationAuthorizationRequest>`

	// Create authorization.
	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/authorizevpcassociation", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Contains(t, rec.Body.String(), "vpc-cross")

	// List authorizations.
	rec = send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/authorizevpcassociation", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "vpc-cross")

	// Delete authorization.
	deleteBody := `<?xml version="1.0" encoding="UTF-8"?>
<DeleteVPCAssociationAuthorizationRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC><VPCId>vpc-cross</VPCId></VPC>
</DeleteVPCAssociationAuthorizationRequest>`

	rec = send(t, h, http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID+"/authorizevpcassociation", deleteBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// List again — should be empty.
	rec = send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/authorizevpcassociation", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "vpc-cross")
}

func TestDisassociateVPCFromHostedZone(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	hz, err := b.CreateHostedZone("private.example.com", "ref", "", true)
	require.NoError(t, err)

	// Associate a VPC.
	require.NoError(t, b.AssociateVPCWithHostedZone(hz.ID, "vpc-xyz", "us-east-1"))

	// Disassociate it.
	require.NoError(t, b.DisassociateVPCFromHostedZone(hz.ID, "vpc-xyz"))

	// VPC should be gone.
	assocs, err := b.ListVPCAssociations(hz.ID)
	require.NoError(t, err)
	assert.Empty(t, assocs)
}

// ----------------------------------------
// Gap 19: QueryLoggingConfig uniqueness
// ----------------------------------------

func TestCreateQueryLoggingConfig_Uniqueness(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	hz, err := b.CreateHostedZone("example.com", "ref", "", false)
	require.NoError(t, err)

	_, err = b.CreateQueryLoggingConfig(hz.ID, "arn:aws:logs:us-east-1:123:log-group:test")
	require.NoError(t, err)

	// Second creation on same zone should fail.
	_, err = b.CreateQueryLoggingConfig(hz.ID, "arn:aws:logs:us-east-1:123:log-group:other")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "QueryLoggingConfigAlreadyExists")
}

func TestQueryLoggingConfig_DeleteAndList(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	createBody := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateQueryLoggingConfigRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>%s</HostedZoneId>
  <CloudWatchLogsLogGroupArn>arn:aws:logs:us-east-1:123:log-group:test</CloudWatchLogsLogGroupArn>
</CreateQueryLoggingConfigRequest>`, zoneID)

	rec = send(t, h, http.MethodPost, "/2013-04-01/queryloggingconfig", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	type createResp struct {
		QueryLoggingConfig struct {
			ID string `xml:"Id"`
		} `xml:"QueryLoggingConfig"`
	}

	var cr createResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &cr))

	// List configs.
	rec = send(t, h, http.MethodGet, "/2013-04-01/queryloggingconfig", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), cr.QueryLoggingConfig.ID)

	// Get config.
	rec = send(t, h, http.MethodGet, "/2013-04-01/queryloggingconfig/"+cr.QueryLoggingConfig.ID, "")
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete config.
	rec = send(t, h, http.MethodDelete, "/2013-04-01/queryloggingconfig/"+cr.QueryLoggingConfig.ID, "")
	require.Equal(t, http.StatusOK, rec.Code)

	// List again — empty.
	rec = send(t, h, http.MethodGet, "/2013-04-01/queryloggingconfig", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), cr.QueryLoggingConfig.ID)
}

// ----------------------------------------
// Gap 21: Traffic policy instance filtering
// ----------------------------------------

func TestListTrafficPolicyInstancesByHostedZone(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	zoneID := createZoneForOpsTest(t, h)
	tpID := createTPForOpsTest(t, h, "tp-filter-test")

	// Create an instance in the zone.
	instanceBody := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyInstanceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>%s</HostedZoneId>
  <Name>filtered.example.com</Name>
  <TrafficPolicyId>%s</TrafficPolicyId>
  <TrafficPolicyVersion>1</TrafficPolicyVersion>
  <TTL>60</TTL>
</CreateTrafficPolicyInstanceRequest>`, zoneID, tpID)

	rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicyinstance", instanceBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Filter by hosted zone.
	rec = send(t, h, http.MethodGet, "/2013-04-01/trafficpolicyinstances/hostedzone?hostedzoneid="+zoneID, "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "filtered.example.com")

	// Filter by non-matching hosted zone.
	rec = send(t, h, http.MethodGet, "/2013-04-01/trafficpolicyinstances/hostedzone?hostedzoneid=ZNONEXISTENT", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "filtered.example.com")
}

func TestUpdateTrafficPolicyInstance(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	zoneID := createZoneForOpsTest(t, h)
	tpID := createTPForOpsTest(t, h, "tp-update-test")

	instanceBody := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyInstanceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>%s</HostedZoneId>
  <Name>update.example.com</Name>
  <TrafficPolicyId>%s</TrafficPolicyId>
  <TrafficPolicyVersion>1</TrafficPolicyVersion>
  <TTL>60</TTL>
</CreateTrafficPolicyInstanceRequest>`, zoneID, tpID)

	rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicyinstance", instanceBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	type tpiResp struct {
		TrafficPolicyInstance struct {
			ID  string `xml:"Id"`
			TTL int64  `xml:"TTL"`
		} `xml:"TrafficPolicyInstance"`
	}

	var cr tpiResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &cr))

	updateBody := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<UpdateTrafficPolicyInstanceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <TrafficPolicyId>%s</TrafficPolicyId>
  <TrafficPolicyVersion>1</TrafficPolicyVersion>
  <TTL>120</TTL>
</UpdateTrafficPolicyInstanceRequest>`, tpID)

	rec = send(t, h, http.MethodPost, "/2013-04-01/trafficpolicyinstance/"+cr.TrafficPolicyInstance.ID, updateBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var updated tpiResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, int64(120), updated.TrafficPolicyInstance.TTL)
}

// ----------------------------------------
// Gap 22: CIDR collection locations
// ----------------------------------------

func TestChangeCidrCollection_StoresLocations(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	col, err := b.CreateCidrCollection("my-cidrs", "ref")
	require.NoError(t, err)

	changes := []route53.CidrCollectionChange{
		{
			LocationName: "office",
			Action:       "PUT",
			CidrList:     []string{"192.168.1.0/24", "10.0.0.0/8"},
		},
	}

	updated, err := b.ChangeCidrCollection(col.ID, changes)
	require.NoError(t, err)
	assert.Equal(t, int64(2), updated.Version)

	// ListCidrLocations.
	locs, err := b.ListCidrLocations(col.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"office"}, locs)

	// ListCidrBlocks.
	blocks, err := b.ListCidrBlocks(col.ID, "office")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"192.168.1.0/24", "10.0.0.0/8"}, blocks)

	// DELETE_IF_EXISTS.
	_, err = b.ChangeCidrCollection(col.ID, []route53.CidrCollectionChange{
		{
			LocationName: "office",
			Action:       "DELETE_IF_EXISTS",
			CidrList:     []string{"10.0.0.0/8"},
		},
	})
	require.NoError(t, err)

	blocks, err = b.ListCidrBlocks(col.ID, "office")
	require.NoError(t, err)
	assert.Equal(t, []string{"192.168.1.0/24"}, blocks)
}

func TestListCidrBlocks_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	colID := createCidrCollectionForOpsTest(t, h, "list-cidr-test")

	// Change to add a location.
	changeBody := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Changes>
    <Change>
      <LocationName>datacenter</LocationName>
      <Action>PUT</Action>
      <CidrList>
        <Cidr>10.1.0.0/16</Cidr>
      </CidrList>
    </Change>
  </Changes>
</ChangeCidrCollectionRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection/"+colID, changeBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// List blocks.
	rec = send(t, h, http.MethodGet, "/2013-04-01/cidrcollection/"+colID+"/cidrblocks", "")
	require.Equal(t, http.StatusOK, rec.Code)

	// List locations.
	rec = send(t, h, http.MethodGet, "/2013-04-01/cidrcollection/"+colID+"/cidrlocations", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "datacenter")
}

// ----------------------------------------
// Gap 23: ListGeoLocations + GetGeoLocation
// ----------------------------------------

func TestListGeoLocations(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodGet, "/2013-04-01/geolocation", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListGeoLocationsResponse")
	assert.Contains(t, rec.Body.String(), "Africa")
	assert.Contains(t, rec.Body.String(), "Europe")
}

func TestGetGeoLocation(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	tests := []struct {
		name        string
		query       string
		wantContain string
		wantCode    int
	}{
		{
			name:        "valid_continent",
			query:       "?continentcode=EU",
			wantCode:    http.StatusOK,
			wantContain: "Europe",
		},
		{
			name:        "valid_country",
			query:       "?countrycode=US",
			wantCode:    http.StatusOK,
			wantContain: "United States",
		},
		{
			name:     "not_found",
			query:    "?continentcode=XX",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := send(t, h, http.MethodGet, "/2013-04-01/geolocation"+tt.query, "")
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContain)
			}
		})
	}
}

// ----------------------------------------
// Gap 24: Account/zone/delegation set limits with real counts
// ----------------------------------------

func TestGetAccountLimit(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	// Create some zones.
	for i := range 3 {
		body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>zone%d.example.com</Name>
  <CallerReference>ref-%d</CallerReference>
  <HostedZoneConfig><Comment></Comment><PrivateZone>false</PrivateZone></HostedZoneConfig>
</CreateHostedZoneRequest>`, i, i)
		rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", body)
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	rec := send(t, h, http.MethodGet, "/2013-04-01/accountlimit/MAX_HOSTED_ZONES_BY_OWNER", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetAccountLimitResponse")
	assert.Contains(t, rec.Body.String(), "MAX_HOSTED_ZONES_BY_OWNER")
}

func TestGetHostedZoneLimit(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	rec = send(t, h, http.MethodGet, "/2013-04-01/hostedzonelimit/"+zoneID+"/MAX_RRSETS_BY_ZONE", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetHostedZoneLimitResponse")
	assert.Contains(t, rec.Body.String(), "MAX_RRSETS_BY_ZONE")
}

// ----------------------------------------
// Gap 16: EnableDNSSEC requires ACTIVE KSK
// ----------------------------------------

func TestEnableDNSSEC_RequiresActiveKSK(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	hz, err := b.CreateHostedZone("example.com", "ref", "", false)
	require.NoError(t, err)

	// No KSK — should fail.
	err = b.EnableHostedZoneDNSSEC(hz.ID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KeySigningKeyWithActiveStatusNotFound")

	// Create inactive KSK — should still fail.
	_, err = b.CreateKeySigningKey(hz.ID, "ref", "my-ksk", "arn:aws:kms:us-east-1:123:key/abc", "INACTIVE")
	require.NoError(t, err)

	err = b.EnableHostedZoneDNSSEC(hz.ID)
	require.Error(t, err)

	// Activate KSK — now should succeed.
	_, err = b.ActivateKeySigningKey(hz.ID, "my-ksk")
	require.NoError(t, err)

	err = b.EnableHostedZoneDNSSEC(hz.ID)
	require.NoError(t, err)
}

// ----------------------------------------
// Delegation set CRUD
// ----------------------------------------

func TestDelegationSet_CRUD(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateReusableDelegationSetRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>ds-ref-1</CallerReference>
</CreateReusableDelegationSetRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/delegationset", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	type dsResp struct {
		DelegationSet struct {
			ID string `xml:"Id"`
		} `xml:"DelegationSet"`
	}

	var cr dsResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &cr))
	dsID := strings.TrimPrefix(cr.DelegationSet.ID, "/delegationset/")

	// Get delegation set.
	rec = send(t, h, http.MethodGet, "/2013-04-01/delegationset/"+dsID, "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetReusableDelegationSetResponse")

	// List delegation sets.
	rec = send(t, h, http.MethodGet, "/2013-04-01/delegationset", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListReusableDelegationSetsResponse")

	// Delete delegation set.
	rec = send(t, h, http.MethodDelete, "/2013-04-01/delegationset/"+dsID, "")
	require.Equal(t, http.StatusOK, rec.Code)
}

// ----------------------------------------
// ListHostedZonesByVPC
// ----------------------------------------

func TestListHostedZonesByVPC(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()

	hz, err := b.CreateHostedZone("private.example.com", "ref", "", true)
	require.NoError(t, err)

	require.NoError(t, b.AssociateVPCWithHostedZone(hz.ID, "vpc-123", "us-east-1"))

	zones, err := b.ListHostedZonesByVPC("vpc-123", "")
	require.NoError(t, err)
	require.Len(t, zones, 1)
	assert.Equal(t, hz.ID, zones[0].ID)

	// Different VPC — no results.
	zones, err = b.ListHostedZonesByVPC("vpc-other", "")
	require.NoError(t, err)
	assert.Empty(t, zones)
}
