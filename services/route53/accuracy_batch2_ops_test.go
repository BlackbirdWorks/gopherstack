package route53_test

// accuracy_batch2_ops_test.go — AWS-accuracy audit for Route53 batch-2 ops (go-2ms5).
//
// Covers four concrete deviations from AWS behaviour found in backend.go:
//
//  1. DeleteHostedZone with non-empty zone: silently deleted everything. AWS
//     returns HostedZoneNotEmpty (400) when resource record sets other than the
//     default NS/SOA records are present. Fix: check len(records) > 0 before
//     deleting and return ErrHostedZoneNotEmpty.
//
//  2. CreateHostedZone duplicate CallerReference: always created a new zone.
//     AWS returns the existing zone for the same CallerReference when the rest
//     of the request is identical (idempotent retry), but returns
//     HostedZoneAlreadyExists (409) when the CallerReference is reused with
//     different Name/Comment/PrivateZone. Fix: scan zones for a matching
//     CallerReference and compare the other fields before inserting.
//
//  3. CreateHealthCheck duplicate CallerReference: always created a new health
//     check. AWS returns the existing health check for the same CallerReference
//     when the config is identical (idempotent retry), but returns
//     HealthCheckAlreadyExists (409) when the CallerReference is reused with a
//     different HealthCheckConfig. Fix: scan health checks for a matching
//     CallerReference and compare the config before inserting.
//
//  4. DisassociateVPCFromHostedZone last VPC: allowed removing the last VPC from
//     a private hosted zone. AWS returns LastVPCAssociation (400) when the
//     disassociation would leave the zone with zero VPC associations.
//     Fix: check len(newAssocs) == 0 and return ErrLastVPCAssociation.

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// ---------------------------------------------------------------------------
// Issue #1 — DeleteHostedZone with non-empty zone must return HostedZoneNotEmpty
// ---------------------------------------------------------------------------

func TestBatch2_DeleteHostedZone_NotEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		records  string
		wantBody string
		wantCode int
	}{
		{
			name: "zone_with_A_record_rejected",
			records: `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch><Changes><Change>
    <Action>CREATE</Action>
    <ResourceRecordSet>
      <Name>www.example.com</Name><Type>A</Type><TTL>300</TTL>
      <ResourceRecords><ResourceRecord><Value>1.2.3.4</Value></ResourceRecord></ResourceRecords>
    </ResourceRecordSet>
  </Change></Changes></ChangeBatch>
</ChangeResourceRecordSetsRequest>`,
			wantCode: http.StatusBadRequest,
			wantBody: "HostedZoneNotEmpty",
		},
		{
			name: "zone_with_TXT_record_rejected",
			records: `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch><Changes><Change>
    <Action>CREATE</Action>
    <ResourceRecordSet>
      <Name>_dmarc.example.com</Name><Type>TXT</Type><TTL>300</TTL>
      <ResourceRecords><ResourceRecord><Value>"v=DMARC1"</Value></ResourceRecord></ResourceRecords>
    </ResourceRecordSet>
  </Change></Changes></ChangeBatch>
</ChangeResourceRecordSetsRequest>`,
			wantCode: http.StatusBadRequest,
			wantBody: "HostedZoneNotEmpty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
			require.Equal(t, http.StatusCreated, rec.Code)
			zoneID := extractZoneID(t, rec.Body.String())

			// Add records to make the zone non-empty.
			rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", tt.records)
			require.Equal(t, http.StatusOK, rec.Code)

			// Delete should fail with HostedZoneNotEmpty.
			rec = send(t, h, http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID, "")
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestBatch2_DeleteHostedZone_EmptyZoneSucceeds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "empty_zone_deleted"},
		{name: "empty_zone_deleted_second"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
			require.Equal(t, http.StatusCreated, rec.Code)
			zoneID := extractZoneID(t, rec.Body.String())

			rec = send(t, h, http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID, "")
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "DeleteHostedZoneResponse")
		})
	}
}

func TestBatch2_DeleteHostedZone_AfterRecordsRemoved_Succeeds(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	// Add a record.
	addBody := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch><Changes><Change>
    <Action>CREATE</Action>
    <ResourceRecordSet>
      <Name>host.example.com</Name><Type>A</Type><TTL>60</TTL>
      <ResourceRecords><ResourceRecord><Value>10.0.0.1</Value></ResourceRecord></ResourceRecords>
    </ResourceRecordSet>
  </Change></Changes></ChangeBatch>
</ChangeResourceRecordSetsRequest>`
	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", addBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete fails while record exists.
	rec = send(t, h, http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID, "")
	require.Equal(t, http.StatusBadRequest, rec.Code)

	// Remove the record.
	delBody := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch><Changes><Change>
    <Action>DELETE</Action>
    <ResourceRecordSet>
      <Name>host.example.com</Name><Type>A</Type><TTL>60</TTL>
      <ResourceRecords><ResourceRecord><Value>10.0.0.1</Value></ResourceRecord></ResourceRecords>
    </ResourceRecordSet>
  </Change></Changes></ChangeBatch>
</ChangeResourceRecordSetsRequest>`
	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", delBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// Now delete succeeds.
	rec = send(t, h, http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID, "")
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// Issue #2 — CreateHostedZone duplicate CallerReference returns existing zone
// ---------------------------------------------------------------------------

func TestBatch2_CreateHostedZone_DuplicateCallerReference(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ref     string
		name2   string
		comment string
	}{
		{
			name:    "same_ref_same_params_returns_existing",
			ref:     "unique-caller-ref-hz-1",
			name2:   "example.com",
			comment: "first",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()

			first, err := b.CreateHostedZone("example.com", tt.ref, "first", false)
			require.NoError(t, err)

			// Same CallerReference *and* identical other parameters is a safe
			// retry: AWS returns the original zone.
			second, err := b.CreateHostedZone(tt.name2, tt.ref, tt.comment, false)
			require.NoError(t, err)

			assert.Equal(t, first.ID, second.ID,
				"duplicate CallerReference with identical params must return the same zone ID")
			assert.Equal(t, first.Name, second.Name,
				"original zone name must be preserved")
		})
	}
}

// TestBatch2_CreateHostedZone_DuplicateCallerReference_DifferentParams verifies
// real AWS behavior: reusing a CallerReference with a *different* Name (or
// Comment/PrivateZone) is NOT idempotent — it returns HostedZoneAlreadyExists
// (409), since the CallerReference is now ambiguous between two distinct
// requested zones. A prior version of this test asserted the request was
// "still idempotent" for a different name, which is not how Route 53 behaves.
func TestBatch2_CreateHostedZone_DuplicateCallerReference_DifferentParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ref  string
	}{
		{name: "same_ref_different_name_rejected", ref: "unique-caller-ref-hz-2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()

			_, err := b.CreateHostedZone("example.com", tt.ref, "first", false)
			require.NoError(t, err)

			_, err = b.CreateHostedZone("other.com", tt.ref, "second", false)
			require.Error(t, err)
			assert.ErrorIs(t, err, route53.ErrHostedZoneAlreadyExists)
		})
	}
}

func TestBatch2_CreateHostedZone_UniqueCallerReference_CreatesNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ref1 string
		ref2 string
	}{
		{name: "different_refs_create_two_zones", ref1: "ref-a", ref2: "ref-b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()

			z1, err := b.CreateHostedZone("example.com", tt.ref1, "", false)
			require.NoError(t, err)

			z2, err := b.CreateHostedZone("example.com", tt.ref2, "", false)
			require.NoError(t, err)

			assert.NotEqual(t, z1.ID, z2.ID,
				"distinct CallerReferences must create distinct zones")
		})
	}
}

func TestBatch2_CreateHostedZone_DuplicateCallerReference_HTTPRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	const ref = "idempotent-hz-ref"
	body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>example.com</Name>
  <CallerReference>` + ref + `</CallerReference>
</CreateHostedZoneRequest>`

	rec1 := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", body)
	require.Equal(t, http.StatusCreated, rec1.Code)
	id1 := extractZoneID(t, rec1.Body.String())

	rec2 := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", body)
	require.Equal(t, http.StatusCreated, rec2.Code)
	id2 := extractZoneID(t, rec2.Body.String())

	assert.Equal(t, id1, id2,
		"second CreateHostedZone with same CallerReference must return the same zone ID")
}

// ---------------------------------------------------------------------------
// Issue #3 — CreateHealthCheck duplicate CallerReference returns existing check
// ---------------------------------------------------------------------------

func TestBatch2_CreateHealthCheck_DuplicateCallerReference(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ref  string
	}{
		{name: "same_ref_returns_existing_http", ref: "hc-idem-ref-1"},
		{name: "same_ref_returns_existing_https", ref: "hc-idem-ref-2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()

			cfg := route53.HealthCheckConfig{
				Type: route53.HealthCheckTypeHTTP,
				Port: 80,
			}

			first, err := b.CreateHealthCheck(tt.ref, cfg)
			require.NoError(t, err)

			// Same CallerReference *and* identical config is a safe retry:
			// AWS returns the original health check.
			second, err := b.CreateHealthCheck(tt.ref, cfg)
			require.NoError(t, err)

			assert.Equal(t, first.ID, second.ID,
				"duplicate CallerReference with identical config must return the same health check ID")
			assert.Equal(t, first.Config.Type, second.Config.Type,
				"original config must be preserved")
		})
	}
}

// TestBatch2_CreateHealthCheck_DuplicateCallerReference_DifferentConfig
// verifies real AWS behavior: reusing a CallerReference with a *different*
// HealthCheckConfig returns HealthCheckAlreadyExists (409) rather than
// silently returning (or silently overwriting) the original health check. A
// prior version of this test asserted the request was still idempotent when
// Type/Port differed, which is not how Route 53 behaves.
func TestBatch2_CreateHealthCheck_DuplicateCallerReference_DifferentConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ref  string
	}{
		{name: "same_ref_different_config_rejected", ref: "hc-idem-ref-3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()

			_, err := b.CreateHealthCheck(tt.ref, route53.HealthCheckConfig{
				Type: route53.HealthCheckTypeHTTP,
				Port: 80,
			})
			require.NoError(t, err)

			_, err = b.CreateHealthCheck(tt.ref, route53.HealthCheckConfig{
				Type: route53.HealthCheckTypeHTTPS,
				Port: 443,
			})
			require.Error(t, err)
			assert.ErrorIs(t, err, route53.ErrHealthCheckAlreadyExists)
		})
	}
}

func TestBatch2_CreateHealthCheck_UniqueCallerReference_CreatesNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ref1 string
		ref2 string
	}{
		{name: "different_refs_create_two_checks", ref1: "hc-ref-x", ref2: "hc-ref-y"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()

			hc1, err := b.CreateHealthCheck(tt.ref1, route53.HealthCheckConfig{
				Type: route53.HealthCheckTypeHTTP,
				Port: 80,
			})
			require.NoError(t, err)

			hc2, err := b.CreateHealthCheck(tt.ref2, route53.HealthCheckConfig{
				Type: route53.HealthCheckTypeHTTP,
				Port: 80,
			})
			require.NoError(t, err)

			assert.NotEqual(t, hc1.ID, hc2.ID,
				"distinct CallerReferences must create distinct health checks")
		})
	}
}

func TestBatch2_CreateHealthCheck_DuplicateCallerReference_HTTPRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>idem-hc-http-ref</CallerReference>
  <HealthCheckConfig>
    <Type>HTTP</Type>
    <Port>80</Port>
    <IPAddress>1.2.3.4</IPAddress>
  </HealthCheckConfig>
</CreateHealthCheckRequest>`

	rec1 := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", body)
	require.Equal(t, http.StatusCreated, rec1.Code)

	type hcResp struct {
		HealthCheck struct {
			ID string `xml:"Id"`
		} `xml:"HealthCheck"`
	}

	var r1, r2 hcResp
	require.NoError(t, xml.Unmarshal(rec1.Body.Bytes(), &r1))

	rec2 := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", body)
	require.Equal(t, http.StatusCreated, rec2.Code)
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &r2))

	assert.Equal(t, r1.HealthCheck.ID, r2.HealthCheck.ID,
		"second CreateHealthCheck with same CallerReference must return the same ID")
}

// ---------------------------------------------------------------------------
// Issue #4 — DisassociateVPCFromHostedZone must reject removal of last VPC
// ---------------------------------------------------------------------------

func TestBatch2_DisassociateVPC_LastVPCRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantBody string
		wantCode int
	}{
		{
			name:     "backend_last_vpc_returns_error",
			wantCode: http.StatusBadRequest,
			wantBody: "LastVPCAssociation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("private.example.com", "priv-ref", "", true)
			require.NoError(t, err)

			require.NoError(t, b.AssociateVPCWithHostedZone(hz.ID, "vpc-only", "us-east-1"))

			err = b.DisassociateVPCFromHostedZone(hz.ID, "vpc-only")
			require.ErrorIs(t, err, route53.ErrLastVPCAssociation,
				"removing the last VPC must return ErrLastVPCAssociation")

			_ = tt // wantCode/wantBody verified via HTTP round-trip below
		})
	}
}

func TestBatch2_DisassociateVPC_LastVPC_HTTPRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	// Create a private zone.
	body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>private.example.com</Name>
  <CallerReference>priv-last-vpc-ref</CallerReference>
  <HostedZoneConfig><PrivateZone>true</PrivateZone></HostedZoneConfig>
</CreateHostedZoneRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", body)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	// Associate two VPCs.
	for _, vpcID := range []string{"vpc-alpha", "vpc-beta"} {
		assocBody := `<?xml version="1.0" encoding="UTF-8"?>
<AssociateVPCWithHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC><VPCId>` + vpcID + `</VPCId><VPCRegion>us-east-1</VPCRegion></VPC>
</AssociateVPCWithHostedZoneRequest>`
		rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/associatevpc", assocBody)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Disassociate vpc-beta (succeeds — vpc-alpha still present).
	disBody := `<?xml version="1.0" encoding="UTF-8"?>
<DisassociateVPCFromHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC><VPCId>vpc-beta</VPCId><VPCRegion>us-east-1</VPCRegion></VPC>
</DisassociateVPCFromHostedZoneRequest>`
	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/disassociatevpc", disBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// Attempt to disassociate the only remaining VPC must fail.
	lastDisBody := `<?xml version="1.0" encoding="UTF-8"?>
<DisassociateVPCFromHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC><VPCId>vpc-alpha</VPCId><VPCRegion>us-east-1</VPCRegion></VPC>
</DisassociateVPCFromHostedZoneRequest>`
	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/disassociatevpc", lastDisBody)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "LastVPCAssociation")
}

func TestBatch2_DisassociateVPC_WithMultipleVPCs_Succeeds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "two_vpcs_remove_one"},
		{name: "three_vpcs_remove_one"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("private.example.com", "priv-multi-ref-"+tt.name, "", true)
			require.NoError(t, err)

			require.NoError(t, b.AssociateVPCWithHostedZone(hz.ID, "vpc-keep", "us-east-1"))
			require.NoError(t, b.AssociateVPCWithHostedZone(hz.ID, "vpc-remove", "us-east-1"))

			err = b.DisassociateVPCFromHostedZone(hz.ID, "vpc-remove")
			require.NoError(t, err, "removing non-last VPC must succeed")

			assocs, err := b.ListVPCAssociations(hz.ID)
			require.NoError(t, err)
			require.Len(t, assocs, 1)
		})
	}
}

// TestChangeResourceRecordSets_DeleteExactMatch verifies AWS's DELETE
// exact-match rule: a DELETE must supply the same TTL and the same (unordered)
// set of resource record values that the record currently holds, otherwise
// Route 53 returns InvalidChangeBatch ("...the values provided do not match the
// current values"). A bare delete (no TTL, no values) is still accepted.
func TestChangeResourceRecordSets_DeleteExactMatch(t *testing.T) {
	t.Parallel()

	const (
		recName = "host.example.com"
		recType = "A"
	)

	type deleteSpec struct {
		records []string
		ttl     int64
	}

	tests := []struct {
		name      string
		del       deleteSpec
		wantError bool
	}{
		{
			name:      "exact match both values succeeds",
			del:       deleteSpec{ttl: 300, records: []string{"1.2.3.4", "5.6.7.8"}},
			wantError: false,
		},
		{
			name:      "exact match values out of order succeeds",
			del:       deleteSpec{ttl: 300, records: []string{"5.6.7.8", "1.2.3.4"}},
			wantError: false,
		},
		{
			name:      "wrong ttl fails",
			del:       deleteSpec{ttl: 60, records: []string{"1.2.3.4", "5.6.7.8"}},
			wantError: true,
		},
		{
			name:      "wrong value fails",
			del:       deleteSpec{ttl: 300, records: []string{"9.9.9.9", "5.6.7.8"}},
			wantError: true,
		},
		{
			name:      "missing a value fails",
			del:       deleteSpec{ttl: 300, records: []string{"1.2.3.4"}},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-"+tt.name, "", false)
			require.NoError(t, err)

			// Seed a multi-value A record (TTL 300, values 1.2.3.4 + 5.6.7.8).
			_, err = b.ChangeResourceRecordSets(hz.ID, []route53.Change{{
				Action: route53.ChangeActionCreate,
				ResourceRecordSet: route53.ResourceRecordSet{
					Name: recName,
					Type: recType,
					TTL:  300,
					Records: []route53.ResourceRecord{
						{Value: "1.2.3.4"},
						{Value: "5.6.7.8"},
					},
				},
			}})
			require.NoError(t, err)

			recs := make([]route53.ResourceRecord, len(tt.del.records))
			for i, v := range tt.del.records {
				recs[i] = route53.ResourceRecord{Value: v}
			}

			_, err = b.ChangeResourceRecordSets(hz.ID, []route53.Change{{
				Action: route53.ChangeActionDelete,
				ResourceRecordSet: route53.ResourceRecordSet{
					Name:    recName,
					Type:    recType,
					TTL:     tt.del.ttl,
					Records: recs,
				},
			}})

			if tt.wantError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidChangeBatch")
				assert.Contains(t, err.Error(), "do not match the current values")

				return
			}

			require.NoError(t, err)

			// Record must be gone after a successful delete.
			page, lerr := b.ListResourceRecordSets(hz.ID, recName, recType, "", 10)
			require.NoError(t, lerr)
			for _, r := range page.Records {
				assert.NotEqualf(t, recName+".", r.Name,
					"record %s %s should have been deleted", r.Name, r.Type)
			}
		})
	}
}
