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

// TestChangeResourceRecordSets_ErrorCases merges TestChangeResourceRecordSets_DeleteNonexistent,
// TestChangeResourceRecordSets_InvalidXML, and TestChangeResourceRecordSets_ZoneNotFound.
func TestChangeResourceRecordSets_ErrorCases(t *testing.T) {
	t.Parallel()

	const deleteNonexistentXML = `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>DELETE</Action>
        <ResourceRecordSet>
          <Name>nonexistent.example.com</Name>
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

	const zoneNotFoundXML = `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>www.example.com</Name>
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

	tests := []struct {
		name      string
		body      string
		wantCode  int
		needsZone bool
	}{
		{
			name:      "delete_nonexistent_record_returns_bad_request",
			needsZone: true,
			body:      deleteNonexistentXML,
			wantCode:  http.StatusBadRequest,
		},
		{
			name:      "invalid_xml_returns_bad_request",
			needsZone: true,
			body:      "bad-xml",
			wantCode:  http.StatusBadRequest,
		},
		{
			name:      "zone_not_found_returns_not_found",
			needsZone: false,
			body:      zoneNotFoundXML,
			wantCode:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			path := "/2013-04-01/hostedzone/ZNONEXISTENT/rrset"

			if tt.needsZone {
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
				require.Equal(t, http.StatusCreated, rec.Code)

				zoneID := extractZoneID(t, rec.Body.String())
				path = "/2013-04-01/hostedzone/" + zoneID + "/rrset"
			}

			rec := send(t, h, http.MethodPost, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
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
			hz, err := b.CreateHostedZone("example.com", "ref-"+tt.name, "", false, "", "", "")
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

func TestNSSOAAutoSeeding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		zoneName      string
		wantNSType    bool
		wantSOAType   bool
		wantMinRecord int
	}{
		{
			name:          "public_zone_seeded_with_ns_and_soa",
			zoneName:      "example.com",
			wantNSType:    true,
			wantSOAType:   true,
			wantMinRecord: 2,
		},
		{
			name:          "private_zone_seeded_with_ns_and_soa",
			zoneName:      "internal.example.com",
			wantNSType:    true,
			wantSOAType:   true,
			wantMinRecord: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone(tt.zoneName, "ref-"+tt.name, "", false, "", "", "")
			require.NoError(t, err)

			pg, err := b.ListResourceRecordSets(hz.ID, "", "", "", 100)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(pg.Records), tt.wantMinRecord,
				"fresh zone must have at least NS + SOA records")

			var hasNS, hasSOA bool
			for _, rrs := range pg.Records {
				switch rrs.Type {
				case "NS":
					hasNS = true
					assert.NotEmpty(t, rrs.Records, "NS record must have nameserver values")
				case "SOA":
					hasSOA = true
					assert.NotEmpty(t, rrs.Records, "SOA record must have a value")
				}
			}
			assert.Equal(t, tt.wantNSType, hasNS, "must have NS record")
			assert.Equal(t, tt.wantSOAType, hasSOA, "must have SOA record")
		})
	}
}

func TestResourceRecordSetCount_IncludesNSSOA(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	hz, err := b.CreateHostedZone("example.com", "ref-count", "", false, "", "", "")
	require.NoError(t, err)

	got, err := b.GetHostedZone(hz.ID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, got.ResourceRecordSetCount, 2,
		"ResourceRecordSetCount must include seeded NS and SOA")
}

func TestRecordTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rrType  string
		value   string
		wantErr bool
	}{
		{name: "A_record", rrType: "A", value: "192.0.2.1"},
		{name: "AAAA_record", rrType: "AAAA", value: "2001:db8::1"},
		{name: "CNAME_record", rrType: "CNAME", value: "target.example.com"},
		{name: "MX_record", rrType: "MX", value: "10 mail.example.com"},
		{name: "TXT_record", rrType: "TXT", value: "\"v=spf1 include:example.com ~all\""},
		{name: "NS_record", rrType: "NS", value: "ns1.example.com"},
		{name: "SRV_record", rrType: "SRV", value: "10 20 5060 sip.example.com"},
		{name: "CAA_record", rrType: "CAA", value: `0 issue "letsencrypt.org"`},
		{name: "PTR_record", rrType: "PTR", value: "host.example.com"},
		{name: "A_invalid_ip", rrType: "A", value: "not-an-ip", wantErr: true},
		{name: "AAAA_ipv4_rejected", rrType: "AAAA", value: "192.0.2.1", wantErr: true},
		{name: "MX_no_prio_rejected", rrType: "MX", value: "mail.example.com", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-rt-"+tt.name, "", false, "", "", "")
			require.NoError(t, err)

			name := "host.example.com."
			if tt.rrType == "CNAME" {
				name = "cname.example.com." // avoid apex CNAME
			}

			changes := []route53.Change{
				{
					Action: route53.ChangeActionCreate,
					ResourceRecordSet: route53.ResourceRecordSet{
						Name:    name,
						Type:    tt.rrType,
						TTL:     300,
						Records: []route53.ResourceRecord{{Value: tt.value}},
					},
				},
			}

			_, err = b.ChangeResourceRecordSets(hz.ID, changes)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAliasRecord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		aliasZone  string
		aliasName  string
		wantInList bool
	}{
		{
			name:       "elb_alias_roundtrip",
			aliasZone:  "Z2FDTNDATAQYW2",
			aliasName:  "my-elb.us-east-1.elb.amazonaws.com",
			wantInList: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-alias-"+tt.name, "", false, "", "", "")
			require.NoError(t, err)

			changes := []route53.Change{
				{
					Action: route53.ChangeActionCreate,
					ResourceRecordSet: route53.ResourceRecordSet{
						Name: "www.example.com.",
						Type: "A",
						AliasTarget: &route53.AliasTarget{
							HostedZoneID:         tt.aliasZone,
							DNSName:              tt.aliasName,
							EvaluateTargetHealth: true,
						},
					},
				},
			}

			_, err = b.ChangeResourceRecordSets(hz.ID, changes)
			require.NoError(t, err)

			pg, err := b.ListResourceRecordSets(hz.ID, "", "", "", 100)
			require.NoError(t, err)

			found := false
			for _, rrs := range pg.Records {
				if rrs.Name == "www.example.com." && rrs.AliasTarget != nil {
					found = true
					assert.Equal(t, tt.aliasZone, rrs.AliasTarget.HostedZoneID)
					assert.Equal(t, tt.aliasName, rrs.AliasTarget.DNSName)
					assert.True(t, rrs.AliasTarget.EvaluateTargetHealth)
				}
			}
			assert.True(t, found, "alias record must be retrievable via ListResourceRecordSets")
		})
	}
}

func TestGetChange_INSYNC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "zone_creation_change_is_insync"},
		{name: "record_change_is_insync"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
			require.Equal(t, http.StatusCreated, rec.Code)
			zoneID := extractZoneID(t, rec.Body.String())

			var changeID string
			if tt.name == "record_change_is_insync" {
				body := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch><Changes><Change>
    <Action>CREATE</Action>
    <ResourceRecordSet>
      <Name>host.example.com</Name><Type>A</Type><TTL>300</TTL>
      <ResourceRecords><ResourceRecord><Value>1.2.3.4</Value></ResourceRecord></ResourceRecords>
    </ResourceRecordSet>
  </Change></Changes></ChangeBatch>
</ChangeResourceRecordSetsRequest>`
				r := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", body)
				require.Equal(t, http.StatusOK, r.Code)

				type changeResp struct {
					ChangeInfo struct {
						ID string `xml:"Id"`
					} `xml:"ChangeInfo"`
				}

				var cr changeResp
				require.NoError(t, xml.Unmarshal(r.Body.Bytes(), &cr))
				parts := []byte(cr.ChangeInfo.ID)
				changeID = string(parts[len("/change/"):])
			} else {
				// Zone creation change ID is "C" + zoneID
				changeID = "C" + zoneID
			}

			rec = send(t, h, http.MethodGet, "/2013-04-01/change/"+changeID, "")
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "INSYNC")
		})
	}
}

func TestUPSERT_CreateThenUpdate(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	hz, err := b.CreateHostedZone("example.com", "ref-upsert", "", false, "", "", "")
	require.NoError(t, err)

	rrs := route53.ResourceRecordSet{
		Name:    "host.example.com.",
		Type:    "A",
		TTL:     300,
		Records: []route53.ResourceRecord{{Value: "1.2.3.4"}},
	}

	// First UPSERT creates.
	_, err = b.ChangeResourceRecordSets(hz.ID, []route53.Change{
		{Action: route53.ChangeActionUpsert, ResourceRecordSet: rrs},
	})
	require.NoError(t, err)

	// Second UPSERT replaces.
	rrs.Records = []route53.ResourceRecord{{Value: "5.6.7.8"}}
	_, err = b.ChangeResourceRecordSets(hz.ID, []route53.Change{
		{Action: route53.ChangeActionUpsert, ResourceRecordSet: rrs},
	})
	require.NoError(t, err)

	pg, err := b.ListResourceRecordSets(hz.ID, "", "", "", 100)
	require.NoError(t, err)

	var found *route53.ResourceRecordSet
	for i := range pg.Records {
		if pg.Records[i].Name == "host.example.com." {
			cp := pg.Records[i]
			found = &cp
		}
	}
	require.NotNil(t, found)
	require.Len(t, found.Records, 1)
	assert.Equal(t, "5.6.7.8", found.Records[0].Value, "UPSERT must replace old value")
}

func TestNSSOAInListHTTP(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	rec = send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", "")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "<Type>NS</Type>", "NS record must appear in ListResourceRecordSets")
	assert.Contains(t, body, "<Type>SOA</Type>", "SOA record must appear in ListResourceRecordSets")
}

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
	hz, err := b.CreateHostedZone("example.com", "ref", "", false, "", "", "")
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

func TestListResourceRecordSets_Pagination(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	hz, err := b.CreateHostedZone("example.com", "ref", "", false, "", "", "")
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

	// Page 1: 3 records (NS + SOA come first alphabetically, then A records).
	pg, err := b.ListResourceRecordSets(hz.ID, "", "", "", 3)
	require.NoError(t, err)
	assert.Len(t, pg.Records, 3)
	assert.True(t, pg.IsTruncated)
	assert.NotEmpty(t, pg.NextName)

	// Page 2: next 3 records.
	pg2, err := b.ListResourceRecordSets(hz.ID, pg.NextName, pg.NextType, pg.NextIdentifier, 3)
	require.NoError(t, err)
	assert.Len(t, pg.Records, 3)

	// Page 3: the last record.
	pg3, err := b.ListResourceRecordSets(hz.ID, pg2.NextName, pg2.NextType, pg2.NextIdentifier, 3)
	require.NoError(t, err)

	// Total = 5 A records + 2 default NS/SOA records seeded at zone creation.
	assert.Equal(t, 7, len(pg.Records)+len(pg2.Records)+len(pg3.Records))
	assert.False(t, pg3.IsTruncated)
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

func TestRoute53Handler_WithZone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setupBody    string
		method       string
		path         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "ListHostedZones_AfterCreate",
			method:       http.MethodGet,
			path:         "/2013-04-01/hostedzone",
			wantCode:     http.StatusOK,
			wantContains: []string{"example.com"},
		},
		{
			name:         "GetHostedZone",
			method:       http.MethodGet,
			path:         "/2013-04-01/hostedzone/{zoneID}",
			wantCode:     http.StatusOK,
			wantContains: []string{"example.com"},
		},
		{
			name:   "ChangeResourceRecordSets_CreateA",
			method: http.MethodPost,
			path:   "/2013-04-01/hostedzone/{zoneID}/rrset",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>www.example.com</Name>
          <Type>A</Type>
          <TTL>300</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>192.0.2.1</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`,
			wantCode:     http.StatusOK,
			wantContains: []string{"INSYNC"},
		},
		{
			name:   "ChangeResourceRecordSets_CreateCNAME",
			method: http.MethodPost,
			path:   "/2013-04-01/hostedzone/{zoneID}/rrset",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>alias.example.com</Name>
          <Type>CNAME</Type>
          <TTL>60</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>www.example.com</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`,
			wantCode: http.StatusOK,
		},
		{
			name: "ListResourceRecordSets",
			setupBody: `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>api.example.com</Name>
          <Type>A</Type>
          <TTL>300</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>10.0.0.1</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`,
			method:       http.MethodGet,
			path:         "/2013-04-01/hostedzone/{zoneID}/rrset",
			wantCode:     http.StatusOK,
			wantContains: []string{"api.example.com", "10.0.0.1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
			require.Equal(t, http.StatusCreated, rec.Code)

			zoneID := extractZoneID(t, rec.Body.String())

			if tt.setupBody != "" {
				setupRec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", tt.setupBody)
				require.Equal(t, http.StatusOK, setupRec.Code)
			}

			path := strings.Replace(tt.path, "{zoneID}", zoneID, 1)
			got := send(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, got.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, got.Body.String(), s)
			}
		})
	}
}

func TestRoute53Handler_ChangeResourceRecordSets_Upsert(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)

	zoneID := extractZoneID(t, rec.Body.String())

	makeChange := func(action, ip string) string {
		return `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>` + action + `</Action>
        <ResourceRecordSet>
          <Name>www.example.com</Name>
          <Type>A</Type>
          <TTL>300</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>` + ip + `</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`
	}

	// Create initial record.
	r1 := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", makeChange("CREATE", "1.2.3.4"))
	require.Equal(t, http.StatusOK, r1.Code)

	// Upsert (update) the record.
	r2 := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", makeChange("UPSERT", "5.6.7.8"))
	require.Equal(t, http.StatusOK, r2.Code)

	// Verify list shows updated IP.
	listRec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", "")
	assert.Contains(t, listRec.Body.String(), "5.6.7.8")
}

func TestRoute53Handler_ChangeResourceRecordSets_Delete(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)

	zoneID := extractZoneID(t, rec.Body.String())

	createXML := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>www.example.com</Name>
          <Type>A</Type>
          <TTL>300</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>192.0.2.1</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", createXML)

	deleteXML := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>DELETE</Action>
        <ResourceRecordSet>
          <Name>www.example.com</Name>
          <Type>A</Type>
          <TTL>300</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>192.0.2.1</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	delRec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", deleteXML)
	assert.Equal(t, http.StatusOK, delRec.Code)

	// Record should be gone.
	listRec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", "")
	assert.NotContains(t, listRec.Body.String(), "192.0.2.1")
}

func TestRoute53Handler_DNSRegistrar_RegisterOnCreate(t *testing.T) {
	t.Parallel()

	registered := make(map[string]bool)
	registrar := &mockDNSRegistrar{registered: registered}

	backend := route53.NewInMemoryBackend()
	backend.SetDNSRegistrar(registrar)
	h := route53.NewHandler(backend)

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)

	zoneID := extractZoneID(t, rec.Body.String())

	changeXML := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>www.example.com</Name>
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

	send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", changeXML)

	assert.True(t, registrar.registered["www.example.com."], "expected www.example.com. to be registered")
}

func TestRoute53Handler_DNSRegistrar_DeregisterOnDelete(t *testing.T) {
	t.Parallel()

	registered := make(map[string]bool)
	registrar := &mockDNSRegistrar{registered: registered}

	backend := route53.NewInMemoryBackend()
	backend.SetDNSRegistrar(registrar)
	h := route53.NewHandler(backend)

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)

	zoneID := extractZoneID(t, rec.Body.String())

	createXML := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>www.example.com</Name>
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

	send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", createXML)
	require.True(t, registrar.registered["www.example.com."])

	deleteXML := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>DELETE</Action>
        <ResourceRecordSet>
          <Name>www.example.com</Name>
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

	send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", deleteXML)
	assert.False(t, registrar.registered["www.example.com."], "expected www.example.com. to be deregistered")
}

// mockDNSRegistrar is a test double for route53.DNSRegistrar.
type mockDNSRegistrar struct {
	registered map[string]bool
	records    map[string][]string // hostname → values
}

func (m *mockDNSRegistrar) RegisterRecord(hostname, _ string, values []string) {
	fqdn := hostname
	if len(fqdn) > 0 && fqdn[len(fqdn)-1] != '.' {
		fqdn += "."
	}
	m.registered[fqdn] = true

	if m.records == nil {
		m.records = make(map[string][]string)
	}

	m.records[fqdn] = append(m.records[fqdn], values...)
}

func (m *mockDNSRegistrar) Deregister(hostname string) {
	fqdn := hostname
	if len(fqdn) > 0 && fqdn[len(fqdn)-1] != '.' {
		fqdn += "."
	}
	delete(m.registered, fqdn)

	if m.records != nil {
		delete(m.records, fqdn)
	}
}
