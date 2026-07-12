package route53_test

// audit_r53_test.go — Phase-B Route53 audit tests (go-wisp-r53-audit).
//
// Covers two high-priority bugs and their surrounding functionality:
//
//  1. NS/SOA auto-seeding: CreateHostedZone now seeds default NS and SOA records
//     so that ListResourceRecordSets on a fresh zone returns 2 records (not 0).
//
//  2. Weight=0 routing: ResourceRecordSet.Weight is now *int64, so explicit
//     Weight=0 (stop-traffic weighted record) is accepted via ChangeResourceRecordSets.
//     Previously it was rejected with "SetIdentifier not allowed without routing policy".
//
// Also covers: all record types, alias records, routing-policy mutual exclusion,
// health check CRUD, GetChange INSYNC, and private zone creation.

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func int64ptr(v int64) *int64 {
	p := new(int64)
	*p = v

	return p
}

// ---------------------------------------------------------------------------
// 1. NS/SOA auto-seeding
// ---------------------------------------------------------------------------

func TestAuditR53_NSSOAAutoSeeding(t *testing.T) {
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
			hz, err := b.CreateHostedZone(tt.zoneName, "ref-"+tt.name, "", false, "")
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

func TestAuditR53_ResourceRecordSetCount_IncludesNSSOA(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	hz, err := b.CreateHostedZone("example.com", "ref-count", "", false, "")
	require.NoError(t, err)

	got, err := b.GetHostedZone(hz.ID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, got.ResourceRecordSetCount, 2,
		"ResourceRecordSetCount must include seeded NS and SOA")
}

func TestAuditR53_DeleteEmptyZone_WithDefaultNSSOA_Succeeds(t *testing.T) {
	t.Parallel()

	// A zone with only NS+SOA (the defaults) must be deletable.
	// This regression test ensures the non-empty check skips default records.
	tests := []struct {
		name string
		ref  string
	}{
		{name: "delete_zone_with_only_defaults", ref: "ref-del-1"},
		{name: "delete_second_zone", ref: "ref-del-2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", tt.ref, "", false, "")
			require.NoError(t, err)

			err = b.DeleteHostedZone(hz.ID)
			assert.NoError(t, err, "zone with only default NS+SOA must be deletable")
		})
	}
}

// ---------------------------------------------------------------------------
// 2. Weight=0 routing (the fixed bug)
// ---------------------------------------------------------------------------

func TestAuditR53_WeightedRouting(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: test struct, cosmetic only
		name      string
		weight    *int64
		wantErr   bool
		wantErrIs string
	}{
		{
			name:    "weight_100_accepted",
			weight:  int64ptr(100),
			wantErr: false,
		},
		{
			name:    "weight_0_accepted", // stop-traffic record — previously rejected
			weight:  int64ptr(0),
			wantErr: false,
		},
		{
			name:    "weight_255_accepted",
			weight:  int64ptr(255),
			wantErr: false,
		},
		{
			name:      "weight_256_rejected",
			weight:    int64ptr(256),
			wantErr:   true,
			wantErrIs: "InvalidChangeBatch",
		},
		{
			name:      "weight_negative_rejected",
			weight:    int64ptr(-1),
			wantErr:   true,
			wantErrIs: "InvalidChangeBatch",
		},
		{
			name:    "nil_weight_no_routing_policy_accepted",
			weight:  nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-wt-"+tt.name, "", false, "")
			require.NoError(t, err)

			setID := ""
			if tt.weight != nil {
				setID = "id1" // SetIdentifier required for routing-policy records
			}

			changes := []route53.Change{
				{
					Action: route53.ChangeActionCreate,
					ResourceRecordSet: route53.ResourceRecordSet{
						Name:          "host.example.com.",
						Type:          "A",
						TTL:           300,
						Records:       []route53.ResourceRecord{{Value: "1.2.3.4"}},
						SetIdentifier: setID,
						Weight:        tt.weight,
					},
				},
			}

			_, err = b.ChangeResourceRecordSets(hz.ID, changes)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAuditR53_WeightZero_RoundTrip(t *testing.T) {
	t.Parallel()

	// Weight=0 must survive a write and be readable back as 0.
	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	body := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch><Changes><Change>
    <Action>CREATE</Action>
    <ResourceRecordSet>
      <Name>w0.example.com</Name><Type>A</Type><TTL>300</TTL>
      <SetIdentifier>stop</SetIdentifier>
      <Weight>0</Weight>
      <ResourceRecords><ResourceRecord><Value>1.2.3.4</Value></ResourceRecord></ResourceRecords>
    </ResourceRecordSet>
  </Change></Changes></ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// Read back and verify <Weight>0</Weight> is present in the response.
	rec = send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Weight>0</Weight>",
		"Weight=0 must be emitted in list response")
}

// ---------------------------------------------------------------------------
// 3. All record types
// ---------------------------------------------------------------------------

func TestAuditR53_RecordTypes(t *testing.T) {
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
			hz, err := b.CreateHostedZone("example.com", "ref-rt-"+tt.name, "", false, "")
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

// ---------------------------------------------------------------------------
// 4. Routing policy mutual exclusion
// ---------------------------------------------------------------------------

func TestAuditR53_RoutingPolicyMutualExclusion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rrs     route53.ResourceRecordSet
		wantErr bool
	}{
		{
			name: "latency_routing_accepted",
			rrs: route53.ResourceRecordSet{
				Name:          "host.example.com.",
				Type:          "A",
				TTL:           300,
				SetIdentifier: "us-east-1",
				Region:        "us-east-1",
				Records:       []route53.ResourceRecord{{Value: "1.2.3.4"}},
			},
		},
		{
			name: "failover_primary_accepted",
			rrs: route53.ResourceRecordSet{
				Name:          "host.example.com.",
				Type:          "A",
				TTL:           300,
				SetIdentifier: "primary",
				Failover:      route53.FailoverPrimary,
				Records:       []route53.ResourceRecord{{Value: "1.2.3.4"}},
			},
		},
		{
			name: "failover_secondary_accepted",
			rrs: route53.ResourceRecordSet{
				Name:          "host.example.com.",
				Type:          "A",
				TTL:           300,
				SetIdentifier: "secondary",
				Failover:      route53.FailoverSecondary,
				Records:       []route53.ResourceRecord{{Value: "2.3.4.5"}},
			},
		},
		{
			name: "multivalue_accepted",
			rrs: route53.ResourceRecordSet{
				Name:             "host.example.com.",
				Type:             "A",
				TTL:              300,
				SetIdentifier:    "mv1",
				MultiValueAnswer: true,
				Records:          []route53.ResourceRecord{{Value: "1.2.3.4"}},
			},
		},
		{
			name: "weight_and_failover_rejected",
			rrs: route53.ResourceRecordSet{
				Name:          "host.example.com.",
				Type:          "A",
				TTL:           300,
				SetIdentifier: "id1",
				Weight:        int64ptr(10),
				Failover:      route53.FailoverPrimary,
				Records:       []route53.ResourceRecord{{Value: "1.2.3.4"}},
			},
			wantErr: true,
		},
		{
			name: "weight_and_latency_rejected",
			rrs: route53.ResourceRecordSet{
				Name:          "host.example.com.",
				Type:          "A",
				TTL:           300,
				SetIdentifier: "id1",
				Weight:        int64ptr(50),
				Region:        "us-east-1",
				Records:       []route53.ResourceRecord{{Value: "1.2.3.4"}},
			},
			wantErr: true,
		},
		{
			name: "no_policy_no_set_id_accepted",
			rrs: route53.ResourceRecordSet{
				Name:    "plain.example.com.",
				Type:    "A",
				TTL:     300,
				Records: []route53.ResourceRecord{{Value: "1.2.3.4"}},
			},
		},
		{
			name: "set_id_without_policy_rejected",
			rrs: route53.ResourceRecordSet{
				Name:          "host.example.com.",
				Type:          "A",
				TTL:           300,
				SetIdentifier: "orphan",
				Records:       []route53.ResourceRecord{{Value: "1.2.3.4"}},
			},
			wantErr: true,
		},
		{
			name: "policy_without_set_id_rejected",
			rrs: route53.ResourceRecordSet{
				Name:    "host.example.com.",
				Type:    "A",
				TTL:     300,
				Region:  "us-east-1",
				Records: []route53.ResourceRecord{{Value: "1.2.3.4"}},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-rp-"+tt.name, "", false, "")
			require.NoError(t, err)

			changes := []route53.Change{
				{Action: route53.ChangeActionCreate, ResourceRecordSet: tt.rrs},
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

// ---------------------------------------------------------------------------
// 5. Alias records
// ---------------------------------------------------------------------------

func TestAuditR53_AliasRecord(t *testing.T) {
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
			hz, err := b.CreateHostedZone("example.com", "ref-alias-"+tt.name, "", false, "")
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

// ---------------------------------------------------------------------------
// 6. Health check CRUD
// ---------------------------------------------------------------------------

func TestAuditR53_HealthCheckCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: test struct, cosmetic only
		name     string
		hcType   route53.HealthCheckType
		ipAddr   string
		fqdn     string
		port     int
		path     string
		wantType route53.HealthCheckType
	}{
		{
			name:     "http_health_check",
			hcType:   route53.HealthCheckTypeHTTP,
			ipAddr:   "1.2.3.4",
			port:     80,
			path:     "/health",
			wantType: route53.HealthCheckTypeHTTP,
		},
		{
			name:     "https_health_check",
			hcType:   route53.HealthCheckTypeHTTPS,
			fqdn:     "api.example.com",
			port:     443,
			path:     "/ping",
			wantType: route53.HealthCheckTypeHTTPS,
		},
		{
			name:     "tcp_health_check",
			hcType:   route53.HealthCheckTypeTCP,
			ipAddr:   "10.0.0.1",
			port:     3306,
			wantType: route53.HealthCheckTypeTCP,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			cfg := route53.HealthCheckConfig{
				Type:                     tt.hcType,
				IPAddress:                tt.ipAddr,
				FullyQualifiedDomainName: tt.fqdn,
				Port:                     tt.port,
				ResourcePath:             tt.path,
			}

			hc, err := b.CreateHealthCheck("ref-"+tt.name, cfg)
			require.NoError(t, err)
			require.NotEmpty(t, hc.ID)

			got, err := b.GetHealthCheck(hc.ID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, got.Config.Type)
			assert.Equal(t, hc.ID, got.ID)

			// Delete and verify gone.
			err = b.DeleteHealthCheck(hc.ID)
			require.NoError(t, err)

			_, err = b.GetHealthCheck(hc.ID)
			assert.Error(t, err)
		})
	}
}

// ---------------------------------------------------------------------------
// 7. GetChange always returns INSYNC
// ---------------------------------------------------------------------------

func TestAuditR53_GetChange_INSYNC(t *testing.T) {
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

// ---------------------------------------------------------------------------
// 8. Multiple weighted records coexist by SetIdentifier
// ---------------------------------------------------------------------------

func TestAuditR53_WeightedRecordsCoexist(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	hz, err := b.CreateHostedZone("example.com", "ref-wcoexist", "", false, "")
	require.NoError(t, err)

	// Create three weighted records for the same name+type.
	cases := []struct { //nolint:govet // fieldalignment: test struct, cosmetic only
		setID  string
		weight int64
		ip     string
	}{
		{"east", 100, "10.0.0.1"},
		{"west", 50, "10.0.0.2"},
		{"stop", 0, "10.0.0.3"}, // Weight=0: valid stop-traffic record
	}

	for _, c := range cases {
		changes := []route53.Change{
			{
				Action: route53.ChangeActionCreate,
				ResourceRecordSet: route53.ResourceRecordSet{
					Name:          "api.example.com.",
					Type:          "A",
					TTL:           300,
					SetIdentifier: c.setID,
					Weight:        int64ptr(c.weight),
					Records:       []route53.ResourceRecord{{Value: c.ip}},
				},
			},
		}
		_, err = b.ChangeResourceRecordSets(hz.ID, changes)
		require.NoError(t, err, "weight=%d record must be accepted", c.weight)
	}

	pg, err := b.ListResourceRecordSets(hz.ID, "", "", "", 100)
	require.NoError(t, err)

	weightedCount := 0
	for _, rrs := range pg.Records {
		if rrs.Name == "api.example.com." {
			weightedCount++
			assert.NotNil(t, rrs.Weight, "Weight must be non-nil for weighted records")
		}
	}
	assert.Equal(t, 3, weightedCount, "all three weighted records must be present")
}

// ---------------------------------------------------------------------------
// 9. Private zone + VPC association
// ---------------------------------------------------------------------------

func TestAuditR53_PrivateZone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		private bool
	}{
		{name: "public_zone_flag_false", private: false},
		{name: "private_zone_flag_true", private: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-pvt-"+tt.name, "", tt.private, "")
			require.NoError(t, err)
			assert.Equal(t, tt.private, hz.PrivateZone)

			got, err := b.GetHostedZone(hz.ID)
			require.NoError(t, err)
			assert.Equal(t, tt.private, got.PrivateZone)
		})
	}
}

// ---------------------------------------------------------------------------
// 10. UPSERT creates and then updates
// ---------------------------------------------------------------------------

func TestAuditR53_UPSERT_CreateThenUpdate(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	hz, err := b.CreateHostedZone("example.com", "ref-upsert", "", false, "")
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

// ---------------------------------------------------------------------------
// 11. NS/SOA via HTTP handler round-trip
// ---------------------------------------------------------------------------

func TestAuditR53_NSSOAInListHTTP(t *testing.T) {
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

// ---------------------------------------------------------------------------
// 12. Geo routing
// ---------------------------------------------------------------------------

func TestAuditR53_GeoRoutingAccepted(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: test struct, cosmetic only
		name    string
		wantErr bool
		geo     *route53.GeoLocation
	}{
		{
			name:    "country_routing",
			geo:     &route53.GeoLocation{CountryCode: "US"},
			wantErr: false,
		},
		{
			name:    "continent_routing",
			geo:     &route53.GeoLocation{ContinentCode: "NA"},
			wantErr: false,
		},
		{
			name: "country_and_subdivision",
			geo: &route53.GeoLocation{
				CountryCode:     "US",
				SubdivisionCode: "CA",
			},
			wantErr: false,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", fmt.Sprintf("ref-geo-%d", i), "", false, "")
			require.NoError(t, err)

			changes := []route53.Change{
				{
					Action: route53.ChangeActionCreate,
					ResourceRecordSet: route53.ResourceRecordSet{
						Name:          fmt.Sprintf("geo%d.example.com.", i),
						Type:          "A",
						TTL:           300,
						SetIdentifier: fmt.Sprintf("geo-id-%d", i),
						GeoLocation:   tt.geo,
						Records:       []route53.ResourceRecord{{Value: "1.2.3.4"}},
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
