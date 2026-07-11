package route53_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// ----------------------------------------
// Refinement check 1 tests
// ----------------------------------------

func TestRefinement1_ZoneCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCount int
		creates   int
	}{
		{
			name:      "single_create",
			creates:   1,
			wantCount: 1,
		},
		{
			name:      "three_creates",
			creates:   3,
			wantCount: 3,
		},
		{
			name:      "zero_creates",
			creates:   0,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()

			for i := range tt.creates {
				_, err := b.CreateHostedZone("example.com", "ref-"+string(rune('A'+i)), "", false)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCount, route53.ZoneCount(b))
		})
	}
}

func TestRefinement1_HealthCheckCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCount int
		creates   int
	}{
		{
			name:      "single_create",
			creates:   1,
			wantCount: 1,
		},
		{
			name:      "two_creates",
			creates:   2,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()

			for i := range tt.creates {
				_, err := b.CreateHealthCheck("ref-"+string(rune('A'+i)), route53.HealthCheckConfig{
					Type: route53.HealthCheckTypeHTTP,
				})
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCount, route53.HealthCheckCount(b))
		})
	}
}

func TestRefinement1_KeySigningKeyCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		kskNames  []string
		wantCount int
	}{
		{
			name:      "one_ksk",
			kskNames:  []string{"key1"},
			wantCount: 1,
		},
		{
			name:      "two_ksks",
			kskNames:  []string{"key1", "key2"},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-1", "", false)
			require.NoError(t, err)

			for _, name := range tt.kskNames {
				_, err = b.CreateKeySigningKey(hz.ID, "caller-"+name, name, "arn:kms:test", "")
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCount, route53.KeySigningKeyCount(b))
		})
	}
}

func TestRefinement1_DuplicateKSK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		kskName string
		wantErr bool
		second  bool
	}{
		{
			name:    "first_create_ok",
			kskName: "mykey",
			second:  false,
			wantErr: false,
		},
		{
			name:    "duplicate_returns_error",
			kskName: "mykey",
			second:  true,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-1", "", false)
			require.NoError(t, err)

			_, err = b.CreateKeySigningKey(hz.ID, "caller-1", tt.kskName, "arn:kms:test", "")
			require.NoError(t, err)

			if !tt.second {
				return
			}

			_, err = b.CreateKeySigningKey(hz.ID, "caller-2", tt.kskName, "arn:kms:test", "")
			if tt.wantErr {
				require.Error(t, err)
				// Real Route 53 returns KeySigningKeyAlreadyExists (409) for a
				// duplicate name within a hosted zone, per the CreateKeySigningKey
				// error list in the aws-sdk-go-v2 route53 model — not the generic
				// InvalidInput this test previously asserted.
				assert.ErrorIs(t, err, route53.ErrKeySigningKeyAlreadyExists)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRefinement1_DuplicateVPC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
		second  bool
	}{
		{
			name:    "first_associate_ok",
			second:  false,
			wantErr: false,
		},
		{
			name:    "duplicate_vpc_returns_error",
			second:  true,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-1", "", true)
			require.NoError(t, err)

			err = b.AssociateVPCWithHostedZone(hz.ID, "vpc-123", "us-east-1")
			require.NoError(t, err)

			if !tt.second {
				return
			}

			err = b.AssociateVPCWithHostedZone(hz.ID, "vpc-123", "us-east-1")
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, route53.ErrInvalidInput)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRefinement1_DeleteZone_CascadesKSK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantKSKCount  int
		deleteZone    bool
		kskCountAfter int
	}{
		{
			name:          "without_delete",
			deleteZone:    false,
			wantKSKCount:  1,
			kskCountAfter: 1,
		},
		{
			name:          "with_delete_cascades_ksk",
			deleteZone:    true,
			wantKSKCount:  1,
			kskCountAfter: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-1", "", false)
			require.NoError(t, err)

			_, err = b.CreateKeySigningKey(hz.ID, "caller-1", "key1", "arn:kms:test", "")
			require.NoError(t, err)

			assert.Equal(t, tt.wantKSKCount, route53.KeySigningKeyCount(b))

			if tt.deleteZone {
				require.NoError(t, b.DeleteHostedZone(hz.ID))
			}

			assert.Equal(t, tt.kskCountAfter, route53.KeySigningKeyCount(b))
		})
	}
}

func TestRefinement1_DeleteZone_CascadesVPC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		deleteZone        bool
		wantVPCAfterCount int
	}{
		{
			name:              "without_delete_vpc_remains",
			deleteZone:        false,
			wantVPCAfterCount: 1,
		},
		{
			name:              "with_delete_cascades_vpc",
			deleteZone:        true,
			wantVPCAfterCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-1", "", true)
			require.NoError(t, err)

			require.NoError(t, b.AssociateVPCWithHostedZone(hz.ID, "vpc-abc", "us-east-1"))
			assert.Equal(t, 1, route53.VPCAssociationCount(b))

			if tt.deleteZone {
				require.NoError(t, b.DeleteHostedZone(hz.ID))
			}

			assert.Equal(t, tt.wantVPCAfterCount, route53.VPCAssociationCount(b))
		})
	}
}

func TestRefinement1_DeleteZone_CascadesQueryLogging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		deleteZone        bool
		wantQLCAfterCount int
	}{
		{
			name:              "without_delete_qlc_remains",
			deleteZone:        false,
			wantQLCAfterCount: 1,
		},
		{
			name:              "with_delete_cascades_qlc",
			deleteZone:        true,
			wantQLCAfterCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-1", "", false)
			require.NoError(t, err)

			_, err = b.CreateQueryLoggingConfig(hz.ID, "arn:aws:logs:us-east-1:123456789012:log-group:test")
			require.NoError(t, err)
			assert.Equal(t, 1, route53.QueryLoggingConfigCount(b))

			if tt.deleteZone {
				require.NoError(t, b.DeleteHostedZone(hz.ID))
			}

			assert.Equal(t, tt.wantQLCAfterCount, route53.QueryLoggingConfigCount(b))
		})
	}
}

func TestRefinement1_ExtractOperation_CreateKSK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		method     string
		wantOp     string
		createZone bool
	}{
		{
			name:   "create_ksk",
			path:   "/2013-04-01/keysigningkey",
			method: http.MethodPost,
			wantOp: "CreateKeySigningKey",
		},
		{
			name:   "get_method_unknown",
			path:   "/2013-04-01/keysigningkey",
			method: http.MethodGet,
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, tt.method, tt.path, "")
			_ = rec

			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestRefinement1_ExtractOperation_ActivateKSK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "activate_ksk",
			path:   "/2013-04-01/keysigningkey/ZXXX/mykey/activate",
			method: http.MethodPost,
			wantOp: "ActivateKeySigningKey",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestRefinement1_ExtractOperation_AssociateVPC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "associate_vpc",
			path:   "/2013-04-01/hostedzone/ZXXX/associatevpc",
			method: http.MethodPost,
			wantOp: "AssociateVPCWithHostedZone",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestRefinement1_ExtractOperation_CreateCidrCollection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "create_cidr_collection",
			path:   "/2013-04-01/cidrcollection",
			method: http.MethodPost,
			wantOp: "CreateCidrCollection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestRefinement1_ExtractOperation_ChangeCidrCollection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "change_cidr_collection",
			path:   "/2013-04-01/cidrcollection/ZXXX",
			method: http.MethodPost,
			wantOp: "ChangeCidrCollection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestRefinement1_ExtractOperation_CreateQueryLogging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "create_query_logging",
			path:   "/2013-04-01/queryloggingconfig",
			method: http.MethodPost,
			wantOp: "CreateQueryLoggingConfig",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestRefinement1_ExtractOperation_CreateDelegationSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "create_delegation_set",
			path:   "/2013-04-01/delegationset",
			method: http.MethodPost,
			wantOp: "CreateReusableDelegationSet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestRefinement1_ExtractOperation_CreateTrafficPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "create_traffic_policy",
			path:   "/2013-04-01/trafficpolicy",
			method: http.MethodPost,
			wantOp: "CreateTrafficPolicy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestRefinement1_ExtractOperation_CreateTrafficPolicyVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "create_traffic_policy_version",
			path:   "/2013-04-01/trafficpolicy/some-policy-id",
			method: http.MethodPost,
			wantOp: "CreateTrafficPolicyVersion",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestRefinement1_ExtractOperation_CreateTrafficPolicyInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "create_traffic_policy_instance",
			path:   "/2013-04-01/trafficpolicyinstance",
			method: http.MethodPost,
			wantOp: "CreateTrafficPolicyInstance",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		addTagsBefore bool
		wantZoneCount int
	}{
		{
			name:          "reset_clears_zones_and_tags",
			addTagsBefore: true,
			wantZoneCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			// Create a zone and set a tag.
			rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
			require.Equal(t, http.StatusCreated, rec.Code)
			zoneID := extractZoneID(t, rec.Body.String())

			if tt.addTagsBefore {
				tagBody := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeTagsForResourceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <AddTags>
    <Tag><Key>env</Key><Value>test</Value></Tag>
  </AddTags>
</ChangeTagsForResourceRequest>`
				tagRec := send(t, h, http.MethodPost, "/2013-04-01/tags/hostedzone/"+zoneID, tagBody)
				assert.Equal(t, http.StatusOK, tagRec.Code)
			}

			h.Reset()

			// After reset, listing zones returns empty.
			listRec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone", "")
			assert.Equal(t, http.StatusOK, listRec.Code)

			type listResp struct {
				HostedZones []struct {
					ID string `xml:"Id"`
				} `xml:"HostedZones>HostedZone"`
			}

			var resp listResp
			require.NoError(t, xml.Unmarshal(listRec.Body.Bytes(), &resp))
			assert.Len(t, resp.HostedZones, tt.wantZoneCount)
		})
	}
}

func TestRefinement1_SnapshotRestore_KSK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantKSKCount int
	}{
		{
			name:         "ksk_survives_snapshot_restore",
			wantKSKCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-1", "", false)
			require.NoError(t, err)

			_, err = b.CreateKeySigningKey(hz.ID, "caller-1", "key1", "arn:kms:test", "ACTIVE")
			require.NoError(t, err)

			snap := b.Snapshot(t.Context())
			require.NotEmpty(t, snap)

			b2 := route53.NewInMemoryBackend()
			require.NoError(t, b2.Restore(t.Context(), snap))

			assert.Equal(t, tt.wantKSKCount, route53.KeySigningKeyCount(b2))
		})
	}
}

func TestRefinement1_SnapshotRestore_VPCAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantVPCCount int
	}{
		{
			name:         "vpc_association_survives_snapshot_restore",
			wantVPCCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-1", "", true)
			require.NoError(t, err)

			require.NoError(t, b.AssociateVPCWithHostedZone(hz.ID, "vpc-123", "us-east-1"))

			snap := b.Snapshot(t.Context())
			require.NotEmpty(t, snap)

			b2 := route53.NewInMemoryBackend()
			require.NoError(t, b2.Restore(t.Context(), snap))

			assert.Equal(t, tt.wantVPCCount, route53.VPCAssociationCount(b2))
		})
	}
}

func TestRefinement1_TrafficPolicyVersioning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		extraVersions   int
		wantPolicyCount int
	}{
		{
			name:            "single_version",
			extraVersions:   0,
			wantPolicyCount: 1,
		},
		{
			name:            "two_versions_same_policy_id",
			extraVersions:   1,
			wantPolicyCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>my-policy</Name>
  <Document>{"AWSPolicyFormatVersion":"2015-10-01"}</Document>
  <Comment>test policy</Comment>
</CreateTrafficPolicyRequest>`
			rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			tpID := extractTrafficPolicyID(t, rec.Body.String())

			for range tt.extraVersions {
				vBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyVersionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Document>{"AWSPolicyFormatVersion":"2015-10-01","v2":true}</Document>
  <Comment>version 2</Comment>
</CreateTrafficPolicyVersionRequest>`
				vRec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy/"+tpID, vBody)
				require.Equal(t, http.StatusCreated, vRec.Code)
			}

			// All versions are under the same policy ID.
			assert.Equal(t, tt.wantPolicyCount, route53.TrafficPolicyCount(h.Backend.(*route53.InMemoryBackend)))
		})
	}
}

func TestRefinement1_DelegationSetResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantIDInResp bool
		wantCode     int
	}{
		{
			name:         "response_includes_id",
			wantIDInResp: true,
			wantCode:     http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateReusableDelegationSetRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>ds-ref-test</CallerReference>
</CreateReusableDelegationSetRequest>`

			rec := send(t, h, http.MethodPost, "/2013-04-01/delegationset", body)
			require.Equal(t, tt.wantCode, rec.Code)

			type dsResp struct {
				DelegationSet struct {
					ID          string   `xml:"Id"`
					NameServers []string `xml:"NameServers>NameServer"`
				} `xml:"DelegationSet"`
			}

			var resp dsResp
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

			if tt.wantIDInResp {
				assert.NotEmpty(t, resp.DelegationSet.ID)
				assert.Contains(t, resp.DelegationSet.ID, "delegationset")
				assert.NotEmpty(t, resp.DelegationSet.NameServers)
			}
		})
	}
}

func TestRefinement1_ChangeCidrCollection_ParseBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
		wantV2   bool
	}{
		{
			name: "with_changes_body",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<ChangeCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Changes>
    <Change>
      <LocationName>us-east-1</LocationName>
      <Action>PUT</Action>
      <CidrList><Cidr>10.0.0.0/8</Cidr></CidrList>
    </Change>
  </Changes>
</ChangeCidrCollectionRequest>`,
			wantCode: http.StatusOK,
			wantV2:   false,
		},
		{
			name:     "empty_body_still_works",
			body:     "",
			wantCode: http.StatusOK,
			wantV2:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			// First create a collection.
			createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>test-cidrs</Name>
  <CallerReference>cidr-ref-parsebody</CallerReference>
</CreateCidrCollectionRequest>`
			createRec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection", createBody)
			require.Equal(t, http.StatusCreated, createRec.Code)

			type colResp struct {
				Collection struct {
					ID string `xml:"Id"`
				} `xml:"Collection"`
			}

			var colR colResp
			require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &colR))
			collectionID := colR.Collection.ID

			rec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection/"+collectionID, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				assert.Contains(t, rec.Body.String(), "ChangeCidrCollectionResponse")
			}
		})
	}
}

func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantEmpty bool
	}{
		{
			name:      "account_id_non_empty",
			wantEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()

			if tt.wantEmpty {
				assert.Empty(t, b.AccountID())
			} else {
				assert.NotEmpty(t, b.AccountID())
			}
		})
	}
}

func TestRefinement1_Region(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantRegion string
	}{
		{
			name:       "region_is_us_east_1",
			wantRegion: "us-east-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			assert.Equal(t, tt.wantRegion, b.Region())
		})
	}
}

func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantLen int
	}{
		{
			name:    "ops_count_is_71",
			wantLen: 71,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			assert.Equal(t, tt.wantLen, route53.HandlerOpsLen(h))
		})
	}
}

// extractOpFromPath calls ExtractOperation on the handler using a synthesized echo context.
func extractOpFromPath(t *testing.T, h *route53.Handler, method, path string) string {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	return h.ExtractOperation(c)
}
