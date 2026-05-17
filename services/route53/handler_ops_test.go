package route53_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// helpers shared across new-ops tests

func createZoneForOpsTest(t *testing.T, h *route53.Handler) string {
	t.Helper()

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)

	return extractZoneID(t, rec.Body.String())
}

func createKSKForOpsTest(t *testing.T, h *route53.Handler, zoneID, name string) {
	t.Helper()

	body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateKeySigningKeyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>ksk-ref-%s</CallerReference>
  <HostedZoneId>%s</HostedZoneId>
  <KeyManagementServiceArn>arn:aws:kms:us-east-1:123456789012:key/mrk-abc123</KeyManagementServiceArn>
  <Name>%s</Name>
  <Status>ACTIVE</Status>
</CreateKeySigningKeyRequest>`, name, zoneID, name)

	rec := send(t, h, http.MethodPost, "/2013-04-01/keysigningkey", body)
	require.Equal(t, http.StatusCreated, rec.Code)
}

func createCidrCollectionForOpsTest(t *testing.T, h *route53.Handler, name string) string {
	t.Helper()

	body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>%s</Name>
  <CallerReference>cidr-ref-%s</CallerReference>
</CreateCidrCollectionRequest>`, name, name)

	rec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	type colResp struct {
		Collection struct {
			ID string `xml:"Id"`
		} `xml:"Collection"`
	}

	var resp colResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	return resp.Collection.ID
}

func createTPForOpsTest(t *testing.T, h *route53.Handler, name string) string {
	t.Helper()

	body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>%s</Name>
  <Document>{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{},"Rules":{}}</Document>
  <Comment>test</Comment>
</CreateTrafficPolicyRequest>`, name)

	rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	type tpResp struct {
		TrafficPolicy struct {
			ID string `xml:"Id"`
		} `xml:"TrafficPolicy"`
	}

	var resp tpResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	return resp.TrafficPolicy.ID
}

// TestRoute53_EnableDisableGetDNSSEC covers EnableHostedZoneDNSSEC,
// DisableHostedZoneDNSSEC, and GetDNSSEC.
func TestRoute53_EnableDisableGetDNSSEC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *route53.Handler) string
		name         string
		method       string
		pathSuffix   string
		wantContains []string
		wantCode     int
	}{
		{
			name: "enable_dnssec_success",
			setup: func(t *testing.T, h *route53.Handler) string {
				t.Helper()

				zoneID := createZoneForOpsTest(t, h)
				createKSKForOpsTest(t, h, zoneID, "main-key")
				return zoneID
			},
			method:       http.MethodPost,
			pathSuffix:   "/enable-dnssec",
			wantCode:     http.StatusOK,
			wantContains: []string{"EnableHostedZoneDNSSECResponse", "INSYNC"},
		},
		{
			name: "enable_dnssec_not_found",
			setup: func(t *testing.T, _ *route53.Handler) string {
				t.Helper()

				return "ZNONEXISTENT"
			},
			method:     http.MethodPost,
			pathSuffix: "/enable-dnssec",
			wantCode:   http.StatusNotFound,
		},
		{
			name: "disable_dnssec_success",
			setup: func(t *testing.T, h *route53.Handler) string {
				t.Helper()

				zoneID := createZoneForOpsTest(t, h)
				createKSKForOpsTest(t, h, zoneID, "disable-key")
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/enable-dnssec", "")
				require.Equal(t, http.StatusOK, rec.Code)

				return zoneID
			},
			method:       http.MethodPost,
			pathSuffix:   "/disable-dnssec",
			wantCode:     http.StatusOK,
			wantContains: []string{"DisableHostedZoneDNSSECResponse", "INSYNC"},
		},
		{
			name: "get_dnssec_not_signed",
			setup: func(t *testing.T, h *route53.Handler) string {
				t.Helper()

				return createZoneForOpsTest(t, h)
			},
			method:       http.MethodGet,
			pathSuffix:   "/dnssec",
			wantCode:     http.StatusOK,
			wantContains: []string{"GetDNSSECResponse", "NOT_SIGNING"},
		},
		{
			name: "get_dnssec_signing",
			setup: func(t *testing.T, h *route53.Handler) string {
				t.Helper()

				zoneID := createZoneForOpsTest(t, h)
				createKSKForOpsTest(t, h, zoneID, "signing-key")
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/enable-dnssec", "")
				require.Equal(t, http.StatusOK, rec.Code)

				return zoneID
			},
			method:       http.MethodGet,
			pathSuffix:   "/dnssec",
			wantCode:     http.StatusOK,
			wantContains: []string{"GetDNSSECResponse", "SIGNING"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			zoneID := tt.setup(t, h)
			path := "/2013-04-01/hostedzone/" + zoneID + tt.pathSuffix
			rec := send(t, h, tt.method, path, "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestRoute53_DeactivateDeleteKeySigningKey covers DeactivateKeySigningKey
// and DeleteKeySigningKey.
func TestRoute53_DeactivateDeleteKeySigningKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *route53.Handler) (zoneID, kskName string)
		name         string
		method       string
		suffix       string
		wantContains []string
		wantCode     int
	}{
		{
			name: "deactivate_ksk_success",
			setup: func(t *testing.T, h *route53.Handler) (string, string) {
				t.Helper()

				zoneID := createZoneForOpsTest(t, h)
				kskName := "mykey"
				createKSKForOpsTest(t, h, zoneID, kskName)
				// Activate first
				rec := send(t, h, http.MethodPost,
					"/2013-04-01/keysigningkey/"+zoneID+"/"+kskName+"/activate", "")
				require.Equal(t, http.StatusOK, rec.Code)

				return zoneID, kskName
			},
			method:       http.MethodPost,
			suffix:       "/deactivate",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeactivateKeySigningKeyResponse", "INSYNC"},
		},
		{
			name: "deactivate_ksk_not_found",
			setup: func(t *testing.T, _ *route53.Handler) (string, string) {
				t.Helper()

				return "ZNONEXISTENT", "badkey"
			},
			method:   http.MethodPost,
			suffix:   "/deactivate",
			wantCode: http.StatusNotFound,
		},
		{
			name: "delete_ksk_success",
			setup: func(t *testing.T, h *route53.Handler) (string, string) {
				t.Helper()

				zoneID := createZoneForOpsTest(t, h)
				kskName := "delkey"
				createKSKForOpsTest(t, h, zoneID, kskName)

				return zoneID, kskName
			},
			method:       http.MethodDelete,
			suffix:       "",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteKeySigningKeyResponse"},
		},
		{
			name: "delete_ksk_not_found",
			setup: func(t *testing.T, _ *route53.Handler) (string, string) {
				t.Helper()

				return "ZNONEXISTENT", "badkey"
			},
			method:   http.MethodDelete,
			suffix:   "",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			zoneID, kskName := tt.setup(t, h)
			path := "/2013-04-01/keysigningkey/" + zoneID + "/" + kskName + tt.suffix
			rec := send(t, h, tt.method, path, "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestRoute53_TrafficPolicyGetDelete covers GetTrafficPolicy and
// DeleteTrafficPolicy.
func TestRoute53_TrafficPolicyGetDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *route53.Handler) (id string, version int)
		name         string
		method       string
		wantContains []string
		wantCode     int
	}{
		{
			name: "get_traffic_policy_success",
			setup: func(t *testing.T, h *route53.Handler) (string, int) {
				t.Helper()

				return createTPForOpsTest(t, h, "my-policy"), 1
			},
			method:       http.MethodGet,
			wantCode:     http.StatusOK,
			wantContains: []string{"TrafficPolicy", "my-policy"},
		},
		{
			name: "get_traffic_policy_not_found",
			setup: func(t *testing.T, _ *route53.Handler) (string, int) {
				t.Helper()

				return "nonexistent-id", 1
			},
			method:   http.MethodGet,
			wantCode: http.StatusNotFound,
		},
		{
			name: "delete_traffic_policy_success",
			setup: func(t *testing.T, h *route53.Handler) (string, int) {
				t.Helper()

				return createTPForOpsTest(t, h, "del-policy"), 1
			},
			method:       http.MethodDelete,
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteTrafficPolicyResponse"},
		},
		{
			name: "delete_traffic_policy_not_found",
			setup: func(t *testing.T, _ *route53.Handler) (string, int) {
				t.Helper()

				return "nonexistent-id", 1
			},
			method:   http.MethodDelete,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			id, version := tt.setup(t, h)
			path := fmt.Sprintf("/2013-04-01/trafficpolicy/%s/%d", id, version)
			rec := send(t, h, tt.method, path, "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestRoute53_ListTrafficPolicies covers ListTrafficPolicies.
func TestRoute53_ListTrafficPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains []string
		setupCount   int
		wantCode     int
	}{
		{
			name:         "list_empty",
			setupCount:   0,
			wantCode:     http.StatusOK,
			wantContains: []string{"ListTrafficPoliciesResponse"},
		},
		{
			name:         "list_with_policies",
			setupCount:   2,
			wantCode:     http.StatusOK,
			wantContains: []string{"ListTrafficPoliciesResponse", "policy-0", "policy-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			for i := range tt.setupCount {
				createTPForOpsTest(t, h, fmt.Sprintf("policy-%d", i))
			}

			rec := send(t, h, http.MethodGet, "/2013-04-01/trafficpolicies", "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestRoute53_ListTrafficPolicyVersions covers ListTrafficPolicyVersions.
func TestRoute53_ListTrafficPolicyVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		useID         string
		wantContains  []string
		setupVersions int
		wantCode      int
	}{
		{
			name:          "list_versions_success",
			setupVersions: 2,
			wantCode:      http.StatusOK,
			wantContains:  []string{"ListTrafficPolicyVersionsResponse", "TrafficPolicy"},
		},
		{
			name:     "list_versions_not_found",
			useID:    "nonexistent-id",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			id := tt.useID
			if id == "" {
				id = createTPForOpsTest(t, h, "versioned-policy")
				// Create additional versions
				for range tt.setupVersions - 1 {
					body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyVersionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Document>{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{},"Rules":{}}</Document>
  <Comment>v2</Comment>
</CreateTrafficPolicyVersionRequest>`
					rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy/"+id, body)
					require.Equal(t, http.StatusCreated, rec.Code)
				}
			}

			rec := send(t, h, http.MethodGet, "/2013-04-01/trafficpolicies/"+id+"/versions", "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestRoute53_TrafficPolicyInstanceGetDelete covers GetTrafficPolicyInstance
// and DeleteTrafficPolicyInstance.
func TestRoute53_TrafficPolicyInstanceGetDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		method       string
		useID        string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "get_instance_success",
			method:       http.MethodGet,
			wantCode:     http.StatusOK,
			wantContains: []string{"TrafficPolicyInstance"},
		},
		{
			name:     "get_instance_not_found",
			method:   http.MethodGet,
			useID:    "nonexistent-id",
			wantCode: http.StatusNotFound,
		},
		{
			name:         "delete_instance_success",
			method:       http.MethodDelete,
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteTrafficPolicyInstanceResponse"},
		},
		{
			name:     "delete_instance_not_found",
			method:   http.MethodDelete,
			useID:    "nonexistent-id",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			id := tt.useID
			if id == "" {
				zoneID := createZoneForOpsTest(t, h)
				tpID := createTPForOpsTest(t, h, "inst-policy")

				body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyInstanceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>%s</HostedZoneId>
  <Name>www.example.com</Name>
  <TrafficPolicyId>%s</TrafficPolicyId>
  <TrafficPolicyVersion>1</TrafficPolicyVersion>
  <TTL>300</TTL>
</CreateTrafficPolicyInstanceRequest>`, zoneID, tpID)

				rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicyinstance", body)
				require.Equal(t, http.StatusCreated, rec.Code)

				type instResp struct {
					TrafficPolicyInstance struct {
						ID string `xml:"Id"`
					} `xml:"TrafficPolicyInstance"`
				}

				var resp instResp
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				id = resp.TrafficPolicyInstance.ID
			}

			rec := send(t, h, tt.method, "/2013-04-01/trafficpolicyinstance/"+id, "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestRoute53_ListTrafficPolicyInstances covers ListTrafficPolicyInstances.
func TestRoute53_ListTrafficPolicyInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains []string
		setupCount   int
		wantCode     int
	}{
		{
			name:         "list_empty",
			setupCount:   0,
			wantCode:     http.StatusOK,
			wantContains: []string{"ListTrafficPolicyInstancesResponse"},
		},
		{
			name:         "list_with_instances",
			setupCount:   2,
			wantCode:     http.StatusOK,
			wantContains: []string{"ListTrafficPolicyInstancesResponse", "TrafficPolicyInstance"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			for i := range tt.setupCount {
				zoneID := createZoneForOpsTest(t, h)
				tpID := createTPForOpsTest(t, h, fmt.Sprintf("inst-tp-%d", i))

				body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyInstanceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>%s</HostedZoneId>
  <Name>www%d.example.com</Name>
  <TrafficPolicyId>%s</TrafficPolicyId>
  <TrafficPolicyVersion>1</TrafficPolicyVersion>
  <TTL>300</TTL>
</CreateTrafficPolicyInstanceRequest>`, zoneID, i, tpID)

				rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicyinstance", body)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := send(t, h, http.MethodGet, "/2013-04-01/trafficpolicyinstances", "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestRoute53_GetTrafficPolicyInstanceCount covers GetTrafficPolicyInstanceCount.
func TestRoute53_GetTrafficPolicyInstanceCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains []string
		setupCount   int
		wantCode     int
	}{
		{
			name:         "count_zero",
			setupCount:   0,
			wantCode:     http.StatusOK,
			wantContains: []string{"GetTrafficPolicyInstanceCountResponse", "0"},
		},
		{
			name:         "count_one",
			setupCount:   1,
			wantCode:     http.StatusOK,
			wantContains: []string{"GetTrafficPolicyInstanceCountResponse", "1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			for i := range tt.setupCount {
				zoneID := createZoneForOpsTest(t, h)
				tpID := createTPForOpsTest(t, h, fmt.Sprintf("count-tp-%d", i))

				body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyInstanceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>%s</HostedZoneId>
  <Name>count%d.example.com</Name>
  <TrafficPolicyId>%s</TrafficPolicyId>
  <TrafficPolicyVersion>1</TrafficPolicyVersion>
  <TTL>300</TTL>
</CreateTrafficPolicyInstanceRequest>`, zoneID, i, tpID)

				rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicyinstance", body)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := send(t, h, http.MethodGet, "/2013-04-01/trafficpolicyinstancecount", "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestRoute53_ListCidrCollections covers ListCidrCollections.
func TestRoute53_ListCidrCollections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains []string
		setupCount   int
		wantCode     int
	}{
		{
			name:         "list_empty",
			setupCount:   0,
			wantCode:     http.StatusOK,
			wantContains: []string{"ListCidrCollectionsResponse"},
		},
		{
			name:         "list_with_collections",
			setupCount:   2,
			wantCode:     http.StatusOK,
			wantContains: []string{"ListCidrCollectionsResponse", "cidr-0", "cidr-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			for i := range tt.setupCount {
				createCidrCollectionForOpsTest(t, h, fmt.Sprintf("cidr-%d", i))
			}

			rec := send(t, h, http.MethodGet, "/2013-04-01/cidrcollection", "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestRoute53_DeleteCidrCollection covers DeleteCidrCollection.
func TestRoute53_DeleteCidrCollection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		useID        string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "delete_success",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteCidrCollectionResponse"},
		},
		{
			name:     "delete_not_found",
			useID:    "nonexistent-id",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			id := tt.useID
			if id == "" {
				id = createCidrCollectionForOpsTest(t, h, "to-delete")
			}

			rec := send(t, h, http.MethodDelete, "/2013-04-01/cidrcollection/"+id, "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}
