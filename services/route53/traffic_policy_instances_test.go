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

func TestCreateTrafficPolicyInstance_Duplicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "duplicate_name_in_zone_rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()

			hz, err := b.CreateHostedZone("example.com", "ref-tpi", "", false, "", "", "")
			require.NoError(t, err)

			tp, err := b.CreateTrafficPolicy(
				"tpi-dup-policy",
				`{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{},"Rules":{}}`,
				"",
			)
			require.NoError(t, err)

			_, err = b.CreateTrafficPolicyInstance(hz.ID, "www.example.com", tp.ID, tp.Version, 60)
			require.NoError(t, err)

			_, err = b.CreateTrafficPolicyInstance(hz.ID, "www.example.com", tp.ID, tp.Version, 60)
			require.Error(t, err)
			assert.ErrorIs(t, err, route53.ErrTrafficPolicyInstanceAlreadyExists)
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

func TestExtractOperation_CreateTrafficPolicyInstance(t *testing.T) {
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

	// Filter by hosted zone. route53@v1.65.6 serializers.go's
	// awsRestxml_serializeOpHttpBindingsListTrafficPolicyInstancesByHostedZoneInput
	// binds HostedZoneId to query key "id", not "hostedzoneid" -- this test
	// previously used "hostedzoneid", which matched the handler's
	// then-matching bug rather than what a real client sends.
	rec = send(t, h, http.MethodGet, "/2013-04-01/trafficpolicyinstances/hostedzone?id="+zoneID, "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "filtered.example.com")

	// Filter by non-matching hosted zone.
	rec = send(t, h, http.MethodGet, "/2013-04-01/trafficpolicyinstances/hostedzone?id=ZNONEXISTENT", "")
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

func TestCreateTrafficPolicyInstance_TTLValidation(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	tpBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>ttl-test-policy</Name>
  <Document>{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{},"Rules":{}}</Document>
</CreateTrafficPolicyRequest>`

	rec = send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy", tpBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var tpr struct {
		TrafficPolicy struct {
			ID string `xml:"Id"`
		} `xml:"TrafficPolicy"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &tpr))
	tpID := tpr.TrafficPolicy.ID

	// TTL = 0 must fail.
	instBody := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyInstanceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>%s</HostedZoneId>
  <Name>www.example.com</Name>
  <TrafficPolicyId>%s</TrafficPolicyId>
  <TrafficPolicyVersion>1</TrafficPolicyVersion>
  <TTL>0</TTL>
</CreateTrafficPolicyInstanceRequest>`, zoneID, tpID)

	rec = send(t, h, http.MethodPost, "/2013-04-01/trafficpolicyinstance", instBody)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidInput")
}

func TestRoute53_CreateTrafficPolicyInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn      func(h *route53.Handler) (zoneID, policyID string)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "create_tpi_success",
			setupFn: func(h *route53.Handler) (string, string) {
				zoneRec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
				require.Equal(t, http.StatusCreated, zoneRec.Code)
				zoneID := extractZoneID(t, zoneRec.Body.String())

				tpBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>tpi-policy</Name>
  <Document>{"AWSPolicyFormatVersion":"2015-10-01"}</Document>
</CreateTrafficPolicyRequest>`
				tpRec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy", tpBody)
				require.Equal(t, http.StatusCreated, tpRec.Code)
				policyID := extractTrafficPolicyID(t, tpRec.Body.String())

				return zoneID, policyID
			},
			wantCode:     http.StatusCreated,
			wantContains: []string{"CreateTrafficPolicyInstanceResponse", "Applied"},
		},
		{
			name: "create_tpi_zone_not_found",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyInstanceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>ZNONEXISTENT</HostedZoneId>
  <Name>tpi.example.com</Name>
  <TrafficPolicyId>FAKEPOLICYID</TrafficPolicyId>
  <TrafficPolicyVersion>1</TrafficPolicyVersion>
  <TTL>300</TTL>
</CreateTrafficPolicyInstanceRequest>`,
			wantCode: http.StatusNotFound,
		},
		{
			name: "create_tpi_invalid_xml",
			body: "not-xml",
			setupFn: func(h *route53.Handler) (string, string) {
				zoneRec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
				require.Equal(t, http.StatusCreated, zoneRec.Code)

				return extractZoneID(t, zoneRec.Body.String()), ""
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			body := tt.body

			if tt.setupFn != nil {
				zoneID, policyID := tt.setupFn(h)
				if body == "" {
					body = `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyInstanceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>` + zoneID + `</HostedZoneId>
  <Name>api.example.com</Name>
  <TrafficPolicyId>` + policyID + `</TrafficPolicyId>
  <TrafficPolicyVersion>1</TrafficPolicyVersion>
  <TTL>60</TTL>
</CreateTrafficPolicyInstanceRequest>`
				}
			}

			rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicyinstance", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}
