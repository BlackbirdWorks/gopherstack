package route53_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

func TestHealthCheckAlreadyExists_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	const body1 = `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>shared-hc-ref</CallerReference>
  <HealthCheckConfig><Type>HTTP</Type><Port>80</Port></HealthCheckConfig>
</CreateHealthCheckRequest>`

	const body2 = `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>shared-hc-ref</CallerReference>
  <HealthCheckConfig><Type>HTTPS</Type><Port>443</Port></HealthCheckConfig>
</CreateHealthCheckRequest>`

	first := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", body1)
	require.Equal(t, http.StatusCreated, first.Code)

	second := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", body2)
	assert.Equal(t, http.StatusConflict, second.Code)
	assert.Contains(t, second.Body.String(), "HealthCheckAlreadyExists")
}

func TestCreateHealthCheck_CalculatedThresholdExceedsChildren(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "threshold_exceeds_child_count_rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()

			child1, err := b.CreateHealthCheck("child-ref-1", route53.HealthCheckConfig{
				Type: route53.HealthCheckTypeHTTP,
				Port: 80,
			})
			require.NoError(t, err)

			child2, err := b.CreateHealthCheck("child-ref-2", route53.HealthCheckConfig{
				Type: route53.HealthCheckTypeHTTP,
				Port: 80,
			})
			require.NoError(t, err)

			const thresholdExceedsChildCount = 5

			_, err = b.CreateHealthCheck("calc-ref-1", route53.HealthCheckConfig{
				Type:              route53.HealthCheckTypeCalculated,
				ChildHealthChecks: []string{child1.ID, child2.ID},
				HealthThreshold:   thresholdExceedsChildCount,
			})
			require.Error(t, err)
			assert.ErrorIs(t, err, route53.ErrInvalidInput)
		})
	}
}

func TestCreateHealthCheck_DuplicateCallerReference(t *testing.T) {
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

// TestCreateHealthCheck_DuplicateCallerReference_DifferentConfig
// verifies real AWS behavior: reusing a CallerReference with a *different*
// HealthCheckConfig returns HealthCheckAlreadyExists (409) rather than
// silently returning (or silently overwriting) the original health check. A
// prior version of this test asserted the request was still idempotent when
// Type/Port differed, which is not how Route 53 behaves.
func TestCreateHealthCheck_DuplicateCallerReference_DifferentConfig(t *testing.T) {
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

func TestCreateHealthCheck_UniqueCallerReference_CreatesNew(t *testing.T) {
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

func TestCreateHealthCheck_DuplicateCallerReference_HTTPRoundTrip(t *testing.T) {
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

func TestHealthCheckCRUD_MultipleTypes(t *testing.T) {
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

func TestHealthCheckCount(t *testing.T) {
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

func TestCreateHealthCheck_AllConfigFields(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>ref-full</CallerReference>
  <HealthCheckConfig>
    <Type>HTTP</Type>
    <IPAddress>1.2.3.4</IPAddress>
    <Port>80</Port>
    <ResourcePath>/health</ResourcePath>
    <FullyQualifiedDomainName>example.com</FullyQualifiedDomainName>
    <SearchString>OK</SearchString>
    <RequestInterval>30</RequestInterval>
    <FailureThreshold>3</FailureThreshold>
    <EnableSNI>true</EnableSNI>
    <Disabled>false</Disabled>
    <MeasureLatency>true</MeasureLatency>
    <InsufficientDataHealthStatus>Healthy</InsufficientDataHealthStatus>
    <Regions>
      <Region>us-east-1</Region>
      <Region>eu-west-1</Region>
    </Regions>
  </HealthCheckConfig>
</CreateHealthCheckRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Extract health check ID from Location header.
	loc := rec.Header().Get("Location")
	require.NotEmpty(t, loc)
	id := loc[strings.LastIndex(loc, "/")+1:]

	// Get the health check back and verify all fields round-tripped.
	rec = send(t, h, http.MethodGet, "/2013-04-01/healthcheck/"+id, "")
	require.Equal(t, http.StatusOK, rec.Code)
	body2 := rec.Body.String()

	assert.Contains(t, body2, "1.2.3.4")
	assert.Contains(t, body2, "/health")
	assert.Contains(t, body2, "OK")
	assert.Contains(t, body2, "<EnableSNI>true</EnableSNI>")
	assert.Contains(t, body2, "<MeasureLatency>true</MeasureLatency>")
	assert.Contains(t, body2, "<InsufficientDataHealthStatus>Healthy</InsufficientDataHealthStatus>")
	assert.Contains(t, body2, "us-east-1")
	assert.Contains(t, body2, "eu-west-1")
}

func TestCreateHealthCheck_CloudWatchMetric_RequiresAlarmIdentifier(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	// Missing AlarmIdentifier.
	body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>ref-cw</CallerReference>
  <HealthCheckConfig>
    <Type>CLOUDWATCH_METRIC</Type>
  </HealthCheckConfig>
</CreateHealthCheckRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidInput")
}

func TestCreateHealthCheck_CloudWatchMetric_WithAlarmIdentifier(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>ref-cw2</CallerReference>
  <HealthCheckConfig>
    <Type>CLOUDWATCH_METRIC</Type>
    <AlarmIdentifier>
      <Name>my-alarm</Name>
      <Region>us-east-1</Region>
    </AlarmIdentifier>
    <InsufficientDataHealthStatus>Unhealthy</InsufficientDataHealthStatus>
  </HealthCheckConfig>
</CreateHealthCheckRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", body)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Contains(t, rec.Body.String(), "my-alarm")
	assert.Contains(t, rec.Body.String(), "Unhealthy")
}

func TestCreateHealthCheck_RecoveryControl_RequiresRoutingControlArn(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>ref-rc</CallerReference>
  <HealthCheckConfig>
    <Type>RECOVERY_CONTROL</Type>
  </HealthCheckConfig>
</CreateHealthCheckRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidInput")
}

func TestCreateHealthCheck_RecoveryControl_WithArn(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>ref-rc2</CallerReference>
  <HealthCheckConfig>
    <Type>RECOVERY_CONTROL</Type>
    <RoutingControlArn>arn:aws:route53-recovery-control::123:controlpanel/abc/routingcontrol/xyz</RoutingControlArn>
  </HealthCheckConfig>
</CreateHealthCheckRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", body)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Contains(t, rec.Body.String(), "RECOVERY_CONTROL")
}

func TestCreateHealthCheck_InsufficientDataHealthStatus_Invalid(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>ref-idhs</CallerReference>
  <HealthCheckConfig>
    <Type>HTTP</Type>
    <IPAddress>1.2.3.4</IPAddress>
    <Port>80</Port>
    <InsufficientDataHealthStatus>BadValue</InsufficientDataHealthStatus>
  </HealthCheckConfig>
</CreateHealthCheckRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidInput")
}

const createHealthCheckXML = `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>hc-ref-1</CallerReference>
  <HealthCheckConfig>
    <Type>HTTP</Type>
    <IPAddress>192.0.2.1</IPAddress>
    <Port>80</Port>
    <ResourcePath>/health</ResourcePath>
    <RequestInterval>30</RequestInterval>
    <FailureThreshold>3</FailureThreshold>
  </HealthCheckConfig>
</CreateHealthCheckRequest>`

const hcMissingCallerRefXML = `<?xml version="1.0"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HealthCheckConfig><Type>HTTP</Type></HealthCheckConfig>
</CreateHealthCheckRequest>`

const hcMissingTypeXML = `<?xml version="1.0"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>ref</CallerReference>
</CreateHealthCheckRequest>`

// extractHealthCheckID parses the health check ID from a CreateHealthCheckResponse XML body.
func extractHealthCheckID(t *testing.T, body string) string {
	t.Helper()

	type createHCResp struct {
		HealthCheck struct {
			ID string `xml:"Id"`
		} `xml:"HealthCheck"`
	}

	var resp createHCResp
	require.NoError(t, xml.Unmarshal([]byte(body), &resp))
	require.NotEmpty(t, resp.HealthCheck.ID)

	return resp.HealthCheck.ID
}

func TestHealthCheck_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		method       string
		path         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "create_health_check",
			method:       http.MethodPost,
			path:         "/2013-04-01/healthcheck",
			body:         createHealthCheckXML,
			wantCode:     http.StatusCreated,
			wantContains: []string{"CreateHealthCheckResponse", "HTTP", "192.0.2.1"},
		},
		{
			name:         "list_health_checks_empty",
			method:       http.MethodGet,
			path:         "/2013-04-01/healthcheck",
			wantCode:     http.StatusOK,
			wantContains: []string{"ListHealthChecksResponse"},
		},
		{
			name:     "create_missing_caller_reference",
			method:   http.MethodPost,
			path:     "/2013-04-01/healthcheck",
			body:     hcMissingCallerRefXML,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create_missing_type",
			method:   http.MethodPost,
			path:     "/2013-04-01/healthcheck",
			body:     hcMissingTypeXML,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create_invalid_xml",
			method:   http.MethodPost,
			path:     "/2013-04-01/healthcheck",
			body:     "not-xml",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get_nonexistent",
			method:   http.MethodGet,
			path:     "/2013-04-01/healthcheck/DOESNOTEXIST",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete_nonexistent",
			method:   http.MethodDelete,
			path:     "/2013-04-01/healthcheck/DOESNOTEXIST",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "status_nonexistent",
			method:   http.MethodGet,
			path:     "/2013-04-01/healthcheck/DOESNOTEXIST/status",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "unsupported_method_healthcheck_root",
			method:   http.MethodDelete,
			path:     "/2013-04-01/healthcheck",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "unsupported_method_healthcheck_status",
			method:   http.MethodPost,
			path:     "/2013-04-01/healthcheck/SOMEID/status",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "unsupported_method_healthcheck_resource",
			method:   http.MethodPut,
			path:     "/2013-04-01/healthcheck/SOMEID",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestHealthCheck_FullLifecycle(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	// Create.
	createRec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", createHealthCheckXML)
	require.Equal(t, http.StatusCreated, createRec.Code)
	hcID := extractHealthCheckID(t, createRec.Body.String())

	// Get.
	getRec := send(t, h, http.MethodGet, "/2013-04-01/healthcheck/"+hcID, "")
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.Contains(t, getRec.Body.String(), "192.0.2.1")

	// List.
	listRec := send(t, h, http.MethodGet, "/2013-04-01/healthcheck", "")
	require.Equal(t, http.StatusOK, listRec.Code)
	assert.Contains(t, listRec.Body.String(), hcID)

	// Status.
	statusRec := send(t, h, http.MethodGet, "/2013-04-01/healthcheck/"+hcID+"/status", "")
	require.Equal(t, http.StatusOK, statusRec.Code)
	assert.Contains(t, statusRec.Body.String(), "Healthy")

	// Update.
	updateBody := `<?xml version="1.0" encoding="UTF-8"?>
<UpdateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <IPAddress>10.0.0.1</IPAddress>
  <Port>443</Port>
</UpdateHealthCheckRequest>`
	updateRec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck/"+hcID, updateBody)
	require.Equal(t, http.StatusOK, updateRec.Code)
	assert.Contains(t, updateRec.Body.String(), "10.0.0.1")

	// Delete.
	delRec := send(t, h, http.MethodDelete, "/2013-04-01/healthcheck/"+hcID, "")
	require.Equal(t, http.StatusOK, delRec.Code)

	// Should be gone.
	getRec2 := send(t, h, http.MethodGet, "/2013-04-01/healthcheck/"+hcID, "")
	assert.Equal(t, http.StatusNotFound, getRec2.Code)
}

func TestHealthCheck_UpdateInvalidXML(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	createRec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", createHealthCheckXML)
	require.Equal(t, http.StatusCreated, createRec.Code)
	hcID := extractHealthCheckID(t, createRec.Body.String())

	rec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck/"+hcID, "not-xml")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHealthCheck_UpdateNonexistent(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	updateBody := `<?xml version="1.0" encoding="UTF-8"?>
<UpdateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <IPAddress>10.0.0.1</IPAddress>
</UpdateHealthCheckRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck/DOESNOTEXIST", updateBody)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHealthCheck_UpdateInverted_PreservesExisting(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	// Create with Inverted=true via a dedicated create body.
	createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>inverted-hc-ref</CallerReference>
  <HealthCheckConfig>
    <Type>HTTP</Type>
    <IPAddress>192.0.2.1</IPAddress>
    <Port>80</Port>
    <Inverted>true</Inverted>
  </HealthCheckConfig>
</CreateHealthCheckRequest>`

	createRec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", createBody)
	require.Equal(t, http.StatusCreated, createRec.Code)
	hcID := extractHealthCheckID(t, createRec.Body.String())

	// Update without sending <Inverted> — existing Inverted value must be preserved.
	updateBody := `<?xml version="1.0" encoding="UTF-8"?>
<UpdateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <IPAddress>10.0.0.1</IPAddress>
</UpdateHealthCheckRequest>`

	updateRec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck/"+hcID, updateBody)
	require.Equal(t, http.StatusOK, updateRec.Code)

	// Verify the updated IP while Inverted is still true.
	getRec := send(t, h, http.MethodGet, "/2013-04-01/healthcheck/"+hcID, "")
	require.Equal(t, http.StatusOK, getRec.Code)
	body := getRec.Body.String()
	assert.Contains(t, body, "10.0.0.1")
	assert.Contains(t, body, "<Inverted>true</Inverted>")
}

func TestHandler_IAMAction_HealthCheck(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{
			name:   "create_health_check",
			method: http.MethodPost,
			path:   "/2013-04-01/healthcheck",
			want:   "route53:CreateHealthCheck",
		},
		{
			name:   "list_health_checks",
			method: http.MethodGet,
			path:   "/2013-04-01/healthcheck",
			want:   "route53:ListHealthChecks",
		},
		{
			name:   "get_health_check",
			method: http.MethodGet,
			path:   "/2013-04-01/healthcheck/HCID123",
			want:   "route53:GetHealthCheck",
		},
		{
			name:   "delete_health_check",
			method: http.MethodDelete,
			path:   "/2013-04-01/healthcheck/HCID123",
			want:   "route53:DeleteHealthCheck",
		},
		{
			name:   "update_health_check",
			method: http.MethodPost,
			path:   "/2013-04-01/healthcheck/HCID123",
			want:   "route53:UpdateHealthCheck",
		},
		{
			name:   "get_health_check_status",
			method: http.MethodGet,
			path:   "/2013-04-01/healthcheck/HCID123/status",
			want:   "route53:GetHealthCheckStatus",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			assert.Equal(t, tt.want, h.IAMAction(req))
		})
	}
}
