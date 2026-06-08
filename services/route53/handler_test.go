package route53_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// send executes a request against the Route 53 handler and returns the response recorder.
func send(t *testing.T, h *route53.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	var req *http.Request

	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/xml")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// newHandler creates a handler with a fresh backend for each test.
func newHandler(t *testing.T) *route53.Handler {
	t.Helper()

	return route53.NewHandler(route53.NewInMemoryBackend())
}

const createZoneXML = `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>example.com</Name>
  <CallerReference>ref-1</CallerReference>
  <HostedZoneConfig>
    <Comment>test zone</Comment>
    <PrivateZone>false</PrivateZone>
  </HostedZoneConfig>
</CreateHostedZoneRequest>`

func extractZoneID(t *testing.T, createBody string) string {
	t.Helper()

	type createResp struct {
		HostedZone struct {
			ID string `xml:"Id"`
		} `xml:"HostedZone"`
	}

	var resp createResp
	require.NoError(t, xml.Unmarshal([]byte(createBody), &resp))

	// ID is in form /hostedzone/{id}
	parts := strings.Split(resp.HostedZone.ID, "/")
	require.NotEmpty(t, parts)

	return parts[len(parts)-1]
}

func TestRoute53Handler(t *testing.T) {
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
			name:         "CreateHostedZone",
			method:       http.MethodPost,
			path:         "/2013-04-01/hostedzone",
			body:         createZoneXML,
			wantCode:     http.StatusCreated,
			wantContains: []string{"example.com", "INSYNC"},
		},
		{
			name:   "CreateHostedZone_MissingName",
			method: http.MethodPost,
			path:   "/2013-04-01/hostedzone",
			body: `<?xml version="1.0"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name></Name>
  <CallerReference>ref-1</CallerReference>
</CreateHostedZoneRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "CreateHostedZone_MissingCallerRef",
			method: http.MethodPost,
			path:   "/2013-04-01/hostedzone",
			body: `<?xml version="1.0"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>example.com</Name>
  <CallerReference></CallerReference>
</CreateHostedZoneRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:         "ListHostedZones_Empty",
			method:       http.MethodGet,
			path:         "/2013-04-01/hostedzone",
			wantCode:     http.StatusOK,
			wantContains: []string{"ListHostedZonesResponse"},
		},
		{
			name:     "GetHostedZone_NotFound",
			method:   http.MethodGet,
			path:     "/2013-04-01/hostedzone/ZNONEXISTENT",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteHostedZone_NotFound",
			method:   http.MethodDelete,
			path:     "/2013-04-01/hostedzone/ZNONEXISTENT",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "ListResourceRecordSets_NotFound",
			method:   http.MethodGet,
			path:     "/2013-04-01/hostedzone/ZNONEXISTENT/rrset",
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

func TestRoute53Handler_DeleteHostedZone(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)

	zoneID := extractZoneID(t, rec.Body.String())
	delRec := send(t, h, http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID, "")
	assert.Equal(t, http.StatusOK, delRec.Code)

	// Zone should no longer be found.
	getRec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID, "")
	assert.Equal(t, http.StatusNotFound, getRec.Code)
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

func TestRoute53Handler_Tags(t *testing.T) {
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
			name:   "AddTags",
			method: http.MethodPost,
			path:   "/2013-04-01/tags/hostedzone/{zoneID}",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<ChangeTagsForResourceRequest>
  <AddTags>
    <Tag><Key>env</Key><Value>prod</Value></Tag>
    <Tag><Key>team</Key><Value>infra</Value></Tag>
  </AddTags>
</ChangeTagsForResourceRequest>`,
			wantCode:     http.StatusOK,
			wantContains: []string{"ChangeTagsForResourceResponse"},
		},
		{
			name:         "ListTags_Empty",
			method:       http.MethodGet,
			path:         "/2013-04-01/tags/hostedzone/{zoneID}",
			wantCode:     http.StatusOK,
			wantContains: []string{"ListTagsForResourceResponse", "hostedzone"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
			require.Equal(t, http.StatusCreated, rec.Code)

			zoneID := extractZoneID(t, rec.Body.String())
			path := strings.Replace(tt.path, "{zoneID}", zoneID, 1)
			got := send(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, got.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, got.Body.String(), s)
			}
		})
	}
}

func TestRoute53Handler_TagRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)

	zoneID := extractZoneID(t, rec.Body.String())
	tagsPath := "/2013-04-01/tags/hostedzone/" + zoneID

	// Add tags.
	addBody := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeTagsForResourceRequest>
  <AddTags>
    <Tag><Key>env</Key><Value>prod</Value></Tag>
    <Tag><Key>team</Key><Value>infra</Value></Tag>
  </AddTags>
</ChangeTagsForResourceRequest>`
	addRec := send(t, h, http.MethodPost, tagsPath, addBody)
	require.Equal(t, http.StatusOK, addRec.Code)

	// List and verify tags exist.
	listRec := send(t, h, http.MethodGet, tagsPath, "")
	assert.Equal(t, http.StatusOK, listRec.Code)
	assert.Contains(t, listRec.Body.String(), "env")
	assert.Contains(t, listRec.Body.String(), "prod")
	assert.Contains(t, listRec.Body.String(), "team")

	// Remove one tag.
	removeBody := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeTagsForResourceRequest>
  <RemoveTagKeys>
    <Key>team</Key>
  </RemoveTagKeys>
</ChangeTagsForResourceRequest>`
	removeRec := send(t, h, http.MethodPost, tagsPath, removeBody)
	require.Equal(t, http.StatusOK, removeRec.Code)

	// Verify only the remaining tag is listed.
	listRec2 := send(t, h, http.MethodGet, tagsPath, "")
	assert.Contains(t, listRec2.Body.String(), "env")
	assert.NotContains(t, listRec2.Body.String(), "team")
}

func TestRoute53Handler_Tags_UnsupportedMethod(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodDelete, "/2013-04-01/tags/hostedzone/ZFAKE", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
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

func TestHandler_IAMAction(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{
			name:   "create_hosted_zone",
			method: http.MethodPost,
			path:   "/2013-04-01/hostedzone",
			want:   "route53:CreateHostedZone",
		},
		{
			name:   "list_hosted_zones",
			method: http.MethodGet,
			path:   "/2013-04-01/hostedzone",
			want:   "route53:ListHostedZones",
		},
		{
			name:   "get_hosted_zone",
			method: http.MethodGet,
			path:   "/2013-04-01/hostedzone/ZONE123",
			want:   "route53:GetHostedZone",
		},
		{
			name:   "delete_hosted_zone",
			method: http.MethodDelete,
			path:   "/2013-04-01/hostedzone/ZONE123",
			want:   "route53:DeleteHostedZone",
		},
		{
			name:   "change_rrset",
			method: http.MethodPost,
			path:   "/2013-04-01/hostedzone/ZONE123/rrset",
			want:   "route53:ChangeResourceRecordSets",
		},
		{
			name:   "list_rrset",
			method: http.MethodGet,
			path:   "/2013-04-01/hostedzone/ZONE123/rrset",
			want:   "route53:ListResourceRecordSets",
		},
		{
			name:   "list_tags",
			method: http.MethodGet,
			path:   "/2013-04-01/tags/hostedzone/ZONE123",
			want:   "route53:ListTagsForResource",
		},
		{
			name:   "change_tags",
			method: http.MethodPost,
			path:   "/2013-04-01/tags/hostedzone/ZONE123",
			want:   "route53:ChangeTagsForResource",
		},
		{name: "non_route53_path", method: http.MethodGet, path: "/s3/bucket", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			assert.Equal(t, tt.want, h.IAMAction(req))
		})
	}
}

// ---- Health check tests ----

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

// ---- Routing policy tests ----

func TestRoutingPolicy_Weighted(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	createRec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, createRec.Code)
	zoneID := extractZoneID(t, createRec.Body.String())

	changeXML := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>www.example.com</Name>
          <Type>A</Type>
          <SetIdentifier>primary-us-east</SetIdentifier>
          <Weight>70</Weight>
          <TTL>300</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>1.1.1.1</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>www.example.com</Name>
          <Type>A</Type>
          <SetIdentifier>secondary-us-west</SetIdentifier>
          <Weight>30</Weight>
          <TTL>300</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>2.2.2.2</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	changeRec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", changeXML)
	require.Equal(t, http.StatusOK, changeRec.Code)

	listRec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", "")
	require.Equal(t, http.StatusOK, listRec.Code)
	body := listRec.Body.String()

	assert.Contains(t, body, "primary-us-east")
	assert.Contains(t, body, "secondary-us-west")
	assert.Contains(t, body, "1.1.1.1")
	assert.Contains(t, body, "2.2.2.2")
	assert.Contains(t, body, "<Weight>70</Weight>")
	assert.Contains(t, body, "<Weight>30</Weight>")
}

func TestRoutingPolicy_Failover(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	createRec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, createRec.Code)
	zoneID := extractZoneID(t, createRec.Body.String())

	// Create health check first.
	hcRec := send(t, h, http.MethodPost, "/2013-04-01/healthcheck", createHealthCheckXML)
	require.Equal(t, http.StatusCreated, hcRec.Code)
	hcID := extractHealthCheckID(t, hcRec.Body.String())

	changeXML := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>api.example.com</Name>
          <Type>A</Type>
          <SetIdentifier>primary</SetIdentifier>
          <Failover>PRIMARY</Failover>
          <HealthCheckId>` + hcID + `</HealthCheckId>
          <TTL>60</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>10.0.1.1</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>api.example.com</Name>
          <Type>A</Type>
          <SetIdentifier>secondary</SetIdentifier>
          <Failover>SECONDARY</Failover>
          <TTL>60</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>10.0.2.1</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	changeRec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", changeXML)
	require.Equal(t, http.StatusOK, changeRec.Code)

	listRec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", "")
	require.Equal(t, http.StatusOK, listRec.Code)
	body := listRec.Body.String()

	assert.Contains(t, body, "PRIMARY")
	assert.Contains(t, body, "SECONDARY")
	assert.Contains(t, body, hcID)
	assert.Contains(t, body, "10.0.1.1")
	assert.Contains(t, body, "10.0.2.1")
}

func TestRoutingPolicy_GeoLocation(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	createRec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, createRec.Code)
	zoneID := extractZoneID(t, createRec.Body.String())

	changeXML := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>geo.example.com</Name>
          <Type>A</Type>
          <SetIdentifier>us-record</SetIdentifier>
          <GeoLocation>
            <CountryCode>US</CountryCode>
          </GeoLocation>
          <TTL>60</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>1.2.3.4</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	changeRec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", changeXML)
	require.Equal(t, http.StatusOK, changeRec.Code)

	listRec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", "")
	require.Equal(t, http.StatusOK, listRec.Code)
	body := listRec.Body.String()

	assert.Contains(t, body, "us-record")
	assert.Contains(t, body, "US")
	assert.Contains(t, body, "1.2.3.4")
}

func TestRoutingPolicy_LatencyBased(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	createRec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, createRec.Code)
	zoneID := extractZoneID(t, createRec.Body.String())

	changeXML := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>CREATE</Action>
        <ResourceRecordSet>
          <Name>latency.example.com</Name>
          <Type>A</Type>
          <SetIdentifier>us-east-1</SetIdentifier>
          <Region>us-east-1</Region>
          <TTL>60</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>3.4.5.6</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

	changeRec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", changeXML)
	require.Equal(t, http.StatusOK, changeRec.Code)

	listRec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", "")
	require.Equal(t, http.StatusOK, listRec.Code)
	body := listRec.Body.String()

	assert.Contains(t, body, "us-east-1")
	assert.Contains(t, body, "3.4.5.6")
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

// ---- New operation tests ----

func TestRoute53_CreateKeySigningKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *route53.Handler) string
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "create_ksk_success",
			setup: func(h *route53.Handler) string {
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
				require.Equal(t, http.StatusCreated, rec.Code)

				return extractZoneID(t, rec.Body.String())
			},
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateKeySigningKeyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>PLACEHOLDER</HostedZoneId>
  <CallerReference>ksk-ref-1</CallerReference>
  <Name>mykey</Name>
  <KeyManagementServiceArn>arn:aws:kms:us-east-1:123456789012:key/abc123</KeyManagementServiceArn>
  <Status>INACTIVE</Status>
</CreateKeySigningKeyRequest>`,
			wantCode:     http.StatusCreated,
			wantContains: []string{"CreateKeySigningKeyResponse", "mykey", "INACTIVE"},
		},
		{
			name: "create_ksk_missing_name",
			setup: func(h *route53.Handler) string {
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
				require.Equal(t, http.StatusCreated, rec.Code)

				return extractZoneID(t, rec.Body.String())
			},
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateKeySigningKeyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>PLACEHOLDER</HostedZoneId>
  <CallerReference>ksk-ref-2</CallerReference>
</CreateKeySigningKeyRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "create_ksk_zone_not_found",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateKeySigningKeyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>ZNONEXISTENT</HostedZoneId>
  <CallerReference>ksk-ref-3</CallerReference>
  <Name>mykey</Name>
</CreateKeySigningKeyRequest>`,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			body := tt.body
			if tt.setup != nil {
				zoneID := tt.setup(h)
				body = strings.ReplaceAll(body, "PLACEHOLDER", zoneID)
			}

			rec := send(t, h, http.MethodPost, "/2013-04-01/keysigningkey", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestRoute53_ActivateKeySigningKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "activate_success",
			wantCode:     http.StatusOK,
			wantContains: []string{"ActivateKeySigningKeyResponse", "ACTIVE"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			// Create zone.
			rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
			require.Equal(t, http.StatusCreated, rec.Code)
			zoneID := extractZoneID(t, rec.Body.String())

			// Create KSK.
			kskBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateKeySigningKeyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>` + zoneID + `</HostedZoneId>
  <CallerReference>ksk-ref-activate</CallerReference>
  <Name>testkey</Name>
  <Status>INACTIVE</Status>
</CreateKeySigningKeyRequest>`
			kskRec := send(t, h, http.MethodPost, "/2013-04-01/keysigningkey", kskBody)
			require.Equal(t, http.StatusCreated, kskRec.Code)

			// Activate.
			got := send(t, h, http.MethodPost, "/2013-04-01/keysigningkey/"+zoneID+"/testkey/activate", "")
			assert.Equal(t, tt.wantCode, got.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, got.Body.String(), s)
			}
		})
	}
}

func TestRoute53_ActivateKeySigningKey_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/keysigningkey/ZNONEXISTENT/nokey/activate", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRoute53_AssociateVPCWithHostedZone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
		useRealZone  bool
	}{
		{
			name:        "associate_vpc_success",
			useRealZone: true,
			body: `<?xml version="1.0" encoding="UTF-8"?>
<AssociateVPCWithHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC>
    <VPCId>vpc-12345</VPCId>
    <VPCRegion>us-east-1</VPCRegion>
  </VPC>
</AssociateVPCWithHostedZoneRequest>`,
			wantCode:     http.StatusOK,
			wantContains: []string{"AssociateVPCWithHostedZoneResponse", "INSYNC"},
		},
		{
			name:        "associate_vpc_missing_vpcid",
			useRealZone: true,
			body: `<?xml version="1.0" encoding="UTF-8"?>
<AssociateVPCWithHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC>
    <VPCRegion>us-east-1</VPCRegion>
  </VPC>
</AssociateVPCWithHostedZoneRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "associate_vpc_zone_not_found",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<AssociateVPCWithHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <VPC><VPCId>vpc-xyz</VPCId></VPC>
</AssociateVPCWithHostedZoneRequest>`,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			var path string
			if tt.useRealZone {
				privateZoneXML := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>private.example.com</Name>
  <CallerReference>ref-private-1</CallerReference>
  <HostedZoneConfig>
    <Comment>private test zone</Comment>
    <PrivateZone>true</PrivateZone>
  </HostedZoneConfig>
</CreateHostedZoneRequest>`
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", privateZoneXML)
				require.Equal(t, http.StatusCreated, rec.Code)
				zoneID := extractZoneID(t, rec.Body.String())
				path = "/2013-04-01/hostedzone/" + zoneID + "/associatevpc"
			} else {
				path = "/2013-04-01/hostedzone/ZNONEXISTENT/associatevpc"
			}

			got := send(t, h, http.MethodPost, path, tt.body)
			assert.Equal(t, tt.wantCode, got.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, got.Body.String(), s)
			}
		})
	}
}

func TestRoute53_CreateCidrCollection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "create_cidr_collection_success",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>my-cidrs</Name>
  <CallerReference>cidr-ref-1</CallerReference>
</CreateCidrCollectionRequest>`,
			wantCode:     http.StatusCreated,
			wantContains: []string{"CreateCidrCollectionResponse", "my-cidrs"},
		},
		{
			name: "create_cidr_collection_missing_name",
			body: `<?xml version="1.0" encoding="UTF-8"?>` +
				`<CreateCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">` +
				`</CreateCidrCollectionRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create_cidr_collection_invalid_xml",
			body:     "not-xml",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestRoute53_ChangeCidrCollection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		collectionID string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "change_success",
			wantCode:     http.StatusOK,
			wantContains: []string{"ChangeCidrCollectionResponse"},
		},
		{
			name:         "change_not_found",
			collectionID: "ZNONEXISTENT",
			wantCode:     http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			collectionID := tt.collectionID

			if collectionID == "" {
				// Create a collection first.
				createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>test-cidrs</Name>
  <CallerReference>cidr-ref-change</CallerReference>
</CreateCidrCollectionRequest>`
				createRec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection", createBody)
				require.Equal(t, http.StatusCreated, createRec.Code)

				type colResp struct {
					Collection struct {
						ID string `xml:"Id"`
					} `xml:"Collection"`
				}

				var resp colResp
				require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &resp))
				collectionID = resp.Collection.ID
			}

			rec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection/"+collectionID, "")
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestRoute53_CreateQueryLoggingConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
		useZone      bool
	}{
		{
			name:    "create_qlc_success",
			useZone: true,
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateQueryLoggingConfigRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>PLACEHOLDER</HostedZoneId>
  <CloudWatchLogsLogGroupArn>arn:aws:logs:us-east-1:123456789012:log-group:my-log-group</CloudWatchLogsLogGroupArn>
</CreateQueryLoggingConfigRequest>`,
			wantCode:     http.StatusCreated,
			wantContains: []string{"CreateQueryLoggingConfigResponse", "my-log-group"},
		},
		{
			name:    "create_qlc_missing_log_group",
			useZone: true,
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateQueryLoggingConfigRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>PLACEHOLDER</HostedZoneId>
</CreateQueryLoggingConfigRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "create_qlc_zone_not_found",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateQueryLoggingConfigRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>ZNONEXISTENT</HostedZoneId>
  <CloudWatchLogsLogGroupArn>arn:aws:logs:us-east-1:123:log-group:test</CloudWatchLogsLogGroupArn>
</CreateQueryLoggingConfigRequest>`,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			body := tt.body

			if tt.useZone {
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
				require.Equal(t, http.StatusCreated, rec.Code)
				zoneID := extractZoneID(t, rec.Body.String())
				body = strings.ReplaceAll(body, "PLACEHOLDER", zoneID)
			}

			rec := send(t, h, http.MethodPost, "/2013-04-01/queryloggingconfig", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestRoute53_CreateReusableDelegationSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "create_rds_success",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateReusableDelegationSetRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CallerReference>ds-ref-1</CallerReference>
</CreateReusableDelegationSetRequest>`,
			wantCode:     http.StatusCreated,
			wantContains: []string{"CreateReusableDelegationSetResponse", "NameServer"},
		},
		{
			name: "create_rds_missing_caller_ref",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateReusableDelegationSetRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
</CreateReusableDelegationSetRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create_rds_invalid_xml",
			body:     "not-xml",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, http.MethodPost, "/2013-04-01/delegationset", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestRoute53_CreateTrafficPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "create_tp_success",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>my-policy</Name>
  <Document>{"AWSPolicyFormatVersion":"2015-10-01"}</Document>
  <Comment>test policy</Comment>
</CreateTrafficPolicyRequest>`,
			wantCode:     http.StatusCreated,
			wantContains: []string{"CreateTrafficPolicyResponse", "my-policy", "test policy"},
		},
		{
			name: "create_tp_missing_name",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Document>{"AWSPolicyFormatVersion":"2015-10-01"}</Document>
</CreateTrafficPolicyRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "create_tp_missing_document",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>my-policy</Name>
</CreateTrafficPolicyRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create_tp_invalid_xml",
			body:     "not-xml",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func extractTrafficPolicyID(t *testing.T, body string) string {
	t.Helper()

	type resp struct {
		TrafficPolicy struct {
			ID string `xml:"Id"`
		} `xml:"TrafficPolicy"`
	}

	var r resp
	require.NoError(t, xml.Unmarshal([]byte(body), &r))
	require.NotEmpty(t, r.TrafficPolicy.ID)

	return r.TrafficPolicy.ID
}

func TestRoute53_CreateTrafficPolicyVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		policyID     string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "create_version_success",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyVersionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Document>{"AWSPolicyFormatVersion":"2015-10-01","v2":true}</Document>
  <Comment>version 2</Comment>
</CreateTrafficPolicyVersionRequest>`,
			wantCode:     http.StatusCreated,
			wantContains: []string{"CreateTrafficPolicyVersionResponse", "version 2"},
		},
		{
			name:     "create_version_not_found",
			policyID: "NONEXISTENT-POLICY-ID",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyVersionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Document>{"v":"2"}</Document>
</CreateTrafficPolicyVersionRequest>`,
			wantCode: http.StatusNotFound,
		},
		{
			name: "create_version_missing_document",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyVersionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
</CreateTrafficPolicyVersionRequest>`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			policyID := tt.policyID

			if policyID == "" && tt.wantCode != http.StatusNotFound {
				// Create initial policy.
				createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>versioned-policy</Name>
  <Document>{"AWSPolicyFormatVersion":"2015-10-01"}</Document>
</CreateTrafficPolicyRequest>`
				createRec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy", createBody)
				require.Equal(t, http.StatusCreated, createRec.Code)
				policyID = extractTrafficPolicyID(t, createRec.Body.String())
			} else if policyID == "" {
				policyID = "NONEXISTENT-POLICY-ID"
			}

			rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy/"+policyID, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
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

func TestRoute53_NewOperations_UnsupportedMethods(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{
			name:   "ksk_root_unsupported_get",
			method: http.MethodGet,
			path:   "/2013-04-01/keysigningkey",
		},
		{
			name:   "ksk_resource_unsupported_get",
			method: http.MethodGet,
			path:   "/2013-04-01/keysigningkey/ZONE123/keyname/activate",
		},
		{
			name:   "traffic_policy_unsupported_get",
			method: http.MethodGet,
			path:   "/2013-04-01/trafficpolicy",
		},
		{
			name:   "traffic_policy_version_unsupported_get",
			method: http.MethodGet,
			path:   "/2013-04-01/trafficpolicy/SOMEID",
		},
		{
			name:   "tp_instance_unsupported_get",
			method: http.MethodGet,
			path:   "/2013-04-01/trafficpolicyinstance",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, tt.method, tt.path, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestRoute53_SnapshotRestore_NewOperations(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	// Create a zone.
	zoneRec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, zoneRec.Code)
	zoneID := extractZoneID(t, zoneRec.Body.String())

	// Create a CIDR collection.
	cidrBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateCidrCollectionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>snap-cidrs</Name>
  <CallerReference>snap-cidr-ref</CallerReference>
</CreateCidrCollectionRequest>`
	cidrRec := send(t, h, http.MethodPost, "/2013-04-01/cidrcollection", cidrBody)
	require.Equal(t, http.StatusCreated, cidrRec.Code)

	// Create a traffic policy.
	tpBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>snap-policy</Name>
  <Document>{"AWSPolicyFormatVersion":"2015-10-01"}</Document>
</CreateTrafficPolicyRequest>`
	tpRec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy", tpBody)
	require.Equal(t, http.StatusCreated, tpRec.Code)
	policyID := extractTrafficPolicyID(t, tpRec.Body.String())

	// Snapshot and restore.
	snap := h.Backend.Snapshot()
	require.NotNil(t, snap)

	newBackend := route53.NewInMemoryBackend()
	require.NoError(t, newBackend.Restore(snap))
	newHandler := route53.NewHandler(newBackend)

	// Verify zone still accessible.
	getRec := send(t, newHandler, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID, "")
	assert.Equal(t, http.StatusOK, getRec.Code)

	// Verify traffic policy version can be created (policy still exists).
	versionBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyVersionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Document>{"AWSPolicyFormatVersion":"2015-10-01","v2":true}</Document>
</CreateTrafficPolicyVersionRequest>`
	versionRec := send(t, newHandler, http.MethodPost, "/2013-04-01/trafficpolicy/"+policyID, versionBody)
	assert.Equal(t, http.StatusCreated, versionRec.Code)
}
