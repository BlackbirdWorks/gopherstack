package route53_test

import (
	"encoding/xml"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/route53"
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

	return route53.NewHandler(route53.NewInMemoryBackend(), slog.Default())
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
		name string
		run  func(t *testing.T)
	}{
		{
			name: "CreateHostedZone",
			run: func(t *testing.T) {
				h := newHandler(t)
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
				assert.Equal(t, http.StatusCreated, rec.Code)
				assert.Contains(t, rec.Body.String(), "example.com")
				assert.Contains(t, rec.Body.String(), "INSYNC")
			},
		},
		{
			name: "CreateHostedZone_MissingName",
			run: func(t *testing.T) {
				h := newHandler(t)
				body := `<?xml version="1.0"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name></Name>
  <CallerReference>ref-1</CallerReference>
</CreateHostedZoneRequest>`
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", body)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "CreateHostedZone_MissingCallerRef",
			run: func(t *testing.T) {
				h := newHandler(t)
				body := `<?xml version="1.0"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>example.com</Name>
  <CallerReference></CallerReference>
</CreateHostedZoneRequest>`
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", body)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "ListHostedZones_Empty",
			run: func(t *testing.T) {
				h := newHandler(t)
				rec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone", "")
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "ListHostedZonesResponse")
			},
		},
		{
			name: "ListHostedZones_AfterCreate",
			run: func(t *testing.T) {
				h := newHandler(t)
				send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
				rec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone", "")
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "example.com")
			},
		},
		{
			name: "GetHostedZone",
			run: func(t *testing.T) {
				h := newHandler(t)
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
				require.Equal(t, http.StatusCreated, rec.Code)

				zoneID := extractZoneID(t, rec.Body.String())
				getRec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID, "")
				assert.Equal(t, http.StatusOK, getRec.Code)
				assert.Contains(t, getRec.Body.String(), "example.com")
			},
		},
		{
			name: "GetHostedZone_NotFound",
			run: func(t *testing.T) {
				h := newHandler(t)
				rec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone/ZNONEXISTENT", "")
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "DeleteHostedZone",
			run: func(t *testing.T) {
				h := newHandler(t)
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
				require.Equal(t, http.StatusCreated, rec.Code)

				zoneID := extractZoneID(t, rec.Body.String())
				delRec := send(t, h, http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID, "")
				assert.Equal(t, http.StatusOK, delRec.Code)

				// Zone should no longer be found.
				getRec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID, "")
				assert.Equal(t, http.StatusNotFound, getRec.Code)
			},
		},
		{
			name: "DeleteHostedZone_NotFound",
			run: func(t *testing.T) {
				h := newHandler(t)
				rec := send(t, h, http.MethodDelete, "/2013-04-01/hostedzone/ZNONEXISTENT", "")
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "ChangeResourceRecordSets_CreateA",
			run: func(t *testing.T) {
				h := newHandler(t)
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
            <ResourceRecord><Value>192.0.2.1</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`

				changeRec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", changeXML)
				assert.Equal(t, http.StatusOK, changeRec.Code)
				assert.Contains(t, changeRec.Body.String(), "INSYNC")
			},
		},
		{
			name: "ChangeResourceRecordSets_CreateCNAME",
			run: func(t *testing.T) {
				h := newHandler(t)
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
</ChangeResourceRecordSetsRequest>`

				changeRec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", changeXML)
				assert.Equal(t, http.StatusOK, changeRec.Code)
			},
		},
		{
			name: "ChangeResourceRecordSets_Upsert",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "ChangeResourceRecordSets_Delete",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "ListResourceRecordSets",
			run: func(t *testing.T) {
				h := newHandler(t)
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
</ChangeResourceRecordSetsRequest>`

				send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", changeXML)

				listRec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", "")
				assert.Equal(t, http.StatusOK, listRec.Code)
				assert.Contains(t, listRec.Body.String(), "api.example.com")
				assert.Contains(t, listRec.Body.String(), "10.0.0.1")
			},
		},
		{
			name: "ListResourceRecordSets_NotFound",
			run: func(t *testing.T) {
				h := newHandler(t)
				rec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone/ZNONEXISTENT/rrset", "")
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "DNSRegistrar_RegisterOnCreate",
			run: func(t *testing.T) {
				registered := make(map[string]bool)
				registrar := &mockDNSRegistrar{registered: registered}

				backend := route53.NewInMemoryBackend()
				backend.SetDNSRegistrar(registrar)
				h := route53.NewHandler(backend, slog.Default())

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
			},
		},
		{
			name: "DNSRegistrar_DeregisterOnDelete",
			run: func(t *testing.T) {
				registered := make(map[string]bool)
				registrar := &mockDNSRegistrar{registered: registered}

				backend := route53.NewInMemoryBackend()
				backend.SetDNSRegistrar(registrar)
				h := route53.NewHandler(backend, slog.Default())

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
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// mockDNSRegistrar is a test double for route53.DNSRegistrar.
type mockDNSRegistrar struct {
	registered map[string]bool
}

func (m *mockDNSRegistrar) Register(hostname string) {
	m.registered[hostname] = true
}

func (m *mockDNSRegistrar) Deregister(hostname string) {
	delete(m.registered, hostname)
}
