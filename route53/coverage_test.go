package route53_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/route53"
)

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend(), slog.Default())
	assert.Equal(t, "Route53", h.Name())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend(), slog.Default())
	assert.Greater(t, h.MatchPriority(), 50) // higher than dashboard
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend(), slog.Default())
	matcher := h.RouteMatcher()
	e := echo.New()

	req := httptest.NewRequest(http.MethodGet, "/2013-04-01/hostedzone", nil)
	assert.True(t, matcher(e.NewContext(req, httptest.NewRecorder())))

	req = httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	assert.False(t, matcher(e.NewContext(req, httptest.NewRecorder())))
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend(), slog.Default())
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateHostedZone")
	assert.Contains(t, ops, "ListHostedZones")
	assert.Contains(t, ops, "GetHostedZone")
	assert.Contains(t, ops, "DeleteHostedZone")
	assert.Contains(t, ops, "ChangeResourceRecordSets")
	assert.Contains(t, ops, "ListResourceRecordSets")
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend(), slog.Default())
	e := echo.New()

	cases := []struct {
		method string
		path   string
		want   string
	}{
		{http.MethodPost, "/2013-04-01/hostedzone", "CreateHostedZone"},
		{http.MethodGet, "/2013-04-01/hostedzone", "ListHostedZones"},
		{http.MethodPost, "/2013-04-01/hostedzone/Z123/rrset", "ChangeResourceRecordSets"},
		{http.MethodGet, "/2013-04-01/hostedzone/Z123/rrset", "ListResourceRecordSets"},
		{http.MethodDelete, "/2013-04-01/hostedzone/Z123", "DeleteHostedZone"},
		{http.MethodGet, "/2013-04-01/hostedzone/Z123", "GetHostedZone"},
		{http.MethodPut, "/2013-04-01/hostedzone", "Unknown"},
	}

	for _, tc := range cases {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		got := h.ExtractOperation(e.NewContext(req, httptest.NewRecorder()))
		assert.Equal(t, tc.want, got, "method=%s path=%s", tc.method, tc.path)
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend(), slog.Default())
	e := echo.New()

	req := httptest.NewRequest(http.MethodGet, "/2013-04-01/hostedzone/ZABC123", nil)
	assert.Equal(t, "ZABC123", h.ExtractResource(e.NewContext(req, httptest.NewRecorder())))

	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/hostedzone/ZABC123/rrset", nil)
	assert.Equal(t, "ZABC123", h.ExtractResource(e.NewContext(req, httptest.NewRecorder())))

	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/hostedzone", nil)
	assert.Empty(t, h.ExtractResource(e.NewContext(req, httptest.NewRecorder())))
}

func TestProvider_Route53_Name(t *testing.T) {
	t.Parallel()

	p := &route53.Provider{}
	assert.Equal(t, "Route53", p.Name())
}

func TestProvider_Route53_Init(t *testing.T) {
	t.Parallel()

	p := &route53.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, svc)
	assert.Equal(t, "Route53", svc.Name())
}

func TestHandler_UnknownEndpoint(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend(), slog.Default())
	e := echo.New()

	req := httptest.NewRequest(http.MethodPatch, "/2013-04-01/hostedzone", nil)
	rec := httptest.NewRecorder()
	err := h.Handler()(e.NewContext(req, rec))
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_InvalidXMLBody(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", "not-xml")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestChangeResourceRecordSets_DeleteNonexistent(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)

	zoneID := extractZoneID(t, rec.Body.String())

	deleteXML := `<?xml version="1.0" encoding="UTF-8"?>
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

	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", deleteXML)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestChangeResourceRecordSets_InvalidXML(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)

	zoneID := extractZoneID(t, rec.Body.String())

	rec = send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", "bad-xml")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestChangeResourceRecordSets_ZoneNotFound(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

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

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/ZNONEXISTENT/rrset", changeXML)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestNewHandler_NilLogger(t *testing.T) {
	t.Parallel()

	// NewHandler should use slog.Default() when logger is nil.
	h := route53.NewHandler(route53.NewInMemoryBackend(), nil)
	require.NotNil(t, h)

	rec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone", "")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestGetHostedZone_InvalidXML(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	// Invalid zone ID path returns not found from backend.
	rec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone/ZNONEXISTENT", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDeleteHostedZone_DeregistersDNS(t *testing.T) {
	t.Parallel()

	registered := make(map[string]bool)
	reg := &mockDNSRegistrar{registered: registered}
	backend := route53.NewInMemoryBackend()
	backend.SetDNSRegistrar(reg)
	h := route53.NewHandler(backend, slog.Default())

	// Create zone.
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	// Add A record.
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
	require.True(t, reg.registered["www.example.com."])

	// Delete zone — should also deregister DNS records.
	delRec := send(t, h, http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID, "")
	require.Equal(t, http.StatusOK, delRec.Code)
	assert.False(t, reg.registered["www.example.com."])
}
