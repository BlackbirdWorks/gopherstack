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

	tests := []struct {
		name string
		want string
	}{
		{name: "returns_route53", want: "Route53"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := route53.NewHandler(route53.NewInMemoryBackend(), slog.Default())
			assert.Equal(t, tt.want, h.Name())
		})
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantAbove   int
	}{
		{name: "higher_than_dashboard", wantAbove: 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := route53.NewHandler(route53.NewInMemoryBackend(), slog.Default())
			assert.Greater(t, h.MatchPriority(), tt.wantAbove)
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		want    bool
	}{
		{name: "route53_path_matches", path: "/2013-04-01/hostedzone", want: true},
		{name: "non_route53_path_no_match", path: "/bucket/key", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := route53.NewHandler(route53.NewInMemoryBackend(), slog.Default())
			matcher := h.RouteMatcher()
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			assert.Equal(t, tt.want, matcher(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantOps []string
	}{
		{
			name: "contains_all_operations",
			wantOps: []string{
				"CreateHostedZone",
				"ListHostedZones",
				"GetHostedZone",
				"DeleteHostedZone",
				"ChangeResourceRecordSets",
				"ListResourceRecordSets",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := route53.NewHandler(route53.NewInMemoryBackend(), slog.Default())
			ops := h.GetSupportedOperations()

			for _, op := range tt.wantOps {
				assert.Contains(t, ops, op)
			}
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{name: "post_hostedzone_is_create", method: http.MethodPost, path: "/2013-04-01/hostedzone", want: "CreateHostedZone"},
		{name: "get_hostedzone_is_list", method: http.MethodGet, path: "/2013-04-01/hostedzone", want: "ListHostedZones"},
		{name: "post_rrset_is_change", method: http.MethodPost, path: "/2013-04-01/hostedzone/Z123/rrset", want: "ChangeResourceRecordSets"},
		{name: "get_rrset_is_list", method: http.MethodGet, path: "/2013-04-01/hostedzone/Z123/rrset", want: "ListResourceRecordSets"},
		{name: "delete_zone_is_delete", method: http.MethodDelete, path: "/2013-04-01/hostedzone/Z123", want: "DeleteHostedZone"},
		{name: "get_zone_is_get", method: http.MethodGet, path: "/2013-04-01/hostedzone/Z123", want: "GetHostedZone"},
		{name: "put_hostedzone_is_unknown", method: http.MethodPut, path: "/2013-04-01/hostedzone", want: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := route53.NewHandler(route53.NewInMemoryBackend(), slog.Default())
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			got := h.ExtractOperation(e.NewContext(req, httptest.NewRecorder()))
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "zone_id_from_path", path: "/2013-04-01/hostedzone/ZABC123", want: "ZABC123"},
		{name: "zone_id_from_rrset_path", path: "/2013-04-01/hostedzone/ZABC123/rrset", want: "ZABC123"},
		{name: "empty_for_list_path", path: "/2013-04-01/hostedzone", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := route53.NewHandler(route53.NewInMemoryBackend(), slog.Default())
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			got := h.ExtractResource(e.NewContext(req, httptest.NewRecorder()))
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestProvider_Route53_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns_route53", want: "Route53"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &route53.Provider{}
			assert.Equal(t, tt.want, p.Name())
		})
	}
}

func TestProvider_Route53_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantSvcName string
	}{
		{name: "initializes_service", wantSvcName: "Route53"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &route53.Provider{}
			ctx := &service.AppContext{Logger: slog.Default()}
			svc, err := p.Init(ctx)
			require.NoError(t, err)
			require.NotNil(t, svc)
			assert.Equal(t, tt.wantSvcName, svc.Name())
		})
	}
}

// TestHandler_SimpleErrorResponses merges TestHandler_UnknownEndpoint,
// TestHandler_InvalidXMLBody, and TestGetHostedZone_InvalidXML.
func TestHandler_SimpleErrorResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		wantCode int
	}{
		{
			name:     "unknown_endpoint_returns_not_found",
			method:   http.MethodPatch,
			path:     "/2013-04-01/hostedzone",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "invalid_xml_body_returns_bad_request",
			method:   http.MethodPost,
			path:     "/2013-04-01/hostedzone",
			body:     "not-xml",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get_nonexistent_zone_returns_not_found",
			method:   http.MethodGet,
			path:     "/2013-04-01/hostedzone/ZNONEXISTENT",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestNewHandler_NilLogger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "nil_logger_uses_default", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := route53.NewHandler(route53.NewInMemoryBackend(), nil)
			require.NotNil(t, h)

			rec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone", "")
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

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
		needsZone bool
		body      string
		wantCode  int
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

func TestDeleteHostedZone_DeregistersDNS(t *testing.T) {
	t.Parallel()

	const addRecordXML = `<?xml version="1.0" encoding="UTF-8"?>
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
		name     string
		hostname string
	}{
		{name: "deregisters_dns_on_zone_delete", hostname: "www.example.com."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			registered := make(map[string]bool)
			reg := &mockDNSRegistrar{registered: registered}
			backend := route53.NewInMemoryBackend()
			backend.SetDNSRegistrar(reg)
			h := route53.NewHandler(backend, slog.Default())

			rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
			require.Equal(t, http.StatusCreated, rec.Code)

			zoneID := extractZoneID(t, rec.Body.String())

			send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", addRecordXML)
			require.True(t, reg.registered[tt.hostname])

			delRec := send(t, h, http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID, "")
			require.Equal(t, http.StatusOK, delRec.Code)
			assert.False(t, reg.registered[tt.hostname])
		})
	}
}
