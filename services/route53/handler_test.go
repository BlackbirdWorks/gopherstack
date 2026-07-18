package route53_test

import (
	"encoding/xml"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/route53"
)

func createZone(t *testing.T, h *route53.Handler, name string) string {
	t.Helper()

	rec := send(
		t,
		h,
		http.MethodPost,
		"/2013-04-01/hostedzone",
		`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
<Name>`+name+`</Name><CallerReference>ref-`+name+`</CallerReference></CreateHostedZoneRequest>`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Extract zone ID from response
	body := rec.Body.String()
	idx := 0
	for i := range body {
		if body[i:min53(i+12, len(body))] == `/hostedzone/` {
			idx = i + 12

			break
		}
	}
	if idx == 0 {
		return ""
	}
	end := idx
	for end < len(body) && body[end] != '<' {
		end++
	}

	return body[idx:end]
}

func min53(a, b int) int {
	if a < b {
		return a
	}

	return b
}

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

			h := route53.NewHandler(route53.NewInMemoryBackend())
			assert.Equal(t, tt.want, h.Name())
		})
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantAbove int
	}{
		{name: "higher_than_dashboard", wantAbove: 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := route53.NewHandler(route53.NewInMemoryBackend())
			assert.Greater(t, h.MatchPriority(), tt.wantAbove)
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "route53_path_matches", path: "/2013-04-01/hostedzone", want: true},
		{name: "non_route53_path_no_match", path: "/bucket/key", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := route53.NewHandler(route53.NewInMemoryBackend())
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

			h := route53.NewHandler(route53.NewInMemoryBackend())
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
		{
			name:   "post_hostedzone_is_create",
			method: http.MethodPost,
			path:   "/2013-04-01/hostedzone",
			want:   "CreateHostedZone",
		},
		{
			name:   "get_hostedzone_is_list",
			method: http.MethodGet,
			path:   "/2013-04-01/hostedzone",
			want:   "ListHostedZones",
		},
		{
			name:   "post_rrset_is_change",
			method: http.MethodPost,
			path:   "/2013-04-01/hostedzone/Z123/rrset",
			want:   "ChangeResourceRecordSets",
		},
		{
			name:   "get_rrset_is_list",
			method: http.MethodGet,
			path:   "/2013-04-01/hostedzone/Z123/rrset",
			want:   "ListResourceRecordSets",
		},
		{
			name:   "delete_zone_is_delete",
			method: http.MethodDelete,
			path:   "/2013-04-01/hostedzone/Z123",
			want:   "DeleteHostedZone",
		},
		{name: "get_zone_is_get", method: http.MethodGet, path: "/2013-04-01/hostedzone/Z123", want: "GetHostedZone"},
		{name: "put_hostedzone_is_unknown", method: http.MethodPut, path: "/2013-04-01/hostedzone", want: "Unknown"},
		// Health check-related operation extraction.
		{
			name:   "delete_healthcheck_is_delete_hc",
			method: http.MethodDelete,
			path:   "/2013-04-01/healthcheck/HCID",
			want:   "DeleteHealthCheck",
		},
		{
			name:   "get_hc_status_is_get_status",
			method: http.MethodGet,
			path:   "/2013-04-01/healthcheck/HCID/status",
			want:   "GetHealthCheckStatus",
		},
		{
			name:   "post_hc_status_is_unknown",
			method: http.MethodPost,
			path:   "/2013-04-01/healthcheck/HCID/status",
			want:   "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := route53.NewHandler(route53.NewInMemoryBackend())
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

			h := route53.NewHandler(route53.NewInMemoryBackend())
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

			h := route53.NewHandler(route53.NewInMemoryBackend())
			require.NotNil(t, h)

			rec := send(t, h, http.MethodGet, "/2013-04-01/hostedzone", "")
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestErrorWireCodesAndStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *route53.Handler)
		name string
	}{
		{
			name: "unknown_cidr_collection_uses_exception_suffix",
			run: func(t *testing.T, h *route53.Handler) {
				t.Helper()

				// GET /cidrcollection/{Id} (no "/cidrblocks" suffix) routes to
				// ListCidrLocations, which needs no request body.
				rec := send(t, h, http.MethodGet, "/2013-04-01/cidrcollection/DOESNOTEXIST", "")
				assert.Equal(t, http.StatusNotFound, rec.Code)
				assert.Contains(t, rec.Body.String(), "NoSuchCidrCollectionException")
			},
		},
		{
			name: "unknown_delegation_set_is_400",
			run: func(t *testing.T, h *route53.Handler) {
				t.Helper()

				rec := send(t, h, http.MethodDelete, "/2013-04-01/delegationset/N-DOESNOTEXIST", "")
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "NoSuchDelegationSet")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			tt.run(t, h)
		})
	}
}

func createZoneForOpsTest(t *testing.T, h *route53.Handler) string {
	t.Helper()

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)

	return extractZoneID(t, rec.Body.String())
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

func TestHandlerReset(t *testing.T) {
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

func TestAccountID(t *testing.T) {
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

func TestRegion(t *testing.T) {
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

func TestHandlerOpsLen(t *testing.T) {
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

func TestGetAccountLimit(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	// Create some zones.
	for i := range 3 {
		body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>zone%d.example.com</Name>
  <CallerReference>ref-%d</CallerReference>
  <HostedZoneConfig><Comment></Comment><PrivateZone>false</PrivateZone></HostedZoneConfig>
</CreateHostedZoneRequest>`, i, i)
		rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", body)
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	rec := send(t, h, http.MethodGet, "/2013-04-01/accountlimit/MAX_HOSTED_ZONES_BY_OWNER", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetAccountLimitResponse")
	assert.Contains(t, rec.Body.String(), "MAX_HOSTED_ZONES_BY_OWNER")
}

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
