package dashboard_test

import (
	"context"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/ocidns"
)

const (
	dnsComp   = "ocid1.compartment.oc1..dnscomp"
	dnsBase   = "/dashboard/api/v1/oci/dns"
	dnsAPI    = dnsBase + "/api"
	dnsSecret = "c3VwZXItc2VjcmV0LXRzaWctdmFsdWU="
)

type dnsFixture struct {
	h   *dashboard.DashboardHandler
	ops *ocidns.Handler
}

func newDNSDashboard(t *testing.T, dataPlane bool) dnsFixture {
	t.Helper()

	ops := ocidns.NewHandler(ocidns.NewInMemoryBackend())
	ops.Port, ops.DNSPort, ops.DNSEphemeral = 0, 0, dataPlane

	require.NoError(t, ops.StartWorker(t.Context()))
	t.Cleanup(func() { ops.Shutdown(context.Background()) })

	return dnsFixture{h: dashboard.NewHandler(dashboard.Config{OCIDNSOps: ops}), ops: ops}
}

func (f dnsFixture) direct(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	e.Any("/*", f.ops.Trusted())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	e.ServeHTTP(rec, req)

	return rec
}

func (f dnsFixture) proxy(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	return lbDo(t, f.h, method, path, body)
}

func (f dnsFixture) zone(t *testing.T, name string) string {
	t.Helper()

	rec := f.proxy(t, http.MethodPost, dnsAPI+"/zones",
		`{"name":"`+name+`","compartmentId":"`+dnsComp+`","zoneType":"PRIMARY","scope":"GLOBAL"}`)
	require.Less(t, rec.Code, http.StatusMultipleChoices, rec.Body.String())

	var z struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &z, jsonx.V1()))

	return z.ID
}

func (f dnsFixture) addRecord(t *testing.T, zoneID, domain, rtype, rdata string) {
	t.Helper()

	rec := f.proxy(t, http.MethodPatch, dnsAPI+"/zones/"+zoneID+"/records",
		`{"items":[{"operation":"ADD","domain":"`+domain+`","rtype":"`+rtype+`","rdata":"`+rdata+`","ttl":60}]}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func TestOCIDNSProxyZoneAndRecordLifecycle(t *testing.T) {
	t.Parallel()

	f := newDNSDashboard(t, false)

	rec := f.proxy(t, http.MethodPost, dnsAPI+"/zones",
		`{"name":"example.com","compartmentId":"`+dnsComp+`","zoneType":"PRIMARY","scope":"GLOBAL",
		 "freeformTags":{"owner":"mallory"},"migrationSource":"DYNECT","externalMasters":[{"address":"10.9.9.9"}]}`)
	require.Less(t, rec.Code, http.StatusMultipleChoices, rec.Body.String())
	assert.NotContains(t, rec.Body.String(), "mallory")
	assert.NotContains(t, rec.Body.String(), "10.9.9.9")

	var zone struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &zone, jsonx.V1()))

	list := f.proxy(t, http.MethodGet, dnsAPI+"/zones?compartmentId="+dnsComp+"&scope=GLOBAL", "")
	require.Equal(t, http.StatusOK, list.Code, list.Body.String())
	assert.Contains(t, list.Body.String(), `"name":"example.com"`)

	records := dnsAPI + "/zones/" + zone.ID + "/records"
	f.addRecord(t, zone.ID, "www.example.com", "A", "192.0.2.1")
	f.addRecord(t, zone.ID, "app.example.com", "CNAME", "www.example.com.")

	got := f.proxy(t, http.MethodGet, records, "")
	require.Equal(t, http.StatusOK, got.Code, got.Body.String())
	assert.Contains(t, got.Body.String(), `"rdata":"192.0.2.1"`)
	assert.Contains(t, got.Body.String(), `"rtype":"CNAME"`)
	require.NotEmpty(t, got.Header().Get("ETag"), "etag is relayed for If-Match")

	edit := f.proxy(t, http.MethodPatch, records, `{"items":[
	 {"operation":"REMOVE","domain":"www.example.com","rtype":"A","rdata":"192.0.2.1","ttl":60},
	 {"operation":"ADD","domain":"www.example.com","rtype":"A","rdata":"192.0.2.2","ttl":120}]}`)
	require.Equal(t, http.StatusOK, edit.Code, edit.Body.String())

	got = f.proxy(t, http.MethodGet, records+"?domain=www.example.com&rtype=A", "")
	assert.Contains(t, got.Body.String(), `"rdata":"192.0.2.2"`)
	assert.NotContains(t, got.Body.String(), `"rdata":"192.0.2.1"`)

	del := f.proxy(t, http.MethodPatch, records,
		`{"items":[{"operation":"REMOVE","domain":"www.example.com","rtype":"A","rdata":"192.0.2.2"}]}`)
	require.Equal(t, http.StatusOK, del.Code, del.Body.String())

	gone := f.proxy(t, http.MethodDelete, dnsAPI+"/zones/"+zone.ID, "")
	assert.Less(t, gone.Code, http.StatusMultipleChoices, gone.Body.String())
}

func TestOCIDNSProxyEtagConflict(t *testing.T) {
	t.Parallel()

	f := newDNSDashboard(t, false)
	id := f.zone(t, "conflict.example")
	records := dnsAPI + "/zones/" + id + "/records"

	first := f.proxy(t, http.MethodGet, records, "")
	etag := first.Header().Get("ETag")
	require.NotEmpty(t, etag)

	body := `{"items":[{"operation":"ADD","domain":"a.conflict.example","rtype":"A","rdata":"192.0.2.1","ttl":60}]}`

	patch := func(ifMatch string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPatch, records, strings.NewReader(body))
		req.Header.Set("If-Match", ifMatch)
		req.Header.Set("Authorization", "Bearer should-not-reach-emulator")
		require.NoError(t, f.h.Handler()(echo.NewContext(req, rec)))

		return rec
	}

	require.Equal(t, http.StatusOK, patch(etag).Code)

	stale := patch(etag)
	assert.Equal(t, http.StatusPreconditionFailed, stale.Code, stale.Body.String())
	assert.Contains(t, stale.Body.String(), "NoEtagMatch")
}

func TestOCIDNSProxyRejectsOutsideAllowList(t *testing.T) {
	t.Parallel()

	f := newDNSDashboard(t, false)
	id := f.zone(t, "reject.example")
	rec := dnsAPI + "/zones/" + id + "/records"

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"delete tsig key", http.MethodDelete, dnsAPI + "/tsigKeys/k1", ""},
		{"get one tsig key", http.MethodGet, dnsAPI + "/tsigKeys/k1", ""},
		{"update zone", http.MethodPut, dnsAPI + "/zones/" + id, `{}`},
		{"create resolver endpoint", http.MethodPost, dnsAPI + "/resolvers/r1/endpoints", `{}`},
		{"create view", http.MethodPost, dnsAPI + "/views", `{}`},
		{"zone content", http.MethodGet, dnsAPI + "/zones/" + id + "/content", ""},
		{"replace records", http.MethodPut, rec, `{"items":[]}`},
		{"domain records", http.MethodGet, rec + "/www.reject.example", ""},
		{"attachments", http.MethodGet, dnsAPI + "/steeringPolicyAttachments", ""},
		{"traversal", http.MethodGet, dnsAPI + "/zones/%2e%2e", ""},
		{"encoded slash", http.MethodGet, dnsAPI + "/zones/a%2Fb", ""},
		{"empty api", http.MethodGet, dnsAPI + "/", ""},
		{"unknown collection", http.MethodGet, dnsAPI + "/admin", ""},
		{"admin path", http.MethodPost, dnsAPI + "/_gopherstack/keys", `{}`},
		{
			"secondary zone", http.MethodPost, dnsAPI + "/zones",
			`{"name":"x.example","compartmentId":"` + dnsComp + `","zoneType":"SECONDARY"}`,
		},
		{
			"private zone without view", http.MethodPost, dnsAPI + "/zones",
			`{"name":"x.example","compartmentId":"` + dnsComp + `","scope":"PRIVATE"}`,
		},
		{
			"global zone with view", http.MethodPost, dnsAPI + "/zones",
			`{"name":"x.example","compartmentId":"` + dnsComp + `","scope":"GLOBAL","viewId":"v"}`,
		},
		{"zone without name", http.MethodPost, dnsAPI + "/zones", `{"compartmentId":"` + dnsComp + `"}`},
		{
			"unknown operation", http.MethodPatch, rec,
			`{"items":[{"operation":"REQUIRE","domain":"a.reject.example","rtype":"A","rdata":"1.1.1.1","ttl":1}]}`,
		},
		{
			"bad record type", http.MethodPatch, rec,
			`{"items":[{"operation":"ADD","domain":"a.reject.example","rtype":"ALIAS","rdata":"x","ttl":1}]}`,
		},
		{
			"add without ttl", http.MethodPatch, rec,
			`{"items":[{"operation":"ADD","domain":"a.reject.example","rtype":"A","rdata":"1.1.1.1"}]}`,
		},
		{
			"bad domain", http.MethodPatch, rec,
			`{"items":[{"operation":"ADD","domain":"a b","rtype":"A","rdata":"1.1.1.1","ttl":1}]}`,
		},
		{"empty items", http.MethodPatch, rec, `{"items":[]}`},
		{"malformed body", http.MethodPatch, rec, `{`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := f.proxy(t, tt.method, tt.path, tt.body)
			assert.Contains(t, []int{http.StatusBadRequest, http.StatusMethodNotAllowed}, got.Code, got.Body.String())
		})
	}
}

func TestOCIDNSProxyTsigSecretStripped(t *testing.T) {
	t.Parallel()

	f := newDNSDashboard(t, false)

	created := f.direct(t, http.MethodPost, "/20180115/tsigKeys",
		`{"name":"transfer.example","algorithm":"hmac-sha256","secret":"`+dnsSecret+`","compartmentId":"`+dnsComp+`"}`)
	require.Less(t, created.Code, http.StatusMultipleChoices, created.Body.String())
	require.Contains(t, created.Body.String(), dnsSecret, "fixture: the emulator returns the secret on create")

	var key struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &key, jsonx.V1()))

	direct := f.direct(t, http.MethodGet, "/20180115/tsigKeys/"+key.ID, "")
	require.Contains(t, direct.Body.String(), dnsSecret, "fixture: the emulator returns the secret on get")

	got := f.proxy(t, http.MethodGet, dnsAPI+"/tsigKeys?compartmentId="+dnsComp, "")
	require.Equal(t, http.StatusOK, got.Code, got.Body.String())
	assert.Contains(t, got.Body.String(), `"name":"transfer.example"`)
	assert.Contains(t, got.Body.String(), `"algorithm":"hmac-sha256"`)
	assert.NotContains(t, got.Body.String(), dnsSecret)
	assert.NotContains(t, got.Body.String(), `"secret"`)

	one := f.proxy(t, http.MethodGet, dnsAPI+"/tsigKeys/"+key.ID, "")
	assert.Equal(t, http.StatusBadRequest, one.Code)
}

func TestOCIDNSProxyReadOnlyLists(t *testing.T) {
	t.Parallel()

	f := newDNSDashboard(t, false)

	view := f.direct(t, http.MethodPost, "/20180115/views", `{"compartmentId":"`+dnsComp+`","displayName":"corp-view"}`)
	require.Less(t, view.Code, http.StatusMultipleChoices, view.Body.String())

	policy := f.direct(t, http.MethodPost, "/20180115/steeringPolicies",
		`{"compartmentId":"`+dnsComp+`","displayName":"failover","template":"FAILOVER","ttl":30,
		 "answers":[{"name":"a","rtype":"A","rdata":"192.0.2.9","pool":"p"}],
		 "rules":[{"ruleType":"FILTER",
		  "defaultAnswerData":[{"answerCondition":"answer.isDisabled != true","shouldKeep":true}]}]}`)
	require.Less(t, policy.Code, http.StatusMultipleChoices, policy.Body.String())

	tests := []struct {
		name string
		path string
		want string
	}{
		{"compartments", dnsBase + "/compartments", dnsComp},
		{"views", dnsAPI + "/views?compartmentId=" + dnsComp, `"displayName":"corp-view"`},
		{"policies", dnsAPI + "/steeringPolicies?compartmentId=" + dnsComp, `"displayName":"failover"`},
		{"resolvers", dnsAPI + "/resolvers?compartmentId=" + dnsComp, `[`},
		{"dataplane", dnsBase + "/dataplane", `"enabled":false`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := f.proxy(t, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, got.Code, got.Body.String())
			assert.Contains(t, got.Body.String(), tt.want)
		})
	}
}

func TestOCIDNSProxyTryIt(t *testing.T) {
	t.Parallel()

	f := newDNSDashboard(t, true)
	port := f.ops.DataPlanePort()
	require.Positive(t, port)

	id := f.zone(t, "tryit.example")
	f.addRecord(t, id, "www.tryit.example", "A", "192.0.2.77")

	dp := f.proxy(t, http.MethodGet, dnsBase+"/dataplane", "")
	assert.Contains(t, dp.Body.String(), `"enabled":true`)

	query := func(body string) *httptest.ResponseRecorder {
		return f.proxy(t, http.MethodPost, dnsBase+"/query", body)
	}

	t.Run("answers from the data plane", func(t *testing.T) {
		t.Parallel()

		got := query(`{"name":"www.tryit.example","type":"A"}`)
		require.Equal(t, http.StatusOK, got.Code, got.Body.String())

		var out map[string]any

		require.NoError(t, json.Unmarshal(got.Body.Bytes(), &out, jsonx.V1()))
		assert.Equal(t, "NOERROR", out["rcode"])
		assert.Equal(t, true, out["authoritative"])

		answer, ok := out["answer"].([]any)
		require.True(t, ok)
		require.Len(t, answer, 1)

		rr, ok := answer[0].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "A", rr["type"])
		assert.Equal(t, "192.0.2.77", rr["data"])
		assert.InDelta(t, 60, rr["ttl"], 0)
	})

	t.Run("loopback only", func(t *testing.T) {
		t.Parallel()

		got := query(`{"name":"www.tryit.example.","type":"A","server":"8.8.8.8:53","address":"203.0.113.1","port":53}`)
		require.Equal(t, http.StatusOK, got.Code, got.Body.String())
		assert.Contains(t, got.Body.String(), `"server":"127.0.0.1:`)
		assert.NotContains(t, got.Body.String(), "8.8.8.8")
		assert.NotContains(t, got.Body.String(), "203.0.113.1")
	})

	t.Run("unknown name is nxdomain", func(t *testing.T) {
		t.Parallel()

		got := query(`{"name":"missing.tryit.example","type":"A"}`)
		require.Equal(t, http.StatusOK, got.Code, got.Body.String())
		assert.Contains(t, got.Body.String(), `"rcode":"NXDOMAIN"`)
	})

	invalid := []struct{ name, body string }{
		{"space in name", `{"name":"bad name.example","type":"A"}`},
		{"empty name", `{"name":"","type":"A"}`},
		{"empty label", `{"name":"a..example","type":"A"}`},
		{"long label", `{"name":"` + strings.Repeat("a", 64) + `.example","type":"A"}`},
		{"long name", `{"name":"` + strings.Repeat("abcdefgh.", 30) + `example","type":"A"}`},
		{"zone transfer", `{"name":"tryit.example","type":"AXFR"}`},
		{"any", `{"name":"tryit.example","type":"ANY"}`},
		{"lowercase type", `{"name":"tryit.example","type":"a"}`},
		{"malformed", `{`},
	}

	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := query(tt.body)
			assert.Equal(t, http.StatusBadRequest, got.Code, got.Body.String())
		})
	}
}

func TestOCIDNSProxyTryItWithoutDataPlane(t *testing.T) {
	t.Parallel()

	f := newDNSDashboard(t, false)

	got := f.proxy(t, http.MethodPost, dnsBase+"/query", `{"name":"www.example.com","type":"A"}`)
	assert.Equal(t, http.StatusServiceUnavailable, got.Code, got.Body.String())
	assert.Contains(t, got.Body.String(), "DataPlaneDisabled")
}

func TestOCIDNSProxyUnavailableWithoutService(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	for _, p := range []struct{ method, path string }{
		{http.MethodGet, dnsBase + "/compartments"},
		{http.MethodGet, dnsBase + "/dataplane"},
		{http.MethodPost, dnsBase + "/query"},
		{http.MethodGet, dnsAPI + "/zones?compartmentId=" + dnsComp},
		{http.MethodPatch, dnsAPI + "/zones/z1/records"},
	} {
		got := lbDo(t, h, p.method, p.path, `{}`)
		assert.Equal(t, http.StatusServiceUnavailable, got.Code, "%s %s", p.method, p.path)
	}
}
