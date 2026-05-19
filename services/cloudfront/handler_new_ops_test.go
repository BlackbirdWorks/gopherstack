package cloudfront_test

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

func newCFHandler(t *testing.T) *cloudfront.Handler {
	t.Helper()
	b := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")

	return cloudfront.NewHandler(b)
}

func cfRequest(t *testing.T, h *cloudfront.Handler, method, path string, body string) *httptest.ResponseRecorder {
	t.Helper()
	var bodyReader io.Reader
	if body != "" {
		bodyReader = bytes.NewBufferString(body)
	} else {
		bodyReader = bytes.NewReader(nil)
	}
	e := echo.New()
	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "application/xml")
	rr := httptest.NewRecorder()
	c := e.NewContext(req, rr)
	if err := h.Handler()(c); err != nil {
		t.Fatalf("handler error: %v", err)
	}

	return rr
}

func cfOK(t *testing.T, h *cloudfront.Handler, method, path, body string) string {
	t.Helper()
	rr := cfRequest(t, h, method, path, body)
	if rr.Code != http.StatusOK && rr.Code != http.StatusCreated && rr.Code != http.StatusNoContent {
		t.Fatalf("%s %s: want 2xx, got %d body: %s", method, path, rr.Code, rr.Body.String())
	}

	return rr.Body.String()
}

func extractXMLID(t *testing.T, xmlStr string) string {
	t.Helper()
	var m struct {
		Inner string `xml:",innerxml"`
	}
	// Simple: look for field between tags
	start := "<Id>"
	end := "</Id>"
	si := strings.Index(xmlStr, start)
	ei := strings.Index(xmlStr, end)
	if si < 0 || ei < 0 {
		return ""
	}
	_ = m

	return xmlStr[si+len(start) : ei]
}

// TestNewOps_TrustStore tests trust store CRUD.
func TestNewOps_TrustStore(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	// Create
	body := `<TrustStoreConfig><Name>my-store</Name><Comment>test</Comment></TrustStoreConfig>`
	out := cfOK(t, h, http.MethodPost, prefix+"trust-store", body)
	id := extractXMLID(t, out)
	if id == "" {
		t.Fatalf("expected Id in response: %s", out)
	}

	// Get
	out2 := cfOK(t, h, http.MethodGet, prefix+"trust-store/"+id, "")
	if extractXMLID(t, out2) != id {
		t.Errorf("get mismatch: %s", out2)
	}

	// List
	out3 := cfOK(t, h, http.MethodGet, prefix+"trust-store", "")
	if !strings.Contains(out3, id) {
		t.Errorf("list missing id %s: %s", id, out3)
	}

	// Update
	cfOK(t, h, http.MethodPut, prefix+"trust-store/"+id,
		`<TrustStoreConfig><Comment>updated</Comment></TrustStoreConfig>`)

	// Delete
	cfOK(t, h, http.MethodDelete, prefix+"trust-store/"+id, "")
}

// TestNewOps_StreamingDistribution tests streaming distribution CRUD.
func TestNewOps_StreamingDistribution(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	sdConfig := `<StreamingDistributionConfig><Comment>test</Comment></StreamingDistributionConfig>`

	// Create
	out := cfOK(t, h, http.MethodPost, prefix+"streaming-distribution", sdConfig)
	id := extractXMLID(t, out)
	if id == "" {
		t.Fatalf("expected Id: %s", out)
	}

	// Get
	out2 := cfOK(t, h, http.MethodGet, prefix+"streaming-distribution/"+id, "")
	if extractXMLID(t, out2) != id {
		t.Errorf("get mismatch: %s", out2)
	}

	// GetConfig
	cfOK(t, h, http.MethodGet, prefix+"streaming-distribution/"+id, "")

	// List
	out3 := cfOK(t, h, http.MethodGet, prefix+"streaming-distribution", "")
	if !strings.Contains(out3, id) {
		t.Errorf("list missing id: %s", out3)
	}

	// Update
	cfOK(t, h, http.MethodPut, prefix+"streaming-distribution/"+id, sdConfig)

	// Delete
	cfOK(t, h, http.MethodDelete, prefix+"streaming-distribution/"+id, "")
}

// TestNewOps_ConnectionGroup tests connection group Get/List/Update/Delete.
func TestNewOps_ConnectionGroup(t *testing.T) {
	t.Parallel()
	b := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
	h := cloudfront.NewHandler(b)
	const prefix = "/2020-05-31/"

	// Create via existing handler
	cgBody := `<CreateConnectionGroupRequest><Name>my-cg</Name><Comment>test</Comment></CreateConnectionGroupRequest>`
	out := cfOK(t, h, http.MethodPost, prefix+"connection-group", cgBody)
	id := extractXMLID(t, out)
	if id == "" {
		t.Fatalf("expected Id: %s", out)
	}

	// Get
	out2 := cfOK(t, h, http.MethodGet, prefix+"connection-group/"+id, "")
	if extractXMLID(t, out2) != id {
		t.Errorf("get mismatch: %s", out2)
	}

	// List
	out3 := cfOK(t, h, http.MethodGet, prefix+"connection-group", "")
	if !strings.Contains(out3, id) {
		t.Errorf("list missing id: %s", out3)
	}

	// Update
	cfOK(t, h, http.MethodPut, prefix+"connection-group/"+id,
		`<UpdateConnectionGroupRequest><Comment>updated</Comment></UpdateConnectionGroupRequest>`)

	// Delete
	cfOK(t, h, http.MethodDelete, prefix+"connection-group/"+id, "")
}

// TestNewOps_AnycastIPList tests anycast IP list Get/List/Update/Delete.
func TestNewOps_AnycastIPList(t *testing.T) {
	t.Parallel()
	b := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
	h := cloudfront.NewHandler(b)
	const prefix = "/2020-05-31/"

	// Create via existing handler
	createBody := `<AnycastIPListRequest><Name>my-list</Name><IPCount>5</IPCount></AnycastIPListRequest>`
	out := cfOK(t, h, http.MethodPost, prefix+"anycast-ip-list", createBody)
	id := extractXMLID(t, out)
	if id == "" {
		t.Fatalf("expected Id: %s", out)
	}

	// Get
	out2 := cfOK(t, h, http.MethodGet, prefix+"anycast-ip-list/"+id, "")
	if extractXMLID(t, out2) != id {
		t.Errorf("get mismatch: %s", out2)
	}

	// List
	out3 := cfOK(t, h, http.MethodGet, prefix+"anycast-ip-list", "")
	if !strings.Contains(out3, id) {
		t.Errorf("list missing id: %s", out3)
	}

	// Update
	cfOK(t, h, http.MethodPut, prefix+"anycast-ip-list/"+id,
		`<AnycastIpListConfig><IPCount>10</IPCount></AnycastIpListConfig>`)

	// Delete
	cfOK(t, h, http.MethodDelete, prefix+"anycast-ip-list/"+id, "")
}

// TestNewOps_MonitoringSubscription tests monitoring subscription Create/Get/Delete.
func TestNewOps_MonitoringSubscription(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const distID = "E1DIST123456"
	const prefix = "/2020-05-31/"
	path := prefix + "distribution/" + distID + "/monitoring-subscription"

	// Create
	body := `<MonitoringSubscription><RealtimeMetricsSubscriptionConfig>` +
		`<RealtimeMetricsSubscriptionStatus>Enabled</RealtimeMetricsSubscriptionStatus>` +
		`</RealtimeMetricsSubscriptionConfig></MonitoringSubscription>`
	cfOK(t, h, http.MethodPost, path, body)

	// Get
	out := cfOK(t, h, http.MethodGet, path, "")
	if !strings.Contains(out, "MonitoringSubscription") {
		t.Errorf("unexpected response: %s", out)
	}

	// Delete
	cfOK(t, h, http.MethodDelete, path, "")
}

// TestNewOps_ResourcePolicy tests resource policy Put/Get/Delete.
func TestNewOps_ResourcePolicy(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const arn = "arn:aws:cloudfront::123456789012:distribution/E1"
	const prefix = "/2020-05-31/"

	// Put
	putBody := fmt.Sprintf(
		`<ResourcePolicy><Policy>{"Version":"2012-10-17"}</Policy><ResourceArn>%s</ResourceArn></ResourcePolicy>`,
		arn,
	)
	cfOK(t, h, http.MethodPost, prefix+"resource-policy", putBody)

	// Get
	out := cfOK(t, h, http.MethodGet, prefix+"resource-policy?arn="+arn, "")
	if !strings.Contains(out, "ResourcePolicy") {
		t.Errorf("unexpected response: %s", out)
	}

	// Delete
	cfOK(t, h, http.MethodDelete, prefix+"resource-policy?arn="+arn, "")
}

// TestNewOps_ListDistributionsByWebACL tests ListDistributionsByWebACLID.
func TestNewOps_ListDistributionsByWebACL(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	// List empty
	out := cfOK(t, h, http.MethodGet, prefix+"distributions/by-web-acl-id/my-waf-id", "")
	if !strings.Contains(out, "DistributionList") {
		t.Errorf("unexpected response: %s", out)
	}
	_ = xml.Unmarshal // ensure xml is imported
}
