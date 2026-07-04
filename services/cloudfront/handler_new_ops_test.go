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

// TestNewOps_TrustStore_BundleAndStatus verifies the certificate bundle, Status, and
// LastModifiedTime round-trip through create/get/update, and that fields omitted on update
// (empty bundle, empty comment) leave the existing values unchanged.
func TestNewOps_TrustStore_BundleAndStatus(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	createBody := `<TrustStoreConfig><Name>bundle-store</Name><Comment>initial</Comment>` +
		`<CertificateAuthorityCertificatesBundle><S3Bucket>my-bucket</S3Bucket><S3Key>ca.pem</S3Key>` +
		`</CertificateAuthorityCertificatesBundle></TrustStoreConfig>`
	out := cfOK(t, h, http.MethodPost, prefix+"trust-store", createBody)
	if !strings.Contains(out, "<Status>Deployed</Status>") {
		t.Errorf("expected Deployed status, got: %s", out)
	}
	if !strings.Contains(out, "<S3Bucket>my-bucket</S3Bucket>") || !strings.Contains(out, "<S3Key>ca.pem</S3Key>") {
		t.Errorf("expected bundle echoed back, got: %s", out)
	}
	if !strings.Contains(out, "<LastModifiedTime>") {
		t.Errorf("expected LastModifiedTime, got: %s", out)
	}
	id := extractXMLID(t, out)

	// Update with only a Comment change: bundle and name must be preserved.
	updateOut := cfOK(t, h, http.MethodPut, prefix+"trust-store/"+id,
		`<TrustStoreConfig><Comment>second</Comment></TrustStoreConfig>`)
	if !strings.Contains(updateOut, "<S3Bucket>my-bucket</S3Bucket>") {
		t.Errorf("expected bundle preserved after comment-only update, got: %s", updateOut)
	}
	if !strings.Contains(updateOut, "<Name>bundle-store</Name>") {
		t.Errorf("expected name preserved after comment-only update, got: %s", updateOut)
	}
	if !strings.Contains(updateOut, "<Comment>second</Comment>") {
		t.Errorf("expected comment updated, got: %s", updateOut)
	}

	// Update with a new bundle: it must fully replace the old one.
	updateOut2 := cfOK(t, h, http.MethodPut, prefix+"trust-store/"+id,
		`<TrustStoreConfig><CertificateAuthorityCertificatesBundle>`+
			`<InlineCertificateBundle>-----BEGIN CERT-----abc-----END CERT-----</InlineCertificateBundle>`+
			`</CertificateAuthorityCertificatesBundle></TrustStoreConfig>`)
	if strings.Contains(updateOut2, "<S3Bucket>my-bucket</S3Bucket>") {
		t.Errorf("expected old bundle replaced, got: %s", updateOut2)
	}
	if !strings.Contains(updateOut2, "-----BEGIN CERT-----abc-----END CERT-----") {
		t.Errorf("expected new inline bundle present, got: %s", updateOut2)
	}
}

// TestNewOps_TrustStore_NameUniqueness verifies that creating a trust store with a name that
// already exists fails with 409 DistributionAlreadyExists, and that renaming to a taken name
// on update also fails.
func TestNewOps_TrustStore_NameUniqueness(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	body := `<TrustStoreConfig><Name>dup-store</Name></TrustStoreConfig>`
	cfOK(t, h, http.MethodPost, prefix+"trust-store", body)

	dupRR := cfRequest(t, h, http.MethodPost, prefix+"trust-store", body)
	if dupRR.Code != http.StatusConflict {
		t.Fatalf("expected 409 on duplicate name, got %d: %s", dupRR.Code, dupRR.Body.String())
	}
	if !strings.Contains(dupRR.Body.String(), "AlreadyExists") {
		t.Errorf("expected AlreadyExists error, got: %s", dupRR.Body.String())
	}

	// A second, distinctly named trust store may not be renamed onto the first's name.
	other := cfOK(t, h, http.MethodPost, prefix+"trust-store",
		`<TrustStoreConfig><Name>other-store</Name></TrustStoreConfig>`)
	otherID := extractXMLID(t, other)

	renameRR := cfRequest(t, h, http.MethodPut, prefix+"trust-store/"+otherID,
		`<TrustStoreConfig><Name>dup-store</Name></TrustStoreConfig>`)
	if renameRR.Code != http.StatusConflict {
		t.Fatalf("expected 409 on rename collision, got %d: %s", renameRR.Code, renameRR.Body.String())
	}
}

// TestNewOps_TrustStore_NotFound verifies Get/Update/Delete on a missing ID return 404
// NoSuchTrustStore.
func TestNewOps_TrustStore_NotFound(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	getRR := cfRequest(t, h, http.MethodGet, prefix+"trust-store/does-not-exist", "")
	if getRR.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on get, got %d: %s", getRR.Code, getRR.Body.String())
	}
	if !strings.Contains(getRR.Body.String(), "NoSuchTrustStore") {
		t.Errorf("expected NoSuchTrustStore error, got: %s", getRR.Body.String())
	}

	updateRR := cfRequest(t, h, http.MethodPut, prefix+"trust-store/does-not-exist",
		`<TrustStoreConfig><Comment>x</Comment></TrustStoreConfig>`)
	if updateRR.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on update, got %d: %s", updateRR.Code, updateRR.Body.String())
	}

	deleteRR := cfRequest(t, h, http.MethodDelete, prefix+"trust-store/does-not-exist", "")
	if deleteRR.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on delete, got %d: %s", deleteRR.Code, deleteRR.Body.String())
	}
}

// TestNewOps_TrustStore_IfMatchEnforcement verifies that a mismatched If-Match header on
// update/delete is rejected with 412 PreconditionFailed, and that the correct ETag succeeds.
func TestNewOps_TrustStore_IfMatchEnforcement(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	const prefix = "/2020-05-31/"

	createRec := doXML(t, h, http.MethodPost, prefix+"trust-store",
		[]byte(`<TrustStoreConfig><Name>etag-store</Name></TrustStoreConfig>`))
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create: got %d: %s", createRec.Code, createRec.Body.String())
	}
	id := extractXMLID(t, createRec.Body.String())
	etag := createRec.Header().Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag on create")
	}

	// Wrong If-Match on update -> 412.
	badUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"trust-store/"+id,
		[]byte(`<TrustStoreConfig><Comment>x</Comment></TrustStoreConfig>`),
		map[string]string{"If-Match": "bogus-etag"})
	if badUpdate.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 on bad If-Match update, got %d: %s", badUpdate.Code, badUpdate.Body.String())
	}

	// Correct If-Match on update -> succeeds, ETag rotates.
	goodUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"trust-store/"+id,
		[]byte(`<TrustStoreConfig><Comment>x</Comment></TrustStoreConfig>`),
		map[string]string{"If-Match": etag})
	if goodUpdate.Code != http.StatusOK {
		t.Fatalf("expected 200 on good If-Match update, got %d: %s", goodUpdate.Code, goodUpdate.Body.String())
	}
	newEtag := goodUpdate.Header().Get("ETag")
	if newEtag == "" || newEtag == etag {
		t.Errorf("expected ETag to rotate on update, old=%s new=%s", etag, newEtag)
	}

	// Wrong If-Match on delete -> 412.
	badDelete := doXMLWithHeaders(t, h, http.MethodDelete, prefix+"trust-store/"+id, nil,
		map[string]string{"If-Match": "bogus-etag"})
	if badDelete.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 on bad If-Match delete, got %d: %s", badDelete.Code, badDelete.Body.String())
	}

	// Correct If-Match on delete -> succeeds.
	goodDelete := doXMLWithHeaders(t, h, http.MethodDelete, prefix+"trust-store/"+id, nil,
		map[string]string{"If-Match": newEtag})
	if goodDelete.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on good If-Match delete, got %d: %s", goodDelete.Code, goodDelete.Body.String())
	}
}

// TestNewOps_TrustStore_Tags verifies tags can be attached to a trust store via the generic
// TagResource/ListTagsForResource endpoints, using the shared tag store.
func TestNewOps_TrustStore_Tags(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	out := cfOK(t, h, http.MethodPost, prefix+"trust-store",
		`<TrustStoreConfig><Name>tagged-store</Name></TrustStoreConfig>`)
	id := extractXMLID(t, out)
	arn := fmt.Sprintf("arn:aws:cloudfront::123456789012:trust-store/%s", id)

	tagBody := `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
		`<Items><Tag><Key>env</Key><Value>prod</Value></Tag></Items></Tags>`
	tagRR := cfRequest(t, h, http.MethodPost, prefix+"tagging?Resource="+arn, tagBody)
	if tagRR.Code != http.StatusNoContent {
		t.Fatalf("expected 204 tagging trust store, got %d: %s", tagRR.Code, tagRR.Body.String())
	}

	listRR := cfRequest(t, h, http.MethodGet, prefix+"tagging?Resource="+arn, "")
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 listing tags, got %d: %s", listRR.Code, listRR.Body.String())
	}
	if !strings.Contains(listRR.Body.String(), "<Key>env</Key>") ||
		!strings.Contains(listRR.Body.String(), "<Value>prod</Value>") {
		t.Errorf("expected env=prod tag in response, got: %s", listRR.Body.String())
	}
}

// TestNewOps_ListDistributionsByTrustStore verifies distributions referencing a trust store
// are returned, and that an unrelated trust store ID yields an empty (but valid) list.
func TestNewOps_ListDistributionsByTrustStore(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	out := cfOK(t, h, http.MethodPost, prefix+"trust-store",
		`<TrustStoreConfig><Name>assoc-store</Name></TrustStoreConfig>`)
	tsID := extractXMLID(t, out)

	distBody := `<DistributionConfig>` +
		`<CallerReference>cr-ts</CallerReference><Enabled>true</Enabled>` +
		`<TrustStoreId>` + tsID + `</TrustStoreId>` +
		`</DistributionConfig>`
	cfOK(t, h, http.MethodPost, prefix+"distribution", distBody)

	resp := cfOK(t, h, http.MethodGet, prefix+"distributions/by-trust-store-id/"+tsID, "")
	if !strings.Contains(resp, "DistributionList") {
		t.Errorf("expected DistributionList, got: %s", resp)
	}
	if strings.Contains(resp, "<Quantity>0</Quantity>") {
		t.Errorf("expected non-empty list, got: %s", resp)
	}

	empty := cfOK(t, h, http.MethodGet, prefix+"distributions/by-trust-store-id/nonexistent-ts", "")
	if !strings.Contains(empty, "<Quantity>0</Quantity>") {
		t.Errorf("expected empty list for unrelated trust store, got: %s", empty)
	}
}

// TestNewOps_TrustStore_Persistence verifies trust stores, their tags, and derived indexes
// (ARN and name-uniqueness) survive a Snapshot/Restore round-trip.
func TestNewOps_TrustStore_Persistence(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	out := cfOK(t, h, http.MethodPost, prefix+"trust-store",
		`<TrustStoreConfig><Name>persist-store</Name><Comment>persisted</Comment></TrustStoreConfig>`)
	id := extractXMLID(t, out)
	arn := fmt.Sprintf("arn:aws:cloudfront::123456789012:trust-store/%s", id)

	tagBody := `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
		`<Items><Tag><Key>env</Key><Value>prod</Value></Tag></Items></Tags>`
	cfOK(t, h, http.MethodPost, prefix+"tagging?Resource="+arn, tagBody)

	snap := h.Snapshot(t.Context())
	if len(snap) == 0 {
		t.Fatal("expected non-empty snapshot")
	}

	restored := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
	if err := restored.Restore(t.Context(), snap); err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	h2 := cloudfront.NewHandler(restored)

	// The restored trust store must still be gettable.
	getOut := cfOK(t, h2, http.MethodGet, prefix+"trust-store/"+id, "")
	if !strings.Contains(getOut, "persisted") {
		t.Errorf("expected persisted comment after restore, got: %s", getOut)
	}

	// Tags must still resolve via the restored ARN index.
	listRR := cfRequest(t, h2, http.MethodGet, prefix+"tagging?Resource="+arn, "")
	if listRR.Code != http.StatusOK {
		t.Fatalf("expected 200 listing tags after restore, got %d: %s", listRR.Code, listRR.Body.String())
	}
	if !strings.Contains(listRR.Body.String(), "<Value>prod</Value>") {
		t.Errorf("expected tag preserved after restore, got: %s", listRR.Body.String())
	}

	// The name-uniqueness index must still be enforced after restore.
	dupRR := cfRequest(t, h2, http.MethodPost, prefix+"trust-store",
		`<TrustStoreConfig><Name>persist-store</Name></TrustStoreConfig>`)
	if dupRR.Code != http.StatusConflict {
		t.Fatalf("expected 409 on duplicate name after restore, got %d: %s", dupRR.Code, dupRR.Body.String())
	}
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
