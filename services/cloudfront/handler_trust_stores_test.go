package cloudfront_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestTrustStore_CRUD tests trust store CRUD.
func TestTrustStore_CRUD(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	// Create. Real CreateTrustStoreInput has no Comment member (cloudfront@v1.67.4
	// api_op_CreateTrustStore.go:28-49) -- only Name and CaCertificatesBundleSource.
	body := `<CreateTrustStoreRequest><Name>my-store</Name>` +
		`<CaCertificatesBundleSource><CaCertificatesBundleS3Location>` +
		`<Bucket>my-bucket</Bucket><Key>ca.pem</Key><Region>us-east-1</Region>` +
		`</CaCertificatesBundleS3Location></CaCertificatesBundleSource></CreateTrustStoreRequest>`
	out := cfOK(t, h, http.MethodPost, prefix+"trust-store", body)
	id := extractXMLID(t, out)
	if id == "" {
		t.Fatalf("expected Id in response: %s", out)
	}
	if !strings.Contains(out, "<Status>active</Status>") {
		t.Errorf("expected TrustStoreStatus 'active' (cloudfront@v1.67.4 types/enums.go:966-973), got: %s", out)
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

	// Update. UpdateTrustStoreInput has no Name/Comment member in the real API --
	// only CaCertificatesBundleSource -- so an empty body is a legitimate no-op update.
	cfOK(t, h, http.MethodPut, prefix+"trust-store/"+id, "")

	// Delete
	cfOK(t, h, http.MethodDelete, prefix+"trust-store/"+id, "")
}

// TestTrustStore_BundleAndStatus verifies Status, NumberOfCaCertificates, and
// LastModifiedTime round-trip through create/get/update, and that fields omitted on update
// (no CaCertificatesBundleSource, no UseClientCertificateOCSPEndpoint) leave the existing
// values unchanged. The real TrustStore response never echoes the CA bundle's content or
// location (cloudfront@v1.67.4 types/types.go:6633-6661), so this cannot assert on it directly.
func TestTrustStore_BundleAndStatus(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	createBody := `<CreateTrustStoreRequest><Name>bundle-store</Name>` +
		`<CaCertificatesBundleSource><CaCertificatesBundleS3Location>` +
		`<Bucket>my-bucket</Bucket><Key>ca.pem</Key><Region>us-east-1</Region>` +
		`</CaCertificatesBundleS3Location></CaCertificatesBundleSource>` +
		`<UseClientCertificateOCSPEndpoint>true</UseClientCertificateOCSPEndpoint></CreateTrustStoreRequest>`
	createRR := cfRequest(t, h, http.MethodPost, prefix+"trust-store", createBody)
	out := createRR.Body.String()
	if !strings.Contains(out, "<Status>active</Status>") {
		t.Errorf("expected active status, got: %s", out)
	}
	if !strings.Contains(out, "<NumberOfCaCertificates>1</NumberOfCaCertificates>") {
		t.Errorf("expected a non-empty bundle to report 1 CA certificate, got: %s", out)
	}
	if !strings.Contains(out, "<UseClientCertificateOCSPEndpoint>true</UseClientCertificateOCSPEndpoint>") {
		t.Errorf("expected UseClientCertificateOCSPEndpoint to round-trip, got: %s", out)
	}
	if !strings.Contains(out, "<LastModifiedTime>") {
		t.Errorf("expected LastModifiedTime, got: %s", out)
	}
	id := extractXMLID(t, out)
	etag := createRR.Header().Get("ETag")

	// Update with an empty body: name, bundle, and UseClientCertificateOCSPEndpoint must all
	// be preserved. UpdateTrustStoreInput has no Name member in the real API, so it can never
	// change via this operation.
	updateRR := cfRequest(t, h, http.MethodPut, prefix+"trust-store/"+id, "")
	updateOut := updateRR.Body.String()
	if !strings.Contains(updateOut, "<Name>bundle-store</Name>") {
		t.Errorf("expected name preserved after no-op update, got: %s", updateOut)
	}
	if !strings.Contains(updateOut, "<UseClientCertificateOCSPEndpoint>true</UseClientCertificateOCSPEndpoint>") {
		t.Errorf("expected UseClientCertificateOCSPEndpoint preserved after no-op update, got: %s", updateOut)
	}
	if updateRR.Header().Get("ETag") == etag {
		t.Errorf("expected ETag to rotate on update, still: %s", etag)
	}

	// Update with a new bundle via the real CaCertificatesBundleSource>
	// CaCertificatesBundleS3Location shape (cloudfront@v1.67.4 serializers.go): the response
	// still reports 1 CA certificate (the replacement bundle is also non-empty).
	updateOut2 := cfOK(t, h, http.MethodPut, prefix+"trust-store/"+id,
		`<CaCertificatesBundleSource><CaCertificatesBundleS3Location>`+
			`<Bucket>new-bucket</Bucket><Key>new-ca.pem</Key><Region>us-east-1</Region>`+
			`</CaCertificatesBundleS3Location></CaCertificatesBundleSource>`)
	if !strings.Contains(updateOut2, "<NumberOfCaCertificates>1</NumberOfCaCertificates>") {
		t.Errorf("expected replacement bundle to still report 1 CA certificate, got: %s", updateOut2)
	}
}

// TestTrustStore_WireShape_RealFields is a regression test for gopherstack-to8g: the create and
// get response XML must carry exactly types.TrustStore's real fields (cloudfront@v1.67.4
// types/types.go:6633-6661) and none of the fabricated ones (Comment,
// CertificateAuthorityCertificatesBundle) the handler used to emit.
func TestTrustStore_WireShape_RealFields(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	createBody := `<CreateTrustStoreRequest><Name>wire-shape-store</Name>` +
		`<CaCertificatesBundleSource><CaCertificatesBundleS3Location>` +
		`<Bucket>my-bucket</Bucket><Key>ca.pem</Key><Region>us-east-1</Region>` +
		`</CaCertificatesBundleS3Location></CaCertificatesBundleSource></CreateTrustStoreRequest>`
	created := cfOK(t, h, http.MethodPost, prefix+"trust-store", createBody)
	id := extractXMLID(t, created)
	got := cfOK(t, h, http.MethodGet, prefix+"trust-store/"+id, "")

	tests := []struct {
		name string
		xml  string
	}{
		{name: "create_response", xml: created},
		{name: "get_response", xml: got},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Contains(t, tt.xml, "<Arn>")
			assert.Contains(t, tt.xml, "<Id>")
			assert.Contains(t, tt.xml, "<Name>wire-shape-store</Name>")
			assert.Contains(t, tt.xml, "<Status>active</Status>")
			assert.Contains(t, tt.xml, "<Reason></Reason>")
			assert.Contains(t, tt.xml, "<NumberOfCaCertificates>1</NumberOfCaCertificates>")
			assert.Contains(t, tt.xml, "<UseClientCertificateOCSPEndpoint>false</UseClientCertificateOCSPEndpoint>")
			assert.Contains(t, tt.xml, "<LastModifiedTime>")
			assert.NotContains(t, tt.xml, "<Comment>", "Comment is not a member of types.TrustStore")
			assert.NotContains(t, tt.xml, "CertificateAuthorityCertificatesBundle",
				"AWS never echoes the CA bundle's content or location")
			assert.NotContains(t, tt.xml, "<S3Bucket>", "AWS never echoes the CA bundle's content or location")
		})
	}
}

// TestTrustStore_NameUniqueness verifies that creating a trust store with a name that
// already exists fails with 409 EntityAlreadyExists (the generic AWS fallback code for
// resources without a dedicated AlreadyExists error type). UpdateTrustStoreInput has no
// Name member in the real API, so renaming via update is not a real scenario to cover here.
func TestTrustStore_NameUniqueness(t *testing.T) {
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
}

// TestTrustStore_NotFound verifies Get/Update/Delete on a missing ID return 404
// EntityNotFound.
func TestTrustStore_NotFound(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	getRR := cfRequest(t, h, http.MethodGet, prefix+"trust-store/does-not-exist", "")
	if getRR.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on get, got %d: %s", getRR.Code, getRR.Body.String())
	}
	if !strings.Contains(getRR.Body.String(), "EntityNotFound") {
		t.Errorf("expected EntityNotFound error, got: %s", getRR.Body.String())
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

// TestTrustStore_IfMatchEnforcement verifies that a mismatched If-Match header on
// update/delete is rejected with 412 PreconditionFailed, and that the correct ETag succeeds.
func TestTrustStore_IfMatchEnforcement(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
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

	// Wrong If-Match on update -> 412. The ETag check happens before body parsing, so
	// the request body's shape doesn't matter here.
	badUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"trust-store/"+id,
		nil, map[string]string{"If-Match": "bogus-etag"})
	if badUpdate.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412 on bad If-Match update, got %d: %s", badUpdate.Code, badUpdate.Body.String())
	}

	// Correct If-Match on update -> succeeds, ETag rotates.
	goodUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"trust-store/"+id,
		nil, map[string]string{"If-Match": etag})
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

// TestTrustStore_Tags verifies tags can be attached to a trust store via the generic
// TagResource/ListTagsForResource endpoints, using the shared tag store.
func TestTrustStore_Tags(t *testing.T) {
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

// TestTrustStore_Persistence verifies trust stores, their tags, and derived indexes
// (ARN and name-uniqueness) survive a Snapshot/Restore round-trip.
func TestTrustStore_Persistence(t *testing.T) {
	t.Parallel()
	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	out := cfOK(t, h, http.MethodPost, prefix+"trust-store",
		`<CreateTrustStoreRequest><Name>persist-store</Name>`+
			`<CaCertificatesBundleSource><CaCertificatesBundleS3Location>`+
			`<Bucket>my-bucket</Bucket><Key>ca.pem</Key><Region>us-east-1</Region>`+
			`</CaCertificatesBundleS3Location></CaCertificatesBundleSource></CreateTrustStoreRequest>`)
	id := extractXMLID(t, out)
	arn := fmt.Sprintf("arn:aws:cloudfront::123456789012:trust-store/%s", id)

	tagBody := `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
		`<Items><Tag><Key>env</Key><Value>prod</Value></Tag></Items></Tags>`
	cfOK(t, h, http.MethodPost, prefix+"tagging?Resource="+arn, tagBody)

	snap := h.Snapshot(t.Context())
	if len(snap) == 0 {
		t.Fatal("expected non-empty snapshot")
	}

	restored := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	if err := restored.Restore(t.Context(), snap); err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	h2 := cloudfront.NewHandler(restored)

	// The restored trust store must still be gettable, with its bundle-derived state intact.
	getOut := cfOK(t, h2, http.MethodGet, prefix+"trust-store/"+id, "")
	if !strings.Contains(getOut, "<Name>persist-store</Name>") {
		t.Errorf("expected name preserved after restore, got: %s", getOut)
	}
	if !strings.Contains(getOut, "<NumberOfCaCertificates>1</NumberOfCaCertificates>") {
		t.Errorf("expected CA cert count preserved after restore, got: %s", getOut)
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

// TestUpdateTrustStore_RealClient is a regression test for gopherstack-ob1g:
// UpdateTrustStoreInput's real root element is CaCertificatesBundleSource (cloudfront@v1.67.4
// serializers.go: awsRestxml_serializeOpUpdateTrustStore's payloadRoot.Local), not
// TrustStoreConfig. A handler expecting the wrong root discards the whole body via
// xml.Unmarshal's error, so the CA bundle update silently no-ops -- driving this through
// the real SDK client is what catches it, since the SDK writes the exact wire shape
// regardless of what the handler expects.
func TestUpdateTrustStore_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	created, err := client.CreateTrustStore(t.Context(), &cfsdk.CreateTrustStoreInput{
		Name: aws.String("trust-store-rc"),
		CaCertificatesBundleSource: &types.CaCertificatesBundleSourceMemberCaCertificatesBundleS3Location{
			Value: types.CaCertificatesBundleS3Location{
				Bucket: aws.String("ca-bucket"),
				Key:    aws.String("ca.pem"),
				Region: aws.String("us-east-1"),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.TrustStore)
	require.False(t, aws.ToBool(created.TrustStore.UseClientCertificateOCSPEndpoint))

	updated, err := client.UpdateTrustStore(t.Context(), &cfsdk.UpdateTrustStoreInput{
		Id:      created.TrustStore.Id,
		IfMatch: created.ETag,
		CaCertificatesBundleSource: &types.CaCertificatesBundleSourceMemberCaCertificatesBundleS3Location{
			Value: types.CaCertificatesBundleS3Location{
				Bucket: aws.String("ca-bucket-2"),
				Key:    aws.String("ca-2.pem"),
				Region: aws.String("us-east-1"),
			},
		},
		UseClientCertificateOCSPEndpoint: aws.Bool(true),
	})
	require.NoError(t, err)

	// The real SDK's TrustStore output shape has no field for the CA bundle at all
	// (types.TrustStore, cloudfront@v1.67.4), so it can't prove the bundle replacement
	// specifically -- but UseClientCertificateOCSPEndpoint is real and echoed, and the
	// unfixed handler (wrong request root, error discarded) left it false because the
	// whole body silently failed to parse.
	assert.True(t, aws.ToBool(updated.TrustStore.UseClientCertificateOCSPEndpoint))

	getOut, err := client.GetTrustStore(t.Context(), &cfsdk.GetTrustStoreInput{Identifier: created.TrustStore.Id})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(getOut.TrustStore.UseClientCertificateOCSPEndpoint))
}

// TestUpdateTrustStore_MalformedBodyHandled verifies a malformed request body is
// rejected with 400 MalformedXML instead of silently no-opping the update
// (gopherstack-ob1g: the previous handler discarded xml.Unmarshal's error).
func TestUpdateTrustStore_MalformedBodyHandled(t *testing.T) {
	t.Parallel()

	h := newCFHandler(t)
	const prefix = "/2020-05-31/"

	created := cfOK(t, h, http.MethodPost, prefix+"trust-store",
		`<TrustStoreConfig><Name>trust-store-malformed</Name></TrustStoreConfig>`)
	id := extractXMLID(t, created)

	rec := cfRequest(t, h, http.MethodPut, prefix+"trust-store/"+id, "<<<not xml")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MalformedXML")
}

// TestListTrustStores_ItemShape_RealClient is a regression test for gopherstack-21my:
// ListTrustStores' item struct (tsSummary, handler_trust_stores.go) omitted ETag, Status,
// and LastModifiedTime entirely (and tagged ARN as "ARN" rather than the real
// deserializer's "Arn" -- a case-only mismatch), even though the sibling GetTrustStore
// (trustStoreXML) already emits Status and LastModifiedTime correctly from the same
// backing TrustStore fields. Seeds two trust stores and asserts every field round-trips.
func TestListTrustStores_ItemShape_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	mk := func(name string) *cfsdk.CreateTrustStoreOutput {
		out, err := client.CreateTrustStore(t.Context(), &cfsdk.CreateTrustStoreInput{
			Name: aws.String(name),
			CaCertificatesBundleSource: &types.CaCertificatesBundleSourceMemberCaCertificatesBundleS3Location{
				Value: types.CaCertificatesBundleS3Location{
					Bucket: aws.String("my-bucket"),
					Key:    aws.String("bundle.pem"),
					Region: aws.String("us-east-1"),
				},
			},
		})
		require.NoError(t, err)

		return out
	}

	first := mk("list-shape-ts-1")
	second := mk("list-shape-ts-2")

	listed, err := client.ListTrustStores(t.Context(), &cfsdk.ListTrustStoresInput{})
	require.NoError(t, err)
	require.Len(t, listed.TrustStoreList, 2)

	byID := make(map[string]types.TrustStoreSummary, 2)
	for _, item := range listed.TrustStoreList {
		require.NotNil(t, item.Id)
		byID[*item.Id] = item
	}

	item1, ok := byID[*first.TrustStore.Id]
	require.True(t, ok)
	assert.Equal(t, aws.ToString(first.TrustStore.Arn), aws.ToString(item1.Arn))
	assert.NotEmpty(t, aws.ToString(item1.ETag), "ETag must round-trip, not decode empty")
	assert.NotEmpty(t, string(item1.Status), "Status must round-trip, not decode empty")
	assert.NotNil(t, item1.LastModifiedTime, "LastModifiedTime must round-trip, not decode nil")

	item2, ok := byID[*second.TrustStore.Id]
	require.True(t, ok)
	assert.Equal(t, aws.ToString(second.TrustStore.Arn), aws.ToString(item2.Arn))
	assert.NotEmpty(t, aws.ToString(item2.ETag))
}
