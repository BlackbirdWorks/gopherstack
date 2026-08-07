package cloudfront_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestAnycastIPList_NameUniqueness verifies CreateAnycastIPList rejects a duplicate
// name and that DeleteAnycastIPList frees the name for reuse.
func TestAnycastIPList_NameUniqueness(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")

	first, err := b.CreateAnycastIPList("dup-name", 3)
	require.NoError(t, err)

	_, err = b.CreateAnycastIPList("dup-name", 5)
	require.Error(t, err)

	require.NoError(t, b.DeleteAnycastIPList(first.ID))

	// Name is free again after delete.
	_, err = b.CreateAnycastIPList("dup-name", 5)
	require.NoError(t, err)
}

// TestAnycastIPList_GeneratedIPs verifies AnycastIPs is populated with IPCount
// entries on create, and regenerated to match a new count on update.
func TestAnycastIPList_GeneratedIPs(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")

	list, err := b.CreateAnycastIPList("ips-list", 4)
	require.NoError(t, err)
	assert.Len(t, list.AnycastIPs, 4)
	assert.NotEmpty(t, list.ETag)

	oldETag := list.ETag
	updated, err := b.UpdateAnycastIPList(list.ID, 7)
	require.NoError(t, err)
	assert.Len(t, updated.AnycastIPs, 7)
	assert.NotEqual(t, oldETag, updated.ETag)

	// A no-op update (ipCount <= 0) leaves the IPs and count unchanged.
	unchanged, err := b.UpdateAnycastIPList(list.ID, 0)
	require.NoError(t, err)
	assert.Len(t, unchanged.AnycastIPs, 7)
}

// TestAnycastIPList_IfMatchEnforcement mirrors the trust store If-Match test:
// a mismatched If-Match header on update/delete is rejected with 412.
func TestAnycastIPList_IfMatchEnforcement(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	const prefix = "/2020-05-31/"

	createRec := doXML(t, h, http.MethodPost, prefix+"anycast-ip-list",
		[]byte(`<AnycastIPListRequest><Name>etag-list</Name><IPCount>3</IPCount></AnycastIPListRequest>`))
	require.Equal(t, http.StatusCreated, createRec.Code)
	id := extractXMLID(t, createRec.Body.String())
	etag := createRec.Header().Get("ETag")
	require.NotEmpty(t, etag)

	badUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"anycast-ip-list/"+id,
		[]byte(`<AnycastIpListConfig><IpCount>9</IpCount></AnycastIpListConfig>`),
		map[string]string{"If-Match": "bogus-etag"})
	assert.Equal(t, http.StatusPreconditionFailed, badUpdate.Code)

	goodUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"anycast-ip-list/"+id,
		[]byte(`<AnycastIpListConfig><IpCount>9</IpCount></AnycastIpListConfig>`),
		map[string]string{"If-Match": etag})
	require.Equal(t, http.StatusOK, goodUpdate.Code)
	newETag := goodUpdate.Header().Get("ETag")
	assert.NotEmpty(t, newETag)
	assert.NotEqual(t, etag, newETag)

	badDelete := doXMLWithHeaders(t, h, http.MethodDelete, prefix+"anycast-ip-list/"+id, nil,
		map[string]string{"If-Match": "bogus-etag"})
	assert.Equal(t, http.StatusPreconditionFailed, badDelete.Code)

	goodDelete := doXMLWithHeaders(t, h, http.MethodDelete, prefix+"anycast-ip-list/"+id, nil,
		map[string]string{"If-Match": newETag})
	assert.Equal(t, http.StatusNoContent, goodDelete.Code)
}

// TestAnycastIPList_Tags verifies tags supplied on create are stored and retrievable
// via the generic ListTagsForResource endpoint.
func TestAnycastIPList_Tags(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	const prefix = "/2020-05-31/"

	createRec := doXML(t, h, http.MethodPost, prefix+"anycast-ip-list",
		[]byte(`<AnycastIPListRequest><Name>tagged-list</Name><IPCount>2</IPCount>`+
			`<Tags><Items><Tag><Key>env</Key><Value>prod</Value></Tag></Items></Tags>`+
			`</AnycastIPListRequest>`))
	require.Equal(t, http.StatusCreated, createRec.Code)
	id := extractXMLID(t, createRec.Body.String())
	arn := fmt.Sprintf("arn:aws:cloudfront::123456789012:anycast-ip-list/%s", id)

	listRec := doXML(t, h, http.MethodGet, prefix+"tagging?Resource="+arn, nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	assert.Contains(t, listRec.Body.String(), "<Key>env</Key>")
	assert.Contains(t, listRec.Body.String(), "<Value>prod</Value>")
}

// TestAnycastIPList_PersistenceRoundTrip verifies AnycastIPList survives a
// Snapshot/Restore cycle including its ETag, tags, generated IPs, and name-uniqueness index.
func TestAnycastIPList_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	list, err := b.CreateAnycastIPList("persist-list", 3)
	require.NoError(t, err)

	h := cloudfront.NewHandler(b)
	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h2 := cloudfront.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	restored, err := b2.GetAnycastIPList(list.ID)
	require.NoError(t, err)
	assert.Equal(t, list.ETag, restored.ETag)
	assert.Len(t, restored.AnycastIPs, 3)

	// The name-uniqueness index survived restore: creating a second list with the same name
	// still fails.
	_, err = b2.CreateAnycastIPList("persist-list", 5)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// ContinuousDeploymentPolicy
// ---------------------------------------------------------------------------

// TestAnycastIPList_CRUD tests anycast IP list Get/List/Update/Delete.
func TestAnycastIPList_CRUD(t *testing.T) {
	t.Parallel()
	b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
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

// TestCreateAnycastIPList covers the CreateAnycastIPList operation.
func TestCreateAnycastIPList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name: "create_anycast_ip_list_success",
			body: []byte(
				`<AnycastIPListRequest><Name>my-anycast-list</Name><IPCount>5</IPCount></AnycastIPListRequest>`,
			),
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<AnycastIPList")
				assert.Contains(t, rec.Body.String(), "<Name>my-anycast-list</Name>")
				assert.Contains(t, rec.Body.String(), "<Status>Deployed</Status>")
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name:       "create_anycast_ip_list_empty_name",
			body:       []byte(`<AnycastIPListRequest><Name></Name><IPCount>5</IPCount></AnycastIPListRequest>`),
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidArgument")
			},
		},
		{
			name:       "create_anycast_ip_list_malformed_xml",
			body:       []byte(`<<<not xml`),
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "MalformedXML")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/anycast-ip-list", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestAnycastIPList_IPCountValidation verifies IPCount must be positive.
func TestAnycastIPList_IPCountValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "zero_count",
			body:       `<AnycastIPListRequest><Name>test-list</Name><IPCount>0</IPCount></AnycastIPListRequest>`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "negative_count",
			body:       `<AnycastIPListRequest><Name>test-list-neg</Name><IPCount>-5</IPCount></AnycastIPListRequest>`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "valid_count",
			body:       `<AnycastIPListRequest><Name>test-list-valid</Name><IPCount>10</IPCount></AnycastIPListRequest>`,
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/anycast-ip-list", []byte(tt.body))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
