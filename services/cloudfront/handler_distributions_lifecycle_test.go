package cloudfront_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestFunctionAssociations covers the function association handlers.
func TestFunctionAssociations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder, string)
		name       string
		method     string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:   "set_and_get_function_associations",
			method: http.MethodPut,
			path:   "",
			body: []byte(
				`<FunctionAssociations>` +
					`<Quantity>1</Quantity>` +
					`<Items>` +
					`<FunctionAssociation>` +
					`<FunctionARN>arn:aws:cloudfront::123456789012:function/my-fn</FunctionARN>` +
					`<EventType>viewer-request</EventType>` +
					`</FunctionAssociation>` +
					`</Items>` +
					`</FunctionAssociations>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("assoc-ref-1", "assoc-dist", true, nil)
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID + "/function-associations"
			},
			wantStatus: http.StatusOK,
			check:      nil,
		},
		{
			name:   "get_function_associations",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("assoc-ref-2", "get-assoc-dist", true, nil)
				require.NoError(t, err)
				associations := []cloudfront.FunctionAssociation{
					{FunctionARN: "arn:aws:cloudfront::123:function/fn", EventType: "viewer-request"},
				}
				require.NoError(t, h.Backend.SetDistributionFunctionAssociations(d.ID, associations))

				return "/2020-05-31/distribution/" + d.ID + "/function-associations"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<FunctionAssociations")
				assert.Contains(t, rec.Body.String(), "<Quantity>1</Quantity>")
			},
		},
		{
			name:   "get_function_associations_dist_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/distribution/doesnotexist/function-associations",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Error>")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			path := tt.path
			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			rec := doXML(t, h, tt.method, path, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.check != nil {
				tt.check(t, rec, path)
			}
		})
	}
}

// TestInMemoryBackend_FunctionAssociations tests function associations directly on the backend.
func TestInMemoryBackend_FunctionAssociations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "set_and_get_associations",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				d, err := b.CreateDistribution("fn-assoc-ref", "fn-assoc-dist", true, nil)
				require.NoError(t, err)

				assocs := []cloudfront.FunctionAssociation{
					{FunctionARN: "arn:aws:cloudfront::123:function/my-fn", EventType: "viewer-request"},
				}
				require.NoError(t, b.SetDistributionFunctionAssociations(d.ID, assocs))

				got, err := b.GetDistributionFunctionAssociations(d.ID)
				require.NoError(t, err)
				require.Len(t, got, 1)
				assert.Equal(t, "arn:aws:cloudfront::123:function/my-fn", got[0].FunctionARN)
			},
		},
		{
			name: "get_associations_dist_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.GetDistributionFunctionAssociations("doesnotexist")
				require.Error(t, err)
			},
		},
		{
			name: "set_associations_dist_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.SetDistributionFunctionAssociations("doesnotexist", nil)
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}

// TestDistributionCRUD covers create, get, update, list, and delete operations.
func TestDistributionCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder, string)
		headers    func(*testing.T, *cloudfront.Handler, string) map[string]string
		name       string
		method     string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:   "create_distribution",
			method: http.MethodPost,
			path:   "/2020-05-31/distribution",
			body:   minimalDistConfig("ref-001", "my-dist", true),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Distribution")
				assert.Contains(t, rec.Body.String(), "<Status>Deployed</Status>")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name:   "get_distribution",
			method: http.MethodGet,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-002", "get-dist", true,
					minimalDistConfig("ref-002", "get-dist", true))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Distribution")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "get_distribution_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/distribution/DOESNOTEXIST",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name:   "get_distribution_config",
			method: http.MethodGet,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-003", "cfg-dist", true,
					minimalDistConfig("ref-003", "cfg-dist", true))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID + "/config"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "DistributionConfig")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "update_distribution",
			method: http.MethodPut,
			path:   "", // set in setup
			body:   minimalDistConfig("ref-004", "updated-dist", false),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-004", "orig-dist", true,
					minimalDistConfig("ref-004", "orig-dist", true))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID + "/config"
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				// path is "/2020-05-31/distribution/{ID}/config" — extract ID
				parts := strings.Split(strings.TrimPrefix(path, "/2020-05-31/distribution/"), "/")
				d, err := h.Backend.GetDistribution(parts[0])
				require.NoError(t, err)

				return map[string]string{"If-Match": d.ETag}
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Distribution")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "list_distributions",
			method: http.MethodGet,
			path:   "/2020-05-31/distribution",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateDistribution("ref-005", "list-dist", true,
					minimalDistConfig("ref-005", "list-dist", true))
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "DistributionList")
			},
		},
		{
			name:   "delete_distribution",
			method: http.MethodDelete,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-006", "del-dist", false,
					minimalDistConfig("ref-006", "del-dist", false))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				id := strings.TrimPrefix(path, "/2020-05-31/distribution/")
				d, err := h.Backend.GetDistribution(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": d.ETag}
			},
			wantStatus: http.StatusNoContent,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder, _ string) { t.Helper() },
		},
		{
			name:   "delete_distribution_not_found",
			method: http.MethodDelete,
			path:   "/2020-05-31/distribution/DOESNOTEXIST",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name:   "update_distribution_precondition_failed",
			method: http.MethodPut,
			path:   "", // set in setup
			body:   minimalDistConfig("ref-007", "updated-dist", false),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-007", "orig-dist", true,
					minimalDistConfig("ref-007", "orig-dist", true))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID + "/config"
			},
			wantStatus: http.StatusPreconditionFailed,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "PreconditionFailed")
			},
		},
		{
			name:   "delete_distribution_precondition_failed",
			method: http.MethodDelete,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-008", "del-dist-2", false,
					minimalDistConfig("ref-008", "del-dist-2", false))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID
			},
			wantStatus: http.StatusPreconditionFailed,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "PreconditionFailed")
			},
		},
		{
			name:   "delete_distribution_not_disabled",
			method: http.MethodDelete,
			path:   "", // set in setup
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-009", "enabled-dist", true,
					minimalDistConfig("ref-009", "enabled-dist", true))
				require.NoError(t, err)

				return "/2020-05-31/distribution/" + d.ID
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				id := strings.TrimPrefix(path, "/2020-05-31/distribution/")
				d, err := h.Backend.GetDistribution(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": d.ETag}
			},
			wantStatus: http.StatusConflict,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "DistributionNotDisabled")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			path := tt.path
			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			var hdrs map[string]string
			if tt.headers != nil {
				hdrs = tt.headers(t, h, path)
			}

			rec := doXMLWithHeaders(t, h, tt.method, path, tt.body, hdrs)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec, path)
		})
	}
}

// TestAssociateAlias covers the AssociateAlias operation.
func TestAssociateAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		alias      string
		wantStatus int
	}{
		{
			name:  "associate_alias_success",
			alias: "www.example.com",
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-aa-001", "alias-dist", true,
					minimalDistConfig("ref-aa-001", "alias-dist", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusOK,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder) { t.Helper() },
		},
		{
			name:  "associate_alias_distribution_not_found",
			alias: "notfound.example.com",
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return "DOESNOTEXIST"
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name:  "associate_alias_empty_alias",
			alias: "",
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-aa-002", "alias-dist2", true,
					minimalDistConfig("ref-aa-002", "alias-dist2", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidArgument")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			distID := tt.setup(t, h)
			path := "/2020-05-31/distribution/" + distID + "/associate-alias"
			if tt.alias != "" {
				path += "?Alias=" + tt.alias
			}

			rec := doXML(t, h, http.MethodPut, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestAssociateAlias_Idempotent verifies associating the same alias twice is safe.
func TestAssociateAlias_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	d, err := h.Backend.CreateDistribution("ref-ai-001", "idempotent-dist", true,
		minimalDistConfig("ref-ai-001", "idempotent-dist", true))
	require.NoError(t, err)

	path := "/2020-05-31/distribution/" + d.ID + "/associate-alias?Alias=idem.example.com"
	rec := doXML(t, h, http.MethodPut, path, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doXML(t, h, http.MethodPut, path, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestAssociateDistributionWebACL covers the AssociateDistributionWebACL operation.
func TestAssociateDistributionWebACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name: "associate_web_acl_success",
			body: []byte(
				`<WebACLAssociation><WebACLId>arn:aws:wafv2:us-east-1:123:global/webacl/test/abc</WebACLId></WebACLAssociation>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-wacl-001", "wacl-dist", true,
					minimalDistConfig("ref-wacl-001", "wacl-dist", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusOK,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder) { t.Helper() },
		},
		{
			name: "associate_web_acl_not_found",
			body: []byte(`<WebACLAssociation><WebACLId>some-acl</WebACLId></WebACLAssociation>`),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return "DOESNOTEXIST"
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name: "associate_web_acl_empty_body",
			body: nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-wacl-002", "wacl-dist2", true,
					minimalDistConfig("ref-wacl-002", "wacl-dist2", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusOK,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder) { t.Helper() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			distID := tt.setup(t, h)
			path := "/2020-05-31/distribution/" + distID + "/associate-web-acl"

			rec := doXML(t, h, http.MethodPut, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestCopyDistribution covers the CopyDistribution operation.
func TestCopyDistribution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name: "copy_distribution_success",
			body: []byte(
				`<CopyDistributionRequest><CallerReference>copy-ref-001</CallerReference></CopyDistributionRequest>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-copy-001", "source-dist", true,
					minimalDistConfig("ref-copy-001", "source-dist", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Distribution")
				assert.Contains(t, rec.Body.String(), "<Status>Deployed</Status>")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name: "copy_distribution_not_found",
			body: []byte(
				`<CopyDistributionRequest><CallerReference>copy-ref-002</CallerReference></CopyDistributionRequest>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return "DOESNOTEXIST"
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name: "copy_distribution_empty_caller_ref",
			body: []byte(`<CopyDistributionRequest><CallerReference></CallerReference></CopyDistributionRequest>`),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-copy-003", "source-dist2", true,
					minimalDistConfig("ref-copy-003", "source-dist2", true))
				require.NoError(t, err)

				return d.ID
			},
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidArgument")
			},
		},
		{
			name: "copy_distribution_malformed_xml",
			body: []byte(`<<<not xml`),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-copy-004", "source-dist3", true,
					minimalDistConfig("ref-copy-004", "source-dist3", true))
				require.NoError(t, err)

				return d.ID
			},
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

			h := newTestHandler()
			distID := tt.setup(t, h)
			path := "/2020-05-31/distribution/" + distID + "/copy"

			rec := doXML(t, h, http.MethodPost, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}
