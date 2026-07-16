package cloudfront_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

func TestParity_CreateInvalidationRequiresCallerReference(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		callerRef string
		wantErr   bool
	}{
		{name: "empty_caller_ref_rejected", callerRef: "", wantErr: true},
		{name: "non_empty_caller_ref_accepted", callerRef: "my-ref", wantErr: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			d, err := b.CreateDistribution("ref-inv", "test", true, nil)
			require.NoError(t, err)

			_, err = b.CreateInvalidation(d.ID, tc.callerRef, []string{"/*"})
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParity_CountInProgressInvalidations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numInvs   int
		wantCount int
	}{
		{name: "no_invalidations", numInvs: 0, wantCount: 0},
		{name: "one_invalidation", numInvs: 1, wantCount: 1},
		{name: "two_invalidations", numInvs: 2, wantCount: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			d, err := b.CreateDistribution("ref-cnt", "test", true, nil)
			require.NoError(t, err)

			for i := range tc.numInvs {
				_, err = b.CreateInvalidation(d.ID, fmt.Sprintf("ref-%d", i), []string{"/*"})
				require.NoError(t, err)
			}

			assert.Equal(t, tc.wantCount, b.CountInProgressInvalidations(d.ID))
		})
	}
}

func TestParity_CreateInvalidationHandlerReturnsInvalidationBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "response_has_invalidation_batch"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			distRec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution",
				minimalDistConfig("ref-cr", "test", true))
			require.Equal(t, http.StatusCreated, distRec.Code)

			distID := extractXMLTag(t, distRec.Body.String())
			require.NotEmpty(t, distID)

			body := []byte(`<InvalidationBatch>` +
				`<CallerReference>cr-1</CallerReference>` +
				`<Paths><Quantity>1</Quantity><Items><Path>/*</Path></Items></Paths>` +
				`</InvalidationBatch>`)

			rec := doXML(t, h, http.MethodPost,
				"/2020-05-31/distribution/"+distID+"/invalidation", body)
			require.Equal(t, http.StatusCreated, rec.Code, tc.name)

			body2 := rec.Body.String()
			assert.Contains(t, body2, "<InvalidationBatch>", tc.name)
			assert.Contains(t, body2, "<CallerReference>cr-1</CallerReference>", tc.name)
			assert.Contains(t, body2, "<Path>/*</Path>", tc.name)
		})
	}
}

// TestInvalidationPathValidation verifies that invalid paths are rejected.
func TestInvalidationPathValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name: "valid_paths_accepted",
			body: `<InvalidationBatch>
				<CallerReference>ref-1</CallerReference>
				<Paths>
					<Quantity>2</Quantity>
					<Items>
						<Path>/images/*</Path>
						<Path>/css/main.css</Path>
					</Items>
				</Paths>
			</InvalidationBatch>`,
			wantCode: http.StatusCreated,
		},
		{
			name: "path_without_slash_rejected",
			body: `<InvalidationBatch>
				<CallerReference>ref-2</CallerReference>
				<Paths>
					<Quantity>1</Quantity>
					<Items><Path>images/no-slash</Path></Items>
				</Paths>
			</InvalidationBatch>`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend()
			d, err := b.CreateDistribution("test-dist", "example.com", true, nil)
			require.NoError(t, err)

			h := cloudfront.NewHandler(b)
			rec := doReq(t, h, http.MethodPost,
				"/2020-05-31/distribution/"+d.ID+"/invalidation",
				tt.body)

			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

// TestInvalidationStubs verifies that invalidation stub endpoints return expected responses.
func TestInvalidationStubs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		method     string
		wantStatus int
	}{
		{
			name:       "create_invalidation",
			method:     http.MethodPost,
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Invalidation")
			},
		},
		{
			name:       "list_invalidations",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "InvalidationList")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			d, err := h.Backend.CreateDistribution("ref-inv", "inv-dist", true,
				minimalDistConfig("ref-inv", "inv-dist", true))
			require.NoError(t, err)

			path := "/2020-05-31/distribution/" + d.ID + "/invalidation"
			body := []byte(
				`<InvalidationBatch>` +
					`<CallerReference>stub-ref</CallerReference>` +
					`<Paths><Quantity>1</Quantity><Items><Path>/*</Path></Items></Paths>` +
					`</InvalidationBatch>`,
			)
			if tt.method == http.MethodGet {
				body = nil
			}
			rec := doXML(t, h, tt.method, path, body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

func TestHandler_CreateInvalidation_ListInvalidations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		callerRef     string
		wantStatus    string
		paths         []string
		wantHTTP      int
		wantListCount int
	}{
		{
			name:          "single_path",
			callerRef:     "ref-001",
			paths:         []string{"/images/*"},
			wantStatus:    "InProgress",
			wantHTTP:      http.StatusCreated,
			wantListCount: 1,
		},
		{
			name:          "wildcard_root",
			callerRef:     "ref-002",
			paths:         []string{"/*"},
			wantStatus:    "InProgress",
			wantHTTP:      http.StatusCreated,
			wantListCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create a distribution first using the minimal helper.
			createRec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution",
				minimalDistConfig("create-ref-inv", "inv-test-dist", true))
			require.Equal(t, http.StatusCreated, createRec.Code)

			// Extract distribution ID from Location header.
			loc := createRec.Header().Get("Location")
			parts := strings.Split(loc, "/")
			distID := parts[len(parts)-1]
			require.NotEmpty(t, distID)

			// Build paths XML.
			var pathItems strings.Builder
			for _, p := range tt.paths {
				fmt.Fprintf(&pathItems, "<Path>%s</Path>", p)
			}

			// CreateInvalidation.
			invXML := fmt.Sprintf(
				`<InvalidationBatch xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">`+
					`<CallerReference>%s</CallerReference>`+
					`<Paths><Quantity>%d</Quantity><Items>%s</Items></Paths>`+
					`</InvalidationBatch>`,
				tt.callerRef, len(tt.paths), pathItems.String(),
			)

			invRec := doXML(t, h, http.MethodPost,
				"/2020-05-31/distribution/"+distID+"/invalidation",
				[]byte(invXML))
			assert.Equal(t, tt.wantHTTP, invRec.Code, "CreateInvalidation status")

			// Verify the response contains the expected status.
			assert.Contains(t, invRec.Body.String(), "<Status>"+tt.wantStatus+"</Status>")

			// Verify CreateTime is an ISO-8601 string (contains 'T'), not a raw integer.
			assert.Contains(t, invRec.Body.String(), "<CreateTime>")
			assert.Contains(t, invRec.Body.String(), "T", "CreateTime must be RFC3339 formatted")

			// ListInvalidations should return the created invalidation.
			listRec := doXML(t, h, http.MethodGet,
				"/2020-05-31/distribution/"+distID+"/invalidation",
				nil)
			assert.Equal(t, http.StatusOK, listRec.Code)
			assert.Contains(t, listRec.Body.String(), "InProgress")

			// Verify the Quantity matches.
			assert.Contains(t, listRec.Body.String(),
				fmt.Sprintf("<Quantity>%d</Quantity>", tt.wantListCount))
		})
	}
}

// TestRefinement1_GetInvalidation tests the GET invalidation by ID handler.
func TestRefinement1_GetInvalidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(*testing.T, *httptest.ResponseRecorder)
		setup      func(*testing.T, *cloudfront.Handler) (string, string)
		name       string
		wantStatus int
	}{
		{
			name: "get_invalidation_success",
			setup: func(t *testing.T, h *cloudfront.Handler) (string, string) {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-gi-001", "gi-dist", true, nil)
				require.NoError(t, err)
				inv, err := h.Backend.CreateInvalidation(d.ID, "caller-gi-001", []string{"/path/*"})
				require.NoError(t, err)

				return d.ID, inv.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Invalidation")
				assert.Contains(t, rec.Body.String(), "<Status>InProgress</Status>")
				assert.Contains(t, rec.Body.String(), "/path/*")
			},
		},
		{
			name: "get_invalidation_not_found",
			setup: func(t *testing.T, h *cloudfront.Handler) (string, string) {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-gi-002", "gi-dist2", true, nil)
				require.NoError(t, err)

				return d.ID, "DOESNOTEXIST"
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchInvalidation")
			},
		},
		{
			name: "get_invalidation_distribution_not_found",
			setup: func(_ *testing.T, _ *cloudfront.Handler) (string, string) {
				return "NOTEXIST", "inv1"
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchDistribution")
			},
		},
		{
			name: "get_invalidation_missing_inv_id",
			setup: func(t *testing.T, h *cloudfront.Handler) (string, string) {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-gi-003", "gi-dist3", true, nil)
				require.NoError(t, err)

				return d.ID, ""
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
			distID, invID := tt.setup(t, h)

			var path string
			if invID != "" {
				path = "/2020-05-31/distribution/" + distID + "/invalidation/" + invID
			} else {
				// No invID in path - triggers missing-ID error.
				path = "/2020-05-31/distribution/" + distID + "/invalidation/"
			}

			rec := doXML(t, h, http.MethodGet, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestRefinement1_SortedInvalidations verifies invalidations are returned sorted by ID.
func TestRefinement1_SortedInvalidations(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)

	d, err := b.CreateDistribution("ref-sorted-inv", "sorted-inv-dist", true, nil)
	require.NoError(t, err)

	for i := range 5 {
		_, err = b.CreateInvalidation(d.ID, fmt.Sprintf("caller-%d", i), []string{"/path"})
		require.NoError(t, err)
	}

	invs, err := b.ListInvalidations(d.ID)
	require.NoError(t, err)
	require.Len(t, invs, 5)

	for i := 1; i < len(invs); i++ {
		assert.LessOrEqual(t, invs[i-1].ID, invs[i].ID,
			"invalidations should be sorted by ID")
	}
}

// TestRefinement1_HandleGetInvalidationPathFallback tests path-based invID parsing.
func TestRefinement1_HandleGetInvalidationPathFallback(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", config.DefaultRegion)
	h := cloudfront.NewHandler(b)

	d, err := b.CreateDistribution("ref-path-fb", "path-fb-dist", true, nil)
	require.NoError(t, err)

	inv, err := b.CreateInvalidation(d.ID, "caller-path-fb", []string{"/assets/*"})
	require.NoError(t, err)

	// The path-based invalidation GET route.
	path := "/2020-05-31/distribution/" + d.ID + "/invalidation/" + inv.ID
	rec := doXML(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), inv.ID)
	assert.Contains(t, rec.Body.String(), "/assets/*")
}

// ---- New tests for newly implemented operations ----
