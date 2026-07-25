package s3control_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	s3control "github.com/blackbirdworks/gopherstack/services/s3control"
)

// ---- MRAP ----

func TestMRAP(t *testing.T) {
	t.Parallel()
	b := s3control.NewInMemoryBackend()
	req := b.CreateMultiRegionAccessPoint("000000000000", "my-mrap", "token-1")

	t.Run("describe MRAP operation", func(t *testing.T) {
		t.Parallel()
		op, err := b.DescribeMultiRegionAccessPointOperation("000000000000", req.RequestTokenARN)
		require.NoError(t, err)
		assert.Equal(t, req.RequestTokenARN, op.RequestTokenARN)
	})

	t.Run("get MRAP policy", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		r := b2.CreateMultiRegionAccessPoint("000000000000", "pol-mrap", "tok-pol")
		_ = r
		b2.PutMultiRegionAccessPointPolicy("000000000000", "pol-mrap", `{"Version":"2012-10-17"}`)
		policy, err := b2.GetMultiRegionAccessPointPolicy("000000000000", "pol-mrap")
		require.NoError(t, err)
		assert.Contains(t, policy, "Version")
	})

	t.Run("get MRAP policy status", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		b2.CreateMultiRegionAccessPoint("000000000000", "st-mrap", "tok-st")
		b2.PutMultiRegionAccessPointPolicy("000000000000", "st-mrap", `{"Version":"2012-10-17"}`)
		isPublic, err := b2.GetMultiRegionAccessPointPolicyStatus("000000000000", "st-mrap")
		require.NoError(t, err)
		assert.True(t, isPublic)
	})

	t.Run("get MRAP routes empty", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		b2.CreateMultiRegionAccessPoint("000000000000", "rt-mrap", "tok-rt")
		routes, err := b2.GetMultiRegionAccessPointRoutes("000000000000", "rt-mrap")
		require.NoError(t, err)
		assert.Empty(t, routes)
	})
}

func TestBackend_MRAP_Operations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *s3control.InMemoryBackend) string
		name    string
		op      string
		wantErr bool
	}{
		{
			name: "get_mrap",
			op:   "get",
			setup: func(b *s3control.InMemoryBackend) string {
				b.CreateMultiRegionAccessPoint("acct1", "mymrap", "")

				return "mymrap"
			},
		},
		{
			name:    "get_missing_mrap",
			op:      "get",
			wantErr: true,
			setup:   func(_ *s3control.InMemoryBackend) string { return "nonexistent" },
		},
		{
			name: "delete_mrap",
			op:   "delete",
			setup: func(b *s3control.InMemoryBackend) string {
				b.CreateMultiRegionAccessPoint("acct1", "mymrap", "")

				return "mymrap"
			},
		},
		{
			name:    "delete_missing_mrap",
			op:      "delete",
			wantErr: true,
			setup:   func(_ *s3control.InMemoryBackend) string { return "nonexistent" },
		},
		{
			name: "list_mraps",
			op:   "list",
			setup: func(b *s3control.InMemoryBackend) string {
				b.CreateMultiRegionAccessPoint("acct1", "mrap1", "")
				b.CreateMultiRegionAccessPoint("acct1", "mrap2", "")

				return ""
			},
		},
		{
			name: "put_mrap_policy",
			op:   "policy",
			setup: func(b *s3control.InMemoryBackend) string {
				b.CreateMultiRegionAccessPoint("acct1", "mymrap", "")

				return "mymrap"
			},
		},
		{
			name:    "put_policy_missing_mrap",
			op:      "policy",
			wantErr: true,
			setup:   func(_ *s3control.InMemoryBackend) string { return "nonexistent" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			mrapName := tt.setup(b)

			switch tt.op {
			case "get":
				mrap, err := b.GetMultiRegionAccessPoint("acct1", mrapName)
				if tt.wantErr {
					// AWS error code must be NoSuchMultiRegionAccessPoint,
					// not the unrelated NoSuchPublicAccessBlockConfiguration
					// sentinel this used to wrongly share.
					require.ErrorContains(t, err, "NoSuchMultiRegionAccessPoint")
				} else {
					require.NoError(t, err)
					assert.Equal(t, mrapName, mrap.Name)
				}
			case "delete":
				err := b.DeleteMultiRegionAccessPoint("acct1", mrapName)
				if tt.wantErr {
					require.ErrorContains(t, err, "NoSuchMultiRegionAccessPoint")
				} else {
					require.NoError(t, err)
					assert.Equal(t, 0, s3control.MRAPCount(b), "delete must actually remove the MRAP")
				}
			case "list":
				mraps := b.ListMultiRegionAccessPoints("acct1")
				assert.Len(t, mraps, 2)
			case "policy":
				err := b.PutMultiRegionAccessPointPolicy("acct1", mrapName, `{"Version":"2012-10-17"}`)
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

// TestBackend_DeleteMultiRegionAccessPoint_ActuallyRemoves locks in the leak
// fix: DeleteMultiRegionAccessPoint previously checked b.mraps.Has(key) and
// returned nil WITHOUT ever calling Delete, so the MRAP stayed retrievable
// via Get/List forever and every create/delete cycle left an unbounded
// ghost row. This asserts the resource, its routes, and its error code are
// all correctly gone after delete -- not just that the call returned nil.
func TestBackend_DeleteMultiRegionAccessPoint_ActuallyRemoves(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateMultiRegionAccessPoint("acct1", "leaky-mrap", "")
	require.NoError(t, b.SubmitMultiRegionAccessPointRoutes("acct1", "leaky-mrap", "<Routes/>"))
	require.Equal(t, 1, s3control.MRAPCount(b))

	require.NoError(t, b.DeleteMultiRegionAccessPoint("acct1", "leaky-mrap"))

	// The MRAP must be gone from the table, not merely "reported deleted".
	assert.Equal(t, 0, s3control.MRAPCount(b))

	_, err := b.GetMultiRegionAccessPoint("acct1", "leaky-mrap")
	require.ErrorContains(t, err, "NoSuchMultiRegionAccessPoint")

	assert.Empty(t, b.ListMultiRegionAccessPoints("acct1"))

	// Route configuration must be cascade-cleaned too.
	routes, err := b.GetMultiRegionAccessPointRoutes("acct1", "leaky-mrap")
	require.Error(t, err)
	assert.Empty(t, routes)

	// Deleting again must report NotFound with the correct AWS error code,
	// not the generic PublicAccessBlock sentinel that was used before.
	err = b.DeleteMultiRegionAccessPoint("acct1", "leaky-mrap")
	require.ErrorContains(t, err, "NoSuchMultiRegionAccessPoint")
}

// TestHandler_DeleteMultiRegionAccessPoint_AsyncRouteActuallyRemoves is the
// HTTP-level counterpart of the leak-fix test above: it drives the delete
// through the real async POST route (the one a real aws-sdk-go-v2 client
// actually uses) and confirms a subsequent Get 404s.
func TestHandler_DeleteMultiRegionAccessPoint_AsyncRouteActuallyRemoves(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)
	h.Backend.CreateMultiRegionAccessPoint("acct1", "async-leaky-mrap", "")

	body := "<DeleteMultiRegionAccessPointRequest>" +
		"<Details><Name>async-leaky-mrap</Name></Details>" +
		"</DeleteMultiRegionAccessPointRequest>"

	rec := doS3Request(t, h, http.MethodPost, "/v20180820/async-requests/mrap/delete", body)
	require.Equal(t, http.StatusOK, rec.Code)

	getRec := doS3Request(t, h, http.MethodGet, "/v20180820/mrap/instances/async-leaky-mrap", "")
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

func TestHandler_GetMultiRegionAccessPoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *s3control.Handler)
		name       string
		mrapName   string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "get_existing",
			mrapName:   "mymrap",
			wantStatus: http.StatusOK,
			wantBody:   "mymrap",
			setup: func(h *s3control.Handler) {
				h.Backend.CreateMultiRegionAccessPoint("acct1", "mymrap", "")
			},
		},
		{
			name:       "get_missing",
			mrapName:   "nonexistent",
			wantStatus: http.StatusNotFound,
			setup:      func(_ *s3control.Handler) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			tt.setup(h)

			rec := doS3Request(t, h, http.MethodGet, "/v20180820/mrap/instances/"+tt.mrapName, "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_DeleteMultiRegionAccessPoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *s3control.Handler)
		name       string
		mrapName   string
		wantStatus int
	}{
		{
			name:       "delete_existing",
			mrapName:   "mymrap",
			wantStatus: http.StatusNoContent,
			setup: func(h *s3control.Handler) {
				h.Backend.CreateMultiRegionAccessPoint("acct1", "mymrap", "")
			},
		},
		{
			name:       "delete_missing",
			mrapName:   "nonexistent",
			wantStatus: http.StatusNotFound,
			setup:      func(_ *s3control.Handler) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			tt.setup(h)

			rec := doS3Request(t, h, http.MethodDelete, "/v20180820/mrap/instances/"+tt.mrapName, "")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteMultiRegionAccessPointAsync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *s3control.Handler)
		name       string
		body       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "async_delete_success",
			wantStatus: http.StatusOK,
			wantBody:   "RequestTokenARN",
			body: "<DeleteMultiRegionAccessPointRequest>" +
				"<Details><Name>mymrap</Name></Details>" +
				"</DeleteMultiRegionAccessPointRequest>",
			setup: func(h *s3control.Handler) {
				h.Backend.CreateMultiRegionAccessPoint("acct1", "mymrap", "")
			},
		},
		{
			name:       "async_delete_missing",
			wantStatus: http.StatusNotFound,
			body: "<DeleteMultiRegionAccessPointRequest>" +
				"<Details><Name>nonexistent</Name></Details>" +
				"</DeleteMultiRegionAccessPointRequest>",
			setup: func(_ *s3control.Handler) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			tt.setup(h)

			rec := doS3Request(t, h, http.MethodPost, "/v20180820/async-requests/mrap/delete/token1", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_ListMultiRegionAccessPoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *s3control.Handler)
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "list_empty",
			wantStatus: http.StatusOK,
			wantBody:   "ListMultiRegionAccessPointsResult",
			setup:      func(_ *s3control.Handler) {},
		},
		{
			name:       "list_with_mraps",
			wantStatus: http.StatusOK,
			wantBody:   "mrap1",
			setup: func(h *s3control.Handler) {
				h.Backend.CreateMultiRegionAccessPoint("acct1", "mrap1", "")
				h.Backend.CreateMultiRegionAccessPoint("acct1", "mrap2", "")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			tt.setup(h)

			rec := doS3Request(t, h, http.MethodGet, "/v20180820/mrap/instances", "")
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestHandler_PutMultiRegionAccessPointPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *s3control.Handler)
		name       string
		body       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "put_policy_success",
			wantStatus: http.StatusOK,
			wantBody:   "RequestTokenARN",
			body: "<PutMultiRegionAccessPointPolicyRequest>" +
				`<Details><Name>mymrap</Name><Policy>{"Version":"2012-10-17"}</Policy></Details>` +
				"</PutMultiRegionAccessPointPolicyRequest>",
			setup: func(h *s3control.Handler) {
				h.Backend.CreateMultiRegionAccessPoint("acct1", "mymrap", "")
			},
		},
		{
			name:       "put_policy_missing",
			wantStatus: http.StatusNotFound,
			body: "<PutMultiRegionAccessPointPolicyRequest>" +
				`<Details><Name>nonexistent</Name><Policy>{"Version":"2012-10-17"}</Policy></Details>` +
				"</PutMultiRegionAccessPointPolicyRequest>",
			setup: func(_ *s3control.Handler) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			tt.setup(h)

			rec := doS3Request(
				t,
				h,
				http.MethodPost,
				"/v20180820/async-requests/mrap/put-policy/token1",
				tt.body,
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestBackend_MRAPCount covers the MRAPRequestCount export.
func TestBackend_MRAPCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantLen int
	}{
		{name: "zero_mraps", wantLen: 0},
		{name: "one_mrap", wantLen: 1},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()

			for j := range i {
				b.CreateMultiRegionAccessPoint("acct1", fmt.Sprintf("mrap%d", j), "")
			}

			assert.Equal(t, tt.wantLen, s3control.MRAPRequestCount(b))
		})
	}
}

// ---- MRAP Routes ----

func TestSubmitMRAPRoutes(t *testing.T) {
	t.Parallel()

	const accountID = "acct1"
	const mrapName = "my-mrap"
	const routesPath = "/v20180820/mrap/instances/" + mrapName + "/routes"

	t.Run("submit routes returns 200", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		h.Backend.CreateMultiRegionAccessPoint(accountID, mrapName, "token")

		rec := doS3Request(
			t,
			h,
			http.MethodPatch,
			routesPath,
			`<SubmitMultiRegionAccessPointRoutesRequest><Routes>r1</Routes></SubmitMultiRegionAccessPointRoutesRequest>`,
		)
		require.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestSubmitMRAPRoutes_Backend(t *testing.T) {
	t.Parallel()

	t.Run("submit on missing MRAP is idempotent", func(t *testing.T) {
		t.Parallel()

		b := s3control.NewInMemoryBackend()
		err := b.SubmitMultiRegionAccessPointRoutes("acct1", "missing", "routes")
		require.NoError(t, err)
	})

	t.Run("submit and retrieve routes", func(t *testing.T) {
		t.Parallel()

		b := s3control.NewInMemoryBackend()
		b.CreateMultiRegionAccessPoint("acct1", "my-mrap", "token")
		err := b.SubmitMultiRegionAccessPointRoutes("acct1", "my-mrap", "route-data")
		require.NoError(t, err)

		routes, err := b.GetMultiRegionAccessPointRoutes("acct1", "my-mrap")
		require.NoError(t, err)
		assert.Equal(t, "route-data", routes)
	})
}

func TestSubmitMRAPRoutes_Table(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mrap     string
		routes   string
		body     string
		wantCode int
	}{
		{
			name:     "submit_routes",
			mrap:     "my-mrap",
			routes:   "<Route><Bucket>b1</Bucket><TrafficDialPercentage>100</TrafficDialPercentage></Route>",
			wantCode: http.StatusOK,
		},
		{
			name:     "submit_empty_routes",
			mrap:     "my-mrap2",
			routes:   "",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			body := `<SubmitMultiRegionAccessPointRoutesRequest>` +
				`<Routes>` + tt.routes + `</Routes>` +
				`</SubmitMultiRegionAccessPointRoutesRequest>`
			rec := doS3ControlNewOpRequest(t, h, http.MethodPatch,
				"/v20180820/mrap/instances/"+tt.mrap+"/routes", "000000000000", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- MRAP Regions tests ----

func TestMRAP_SetRegions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "single_region", regions: []string{"my-bucket"}},
		{name: "multi_region", regions: []string{"bucket-us-east-1", "bucket-eu-west-1"}},
		{name: "no_regions", regions: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			b.CreateMultiRegionAccessPoint("000000000000", "my-mrap", "token1")

			err := b.SetMRAPRegions("000000000000", "my-mrap", tt.regions)
			require.NoError(t, err)

			mrap, err := b.GetMultiRegionAccessPoint("000000000000", "my-mrap")
			require.NoError(t, err)
			assert.Equal(t, tt.regions, mrap.Regions)
		})
	}
}

func TestMRAP_SetRegions_MissingMRAP(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	err := b.SetMRAPRegions("000000000000", "nonexistent", []string{"bucket"})
	require.Error(t, err)
}

func TestHandler_CreateMRAP_WithRegions(t *testing.T) {
	t.Parallel()

	h := s3control.NewHandler(s3control.NewInMemoryBackend())
	body := `<CreateMultiRegionAccessPointRequest>
<ClientToken>tok-1</ClientToken>
<Details>
<Name>my-mrap</Name>
<Regions>
<Region><Bucket>bucket-us-east-1</Bucket></Region>
<Region><Bucket>bucket-eu-west-1</Bucket></Region>
</Regions>
</Details>
</CreateMultiRegionAccessPointRequest>`

	rec := doS3ControlNewOpRequest(
		t, h, http.MethodPost,
		"/v20180820/async-requests/mrap/create",
		"000000000000",
		body,
	)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RequestTokenARN")

	// Verify stored regions.
	mrap, err := h.Backend.GetMultiRegionAccessPoint("000000000000", "my-mrap")
	require.NoError(t, err)
	assert.Len(t, mrap.Regions, 2)
	assert.Equal(t, "bucket-us-east-1", mrap.Regions[0])
	assert.Equal(t, "bucket-eu-west-1", mrap.Regions[1])
}

func TestHandler_GetMRAP_ReturnsRegions(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateMultiRegionAccessPoint("000000000000", "my-mrap", "token1")
	_ = b.SetMRAPRegions("000000000000", "my-mrap", []string{"bucket-us-east-1"})
	h := s3control.NewHandler(b)

	rec := doS3ControlNewOpRequest(
		t, h, http.MethodGet,
		"/v20180820/mrap/instances/my-mrap",
		"000000000000",
		"",
	)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "GetMultiRegionAccessPointResult")
	assert.Contains(t, body, "bucket-us-east-1")
	assert.Contains(t, body, "CreatedAt")
}

// ---- MRAP CreatedAt tests ----

func TestMRAP_CreatedAtSet(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateMultiRegionAccessPoint("000000000000", "my-mrap", "token1")

	mrap, err := b.GetMultiRegionAccessPoint("000000000000", "my-mrap")
	require.NoError(t, err)
	assert.NotEmpty(t, mrap.CreatedAt)
}

func TestCreateMultiRegionAccessPoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		accountID        string
		body             string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name:      "creates_mrap_async_request",
			accountID: "123456789012",
			body: `<CreateMultiRegionAccessPointRequest>
<ClientToken>idempotency-token-123</ClientToken>
<Details>
<Name>my-mrap</Name>
</Details>
</CreateMultiRegionAccessPointRequest>`,
			wantStatus:       http.StatusOK,
			wantBodyContains: "RequestTokenARN",
		},
		{
			name:      "creates_mrap_with_empty_details",
			accountID: "000000000000",
			body: `<CreateMultiRegionAccessPointRequest>
<ClientToken>token-456</ClientToken>
<Details></Details>
</CreateMultiRegionAccessPointRequest>`,
			wantStatus:       http.StatusOK,
			wantBodyContains: "RequestTokenARN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			rec := doS3ControlNewOpRequest(
				t,
				h,
				http.MethodPost,
				"/v20180820/async-requests/mrap/create",
				tt.accountID,
				tt.body,
			)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBodyContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContains)
			}
		})
	}
}

func TestListMultiRegionAccessPoints_Pagination(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	for i := range 4 {
		b.CreateMultiRegionAccessPoint("acct1", fmt.Sprintf("mrap-%d", i), "")
	}
	h := s3control.NewHandler(b)

	tests := []struct {
		path          string
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          "/v20180820/mrap/instances",
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          "/v20180820/mrap/instances?maxResults=2",
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doS3Request(t, h, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				XMLName      xml.Name `xml:"ListMultiRegionAccessPointsResult"`
				NextToken    string   `xml:"NextToken"`
				AccessPoints []struct {
					Name string `xml:"Name"`
				} `xml:"AccessPoints>item"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.AccessPoints, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}
