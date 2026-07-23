package s3control_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

func TestBackend_AccessPoint_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *s3control.InMemoryBackend)
		name     string
		wantName string
		wantErr  bool
	}{
		{
			name:     "get_existing",
			wantName: "myap",
			setup: func(b *s3control.InMemoryBackend) {
				b.AddAccessPointInternal("acct1", "myap", "mybucket")
			},
		},
		{
			name:    "get_missing",
			wantErr: true,
			setup:   func(_ *s3control.InMemoryBackend) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			tt.setup(b)

			ap, err := b.GetAccessPoint("acct1", "myap")

			if tt.wantErr {
				// AWS error code must be NoSuchAccessPoint, not the
				// unrelated NoSuchPublicAccessBlockConfiguration sentinel
				// this used to wrongly share.
				require.ErrorContains(t, err, "NoSuchAccessPoint")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantName, ap.Name)
		})
	}
}

func TestBackend_DeleteAccessPoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *s3control.InMemoryBackend)
		name    string
		wantErr bool
	}{
		{
			name: "delete_existing",
			setup: func(b *s3control.InMemoryBackend) {
				b.AddAccessPointInternal("acct1", "myap", "mybucket")
			},
		},
		{
			name:    "delete_missing",
			wantErr: true,
			setup:   func(_ *s3control.InMemoryBackend) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			tt.setup(b)

			err := b.DeleteAccessPoint("acct1", "myap")

			if tt.wantErr {
				require.ErrorContains(t, err, "NoSuchAccessPoint")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 0, s3control.AccessPointCount(b))
		})
	}
}

// TestBackend_DeleteAccessPoint_CascadeCleansState locks in the
// ghost-map-row fix: DeleteAccessPoint previously only cleaned
// accessPointPolicies, leaving the access point's scope, per-AP
// PublicAccessBlock, and generic resource tags behind forever. A
// delete/recreate cycle under the same name would otherwise silently
// resurrect the deleted access point's stale scope/PAB/tags.
func TestBackend_DeleteAccessPoint_CascadeCleansState(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	ap := b.CreateAccessPoint("acct1", "cascade-ap", "mybucket")
	require.NoError(t, b.PutAccessPointPolicy("acct1", "cascade-ap", `{"p":1}`))
	require.NoError(t, b.PutAccessPointScope("acct1", "cascade-ap", "<Scope/>"))
	require.NoError(t, b.PutAccessPointPublicAccessBlock("acct1", "cascade-ap", s3control.PublicAccessBlock{
		BlockPublicAcls: true,
	}))
	b.TagResource(ap.AccessPointArn, map[string]string{"env": "test"})

	require.NoError(t, b.DeleteAccessPoint("acct1", "cascade-ap"))

	// Recreate under the identical name/ARN and confirm none of the
	// deleted access point's state leaked forward.
	b.CreateAccessPoint("acct1", "cascade-ap", "mybucket")

	_, err := b.GetAccessPointPolicy("acct1", "cascade-ap")
	require.Error(t, err, "policy must not survive delete")

	scope, err := b.GetAccessPointScope("acct1", "cascade-ap")
	require.NoError(t, err)
	assert.Empty(t, scope, "scope must not survive delete")

	_, err = b.GetAccessPointPublicAccessBlock("acct1", "cascade-ap")
	require.Error(t, err, "per-AP PublicAccessBlock must not survive delete")

	assert.Empty(t, b.ListTagsForResource(ap.AccessPointArn), "tags must not survive delete")
}

func TestBackend_ListAccessPoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *s3control.InMemoryBackend)
		name     string
		wantName string
		wantLen  int
	}{
		{
			name:  "empty",
			setup: func(_ *s3control.InMemoryBackend) {},
		},
		{
			name:    "one_access_point",
			wantLen: 1,
			setup: func(b *s3control.InMemoryBackend) {
				b.AddAccessPointInternal("acct1", "ap1", "bucket1")
			},
		},
		{
			name:    "multiple_access_points",
			wantLen: 2,
			setup: func(b *s3control.InMemoryBackend) {
				b.AddAccessPointInternal("acct1", "ap1", "bucket1")
				b.AddAccessPointInternal("acct1", "ap2", "bucket2")
			},
		},
		{
			name:    "filtered_by_account",
			wantLen: 1,
			setup: func(b *s3control.InMemoryBackend) {
				b.AddAccessPointInternal("acct1", "ap1", "bucket1")
				b.AddAccessPointInternal("acct2", "ap2", "bucket2")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			tt.setup(b)

			aps := b.ListAccessPoints("acct1")
			assert.Len(t, aps, tt.wantLen)
		})
	}
}

func TestBackend_AccessPointPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(b *s3control.InMemoryBackend)
		name       string
		op         string
		wantPolicy string
		wantErr    bool
	}{
		{
			name:       "put_and_get_policy",
			op:         "get",
			wantPolicy: `{"Version":"2012-10-17"}`,
			setup: func(b *s3control.InMemoryBackend) {
				b.AddAccessPointInternal("acct1", "ap1", "bucket1")

				err := b.PutAccessPointPolicy("acct1", "ap1", `{"Version":"2012-10-17"}`)
				require.NoError(t, err)
			},
		},
		{
			name:    "put_policy_missing_ap",
			op:      "put",
			wantErr: true,
			setup:   func(_ *s3control.InMemoryBackend) {},
		},
		{
			name:    "get_policy_no_policy_set",
			op:      "get",
			wantErr: true,
			setup: func(b *s3control.InMemoryBackend) {
				b.AddAccessPointInternal("acct1", "ap1", "bucket1")
			},
		},
		{
			name:    "get_policy_missing_ap",
			op:      "get",
			wantErr: true,
			setup:   func(_ *s3control.InMemoryBackend) {},
		},
		{
			name: "delete_policy",
			op:   "delete",
			setup: func(b *s3control.InMemoryBackend) {
				b.AddAccessPointInternal("acct1", "ap1", "bucket1")

				err := b.PutAccessPointPolicy("acct1", "ap1", `{"Version":"2012-10-17"}`)
				require.NoError(t, err)
			},
		},
		{
			name:    "delete_policy_missing_ap",
			op:      "delete",
			wantErr: true,
			setup:   func(_ *s3control.InMemoryBackend) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			tt.setup(b)

			switch tt.op {
			case "put":
				err := b.PutAccessPointPolicy("acct1", "ap1", `{"Version":"2012-10-17"}`)
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			case "get":
				policy, err := b.GetAccessPointPolicy("acct1", "ap1")
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.wantPolicy, policy)
				}
			case "delete":
				err := b.DeleteAccessPointPolicy("acct1", "ap1")
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestHandler_GetAccessPoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *s3control.Handler)
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "get_existing",
			wantStatus: http.StatusOK,
			wantBody:   "myap",
			setup: func(h *s3control.Handler) {
				h.Backend.AddAccessPointInternal("acct1", "myap", "mybucket")
			},
		},
		{
			name:       "get_missing",
			wantStatus: http.StatusNotFound,
			setup:      func(_ *s3control.Handler) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			tt.setup(h)

			rec := doS3Request(t, h, http.MethodGet, "/v20180820/accesspoint/myap", "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_DeleteAccessPoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *s3control.Handler)
		name       string
		wantStatus int
	}{
		{
			name:       "delete_existing",
			wantStatus: http.StatusNoContent,
			setup: func(h *s3control.Handler) {
				h.Backend.AddAccessPointInternal("acct1", "myap", "mybucket")
			},
		},
		{
			name:       "delete_missing",
			wantStatus: http.StatusNotFound,
			setup:      func(_ *s3control.Handler) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			tt.setup(h)

			rec := doS3Request(t, h, http.MethodDelete, "/v20180820/accesspoint/myap", "")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListAccessPoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *s3control.Handler)
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "empty_list",
			wantStatus: http.StatusOK,
			wantBody:   "ListAccessPointsResult",
			setup:      func(_ *s3control.Handler) {},
		},
		{
			name:       "list_with_items",
			wantStatus: http.StatusOK,
			wantBody:   "ap1",
			setup: func(h *s3control.Handler) {
				h.Backend.AddAccessPointInternal("acct1", "ap1", "bucket1")
				h.Backend.AddAccessPointInternal("acct1", "ap2", "bucket2")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			tt.setup(h)

			rec := doS3Request(t, h, http.MethodGet, "/v20180820/accesspoint", "")
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestHandler_AccessPointPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *s3control.Handler)
		name       string
		method     string
		body       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "put_policy_success",
			method:     http.MethodPut,
			wantStatus: http.StatusCreated,
			body:       `<PutAccessPointPolicyRequest><Policy>{"Version":"2012-10-17"}</Policy></PutAccessPointPolicyRequest>`,
			setup: func(h *s3control.Handler) {
				h.Backend.AddAccessPointInternal("acct1", "ap1", "bucket1")
			},
		},
		{
			name:       "put_policy_missing_ap",
			method:     http.MethodPut,
			wantStatus: http.StatusNotFound,
			body:       `<PutAccessPointPolicyRequest><Policy>{"Version":"2012-10-17"}</Policy></PutAccessPointPolicyRequest>`,
			setup:      func(_ *s3control.Handler) {},
		},
		{
			name:       "get_policy_success",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
			wantBody:   "Policy",
			setup: func(h *s3control.Handler) {
				h.Backend.AddAccessPointInternal("acct1", "ap1", "bucket1")

				err := h.Backend.PutAccessPointPolicy("acct1", "ap1", `{"Version":"2012-10-17"}`)
				require.NoError(t, err)
			},
		},
		{
			name:       "get_policy_no_policy",
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
			setup: func(h *s3control.Handler) {
				h.Backend.AddAccessPointInternal("acct1", "ap1", "bucket1")
			},
		},
		{
			name:       "delete_policy_success",
			method:     http.MethodDelete,
			wantStatus: http.StatusNoContent,
			setup: func(h *s3control.Handler) {
				h.Backend.AddAccessPointInternal("acct1", "ap1", "bucket1")

				err := h.Backend.PutAccessPointPolicy("acct1", "ap1", `{"Version":"2012-10-17"}`)
				require.NoError(t, err)
			},
		},
		{
			name:       "delete_policy_missing_ap",
			method:     http.MethodDelete,
			wantStatus: http.StatusNotFound,
			setup:      func(_ *s3control.Handler) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			tt.setup(h)

			rec := doS3Request(t, h, tt.method, "/v20180820/accesspoint/ap1/policy", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_GetAccessPointPolicyStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "always_returns_not_public",
			wantStatus: http.StatusOK,
			wantBody:   "PolicyStatus",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			rec := doS3Request(t, h, http.MethodGet, "/v20180820/accesspoint/ap1/policyStatus", "")
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// xmlUnmarshalGetAccessPoint unmarshals the GetAccessPoint response.
type xmlUnmarshalGetAccessPoint struct {
	XMLName xml.Name `xml:"GetAccessPointResult"`
	Name    string   `xml:"Name"`
	Bucket  string   `xml:"Bucket"`
}

// TestHandler_GetAccessPoint_ResponseFields validates that the response XML fields are correct.
func TestHandler_GetAccessPoint_ResponseFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		apName     string
		bucket     string
		wantName   string
		wantBucket string
	}{
		{
			name:       "correct_fields_in_response",
			apName:     "ap1",
			bucket:     "bucket1",
			wantName:   "ap1",
			wantBucket: "bucket1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			h.Backend.AddAccessPointInternal("acct1", tt.apName, tt.bucket)

			rec := doS3Request(t, h, http.MethodGet, "/v20180820/accesspoint/"+tt.apName, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp xmlUnmarshalGetAccessPoint
			err := xml.Unmarshal(rec.Body.Bytes(), &resp)
			require.NoError(t, err)

			assert.Equal(t, tt.wantName, resp.Name)
			assert.Equal(t, tt.wantBucket, resp.Bucket)
		})
	}
}

func TestCreateAccessPoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		accountID        string
		apName           string
		body             string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name:      "creates_access_point",
			accountID: "123456789012",
			apName:    "my-access-point",
			body: `<CreateAccessPointRequest>
<Bucket>my-bucket</Bucket>
</CreateAccessPointRequest>`,
			wantStatus:       http.StatusOK,
			wantBodyContains: "AccessPointArn",
		},
		{
			name:             "creates_access_point_no_bucket",
			accountID:        "000000000000",
			apName:           "another-access-point",
			body:             "",
			wantStatus:       http.StatusOK,
			wantBodyContains: "AccessPointArn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			path := "/v20180820/accesspoint/" + tt.apName
			rec := doS3ControlNewOpRequest(t, h, http.MethodPut, path, tt.accountID, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBodyContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContains)
			}
		})
	}
}

func TestListAccessPoints_Pagination(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	for i := range 4 {
		b.AddAccessPointInternal("acct1", fmt.Sprintf("ap-%d", i), fmt.Sprintf("bucket-%d", i))
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
			path:          "/v20180820/accesspoint",
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          "/v20180820/accesspoint?maxResults=2",
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
				XMLName      xml.Name `xml:"ListAccessPointsResult"`
				NextToken    string   `xml:"NextToken"`
				AccessPoints []struct {
					Name string `xml:"Name"`
				} `xml:"AccessPointList>AccessPoint"`
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

func TestListAccessPoints_BucketFilter(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.AddAccessPointInternal("acct1", "ap-a1", "bucket-a")
	b.AddAccessPointInternal("acct1", "ap-a2", "bucket-a")
	b.AddAccessPointInternal("acct1", "ap-b1", "bucket-b")
	h := s3control.NewHandler(b)

	tests := []struct {
		path    string
		name    string
		wantLen int
	}{
		{name: "filter_bucket_a", path: "/v20180820/accesspoint?bucket=bucket-a", wantLen: 2},
		{name: "filter_bucket_b", path: "/v20180820/accesspoint?bucket=bucket-b", wantLen: 1},
		{name: "no_filter_all", path: "/v20180820/accesspoint", wantLen: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doS3Request(t, h, http.MethodGet, tt.path, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				XMLName      xml.Name `xml:"ListAccessPointsResult"`
				AccessPoints []struct {
					Name string `xml:"Name"`
				} `xml:"AccessPointList>AccessPoint"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.AccessPoints, tt.wantLen)
		})
	}
}

func TestCreateAccessPoint_ShortAccountID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
	}{
		{name: "short_id", accountID: "abc"},
		{name: "empty_id", accountID: ""},
		{name: "long_id", accountID: "123456789012"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			// Should not panic regardless of accountID length
			ap := b.CreateAccessPoint(tt.accountID, "my-ap", "my-bucket")
			assert.NotNil(t, ap)
			assert.NotEmpty(t, ap.Alias)
		})
	}
}
