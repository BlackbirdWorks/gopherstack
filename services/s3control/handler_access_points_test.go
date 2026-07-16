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
				require.Error(t, err)

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
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 0, s3control.AccessPointCount(b))
		})
	}
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

// ---- Per-AP PublicAccessBlock tests ----

func TestAccessPointPublicAccessBlock_PutGetDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		apName    string
		wantBlock bool
		wantCode  int
	}{
		{
			name:      "put_and_get_pab",
			apName:    "my-ap",
			wantBlock: true,
			wantCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			b.CreateAccessPoint("000000000000", tt.apName, "my-bucket")

			cfg := s3control.PublicAccessBlock{
				AccountID:             "000000000000",
				BlockPublicAcls:       true,
				IgnorePublicAcls:      true,
				BlockPublicPolicy:     false,
				RestrictPublicBuckets: true,
			}

			err := b.PutAccessPointPublicAccessBlock("000000000000", tt.apName, cfg)
			require.NoError(t, err)

			got, err := b.GetAccessPointPublicAccessBlock("000000000000", tt.apName)
			require.NoError(t, err)
			assert.True(t, got.BlockPublicAcls)
			assert.True(t, got.IgnorePublicAcls)
			assert.False(t, got.BlockPublicPolicy)
			assert.True(t, got.RestrictPublicBuckets)

			err = b.DeleteAccessPointPublicAccessBlock("000000000000", tt.apName)
			require.NoError(t, err)

			_, err = b.GetAccessPointPublicAccessBlock("000000000000", tt.apName)
			require.Error(t, err)
		})
	}
}

func TestAccessPointPublicAccessBlock_MissingAP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(b *s3control.InMemoryBackend) error
		name string
	}{
		{
			name: "get_pab_missing_ap",
			fn: func(b *s3control.InMemoryBackend) error {
				_, err := b.GetAccessPointPublicAccessBlock("000000000000", "nonexistent")

				return err
			},
		},
		{
			name: "put_pab_missing_ap",
			fn: func(b *s3control.InMemoryBackend) error {
				return b.PutAccessPointPublicAccessBlock(
					"000000000000", "nonexistent",
					s3control.PublicAccessBlock{},
				)
			},
		},
		{
			name: "delete_pab_missing_ap",
			fn: func(b *s3control.InMemoryBackend) error {
				return b.DeleteAccessPointPublicAccessBlock("000000000000", "nonexistent")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			err := tt.fn(b)
			require.Error(t, err)
		})
	}
}

func TestHandler_AccessPointPublicAccessBlock(t *testing.T) {
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
			name:   "put_pab",
			method: http.MethodPut,
			body: `<PutAccessPointPublicAccessBlockRequest>
<PublicAccessBlockConfiguration>
<BlockPublicAcls>true</BlockPublicAcls>
<IgnorePublicAcls>false</IgnorePublicAcls>
<BlockPublicPolicy>true</BlockPublicPolicy>
<RestrictPublicBuckets>false</RestrictPublicBuckets>
</PublicAccessBlockConfiguration>
</PutAccessPointPublicAccessBlockRequest>`,
			wantStatus: http.StatusOK,
			setup: func(h *s3control.Handler) {
				h.Backend.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
			},
		},
		{
			name:   "put_pab_missing_ap",
			method: http.MethodPut,
			body: `<PutAccessPointPublicAccessBlockRequest>` +
				`<PublicAccessBlockConfiguration>` +
				`<BlockPublicAcls>true</BlockPublicAcls>` +
				`</PublicAccessBlockConfiguration>` +
				`</PutAccessPointPublicAccessBlockRequest>`,
			wantStatus: http.StatusNotFound,
			setup:      func(_ *s3control.Handler) {},
		},
		{
			name:       "get_pab_not_set",
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
			setup: func(h *s3control.Handler) {
				h.Backend.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
			},
		},
		{
			name:       "get_pab_after_put",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
			wantBody:   "GetAccessPointPublicAccessBlockResult",
			setup: func(h *s3control.Handler) {
				h.Backend.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
				_ = h.Backend.PutAccessPointPublicAccessBlock(
					"000000000000", "my-ap",
					s3control.PublicAccessBlock{BlockPublicAcls: true},
				)
			},
		},
		{
			name:       "delete_pab",
			method:     http.MethodDelete,
			wantStatus: http.StatusNoContent,
			setup: func(h *s3control.Handler) {
				h.Backend.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
				_ = h.Backend.PutAccessPointPublicAccessBlock(
					"000000000000", "my-ap",
					s3control.PublicAccessBlock{BlockPublicAcls: true},
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := s3control.NewHandler(s3control.NewInMemoryBackend())
			tt.setup(h)

			rec := doS3ControlNewOpRequest(
				t, h, tt.method,
				"/v20180820/accesspoint/my-ap/publicAccessBlock",
				"000000000000",
				tt.body,
			)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// ---- AccessPoint VPC config tests ----

func TestAccessPoint_VpcConfig_SetAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		vpcID           string
		bucketAccountID string
		wantOrigin      string
		wantAlias       bool
	}{
		{
			name:       "vpc_access_point",
			vpcID:      "vpc-abc123",
			wantOrigin: "VPC",
			wantAlias:  false,
		},
		{
			name:            "internet_with_bucket_account",
			vpcID:           "",
			bucketAccountID: "111122223333",
			wantOrigin:      "Internet",
			wantAlias:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			b.CreateAccessPoint("000000000000", "my-ap", "my-bucket")

			err := b.SetAccessPointVpcConfig("000000000000", "my-ap", tt.vpcID, tt.bucketAccountID)
			require.NoError(t, err)

			ap, err := b.GetAccessPoint("000000000000", "my-ap")
			require.NoError(t, err)
			assert.Equal(t, tt.wantOrigin, ap.NetworkOrigin)
			assert.Equal(t, tt.vpcID, ap.VpcID)
			assert.Equal(t, tt.bucketAccountID, ap.BucketAccountID)

			if tt.wantAlias {
				assert.NotEmpty(t, ap.Alias)
			} else {
				assert.Empty(t, ap.Alias)
			}
		})
	}
}

func TestAccessPoint_VpcConfig_MissingAP(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	err := b.SetAccessPointVpcConfig("000000000000", "nonexistent", "vpc-123", "")
	require.Error(t, err)
}

func TestHandler_GetAccessPoint_NetworkOrigin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(b *s3control.InMemoryBackend)
		wantOrigin     string
		wantVpcPresent bool
	}{
		{
			name: "internet_access_point",
			setup: func(b *s3control.InMemoryBackend) {
				b.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
			},
			wantOrigin:     "Internet",
			wantVpcPresent: false,
		},
		{
			name: "vpc_access_point",
			setup: func(b *s3control.InMemoryBackend) {
				b.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
				_ = b.SetAccessPointVpcConfig("000000000000", "my-ap", "vpc-abc123", "")
			},
			wantOrigin:     "VPC",
			wantVpcPresent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			tt.setup(b)
			h := s3control.NewHandler(b)

			rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/accesspoint/my-ap", "000000000000", "")

			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantOrigin)
			if tt.wantVpcPresent {
				assert.Contains(t, rec.Body.String(), "vpc-abc123")
			}
		})
	}
}

func TestHandler_CreateAccessPoint_WithVpc(t *testing.T) {
	t.Parallel()

	h := s3control.NewHandler(s3control.NewInMemoryBackend())
	body := `<CreateAccessPointRequest>
<Bucket>my-bucket</Bucket>
<VpcConfiguration><VpcId>vpc-xyz789</VpcId></VpcConfiguration>
<BucketAccountId>111122223333</BucketAccountId>
</CreateAccessPointRequest>`

	rec := doS3ControlNewOpRequest(
		t, h, http.MethodPut,
		"/v20180820/accesspoint/my-vpc-ap",
		"000000000000",
		body,
	)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateAccessPointResult")

	// Verify stored state.
	ap, err := h.Backend.GetAccessPoint("000000000000", "my-vpc-ap")
	require.NoError(t, err)
	assert.Equal(t, "vpc-xyz789", ap.VpcID)
	assert.Equal(t, "VPC", ap.NetworkOrigin)
	assert.Equal(t, "111122223333", ap.BucketAccountID)
	assert.Empty(t, ap.Alias) // VPC APs have no alias
}

func TestHandler_ListAccessPoints_IncludesAlias(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateAccessPoint("000000000000", "ap-one", "bucket-one")
	h := s3control.NewHandler(b)

	rec := doS3ControlNewOpRequest(t, h, http.MethodGet, "/v20180820/accesspoint", "000000000000", "")

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "ListAccessPointsResult")
	assert.Contains(t, body, "Internet")
	assert.Contains(t, body, "s3alias") // alias present for Internet APs
}

// ---- AccessPoint CreationDate tests ----

func TestAccessPoint_CreationDateSet(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	ap := b.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
	assert.NotEmpty(t, ap.CreationDate)
}

func TestHandler_GetAccessPoint_IncludesCreationDate(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
	h := s3control.NewHandler(b)

	rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
		"/v20180820/accesspoint/my-ap", "000000000000", "")

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreationDate")
}
