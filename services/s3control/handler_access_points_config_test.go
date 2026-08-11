package s3control_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

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

// TestHandler_GetAccessPoint_IncludesPublicAccessBlockConfiguration locks in
// the real wire shape: GetAccessPointOutput carries
// PublicAccessBlockConfiguration inline, no standalone operation (see
// dispatchAccessPointSubResourceOps in handler_access_points.go). A client
// that creates an access point with a PublicAccessBlockConfiguration must
// see it echoed back on GetAccessPoint.
func TestHandler_GetAccessPoint_IncludesPublicAccessBlockConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *s3control.Handler)
		name       string
		wantBody   string
		wantAbsent string
	}{
		{
			name: "pab_set_via_create",
			setup: func(h *s3control.Handler) {
				h.Backend.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
				_ = h.Backend.PutAccessPointPublicAccessBlock(
					"000000000000", "my-ap",
					s3control.PublicAccessBlock{BlockPublicAcls: true, RestrictPublicBuckets: true},
				)
			},
			wantBody: "PublicAccessBlockConfiguration",
		},
		{
			name: "pab_not_set",
			setup: func(h *s3control.Handler) {
				h.Backend.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
			},
			wantAbsent: "PublicAccessBlockConfiguration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := s3control.NewHandler(s3control.NewInMemoryBackend())
			tt.setup(h)

			rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/accesspoint/my-ap", "000000000000", "")

			require.Equal(t, http.StatusOK, rec.Code)
			body := rec.Body.String()

			if tt.wantBody != "" {
				assert.Contains(t, body, tt.wantBody)
				assert.Contains(t, body, "<BlockPublicAcls>true</BlockPublicAcls>")
			}

			if tt.wantAbsent != "" {
				assert.NotContains(t, body, tt.wantAbsent)
			}
		})
	}
}

// TestNoFabricatedAccessPointPublicAccessBlockRoute locks in the op-deletion
// fix: the standalone GetAccessPointPublicAccessBlock /
// PutAccessPointPublicAccessBlock / DeleteAccessPointPublicAccessBlock REST
// operations do not exist in aws-sdk-go-v2/service/s3control and must never
// be routable, since a fabricated route pollutes GetSupportedOperations()
// with operation names no real SDK client can ever construct.
func TestNoFabricatedAccessPointPublicAccessBlockRoute(t *testing.T) {
	t.Parallel()

	h := s3control.NewHandler(s3control.NewInMemoryBackend())
	h.Backend.CreateAccessPoint("000000000000", "my-ap", "my-bucket")

	rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
		"/v20180820/accesspoint/my-ap/publicAccessBlock", "000000000000", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)

	ops := h.GetSupportedOperations()
	assert.NotContains(t, ops, "GetAccessPointPublicAccessBlock")
	assert.NotContains(t, ops, "PutAccessPointPublicAccessBlock")
	assert.NotContains(t, ops, "DeleteAccessPointPublicAccessBlock")
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
