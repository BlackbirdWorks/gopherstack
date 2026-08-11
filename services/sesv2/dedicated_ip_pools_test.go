package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// TestCreateDedicatedIPPool tests dedicated IP pool creation.
func TestCreateDedicatedIPPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setup       func(*sesv2.InMemoryBackend)
		poolName    string
		scalingMode string
		wantErr     bool
	}{
		{
			name:        "standard_mode",
			setup:       func(*sesv2.InMemoryBackend) {},
			poolName:    "my-pool",
			scalingMode: "STANDARD",
		},
		{
			name:        "managed_mode",
			setup:       func(*sesv2.InMemoryBackend) {},
			poolName:    "my-pool-2",
			scalingMode: "MANAGED",
		},
		{
			name:        "invalid_scaling_mode",
			setup:       func(*sesv2.InMemoryBackend) {},
			poolName:    "my-pool",
			scalingMode: "INVALID",
			wantErr:     true,
		},
		{
			name:        "empty_name",
			setup:       func(*sesv2.InMemoryBackend) {},
			poolName:    "",
			scalingMode: "STANDARD",
			wantErr:     true,
		},
		{
			name: "duplicate",
			setup: func(b *sesv2.InMemoryBackend) {
				_, _ = b.CreateDedicatedIPPool("my-pool", "STANDARD", nil)
			},
			poolName:    "my-pool",
			scalingMode: "STANDARD",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			tt.setup(backend)

			_, err := backend.CreateDedicatedIPPool(tt.poolName, tt.scalingMode, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 1, sesv2.DedicatedIPPoolCount(backend))
		})
	}
}

// TestCreateDedicatedIPPoolHTTP tests CreateDedicatedIpPool via HTTP.
func TestCreateDedicatedIPPoolHTTP(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	body := map[string]any{"PoolName": "my-pool", "ScalingMode": "STANDARD"}
	rec := doReq(t, h, http.MethodPost, "/v2/email/dedicated-ip-pools", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestCreateDedicatedIPPoolHTTPInvalidMode tests invalid scaling mode.
func TestCreateDedicatedIPPoolHTTPInvalidMode(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	body := map[string]any{"PoolName": "my-pool", "ScalingMode": "INVALID"}
	rec := doReq(t, h, http.MethodPost, "/v2/email/dedicated-ip-pools", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestGetDedicatedIPPool tests the GetDedicatedIpPool operation.
func TestGetDedicatedIPPool(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/dedicated-ip-pools", map[string]any{
		"PoolName":    "GetIPPool",
		"ScalingMode": "STANDARD",
	})

	rec := doRequest(t, h, http.MethodGet, "/v2/email/dedicated-ip-pools/GetIPPool", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestDeleteDedicatedIPPool tests the DeleteDedicatedIpPool operation.
func TestDeleteDedicatedIPPool(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/dedicated-ip-pools", map[string]any{
		"PoolName":    "DelIPPool",
		"ScalingMode": "STANDARD",
	})

	rec := doRequest(t, h, http.MethodDelete, "/v2/email/dedicated-ip-pools/DelIPPool", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListDedicatedIPPools tests the ListDedicatedIpPools operation.
func TestListDedicatedIPPools(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/dedicated-ip-pools", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestPutDedicatedIPPoolScalingAttributes tests the PutDedicatedIpPoolScalingAttributes operation.
func TestPutDedicatedIPPoolScalingAttributes(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/dedicated-ip-pools", map[string]any{
		"PoolName":    "ScalingPool",
		"ScalingMode": "STANDARD",
	})

	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/dedicated-ip-pools/ScalingPool/scaling",
		map[string]any{
			"ScalingMode": "STANDARD",
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}
