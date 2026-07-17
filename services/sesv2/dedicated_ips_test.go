package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestGetDedicatedIPs tests the GetDedicatedIps operation.
func TestGetDedicatedIPs(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/dedicated-ips", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetDedicatedIP tests the GetDedicatedIp operation.
func TestGetDedicatedIP(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/dedicated-ips/1.2.3.4", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestPutDedicatedIPInPool tests the PutDedicatedIpInPool operation.
func TestPutDedicatedIPInPool(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/dedicated-ip-pools", map[string]any{
		"PoolName":    "test-pool",
		"ScalingMode": "STANDARD",
	})

	rec := doRequest(t, h, http.MethodPut, "/v2/email/dedicated-ips/1.2.3.4/pool", map[string]any{
		"DestinationPoolName": "test-pool",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestPutDedicatedIPWarmupAttributes tests the PutDedicatedIpWarmupAttributes operation.
func TestPutDedicatedIPWarmupAttributes(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPut, "/v2/email/dedicated-ips/1.2.3.4/warmup", map[string]any{
		"WarmupPercentage": 50,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
