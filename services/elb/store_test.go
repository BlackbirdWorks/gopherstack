package elb_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elb"
)

// TestBackendReset verifies that Reset() clears all backend state.
func TestBackendReset(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "lb-reset-1")
	mustCreateLB(t, h, "lb-reset-2")

	require.Equal(t, 2, b.LoadBalancerCount())

	b.Reset()

	assert.Equal(t, 0, b.LoadBalancerCount())
	assert.Equal(t, 0, b.PolicyCount())
}

// TestBackendMultipleResetCycle verifies that repeated Reset calls are safe.
func TestBackendMultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)

	for range 5 {
		mustCreateLB(t, h, "cycle-lb")
		b.Reset()
		assert.Equal(t, 0, b.LoadBalancerCount())
	}
}

// TestBackendExportCountHelpers verifies LoadBalancerCount and PolicyCount.
func TestBackendExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)

	assert.Equal(t, 0, b.LoadBalancerCount())
	assert.Equal(t, 0, b.PolicyCount())

	mustCreateLB(t, h, "count-lb")

	assert.Equal(t, 1, b.LoadBalancerCount())

	doELB(t, h, url.Values{
		"Action":           {"CreateAppCookieStickinessPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"count-lb"},
		"PolicyName":       {"my-policy"},
		"CookieName":       {"SESS"},
	})

	assert.Equal(t, 1, b.PolicyCount())
}
