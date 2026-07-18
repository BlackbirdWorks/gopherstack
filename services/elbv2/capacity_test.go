package elbv2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestELBv2_DescribeCapacityReservation verifies the handler succeeds for a known LB.
func TestELBv2_DescribeCapacityReservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "existing_lb",
			setup: func(t *testing.T, h *elbv2.Handler) string {
				t.Helper()

				return mustCreateLB(t, h, "cap-lb")
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_lb_arn",
			setup: func(t *testing.T, _ *elbv2.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			lbArn := tt.setup(t, h)

			vals := url.Values{
				"Action":  {"DescribeCapacityReservation"},
				"Version": {"2015-12-01"},
			}
			if lbArn != "" {
				vals.Set("LoadBalancerArn", lbArn)
			}

			rec := doELBv2(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestDescribeCapacityReservation(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "cap-res-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeCapacityReservation"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestModifyCapacityReservation(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "mod-cap-res-lb")

	rec := doELBv2(t, h, url.Values{
		"Action":          {"ModifyCapacityReservation"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
		"MinimumLoadBalancerCapacity.CapacityUnits": {"100"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
