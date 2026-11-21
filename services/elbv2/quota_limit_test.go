package elbv2_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

func TestQuotaLimiterLoadBalancers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		lbType    string
		code      string
		limit     float64
		creates   int
		wantCount int
		known     bool
		wantErr   bool
	}{
		{
			name: "alb over limit", lbType: "application", code: "L-53DA6B97", limit: 2, known: true,
			creates: 3, wantCount: 2, wantErr: true,
		},
		{
			name: "alb default type", code: "L-53DA6B97", limit: 1, known: true,
			creates: 2, wantCount: 1, wantErr: true,
		},
		{
			name: "nlb over limit", lbType: "network", code: "L-69A177A2", limit: 2, known: true,
			creates: 3, wantCount: 2, wantErr: true,
		},
		{
			name: "nlb within limit", lbType: "network", code: "L-69A177A2", limit: 3, known: true,
			creates: 3, wantCount: 3,
		},
		{
			name: "alb quota does not cap nlb", lbType: "network", code: "L-53DA6B97", limit: 1, known: true,
			creates: 2, wantCount: 2,
		},
		{name: "unknown quota", lbType: "network", code: "L-69A177A2", limit: 1, creates: 3, wantCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			h.SetQuotaLimiter(func(_, _, svc, code string) (float64, bool) {
				return tt.limit, tt.known && svc == "elasticloadbalancing" && code == tt.code
			})

			var err error

			for i := range tt.creates {
				_, err = h.Backend.CreateLoadBalancer(elbv2.CreateLoadBalancerInput{
					Name: "lb-" + strconv.Itoa(i), Type: tt.lbType,
				})
			}

			if tt.wantErr {
				require.ErrorIs(t, err, elbv2.ErrTooManyLoadBalancers)
			} else {
				require.NoError(t, err)
			}

			lbType := tt.lbType
			if lbType == "" {
				lbType = "application"
			}

			assert.Equal(t, tt.wantCount, h.LoadBalancerCount("us-east-1", lbType))
			assert.Equal(t, 0, h.LoadBalancerCount("us-west-2", lbType))
		})
	}
}
