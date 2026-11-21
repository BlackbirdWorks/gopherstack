package elb_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elb"
)

func TestQuotaLimiterClassicLoadBalancers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		limit     float64
		creates   int
		wantCount int
		known     bool
		wantErr   bool
	}{
		{name: "lower limit rejects third", limit: 2, known: true, creates: 3, wantCount: 2, wantErr: true},
		{name: "higher limit lifts the default cap", limit: 25, known: true, creates: 22, wantCount: 22},
		{name: "unknown quota keeps default cap", limit: 1, creates: 21, wantCount: 20, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elb.NewInMemoryBackend("123456789012", "us-east-1")
			b.SetQuotaLimiter(func(_, region, svc, code string) (float64, bool) {
				match := region == "us-east-1" && svc == "elasticloadbalancing" && code == "L-E9E9831D"

				return tt.limit, tt.known && match
			})

			var err error

			for i := range tt.creates {
				_, err = b.CreateLoadBalancer(t.Context(), elb.CreateLoadBalancerInput{
					LoadBalancerName:  "lb" + strconv.Itoa(i),
					AvailabilityZones: []string{"us-east-1a"},
					Listeners:         []elb.Listener{{Protocol: "HTTP", LoadBalancerPort: 80, InstancePort: 8080}},
				})
			}

			if tt.wantErr {
				require.ErrorIs(t, err, elb.ErrTooManyLoadBalancers)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCount, b.RegionLoadBalancerCount("us-east-1"))
			assert.Equal(t, 0, b.RegionLoadBalancerCount("us-west-2"))
		})
	}
}
