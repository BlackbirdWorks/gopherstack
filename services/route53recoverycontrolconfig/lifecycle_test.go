package route53recoverycontrolconfig_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cc "github.com/blackbirdworks/gopherstack/services/route53recoverycontrolconfig"
)

func TestActivationDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantStatus    string
		delay         time.Duration
		elapsed       time.Duration
		wantEndpoints bool
	}{
		{name: "instant by default", wantStatus: "DEPLOYED", wantEndpoints: true},
		{name: "pending inside the window", delay: time.Minute, elapsed: 10 * time.Second, wantStatus: "PENDING"},
		{
			name:          "deployed after the window",
			delay:         time.Minute,
			elapsed:       2 * time.Minute,
			wantStatus:    "DEPLOYED",
			wantEndpoints: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := cc.NewInMemoryBackend(testAccount)
				b.SetActivationDelay(tt.delay)

				cl, err := b.CreateCluster("c", "", "", nil)
				require.NoError(t, err)

				rc, err := b.CreateRoutingControl(cl.ClusterArn, "", "r", "")
				require.NoError(t, err)

				time.Sleep(tt.elapsed)

				got, err := b.DescribeCluster(cl.ClusterArn)
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, got.Status)
				assert.Equal(t, tt.wantEndpoints, len(got.ClusterEndpoints) > 0)

				ctl, err := b.DescribeRoutingControl(rc.RoutingControlArn)
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, ctl.Status)
			})
		})
	}
}

func TestPendingDeletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		elapsed    time.Duration
		wantGone   bool
	}{
		{name: "visible while deleting", elapsed: 10 * time.Second, wantStatus: "PENDING_DELETION"},
		{name: "gone after the window", elapsed: 2 * time.Minute, wantGone: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := cc.NewInMemoryBackend(testAccount)
				b.SetActivationDelay(time.Minute)

				cl, err := b.CreateCluster("c", "", "", nil)
				require.NoError(t, err)

				time.Sleep(2 * time.Minute)
				require.NoError(t, b.DeleteCluster(cl.ClusterArn))
				time.Sleep(tt.elapsed)

				got, err := b.DescribeCluster(cl.ClusterArn)
				if tt.wantGone {
					require.Error(t, err)

					list, _, lErr := b.ListClusters("", 0)
					require.NoError(t, lErr)
					assert.Empty(t, list)

					return
				}

				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, got.Status)

				list, _, err := b.ListClusters("", 0)
				require.NoError(t, err)
				assert.Len(t, list, 1)
			})
		})
	}
}
