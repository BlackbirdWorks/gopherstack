package arczonalshift_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"

	zs "github.com/blackbirdworks/gopherstack/services/arczonalshift"
	cw "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	elbv2 "github.com/blackbirdworks/gopherstack/services/elbv2"
)

// bubble is a backend wired to real ELBv2/CloudWatch siblings, for tests that move the fake clock.
type bubble struct {
	b      *zs.InMemoryBackend
	alarms *cw.InMemoryBackend
	lb     string
}

func newBubble(t *testing.T) *bubble {
	t.Helper()

	elb := elbv2.NewInMemoryBackend(testAccount, testRegion)
	t.Cleanup(elb.Close)

	alarms := cw.NewInMemoryBackendWithConfig(testAccount, testRegion)
	b := zs.NewInMemoryBackend(testAccount)
	b.SetAppConfig(&siblings{elb: elbv2.NewHandler(elb), cw: cw.NewHandler(alarms)})

	lb, err := elb.CreateLoadBalancer(
		elbv2.CreateLoadBalancerInput{Name: "web", Type: "application", Subnets: []string{"s1", "s2", "s3"}},
	)
	require.NoError(t, err)

	return &bubble{b: b, alarms: alarms, lb: lb.LoadBalancerArn}
}

func (x *bubble) alarm(t *testing.T, name string) string {
	t.Helper()

	require.NoError(t, x.alarms.PutMetricAlarm(&cw.MetricAlarm{AlarmName: name, StateValue: "OK"}))

	return "arn:aws:cloudwatch:" + testRegion + ":" + testAccount + ":alarm:" + name
}

func statusOf(t *testing.T, b *zs.InMemoryBackend, id string) string {
	t.Helper()

	all, _, err := b.ListZonalShifts("", "", "", 0)
	require.NoError(t, err)

	for _, s := range all {
		if s.ZonalShiftID == id {
			return s.Status
		}
	}

	t.Fatalf("shift %s not listed", id)

	return ""
}

func TestZonalShiftExpiresOverTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    string
		extend  string
		sleep   time.Duration
		afterUp time.Duration
		cancel  bool
	}{
		{name: "active before expiry", sleep: 9 * time.Minute, want: "ACTIVE"},
		{name: "expired at expiry", sleep: 10 * time.Minute, want: "EXPIRED"},
		{name: "extended by update", sleep: 9 * time.Minute, extend: "30m", afterUp: 20 * time.Minute, want: "ACTIVE"},
		{
			name:    "extension also expires",
			sleep:   9 * time.Minute,
			extend:  "30m",
			afterUp: 31 * time.Minute,
			want:    "EXPIRED",
		},
		{name: "cancel wins over expiry", sleep: time.Minute, cancel: true, afterUp: time.Hour, want: "CANCELED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				x := newBubble(t)

				s, err := x.b.StartZonalShift(x.lb, "us-east-1a", "c", "10m")
				require.NoError(t, err)

				time.Sleep(tt.sleep)

				switch {
				case tt.extend != "":
					_, err = x.b.UpdateZonalShift(s.ZonalShiftID, "", tt.extend)
					require.NoError(t, err)
				case tt.cancel:
					_, err = x.b.CancelZonalShift(s.ZonalShiftID)
					require.NoError(t, err)
				}

				time.Sleep(tt.afterUp)

				assert.Equal(t, tt.want, statusOf(t, x.b, s.ZonalShiftID))

				if tt.want == "EXPIRED" {
					_, err = x.b.UpdateZonalShift(s.ZonalShiftID, "late", "")
					require.Error(t, err, "an expired shift cannot be updated")

					_, err = x.b.CancelZonalShift(s.ZonalShiftID)
					assert.Error(t, err, "an expired shift cannot be canceled")
				}
			})
		})
	}
}

func TestExpiredShiftNoLongerAppliesWeights(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		x := newBubble(t)

		_, err := x.b.StartZonalShift(x.lb, "us-east-1a", "c", "10m")
		require.NoError(t, err)

		res, err := x.b.GetManagedResource(x.lb)
		require.NoError(t, err)
		assert.InDelta(t, float32(0), res.AppliedWeights["use1-az1"], 0.0001)

		time.Sleep(11 * time.Minute)

		res, err = x.b.GetManagedResource(x.lb)
		require.NoError(t, err)
		assert.InDelta(t, float32(1), res.AppliedWeights["use1-az1"], 0.0001)
		assert.Empty(t, res.ZonalShifts)

		_, err = x.b.StartZonalShift(x.lb, "us-east-1a", "again", "10m")
		require.NoError(t, err, "an expired shift does not block a new one for the same zone")
	})
}

func requireReason(t *testing.T, err error, reason string) {
	t.Helper()

	var re *restroute.Error

	require.ErrorAs(t, err, &re)
	assert.Equal(t, reason, re.Extra["reason"])
}
