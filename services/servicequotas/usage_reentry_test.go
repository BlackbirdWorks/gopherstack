package servicequotas_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqsdk "github.com/aws/aws-sdk-go-v2/service/servicequotas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

type reentrantUsage struct {
	lookup   func(account, region, serviceCode, quotaCode string) (float64, bool)
	timedOut *atomic.Bool
}

func (u reentrantUsage) QuotaUsage(_, _, _, _ string) (float64, bool) {
	if u.timedOut.Load() {
		return 1, true
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		u.lookup(awsmeta.DefaultAccount, "", "iam", "L-F4A5425F")
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		u.timedOut.Store(true)
	}

	return 1, true
}

func TestUsageSourceRunsOutsideBackendLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "report", run: func(t *testing.T, f *fixture) {
			t.Helper()

			start, err := f.client.StartQuotaUtilizationReport(t.Context(), &sqsdk.StartQuotaUtilizationReportInput{})
			require.NoError(t, err)
			f.clock.advance(1100 * time.Millisecond)

			_, err = f.client.GetQuotaUtilizationReport(t.Context(),
				&sqsdk.GetQuotaUtilizationReportInput{ReportId: start.ReportId, MaxResults: aws.Int32(100)})
			require.NoError(t, err)
		}},
		{name: "tick", run: func(t *testing.T, f *fixture) {
			t.Helper()

			_, err := f.client.StartAutoManagement(t.Context(), &sqsdk.StartAutoManagementInput{
				OptInLevel: "ACCOUNT", OptInType: "NotifyOnly",
				NotificationArn: aws.String("arn:aws:sns:us-east-1:123456789012:quota-alerts"),
			})
			require.NoError(t, err)

			f.backend.Tick(t.Context())
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			var timedOut atomic.Bool

			f.backend.SetUsageSource(reentrantUsage{lookup: f.backend.QuotaLimit, timedOut: &timedOut})

			tc.run(t, f)

			assert.False(t, timedOut.Load(), "usage source ran under the backend lock")
		})
	}
}
