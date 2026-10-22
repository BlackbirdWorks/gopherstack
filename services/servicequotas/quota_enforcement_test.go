package servicequotas_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqsdk "github.com/aws/aws-sdk-go-v2/service/servicequotas"
	sqtypes "github.com/aws/aws-sdk-go-v2/service/servicequotas/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

type notice struct{ topic, subject, message string }

type fakeNotifier struct {
	got []notice
	mu  sync.Mutex
}

func (n *fakeNotifier) Notify(_ context.Context, topic, subject, message string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.got = append(n.got, notice{topic, subject, message})

	return nil
}

func (n *fakeNotifier) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()

	return len(n.got)
}

type mutableUsage struct{ v float64 }

func (m *mutableUsage) QuotaUsage(_, _, svc, code string) (float64, bool) {
	return m.v, svc == "s3" && code == "L-DC2B2D3D"
}

func TestQuotaLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		service string
		quota   string
		want    float64
		wantOK  bool
	}{
		{name: "default", service: "s3", quota: "L-DC2B2D3D", want: 100, wantOK: true},
		{name: "unknown quota", service: "s3", quota: "L-NOPE", wantOK: false},
		{name: "unknown service", service: "nope", quota: "L-NOPE", wantOK: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			got, ok := f.backend.QuotaLimit("", "", tc.service, tc.quota)
			assert.Equal(t, tc.wantOK, ok)
			assert.InDelta(t, tc.want, got, 0.001)
		})
	}
}

func TestQuotaLimit_FollowsApprovedIncrease(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	requestIncrease(t, f, 20, nil)
	f.clock.advance(1100 * time.Millisecond)

	got, ok := f.backend.QuotaLimit(awsmeta.DefaultAccount, "us-east-1", "ec2", "L-1216C47A")
	require.True(t, ok)
	assert.InDelta(t, 20, got, 0.001)

	other, _ := f.backend.QuotaLimit("999999999999", "us-east-1", "ec2", "L-1216C47A")
	assert.InDelta(t, 5, other, 0.001)
}

func TestRequests_DeniedAboveMax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    sqtypes.RequestStatus
		desired float64
		applied float64
	}{
		{name: "within max", desired: 4000, want: sqtypes.RequestStatusApproved, applied: 4000},
		{name: "above max", desired: 5001, want: sqtypes.RequestStatusDenied, applied: 1000},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			out, err := f.client.RequestServiceQuotaIncrease(t.Context(), &sqsdk.RequestServiceQuotaIncreaseInput{
				ServiceCode:  aws.String("iam"),
				QuotaCode:    aws.String("L-F4A5425F"),
				DesiredValue: aws.Float64(tc.desired),
			})
			require.NoError(t, err)
			assert.Equal(t, sqtypes.RequestStatusPending, out.RequestedQuota.Status)

			f.clock.advance(600 * time.Millisecond)

			got := getRequest(t, f, out.RequestedQuota.Id)
			assert.Equal(t, sqtypes.RequestStatusCaseOpened, got.Status)

			f.clock.advance(600 * time.Millisecond)

			got = getRequest(t, f, out.RequestedQuota.Id)
			assert.Equal(t, tc.want, got.Status)

			limit, _ := f.backend.QuotaLimit(awsmeta.DefaultAccount, "", "iam", "L-F4A5425F")
			assert.InDelta(t, tc.applied, limit, 0.001)
		})
	}
}

func TestReport_IncludesTrackedQuotasWithoutUsageMetric(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetUsageSource(&mutableUsage{v: 50})

	start, err := f.client.StartQuotaUtilizationReport(t.Context(), &sqsdk.StartQuotaUtilizationReportInput{})
	require.NoError(t, err)
	f.clock.advance(1100 * time.Millisecond)

	out, err := f.client.GetQuotaUtilizationReport(t.Context(),
		&sqsdk.GetQuotaUtilizationReportInput{ReportId: start.ReportId, MaxResults: aws.Int32(100)})
	require.NoError(t, err)

	for _, q := range out.Quotas {
		if aws.ToString(q.QuotaCode) == "L-DC2B2D3D" {
			assert.InDelta(t, 50, aws.ToFloat64(q.Utilization), 0.001)

			return
		}
	}

	require.Fail(t, "S3 bucket quota missing from the report")
}

func TestAutoManagementTick(t *testing.T) {
	t.Parallel()

	steps := []struct {
		name   string
		expect string
		usage  float64
		wantN  int
	}{
		{name: "below", usage: 79, wantN: 0},
		{name: "warn", usage: 80, wantN: 1, expect: "80%"},
		{name: "warn again", usage: 90, wantN: 1},
		{name: "critical", usage: 95, wantN: 2, expect: "95%"},
		{name: "drop below", usage: 10, wantN: 2},
		{name: "recross", usage: 85, wantN: 3, expect: "80%"},
	}

	f := newFixture(t)
	usage := &mutableUsage{}
	notifier := &fakeNotifier{}
	f.backend.SetUsageSource(usage)
	f.backend.SetNotifier(notifier)

	_, err := f.client.StartAutoManagement(t.Context(), &sqsdk.StartAutoManagementInput{
		OptInLevel: "ACCOUNT", OptInType: "NotifyOnly",
		NotificationArn: aws.String("arn:aws:sns:us-east-1:123456789012:quota-alerts"),
	})
	require.NoError(t, err)

	for _, st := range steps {
		usage.v = st.usage
		f.backend.Tick(t.Context())
		require.Equal(t, st.wantN, notifier.count(), st.name)

		if st.expect != "" {
			last := notifier.got[len(notifier.got)-1]
			assert.Contains(t, last.subject, st.expect, st.name)
			assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:quota-alerts", last.topic)
		}
	}
}

func TestAutoManagementTick_Disabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, f *fixture)
		name  string
	}{
		{name: "never started", setup: func(*testing.T, *fixture) {}},
		{name: "no topic", setup: func(t *testing.T, f *fixture) {
			t.Helper()

			_, err := f.client.StartAutoManagement(t.Context(),
				&sqsdk.StartAutoManagementInput{OptInLevel: "ACCOUNT", OptInType: "NotifyOnly"})
			require.NoError(t, err)
		}},
		{name: "excluded", setup: func(t *testing.T, f *fixture) {
			t.Helper()

			_, err := f.client.StartAutoManagement(t.Context(), &sqsdk.StartAutoManagementInput{
				OptInLevel: "ACCOUNT", OptInType: "NotifyOnly",
				NotificationArn: aws.String("arn:aws:sns:us-east-1:123456789012:t"),
				ExclusionList:   map[string][]string{"s3": {"L-DC2B2D3D"}},
			})
			require.NoError(t, err)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			notifier := &fakeNotifier{}
			f.backend.SetUsageSource(&mutableUsage{v: 99})
			f.backend.SetNotifier(notifier)
			tc.setup(t, f)
			f.backend.Tick(t.Context())
			assert.Zero(t, notifier.count())
		})
	}
}
