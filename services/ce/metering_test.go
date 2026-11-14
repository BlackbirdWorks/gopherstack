package ce_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	costexplorersdk "github.com/aws/aws-sdk-go-v2/service/costexplorer"
	cetypes "github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
	"github.com/blackbirdworks/gopherstack/services/pricing"
)

const (
	acct         = "123456789012"
	ec2Service   = "Amazon Elastic Compute Cloud - Compute"
	boxUsage     = "BoxUsage:t3.medium"
	t3mediumRate = 0.0416
	meterDay     = "2031-05-01"
)

type testClock struct{ now atomic.Pointer[time.Time] }

func newTestClock(t time.Time) *testClock {
	c := &testClock{}
	c.now.Store(&t)

	return c
}

func (c *testClock) Now() time.Time { return *c.now.Load() }

func (c *testClock) Advance(d time.Duration) {
	n := c.Now().Add(d)
	c.now.Store(&n)
}

func newMeteredBackend(clock *testClock) *ce.InMemoryBackend {
	b := ce.NewInMemoryBackend(acct, "us-east-1")
	b.SetClock(clock.Now)
	b.SetPriceLookup(pricing.OnDemandRate)

	return b
}

func ec2Usage(start, end time.Time, id string, qty float64) ce.Usage {
	return ce.Usage{
		Service: ec2Service, PriceService: "AmazonEC2", UsageType: boxUsage, Operation: "RunInstances",
		AvailabilityZone: "us-east-1a", InstanceType: "t3.medium", ResourceID: id, Unit: "Hrs",
		PriceMatch: map[string]string{"usagetype": boxUsage, "operation": "RunInstances"},
		Tags:       map[string]string{"Team": "core"}, Quantity: qty, Start: start, End: end,
	}
}

func meteredRows(b *ce.InMemoryBackend, start, end string) []ce.CostEntry {
	var out []ce.CostEntry

	for _, e := range b.LedgerEntries(start, end) {
		if e.ResourceID != "" {
			out = append(out, e)
		}
	}

	return out
}

func TestRecordUsagePricesFromPriceList(t *testing.T) {
	t.Parallel()

	day := time.Date(2031, 5, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		mutate   func(*ce.Usage)
		name     string
		wantCost float64
		wantQty  float64
		wantRows int
	}{
		{name: "priced", mutate: func(*ce.Usage) {}, wantCost: 2 * t3mediumRate, wantQty: 2, wantRows: 1},
		{
			name: "region without price", mutate: func(u *ce.Usage) { u.Region = "eu-north-1" },
			wantCost: 0, wantQty: 2, wantRows: 1,
		},
		{name: "zero quantity", mutate: func(u *ce.Usage) { u.Quantity = 0 }, wantRows: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newMeteredBackend(newTestClock(day))
			u := ec2Usage(day.Add(time.Hour), day.Add(3*time.Hour), "i-0abc", 2)
			tc.mutate(&u)
			b.RecordUsage(u)

			rows := meteredRows(b, meterDay, "2031-05-02")
			require.Len(t, rows, tc.wantRows)

			if tc.wantRows == 0 {
				return
			}

			assert.InDelta(t, tc.wantCost, rows[0].UnblendedCost, 1e-9)
			assert.InDelta(t, tc.wantQty, rows[0].UsageQuantity, 1e-9)
			assert.Equal(t, "i-0abc", rows[0].ResourceID)
			assert.Equal(t, "us-east-1a", rows[0].AvailabilityZone)
			assert.Equal(t, "core", rows[0].Tags["Team"])
		})
	}
}

func TestRecordUsageSplitsAcrossDaysAndMergesRows(t *testing.T) {
	t.Parallel()

	day := time.Date(2031, 5, 1, 0, 0, 0, 0, time.UTC)

	b := newMeteredBackend(newTestClock(day))
	b.RecordUsage(ec2Usage(day.Add(23*time.Hour), day.Add(25*time.Hour), "i-0abc", 2))
	b.RecordUsage(ec2Usage(day.Add(22*time.Hour), day.Add(23*time.Hour), "i-0abc", 1))

	first := meteredRows(b, meterDay, "2031-05-02")
	second := meteredRows(b, "2031-05-02", "2031-05-03")

	require.Len(t, first, 1)
	require.Len(t, second, 1)
	assert.InDelta(t, 2, first[0].UsageQuantity, 1e-9)
	assert.InDelta(t, 1, second[0].UsageQuantity, 1e-9)
	assert.InDelta(t, 3*t3mediumRate, first[0].UnblendedCost+second[0].UnblendedCost, 1e-9)
}

func TestAccrueBooksHeldResourcesForTheirLifetime(t *testing.T) {
	t.Parallel()

	start := time.Date(2031, 5, 1, 10, 0, 0, 0, time.UTC)
	clock := newTestClock(start)
	b := newMeteredBackend(clock)

	var running atomic.Bool

	running.Store(true)
	b.AddHoldingSource(func() []ce.Holding {
		if !running.Load() {
			return nil
		}

		return []ce.Holding{{Usage: ec2Usage(time.Time{}, time.Time{}, "i-0abc", 1)}}
	})

	clock.Advance(3 * time.Hour)
	b.Accrue()

	rows := meteredRows(b, meterDay, "2031-05-02")
	require.Len(t, rows, 1)
	assert.InDelta(t, 3, rows[0].UsageQuantity, 1e-9)
	assert.InDelta(t, 3*t3mediumRate, rows[0].UnblendedCost, 1e-9)

	running.Store(false)
	clock.Advance(5 * time.Hour)
	b.Accrue()

	rows = meteredRows(b, meterDay, "2031-05-02")
	require.Len(t, rows, 1)
	assert.InDelta(t, 3, rows[0].UsageQuantity, 1e-9)
}

func TestAccrueDrainsUsageSources(t *testing.T) {
	t.Parallel()

	clock := newTestClock(time.Date(2031, 5, 1, 10, 0, 0, 0, time.UTC))
	b := newMeteredBackend(clock)

	var calls atomic.Int32

	b.AddUsageSource(func() []ce.Usage {
		if calls.Add(1) > 1 {
			return nil
		}

		return []ce.Usage{{
			Service: "Amazon DynamoDB", PriceService: "AmazonDynamoDB", UsageType: "WriteRequestUnits",
			Operation: "PayPerRequestThroughput", ResourceID: "arn:aws:dynamodb:us-east-1:" + acct + ":table/t",
			Unit: "WriteRequestUnits", PriceMatch: map[string]string{"usagetype": "WriteRequestUnits"}, Quantity: 1000,
		}}
	})

	b.Accrue()
	b.Accrue()

	rows := meteredRows(b, meterDay, "2031-05-02")
	require.Len(t, rows, 1)
	assert.InDelta(t, 1000, rows[0].UsageQuantity, 1e-9)
	assert.InDelta(t, 1000*0.00000125, rows[0].UnblendedCost, 1e-12)
}

func TestMeteredRowsSurviveSnapshotAndResetClearsThem(t *testing.T) {
	t.Parallel()

	day := time.Date(2031, 5, 1, 0, 0, 0, 0, time.UTC)
	b := newMeteredBackend(newTestClock(day))
	b.RecordUsage(ec2Usage(day, day.Add(time.Hour), "i-0abc", 1))

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := newMeteredBackend(newTestClock(day))
	restored.RecordUsage(ec2Usage(day, day.Add(time.Hour), "i-0other", 1))
	require.NoError(t, restored.Restore(t.Context(), snap))

	rows := meteredRows(restored, meterDay, "2031-05-02")
	require.Len(t, rows, 1)
	assert.Equal(t, "i-0abc", rows[0].ResourceID)

	restored.Reset()
	assert.Empty(t, meteredRows(restored, meterDay, "2031-05-02"))
}

func TestGetCostAndUsageWithResourcesReturnsMeteredRows(t *testing.T) {
	t.Parallel()

	day := time.Date(2031, 5, 1, 0, 0, 0, 0, time.UTC)
	b := newMeteredBackend(newTestClock(day))
	b.RecordUsage(ec2Usage(day, day.Add(2*time.Hour), "i-0abc", 2))

	other := ec2Usage(day, day.Add(time.Hour), "i-0def", 1)
	other.Tags = map[string]string{"Team": "web"}
	b.RecordUsage(other)

	client := newTestCEClient(t, ce.NewHandler(b))
	period := &cetypes.DateInterval{Start: aws.String(meterDay), End: aws.String("2031-05-02")}
	filter := &cetypes.Expression{Dimensions: &cetypes.DimensionValues{
		Key: cetypes.DimensionService, Values: []string{ec2Service},
	}}

	tests := []struct {
		want    map[string]string
		name    string
		groupBy cetypes.GroupDefinition
	}{
		{
			name: "resource",
			groupBy: cetypes.GroupDefinition{
				Type: cetypes.GroupDefinitionTypeDimension,
				Key:  aws.String("RESOURCE_ID"),
			},
			want: map[string]string{"i-0abc": "0.0832", "i-0def": "0.0416"},
		},
		{
			name:    "tag",
			groupBy: cetypes.GroupDefinition{Type: cetypes.GroupDefinitionTypeTag, Key: aws.String("Team")},
			want:    map[string]string{"Team$core": "0.0832", "Team$web": "0.0416"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.GetCostAndUsageWithResources(
				t.Context(),
				&costexplorersdk.GetCostAndUsageWithResourcesInput{
					TimePeriod: period, Granularity: cetypes.GranularityDaily, Metrics: []string{"UnblendedCost"},
					Filter: filter, GroupBy: []cetypes.GroupDefinition{tc.groupBy},
				},
			)
			require.NoError(t, err)
			require.Len(t, out.ResultsByTime, 1)

			got := map[string]string{}
			for _, g := range out.ResultsByTime[0].Groups {
				got[g.Keys[0]] = aws.ToString(g.Metrics["UnblendedCost"].Amount)
			}

			assert.Equal(t, tc.want, got)
		})
	}
}

func TestGetCostAndUsageWithResourcesExcludesResourceLessRows(t *testing.T) {
	t.Parallel()

	b := ce.NewInMemoryBackend(acct, "us-east-1")
	client := newTestCEClient(t, ce.NewHandler(b))
	end := time.Now().UTC().AddDate(0, 0, 1).Format("2006-01-02")
	start := time.Now().UTC().AddDate(0, 0, -3).Format("2006-01-02")

	out, err := client.GetCostAndUsageWithResources(t.Context(), &costexplorersdk.GetCostAndUsageWithResourcesInput{
		TimePeriod:  &cetypes.DateInterval{Start: aws.String(start), End: aws.String(end)},
		Granularity: cetypes.GranularityDaily, Metrics: []string{"UnblendedCost"},
		Filter: &cetypes.Expression{Dimensions: &cetypes.DimensionValues{
			Key: cetypes.DimensionService, Values: []string{ec2Service},
		}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.ResultsByTime)

	for _, r := range out.ResultsByTime {
		assert.Equal(t, "0.0000", aws.ToString(r.Total["UnblendedCost"].Amount))
	}
}

func TestGetCostAndUsageIncludesMeteredCost(t *testing.T) {
	t.Parallel()

	day := time.Date(2031, 5, 1, 0, 0, 0, 0, time.UTC)
	b := newMeteredBackend(newTestClock(day))
	b.RecordUsage(ec2Usage(day, day.Add(2*time.Hour), "i-0abc", 2))

	client := newTestCEClient(t, ce.NewHandler(b))

	out, err := client.GetCostAndUsage(t.Context(), &costexplorersdk.GetCostAndUsageInput{
		TimePeriod:  &cetypes.DateInterval{Start: aws.String(meterDay), End: aws.String("2031-05-02")},
		Granularity: cetypes.GranularityDaily, Metrics: []string{"UnblendedCost"},
		Filter: &cetypes.Expression{Dimensions: &cetypes.DimensionValues{
			Key: cetypes.DimensionResourceId, Values: []string{"i-0abc"},
		}},
	})
	require.NoError(t, err)
	require.Len(t, out.ResultsByTime, 1)
	assert.Equal(t, "0.0832", aws.ToString(out.ResultsByTime[0].Total["UnblendedCost"].Amount))
}
