package cloudwatch_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

func benchDims() []cloudwatch.Dimension {
	return []cloudwatch.Dimension{
		{Name: "Service", Value: "api"},
		{Name: "Env", Value: "prod"},
		{Name: "Az", Value: "us-east-1a"},
	}
}

func seedBenchMetrics(b *testing.B, points int) (*cloudwatch.InMemoryBackend, time.Time) {
	b.Helper()

	bk := cloudwatch.NewInMemoryBackend()
	base := time.Now().UTC().Add(-time.Hour)
	data := make([]cloudwatch.MetricDatum, 0, points)

	for i := range points {
		v := float64(i)
		data = append(data, cloudwatch.MetricDatum{
			MetricName: "Requests", Dimensions: benchDims(),
			Timestamp: base.Add(time.Duration(i) * time.Second),
			Value:     v, HasValue: true, Count: 1, Sum: v, Min: v, Max: v,
		})
	}

	if err := bk.PutMetricData("Bench/NS", data); err != nil {
		b.Fatal(err)
	}

	return bk, base
}

func BenchmarkBackendPutMetricData(b *testing.B) {
	bk, base := seedBenchMetrics(b, 10)
	d := []cloudwatch.MetricDatum{{
		MetricName: "Requests", Dimensions: benchDims(), Timestamp: base,
		Value: 1, HasValue: true, Count: 1, Sum: 1, Min: 1, Max: 1,
	}}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if err := bk.PutMetricData("Bench/NS", d); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetMetricStatistics(b *testing.B) {
	bk, base := seedBenchMetrics(b, 900)
	end := base.Add(time.Hour)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		_, err := bk.GetMetricStatistics("Bench/NS", "Requests", benchDims(), base, end, 60,
			[]string{"Average", "Sum", "Maximum"}, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetMetricDataMath(b *testing.B) {
	bk, base := seedBenchMetrics(b, 900)
	end := base.Add(time.Hour)
	q := []cloudwatch.MetricDataQuery{
		{ID: "m1", MetricStat: cloudwatch.MetricStat{
			Namespace: "Bench/NS", MetricName: "Requests", Dimensions: benchDims(), Period: 60, Stat: "Sum",
		}},
		{ID: "e1", Expression: "m1 * 2", ReturnData: true},
		{ID: "e2", Expression: "e1 + m1", ReturnData: true},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := bk.GetMetricData(q, base, end); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEvaluateAlarms(b *testing.B) {
	bk, base := seedBenchMetrics(b, 600)
	now := base.Add(time.Hour)

	for i := range 200 {
		err := bk.PutMetricAlarm(&cloudwatch.MetricAlarm{
			AlarmName: "alarm-" + strconv.Itoa(i), Namespace: "Bench/NS", MetricName: "Requests",
			Dimensions: benchDims(), Period: 60, EvaluationPeriods: 3, Statistic: "Sum",
			Threshold: 1e12, ComparisonOperator: "GreaterThanThreshold", StateValue: "OK",
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		bk.EvaluateAlarms(ctx, now)
	}
}
