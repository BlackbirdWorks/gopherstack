package gcpmonitoring

import (
	"math"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	kindGauge      = metricpb.MetricDescriptor_GAUGE
	kindDelta      = metricpb.MetricDescriptor_DELTA
	kindCumulative = metricpb.MetricDescriptor_CUMULATIVE
	vtBool         = metricpb.MetricDescriptor_BOOL
	vtInt64        = metricpb.MetricDescriptor_INT64
	vtDouble       = metricpb.MetricDescriptor_DOUBLE
	vtString       = metricpb.MetricDescriptor_STRING
	vtDist         = metricpb.MetricDescriptor_DISTRIBUTION
)

// valueTypeOf reports the value type a TypedValue carries, or UNSPECIFIED when no value is set.
func valueTypeOf(v *monitoringpb.TypedValue) metricpb.MetricDescriptor_ValueType {
	switch v.GetValue().(type) {
	case *monitoringpb.TypedValue_BoolValue:
		return vtBool
	case *monitoringpb.TypedValue_Int64Value:
		return vtInt64
	case *monitoringpb.TypedValue_DoubleValue:
		return vtDouble
	case *monitoringpb.TypedValue_StringValue:
		return vtString
	case *monitoringpb.TypedValue_DistributionValue:
		return vtDist
	}

	return metricpb.MetricDescriptor_VALUE_TYPE_UNSPECIFIED
}

// number reads a numeric or boolean TypedValue as a float64; other values read as zero.
func number(v *monitoringpb.TypedValue) float64 {
	switch x := v.GetValue().(type) {
	case *monitoringpb.TypedValue_Int64Value:
		return float64(x.Int64Value)
	case *monitoringpb.TypedValue_DoubleValue:
		return x.DoubleValue
	case *monitoringpb.TypedValue_BoolValue:
		if x.BoolValue {
			return 1
		}
	}

	return 0
}

func intValue(n int64) *monitoringpb.TypedValue {
	return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: n}}
}

func doubleValue(f float64) *monitoringpb.TypedValue {
	return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: f}}
}

func boolValue(v bool) *monitoringpb.TypedValue {
	return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_BoolValue{BoolValue: v}}
}

// numberOf builds a TypedValue of the given type from a float64.
func numberOf(vt metricpb.MetricDescriptor_ValueType, f float64) *monitoringpb.TypedValue {
	if vt == vtInt64 {
		return intValue(int64(math.Round(f)))
	}

	return doubleValue(f)
}

func pointAt(start, end time.Time, v *monitoringpb.TypedValue) *monitoringpb.Point {
	iv := &monitoringpb.TimeInterval{EndTime: timestamppb.New(end)}
	if !start.IsZero() {
		iv.StartTime = timestamppb.New(start)
	}

	return &monitoringpb.Point{Interval: iv, Value: v}
}

func endOf(p *monitoringpb.Point) time.Time { return p.GetInterval().GetEndTime().AsTime() }

func startOf(p *monitoringpb.Point) time.Time {
	if p.GetInterval().GetStartTime() == nil {
		return endOf(p)
	}

	return p.GetInterval().GetStartTime().AsTime()
}
