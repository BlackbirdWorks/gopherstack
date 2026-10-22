package gcpmonitoring

import (
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
)

const (
	pct99 = 99
	pct95 = 95
	pct50 = 50
	pct05 = 5

	minAlignmentPeriod = 60 * time.Second
	maxAlignBuckets    = 10000
	staleness          = 5 * time.Minute
)

type (
	aligner = monitoringpb.Aggregation_Aligner
	reducer = monitoringpb.Aggregation_Reducer
)

const (
	alignNone       = monitoringpb.Aggregation_ALIGN_NONE
	alignDelta      = monitoringpb.Aggregation_ALIGN_DELTA
	alignRate       = monitoringpb.Aggregation_ALIGN_RATE
	alignNextOlder  = monitoringpb.Aggregation_ALIGN_NEXT_OLDER
	alignMin        = monitoringpb.Aggregation_ALIGN_MIN
	alignMax        = monitoringpb.Aggregation_ALIGN_MAX
	alignMean       = monitoringpb.Aggregation_ALIGN_MEAN
	alignCount      = monitoringpb.Aggregation_ALIGN_COUNT
	alignSum        = monitoringpb.Aggregation_ALIGN_SUM
	alignStddev     = monitoringpb.Aggregation_ALIGN_STDDEV
	alignCountTrue  = monitoringpb.Aggregation_ALIGN_COUNT_TRUE
	alignCountFalse = monitoringpb.Aggregation_ALIGN_COUNT_FALSE
	alignFracTrue   = monitoringpb.Aggregation_ALIGN_FRACTION_TRUE
	alignP99        = monitoringpb.Aggregation_ALIGN_PERCENTILE_99
	alignP95        = monitoringpb.Aggregation_ALIGN_PERCENTILE_95
	alignP50        = monitoringpb.Aggregation_ALIGN_PERCENTILE_50
	alignP05        = monitoringpb.Aggregation_ALIGN_PERCENTILE_05

	reduceNone       = monitoringpb.Aggregation_REDUCE_NONE
	reduceMean       = monitoringpb.Aggregation_REDUCE_MEAN
	reduceMin        = monitoringpb.Aggregation_REDUCE_MIN
	reduceMax        = monitoringpb.Aggregation_REDUCE_MAX
	reduceSum        = monitoringpb.Aggregation_REDUCE_SUM
	reduceStddev     = monitoringpb.Aggregation_REDUCE_STDDEV
	reduceCount      = monitoringpb.Aggregation_REDUCE_COUNT
	reduceCountTrue  = monitoringpb.Aggregation_REDUCE_COUNT_TRUE
	reduceCountFalse = monitoringpb.Aggregation_REDUCE_COUNT_FALSE
	reduceFracTrue   = monitoringpb.Aggregation_REDUCE_FRACTION_TRUE
)

func isNumeric(vt metricpb.MetricDescriptor_ValueType) bool { return vt == vtInt64 || vt == vtDouble }

func isAccumulating(k metricpb.MetricDescriptor_MetricKind) bool {
	return k == kindDelta || k == kindCumulative
}

func percentileOf(a aligner) (float64, bool) {
	switch a {
	case alignP99:
		return pct99, true
	case alignP95:
		return pct95, true
	case alignP50:
		return pct50, true
	case alignP05:
		return pct05, true
	default:
		return 0, false
	}
}

func alignerApplies(a aligner, k metricpb.MetricDescriptor_MetricKind, vt metricpb.MetricDescriptor_ValueType) bool {
	if _, isP := percentileOf(a); isP {
		return vt == vtDist
	}

	switch a {
	case alignNone, alignNextOlder:
		return true
	case alignDelta, alignRate:
		return isAccumulating(k) && isNumeric(vt)
	case alignMin, alignMax, alignSum, alignStddev:
		return !isCumulativeKind(k) && isNumeric(vt)
	case alignMean:
		return !isCumulativeKind(k) && (isNumeric(vt) || vt == vtDist)
	case alignCount:
		return !isCumulativeKind(k)
	case alignCountTrue, alignCountFalse, alignFracTrue:
		return vt == vtBool
	default:
		return false
	}
}

func isCumulativeKind(k metricpb.MetricDescriptor_MetricKind) bool { return k == kindCumulative }

func reducerApplies(r reducer, vt metricpb.MetricDescriptor_ValueType) bool {
	switch r {
	case reduceNone, reduceCount:
		return true
	case reduceMean, reduceMin, reduceMax, reduceSum, reduceStddev:
		return isNumeric(vt)
	case reduceCountTrue, reduceCountFalse, reduceFracTrue:
		return vt == vtBool
	default:
		return false
	}
}

// outputTypes gives the kind and value type an aligner produces from a series of the given kind and type.
func outputTypes(
	a aligner, k metricpb.MetricDescriptor_MetricKind, vt metricpb.MetricDescriptor_ValueType,
) (metricpb.MetricDescriptor_MetricKind, metricpb.MetricDescriptor_ValueType) {
	if _, isP := percentileOf(a); isP {
		return kindGauge, vtDouble
	}

	switch a {
	case alignDelta:
		return kindDelta, vt
	case alignRate:
		return kindGauge, vtDouble
	case alignMean, alignStddev:
		return k, vtDouble
	case alignCount, alignCountTrue, alignCountFalse:
		return k, vtInt64
	case alignFracTrue:
		return kindGauge, vtDouble
	default:
		return k, vt
	}
}

func validateAggregation(agg *monitoringpb.Aggregation, field string) error {
	if agg == nil {
		return nil
	}

	if agg.GetPerSeriesAligner() == alignNone && agg.GetCrossSeriesReducer() != reduceNone {
		return errInvalid("Field %s.per_series_aligner had an invalid value of \"ALIGN_NONE\": "+
			"An aligner must be specified when a cross series reducer is used.", field)
	}

	if agg.GetPerSeriesAligner() != alignNone {
		p := agg.GetAlignmentPeriod().AsDuration()
		if agg.GetAlignmentPeriod() == nil || p < minAlignmentPeriod {
			return errInvalid("Field %s.alignment_period had an invalid value of %q: "+
				"The alignment period must be at least 60 seconds.", field, p)
		}
	}

	if len(agg.GetGroupByFields()) > 0 && agg.GetCrossSeriesReducer() == reduceNone {
		return errInvalid("Field %s.group_by_fields had an invalid value: "+
			"group_by_fields requires a cross_series_reducer.", field)
	}

	for _, f := range agg.GetGroupByFields() {
		if _, ok := groupField(f); !ok {
			return errInvalid("Field %s.group_by_fields had an invalid value of %q: unknown field.", field, f)
		}
	}

	return nil
}
