package gcpmonitoring

import (
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
)

// missingDataDecides reports whether the mode evaluates a condition without data (ACTIVE or INACTIVE); NO_OP and
// unspecified leave the incident state alone.
func missingDataDecides(m monitoringpb.AlertPolicy_Condition_EvaluationMissingData) bool {
	return m == monitoringpb.AlertPolicy_Condition_EVALUATION_MISSING_DATA_ACTIVE ||
		m == monitoringpb.AlertPolicy_Condition_EVALUATION_MISSING_DATA_INACTIVE
}

// vanishedObservationsLocked reports series that exist in the store but have no point in the evaluation span, as
// not met (INACTIVE) or met (ACTIVE); other modes report nothing.
func (b *InMemoryBackend) vanishedObservationsLocked(
	project string, t *monitoringpb.AlertPolicy_Condition_MetricThreshold, now time.Time, extra []*seriesRec,
	seen map[string]struct{}, period time.Duration,
) []observation {
	mode := t.GetEvaluationMissingData()
	if !missingDataDecides(mode) {
		return nil
	}

	all := b.thresholdSeriesLocked(project, t, window{start: now.Add(-maxPointAge), end: now}, extra, period, now)

	var out []observation

	for _, s := range all {
		id := seriesID(s)
		if _, ok := seen[id]; ok {
			continue
		}

		seen[id] = struct{}{}
		out = append(out, observation{
			id: id, resKey: resourceKeyOf(s.resource), metricType: s.metric.GetType(),
			metricLabels: s.metric.GetLabels(), resourceType: s.resource.GetType(),
			resourceLabels: s.resource.GetLabels(), threshold: t.GetThresholdValue(),
			met: mode == monitoringpb.AlertPolicy_Condition_EVALUATION_MISSING_DATA_ACTIVE,
		})
	}

	return out
}
