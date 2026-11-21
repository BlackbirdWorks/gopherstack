package connect

import (
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// managedMetricUnits lists the AWS-managed metrics GetMetricDataV2 computes; IDs are the metric names.
const (
	unitInteger        = "INTEGER"
	unitPercent        = "PERCENT"
	metricServiceLevel = "SERVICE_LEVEL"
)

func managedMetricUnits() map[string]string {
	return map[string]string{
		"CONTACTS_CREATED": unitInteger, "CONTACTS_QUEUED": unitInteger, "CONTACTS_HANDLED": unitInteger,
		metricAbandoned: unitInteger, "ABANDONMENT_RATE": unitPercent, metricServiceLevel: unitPercent,
		"AVG_HANDLE_TIME": unitSeconds, "AVG_CONTACT_DURATION": unitSeconds, "AVG_QUEUE_ANSWER_TIME": unitSeconds,
		"AVG_ABANDON_TIME": unitSeconds, "MAX_QUEUED_TIME": unitSeconds,
		"CONTACTS_TRANSFERRED_IN": unitInteger, "CONTACTS_TRANSFERRED_OUT": unitInteger, "SUM_HANDLE_TIME": unitSeconds,
	}
}

func (b *Backend) managedMetric(inst, name string) MetricDefinition {
	return MetricDefinition{
		Arn: arn.Build("connect", b.region, b.accountID, "instance/"+inst+"/metric/"+name),
		ID:  name, Name: name, Type: metricAWS, Status: metricPublished, Unit: managedMetricUnits()[name],
		Groupings: v2Dims(), Filters: []any{}, SupportsCustomCalc: true,
	}
}

func (b *Backend) managedMetricSummaries(inst string) []MetricSummary {
	names := make([]string, 0, len(managedMetricUnits()))
	for n := range managedMetricUnits() {
		names = append(names, n)
	}

	slices.Sort(names)

	out := make([]MetricSummary, 0, len(names))

	for _, n := range names {
		m := b.managedMetric(inst, n)
		out = append(out, MetricSummary{Arn: m.Arn, ID: m.ID, Name: m.Name, Status: m.Status, Type: m.Type})
	}

	return out
}

func isManagedMetricID(id string) bool {
	_, ok := managedMetricUnits()[strings.ToUpper(id)]

	return ok
}
