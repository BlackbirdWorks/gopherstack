package cloudwatch

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// PutMetricAlarm creates or updates an alarm.
func (b *InMemoryBackend) PutMetricAlarm(alarm *MetricAlarm) error {
	if alarm.AlarmName == "" {
		return ErrAlarmNameRequired
	}

	// AWS validation: Statistic and ExtendedStatistic are mutually exclusive.
	if alarm.Statistic != "" && alarm.ExtendedStatistic != "" {
		return fmt.Errorf(
			"%w: Statistic and ExtendedStatistic are mutually exclusive",
			ErrValidation,
		)
	}

	// AWS validation: DatapointsToAlarm must not exceed EvaluationPeriods.
	if alarm.DatapointsToAlarm > 0 && alarm.DatapointsToAlarm > alarm.EvaluationPeriods {
		return fmt.Errorf(
			"%w: DatapointsToAlarm (%d) must not exceed EvaluationPeriods (%d)",
			ErrValidation,
			alarm.DatapointsToAlarm,
			alarm.EvaluationPeriods,
		)
	}

	b.mu.Lock("PutMetricAlarm")
	defer b.mu.Unlock()

	isNew := !b.alarms.Has(alarm.AlarmName)

	if alarm.AlarmArn == "" {
		alarm.AlarmArn = arn.Build("cloudwatch", b.region, b.accountID, "alarm:"+alarm.AlarmName)
	}
	if alarm.StateValue == "" {
		alarm.StateValue = alarmStateInsufficientData
	}
	now := time.Now().UTC()
	if alarm.CreatedAt.IsZero() {
		alarm.CreatedAt = now
	}
	// Preserve the state-transitioned timestamp from an existing alarm if the state did not change.
	if existing, ok := b.alarms.Get(alarm.AlarmName); ok {
		if existing.StateValue == alarm.StateValue {
			alarm.StateTransitionedTimestamp = existing.StateTransitionedTimestamp
		} else {
			alarm.StateTransitionedTimestamp = now
		}
	} else {
		alarm.StateTransitionedTimestamp = now
	}
	alarm.AlarmConfigurationUpdatedTimestamp = now

	cp := *alarm
	b.alarms.Put(&cp)

	histType := historyTypeConfigurationUpdate
	historySummary := fmt.Sprintf("Alarm %q updated", alarm.AlarmName)
	if isNew {
		historySummary = fmt.Sprintf("Alarm %q created", alarm.AlarmName)
	}
	b.appendHistory(alarm.AlarmName, "MetricAlarm", histType, historySummary, "")

	return nil
}

// DescribeAlarms lists a page of alarms, optionally filtered by name, type, prefix, and/or state.
// alarmTypes can contain "MetricAlarm", "CompositeAlarm", and/or "LogAlarm".
// Per the real DescribeAlarmsInput.AlarmTypes doc comment ("If you omit this
// parameter, only metric alarms are returned, even if composite alarms or
// log alarms exist in the account" -- confirmed against
// aws-sdk-go-v2/service/cloudwatch@v1.66.3/api_op_DescribeAlarms.go), omitting
// alarmTypes returns ONLY metric alarms: composite and log alarms are both
// included only when explicitly requested via alarmTypes (bd gopherstack-yvb7
// -- composite alarms were previously defaulted-in alongside metric alarms,
// which the LogAlarm family correctly never did).
// MaxRecords applies to the total combined result set (metric + composite + log).
func (b *InMemoryBackend) DescribeAlarms(
	alarmNames []string,
	alarmTypes []string,
	alarmNamePrefix, stateValue, nextToken string,
	maxRecords int,
) (page.Page[MetricAlarm], page.Page[CompositeAlarm], page.Page[LogAlarm], error) {
	b.mu.RLock("DescribeAlarms")
	defer b.mu.RUnlock()

	nameSet := toSet(alarmNames)
	typeSet := toSet(alarmTypes)
	includeMetric := len(typeSet) == 0 || typeSet["MetricAlarm"]
	includeComposite := typeSet["CompositeAlarm"]
	includeLog := typeSet[alarmTypeLogAlarm]

	metricResult := b.collectMetricAlarms(nameSet, alarmNamePrefix, stateValue, includeMetric)
	compositeResult := b.collectCompositeAlarms(
		nameSet,
		alarmNamePrefix,
		stateValue,
		includeComposite,
	)
	logResult := b.collectLogAlarms(nameSet, alarmNamePrefix, stateValue, includeLog)

	// Apply a single combined page limit so MaxRecords constrains the total result set.
	limit := maxRecords
	if limit <= 0 {
		limit = cwDefaultDescribeAlarmsLimit
	}
	metricSlice, compositeSlice, logSlice, next := paginateAlarmResults(
		metricResult, compositeResult, logResult, nextToken, limit,
	)

	return page.Page[MetricAlarm]{Data: metricSlice, Next: next},
		page.Page[CompositeAlarm]{Data: compositeSlice, Next: next},
		page.Page[LogAlarm]{Data: logSlice, Next: next},
		nil
}

// paginateAlarmResults applies a single combined page window across the three
// alarm-type result lists (already filtered and sorted by the caller) so that
// MaxRecords/NextToken constrain and page through the total combined result
// set the same way real DescribeAlarms pagination does.
func paginateAlarmResults(
	metricResult []MetricAlarm,
	compositeResult []CompositeAlarm,
	logResult []LogAlarm,
	nextToken string,
	limit int,
) ([]MetricAlarm, []CompositeAlarm, []LogAlarm, string) {
	combinedTotal := len(metricResult) + len(compositeResult) + len(logResult)
	start := min(page.DecodeToken(nextToken), combinedTotal)
	end := start + limit
	var next string
	if end < combinedTotal {
		next = page.EncodeToken(end)
	} else {
		end = combinedTotal
	}

	var metricSlice []MetricAlarm
	var compositeSlice []CompositeAlarm
	var logSlice []LogAlarm
	for i := start; i < end; i++ {
		switch {
		case i < len(metricResult):
			metricSlice = append(metricSlice, metricResult[i])
		case i < len(metricResult)+len(compositeResult):
			compositeSlice = append(compositeSlice, compositeResult[i-len(metricResult)])
		default:
			logSlice = append(logSlice, logResult[i-len(metricResult)-len(compositeResult)])
		}
	}
	if metricSlice == nil {
		metricSlice = []MetricAlarm{}
	}
	if compositeSlice == nil {
		compositeSlice = []CompositeAlarm{}
	}
	if logSlice == nil {
		logSlice = []LogAlarm{}
	}

	return metricSlice, compositeSlice, logSlice, next
}

// toSet converts a string slice to a set (map[string]bool).
func toSet(ss []string) map[string]bool {
	m := make(map[string]bool, len(ss))
	for _, s := range ss {
		m[s] = true
	}

	return m
}

// collectMetricAlarms returns filtered and sorted metric alarms.
// Caller must hold b.mu (read lock).
func (b *InMemoryBackend) collectMetricAlarms(
	nameSet map[string]bool,
	alarmNamePrefix, stateValue string,
	include bool,
) []MetricAlarm {
	if !include {
		return nil
	}

	var result []MetricAlarm

	for _, alarm := range b.alarms.All() {
		if len(nameSet) > 0 && !nameSet[alarm.AlarmName] {
			continue
		}

		if alarmNamePrefix != "" && !strings.HasPrefix(alarm.AlarmName, alarmNamePrefix) {
			continue
		}

		if stateValue != "" && alarm.StateValue != stateValue {
			continue
		}

		result = append(result, *alarm)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AlarmName < result[j].AlarmName
	})

	return result
}

// collectCompositeAlarms returns filtered and sorted composite alarms.
// Caller must hold b.mu (read lock).
func (b *InMemoryBackend) collectCompositeAlarms(
	nameSet map[string]bool,
	alarmNamePrefix, stateValue string,
	include bool,
) []CompositeAlarm {
	if !include {
		return nil
	}

	var result []CompositeAlarm

	for _, alarm := range b.compositeAlarms.All() {
		if len(nameSet) > 0 && !nameSet[alarm.AlarmName] {
			continue
		}

		if alarmNamePrefix != "" && !strings.HasPrefix(alarm.AlarmName, alarmNamePrefix) {
			continue
		}

		if stateValue != "" && alarm.StateValue != stateValue {
			continue
		}

		result = append(result, *alarm)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AlarmName < result[j].AlarmName
	})

	return result
}

// DescribeAlarmsForMetric returns metric alarms associated with a specific metric.
func (b *InMemoryBackend) DescribeAlarmsForMetric(
	namespace, metricName string,
	dimensions []Dimension,
	alarmNames []string,
	nextToken string,
	maxRecords int,
) (page.Page[MetricAlarm], error) {
	b.mu.RLock("DescribeAlarmsForMetric")
	defer b.mu.RUnlock()

	nameSet := make(map[string]bool, len(alarmNames))
	for _, n := range alarmNames {
		nameSet[n] = true
	}

	var result []MetricAlarm
	for _, alarm := range b.alarms.All() {
		if namespace != "" && alarm.Namespace != namespace {
			continue
		}
		if metricName != "" && alarm.MetricName != metricName {
			continue
		}
		if len(nameSet) > 0 && !nameSet[alarm.AlarmName] {
			continue
		}
		if len(dimensions) > 0 && !dimsContainAll(alarm.Dimensions, dimensions) {
			continue
		}
		result = append(result, *alarm)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AlarmName < result[j].AlarmName
	})

	return page.New(result, nextToken, maxRecords, cwDefaultDescribeForMetricLimit), nil
}

// DeleteAlarms removes alarms by name (metric and composite).
func (b *InMemoryBackend) DeleteAlarms(alarmNames []string) error {
	b.mu.Lock("DeleteAlarms")
	defer b.mu.Unlock()

	for _, name := range alarmNames {
		b.alarms.Delete(name)
		b.compositeAlarms.Delete(name)
		b.logAlarms.Delete(name)
		// Release the per-alarm history so it cannot accumulate across the
		// lifetime of the backend once the alarm itself is gone.
		delete(b.alarmHistory, name)
	}

	return nil
}

// EnableAlarmActions enables action execution for the given alarms.
func (b *InMemoryBackend) EnableAlarmActions(alarmNames []string) error {
	b.mu.Lock("EnableAlarmActions")
	defer b.mu.Unlock()

	for _, name := range alarmNames {
		if a, ok := b.alarms.Get(name); ok {
			a.ActionsEnabled = true
		}
		if ca, ok := b.compositeAlarms.Get(name); ok {
			ca.ActionsEnabled = true
		}
		if la, ok := b.logAlarms.Get(name); ok {
			la.ActionsEnabled = true
		}
	}

	return nil
}

// DisableAlarmActions disables action execution for the given alarms.
func (b *InMemoryBackend) DisableAlarmActions(alarmNames []string) error {
	b.mu.Lock("DisableAlarmActions")
	defer b.mu.Unlock()

	for _, name := range alarmNames {
		if a, ok := b.alarms.Get(name); ok {
			a.ActionsEnabled = false
		}
		if ca, ok := b.compositeAlarms.Get(name); ok {
			ca.ActionsEnabled = false
		}
		if la, ok := b.logAlarms.Get(name); ok {
			la.ActionsEnabled = false
		}
	}

	return nil
}

// GetAlarmARNs returns the ARNs for the given alarm names (metric + composite + log).
// Used by the HTTP handler to clean up tag entries on delete.
func (b *InMemoryBackend) GetAlarmARNs(names []string) []string {
	b.mu.RLock("GetAlarmARNs")
	defer b.mu.RUnlock()

	arns := make([]string, 0, len(names))
	for _, name := range names {
		if a, ok := b.alarms.Get(name); ok && a.AlarmArn != "" {
			arns = append(arns, a.AlarmArn)
		}
		if ca, ok := b.compositeAlarms.Get(name); ok && ca.AlarmArn != "" {
			arns = append(arns, ca.AlarmArn)
		}
		if la, ok := b.logAlarms.Get(name); ok && la.AlarmArn != "" {
			arns = append(arns, la.AlarmArn)
		}
	}

	return arns
}
