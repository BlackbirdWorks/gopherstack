package cloudwatch

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	opGreater        = "GreaterThanThreshold"
	opGreaterOrEqual = "GreaterThanOrEqualToThreshold"
	opLess           = "LessThanThreshold"
	opLessOrEqual    = "LessThanOrEqualToThreshold"
	codeInvalidParam = "InvalidParameterValue"
)

type alarmEvalDetail struct {
	values       map[int]float64
	start        time.Time
	treatMissing string
	period       time.Duration
	evalPeriods  int
	needed       int
	breaching    int
	missing      int
}

func alarmPeriodDuration(a MetricAlarm) time.Duration {
	period := a.Period
	if period <= 0 {
		for _, q := range a.Metrics {
			if q.MetricStat.Period > 0 {
				period = q.MetricStat.Period

				break
			}
			if q.Period > 0 {
				period = q.Period

				break
			}
		}
	}

	return time.Duration(period) * time.Second
}

func comparisonPhrase(op string) string {
	switch op {
	case opGreater:
		return "greater than"
	case opGreaterOrEqual:
		return "greater than or equal to"
	case opLess:
		return "less than"
	case opLessOrEqual:
		return "less than or equal to"
	default:
		return ""
	}
}

func javaFloat(v float64) string {
	s := strconv.FormatFloat(v, 'f', -1, 64)
	if !strings.ContainsAny(s, ".eE") {
		s += ".0"
	}

	return s
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}

	return many
}

type alarmPoint struct {
	at  time.Time
	val float64
}

func (d alarmEvalDetail) points(keep func(float64) bool) []alarmPoint {
	idxs := make([]int, 0, len(d.values))
	for i := range d.values {
		idxs = append(idxs, i)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(idxs)))

	pts := make([]alarmPoint, 0, len(idxs))
	for _, i := range idxs {
		if keep == nil || keep(d.values[i]) {
			pts = append(pts, alarmPoint{at: d.start.Add(time.Duration(i) * d.period), val: d.values[i]})
		}
	}

	return pts
}

func formatAlarmPoints(pts []alarmPoint) string {
	parts := make([]string, len(pts))
	for i, p := range pts {
		parts[i] = fmt.Sprintf("%s (%s)", javaFloat(p.val), p.at.UTC().Format("02/01/06 15:04:05"))
	}

	return strings.Join(parts, ", ")
}

// buildAlarmStateReason renders the StateReason/StateReasonData CloudWatch attaches to an
// evaluator-driven transition. Operators without a plain-language phrase (anomaly bands)
// fall back to a count-only reason.
func buildAlarmStateReason(a MetricAlarm, newState string, d alarmEvalDetail, now time.Time) (string, string) {
	phrase := comparisonPhrase(a.ComparisonOperator)
	thr := javaFloat(a.Threshold)

	switch newState {
	case alarmStateInsufficientData:
		n := d.evalPeriods - len(d.values)
		if n < 1 {
			n = d.evalPeriods
		}

		return fmt.Sprintf("Insufficient Data: %d %s unknown.", n, plural(n, "datapoint was", "datapoints were")),
			alarmReasonData(a, d, now)
	case alarmStateAlarm, alarmStateOK:
		if phrase == "" {
			return fmt.Sprintf("Threshold Crossed: %d out of the last %d datapoints breached the threshold.",
				d.breaching, d.evalPeriods), alarmReasonData(a, d, now)
		}

		return thresholdReason(a, newState, d, phrase, thr), alarmReasonData(a, d, now)
	default:
		return "", ""
	}
}

func thresholdReason(a MetricAlarm, newState string, d alarmEvalDetail, phrase, thr string) string {
	breaches := func(v float64) bool {
		return breachesThreshold(v, a.Threshold, a.Threshold, a.ComparisonOperator)
	}

	var pts []alarmPoint
	verb := phrase
	transition := "OK -> ALARM"
	needed := d.needed

	if newState == alarmStateAlarm {
		pts = d.points(breaches)
	} else {
		pts = d.points(func(v float64) bool { return !breaches(v) })
		verb = "not " + phrase
		transition = "ALARM -> OK"
		needed = d.evalPeriods - d.needed + 1
	}

	if len(pts) == 0 {
		return fmt.Sprintf(
			"Threshold Crossed: no datapoints were received for %d %s and %d missing %s treated as [%s].",
			d.missing, plural(d.missing, "period", "periods"),
			d.missing, plural(d.missing, "datapoint was", "datapoints were"), missingTreatment(d.treatMissing),
		)
	}

	return fmt.Sprintf(
		"Threshold Crossed: %d out of the last %d datapoints [%s] %s %s the threshold (%s) "+
			"(minimum %d %s for %s transition).",
		len(pts), d.evalPeriods, formatAlarmPoints(pts), plural(len(pts), "was", "were"), verb, thr,
		needed, plural(needed, "datapoint", "datapoints"), transition,
	)
}

func missingTreatment(t string) string {
	switch t {
	case "breaching":
		return "Breaching"
	case "notBreaching":
		return "NonBreaching"
	default:
		return "Missing"
	}
}

func alarmReasonData(a MetricAlarm, d alarmEvalDetail, now time.Time) string {
	const stamp = "2006-01-02T15:04:05.000-0700"

	pts := d.points(nil)
	recent := make([]float64, len(pts))
	evaluated := make([]map[string]any, len(pts))
	for i, p := range pts {
		recent[i] = p.val
		evaluated[i] = map[string]any{"timestamp": p.at.UTC().Format(stamp), "value": p.val}
	}

	stat := a.Statistic
	if stat == "" {
		stat = a.ExtendedStatistic
	}

	data := map[string]any{
		"version":             "1.0",
		"queryDate":           now.UTC().Format(stamp),
		"startDate":           d.start.UTC().Format(stamp),
		"statistic":           stat,
		"period":              int(d.period / time.Second),
		"recentDatapoints":    recent,
		"threshold":           a.Threshold,
		"evaluatedDatapoints": evaluated,
	}

	bs, err := json.Marshal(data)
	if err != nil {
		return ""
	}

	return string(bs)
}
