package applicationautoscaling

import (
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"

	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

const (
	targetTrackingAlarmPrefix = "TargetTracking-"
	alarmHighKind             = "AlarmHigh"
	alarmLowKind              = "AlarmLow"

	statAverage          = "Average"
	metricCPU            = "CPUUtilization"
	nsEC2Spot            = "AWS/EC2Spot"
	albLabelSlashes      = 5
	alarmPeriodSeconds   = 60
	scaleOutDatapoints   = 3
	scaleInDatapoints    = 15
	scaleInThresholdPart = 0.9
)

type metricShape struct {
	namespace  string
	metricName string
	statistic  string
	unit       string
	queries    []cwbackend.MetricDataQuery
	dims       []cwbackend.Dimension
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)

	return m
}

func asString(v any) string {
	s, _ := v.(string)

	return s
}

func dimensionsOf(v any) []cwbackend.Dimension {
	raw, _ := v.([]any)
	out := make([]cwbackend.Dimension, 0, len(raw))

	for _, d := range raw {
		m := asMap(d)
		out = append(out, cwbackend.Dimension{Name: asString(m["Name"]), Value: asString(m["Value"])})
	}

	return out
}

// metricFor resolves the alarm metric for a target-tracking configuration; ok is false for
// predefined metric types with no known CloudWatch mapping.
func metricFor(resourceID string, cfg map[string]any) (metricShape, bool) {
	if custom := asMap(cfg["CustomizedMetricSpecification"]); custom != nil {
		return customMetric(custom), true
	}

	spec := asMap(cfg["PredefinedMetricSpecification"])
	if spec == nil {
		return metricShape{}, false
	}

	return predefinedMetric(asString(spec["PredefinedMetricType"]), resourceID, asString(spec["ResourceLabel"]))
}

func customMetric(c map[string]any) metricShape {
	if raw, ok := c["Metrics"].([]any); ok && len(raw) > 0 {
		queries := make([]cwbackend.MetricDataQuery, 0, len(raw))

		for _, q := range raw {
			m := asMap(q)
			ret := true

			if v, has := m["ReturnData"].(bool); has {
				ret = v
			}

			dq := cwbackend.MetricDataQuery{
				ID: asString(m["Id"]), Label: asString(m["Label"]), Expression: asString(m["Expression"]),
				ReturnData: ret, Period: alarmPeriodSeconds,
			}

			if ms := asMap(m["MetricStat"]); ms != nil {
				metric := asMap(ms["Metric"])
				dq.MetricStat = cwbackend.MetricStat{
					Namespace:  asString(metric["Namespace"]),
					MetricName: asString(metric["MetricName"]),
					Dimensions: dimensionsOf(
						metric["Dimensions"],
					),
					Stat:   asString(ms["Stat"]),
					Period: alarmPeriodSeconds,
				}
			}

			queries = append(queries, dq)
		}

		return metricShape{queries: queries}
	}

	stat := asString(c["Statistic"])
	if stat == "" {
		stat = statAverage
	}

	return metricShape{
		namespace: asString(c["Namespace"]), metricName: asString(c["MetricName"]),
		dims: dimensionsOf(c["Dimensions"]), statistic: stat, unit: asString(c["Unit"]),
	}
}

func dim(name, value string) cwbackend.Dimension {
	return cwbackend.Dimension{Name: name, Value: value}
}

// slashParts splits s on "/" into a fixed array so callers can index segments without bounds checks.
func slashParts(s string) [6]string {
	var out [6]string

	copy(out[:], strings.Split(s, "/"))

	return out
}

func afterColon(s string) string {
	_, rest, _ := strings.Cut(s, ":")

	return rest
}

type predefinedSpec struct {
	dims      func(resourceID, label string) []cwbackend.Dimension
	namespace string
	metric    string
	stat      string
}

func ecsDims(id, _ string) []cwbackend.Dimension {
	p := slashParts(id)

	return []cwbackend.Dimension{dim("ClusterName", p[1]), dim("ServiceName", p[2])}
}

func readerDims(id, _ string) []cwbackend.Dimension {
	return []cwbackend.Dimension{dim("DBClusterIdentifier", afterColon(id)), dim("Role", "READER")}
}

func roleDims(role string) func(string, string) []cwbackend.Dimension {
	return func(id, _ string) []cwbackend.Dimension {
		return []cwbackend.Dimension{dim("ReplicationGroupId", slashParts(id)[1]), dim("Role", role)}
	}
}

func spotFleetDims(id, _ string) []cwbackend.Dimension {
	return []cwbackend.Dimension{dim("FleetRequestId", slashParts(id)[1])}
}

func albDims(_, label string) []cwbackend.Dimension {
	p := slashParts(label)

	return []cwbackend.Dimension{
		dim("LoadBalancer", p[0]+"/"+p[1]+"/"+p[2]),
		dim("TargetGroup", p[3]+"/"+p[4]+"/"+p[5]),
	}
}

func sageMakerDims(id, _ string) []cwbackend.Dimension {
	p := slashParts(id)

	return []cwbackend.Dimension{dim("EndpointName", p[1]), dim("VariantName", p[3])}
}

func lambdaDims(id, _ string) []cwbackend.Dimension {
	name, _, _ := strings.Cut(afterColon(id), ":")

	return []cwbackend.Dimension{dim("FunctionName", name), dim("Resource", afterColon(id))}
}

func appStreamDims(id, _ string) []cwbackend.Dimension {
	return []cwbackend.Dimension{dim("Fleet", slashParts(id)[1])}
}

func predefinedTable() map[string]predefinedSpec {
	return map[string]predefinedSpec{
		"ECSServiceAverageCPUUtilization":          {ecsDims, "AWS/ECS", metricCPU, statAverage},
		"ECSServiceAverageMemoryUtilization":       {ecsDims, "AWS/ECS", "MemoryUtilization", statAverage},
		"ALBRequestCountPerTarget":                 {albDims, "AWS/ApplicationELB", "RequestCountPerTarget", "Sum"},
		"RDSReaderAverageCPUUtilization":           {readerDims, "AWS/RDS", metricCPU, statAverage},
		"RDSReaderAverageDatabaseConnections":      {readerDims, "AWS/RDS", "DatabaseConnections", statAverage},
		"NeptuneReaderAverageCPUUtilization":       {readerDims, "AWS/Neptune", metricCPU, statAverage},
		"SageMakerVariantInvocationsPerInstance":   {sageMakerDims, "AWS/SageMaker", "InvocationsPerInstance", "Sum"},
		"EC2SpotFleetRequestAverageCPUUtilization": {spotFleetDims, nsEC2Spot, metricCPU, statAverage},
		"EC2SpotFleetRequestAverageNetworkIn":      {spotFleetDims, nsEC2Spot, "NetworkIn", statAverage},
		"EC2SpotFleetRequestAverageNetworkOut":     {spotFleetDims, nsEC2Spot, "NetworkOut", statAverage},
		"LambdaProvisionedConcurrencyUtilization": {
			lambdaDims, "AWS/Lambda", "ProvisionedConcurrencyUtilization", statAverage,
		},
		"ElastiCachePrimaryEngineCPUUtilization": {
			roleDims("Primary"), "AWS/ElastiCache", "EngineCPUUtilization", statAverage,
		},
		"ElastiCacheReplicaEngineCPUUtilization": {
			roleDims("Replica"), "AWS/ElastiCache", "EngineCPUUtilization", statAverage,
		},
		"AppStreamAverageCapacityUtilization": {appStreamDims, "AWS/AppStream", "CapacityUtilization", statAverage},
	}
}

func predefinedMetric(metricType, resourceID, label string) (metricShape, bool) {
	switch metricType {
	case "DynamoDBReadCapacityUtilization":
		return dynamoDBUtilization(resourceID, "ConsumedReadCapacityUnits", "ProvisionedReadCapacityUnits")
	case "DynamoDBWriteCapacityUtilization":
		return dynamoDBUtilization(resourceID, "ConsumedWriteCapacityUnits", "ProvisionedWriteCapacityUnits")
	}

	spec, ok := predefinedTable()[metricType]
	if !ok {
		return metricShape{}, false
	}

	if metricType == "ALBRequestCountPerTarget" &&
		strings.Count(label, "/") < albLabelSlashes {
		return metricShape{}, false
	}

	return metricShape{
		namespace: spec.namespace, metricName: spec.metric, statistic: spec.stat, dims: spec.dims(resourceID, label),
	}, true
}

// dynamoDBUtilization is consumed capacity as a percentage of provisioned capacity.
func dynamoDBUtilization(resourceID, consumed, provisioned string) (metricShape, bool) {
	p := slashParts(resourceID)

	dims := []cwbackend.Dimension{dim("TableName", p[1])}
	if p[2] == "index" {
		dims = append(dims, dim("GlobalSecondaryIndexName", p[3]))
	}

	stat := func(id, name, s string) cwbackend.MetricDataQuery {
		return cwbackend.MetricDataQuery{
			ID: id, Period: alarmPeriodSeconds,
			MetricStat: cwbackend.MetricStat{
				Namespace: "AWS/DynamoDB", MetricName: name, Dimensions: dims, Stat: s, Period: alarmPeriodSeconds,
			},
		}
	}

	return metricShape{queries: []cwbackend.MetricDataQuery{
		stat("m1", consumed, "Sum"),
		stat("m2", provisioned, statAverage),
		{ID: "e1", Expression: "(m1/(m2*60))*100", Label: "Utilization", ReturnData: true, Period: alarmPeriodSeconds},
	}}, true
}

func (b *InMemoryBackend) alarmSpec(
	p *ScalingPolicy,
	kind, name string,
	shape metricShape,
	threshold float64,
) *cwbackend.MetricAlarm {
	op, evals := "GreaterThanThreshold", int32(scaleOutDatapoints)
	if kind == alarmLowKind {
		op, evals = "LessThanThreshold", scaleInDatapoints
	}

	a := &cwbackend.MetricAlarm{
		AlarmName:          name,
		AlarmDescription:   "DO NOT EDIT OR DELETE. For TargetTrackingScaling policy " + p.ARN + ".",
		ComparisonOperator: op,
		Threshold:          threshold,
		Period:             alarmPeriodSeconds,
		EvaluationPeriods:  evals,
		DatapointsToAlarm:  evals,
		TreatMissingData:   "missing",
		AlarmActions:       []string{p.ARN},
		ActionsEnabled:     true,
	}

	if len(shape.queries) > 0 {
		a.Metrics = shape.queries
		a.Period = 0
		a.ThresholdMetricID = ""

		return a
	}

	a.Namespace, a.MetricName, a.Dimensions = shape.namespace, shape.metricName, shape.dims
	a.Statistic, a.Unit = shape.statistic, shape.unit

	return a
}

// targetTrackingAlarms returns the CloudWatch alarms AWS manages for policy p. Must not hold b.mu.
func targetTrackingAlarms(cw cwbackend.StorageBackend, p *ScalingPolicy) []cwbackend.MetricAlarm {
	var out []cwbackend.MetricAlarm

	token := ""

	for {
		metric, _, _, err := cw.DescribeAlarms(nil, nil, targetTrackingAlarmPrefix, "", token, 0, "", "", "")
		if err != nil {
			return out
		}

		for _, a := range metric.Data {
			if slices.Contains(a.AlarmActions, p.ARN) {
				out = append(out, a)
			}
		}

		if metric.Next == "" {
			return out
		}

		token = metric.Next
	}
}

// syncTargetTrackingAlarms creates, updates or removes the AlarmHigh/AlarmLow alarms backing p. Must not hold b.mu.
func (b *InMemoryBackend) syncTargetTrackingAlarms(p *ScalingPolicy) error {
	cw, ok := b.cloudWatchBackend()
	if !ok {
		return nil
	}

	existing := targetTrackingAlarms(cw, p)
	want := map[string]*cwbackend.MetricAlarm{}

	if p.PolicyType == policyTypeTargetTrackingScaling {
		if shape, mapped := metricFor(p.ResourceID, p.TargetTrackingConfig); mapped {
			target, _ := p.TargetTrackingConfig["TargetValue"].(float64)
			disableScaleIn, _ := p.TargetTrackingConfig["DisableScaleIn"].(bool)

			want[alarmHighKind] = b.alarmSpec(p, alarmHighKind, "", shape, target)
			if !disableScaleIn {
				want[alarmLowKind] = b.alarmSpec(p, alarmLowKind, "", shape, target*scaleInThresholdPart)
			}
		}
	}

	var stale []string

	for _, a := range existing {
		kind := alarmKind(a.AlarmName)
		if spec, keep := want[kind]; keep && spec.AlarmName == "" {
			spec.AlarmName = a.AlarmName
		} else {
			stale = append(stale, a.AlarmName)
		}
	}

	for kind, spec := range want {
		if spec.AlarmName == "" {
			spec.AlarmName = fmt.Sprintf("%s%s-%s-%s", targetTrackingAlarmPrefix, p.ResourceID, kind, uuid.NewString())
		}

		if err := cw.PutMetricAlarm(spec); err != nil {
			return fmt.Errorf("%w: creating %s alarm: %w", ErrValidation, kind, err)
		}
	}

	if len(stale) > 0 {
		return cw.DeleteAlarms(stale) //nolint:wrapcheck // cloudwatch error surfaces as-is
	}

	return nil
}

func alarmKind(name string) string {
	switch {
	case strings.Contains(name, "-"+alarmHighKind+"-"):
		return alarmHighKind
	case strings.Contains(name, "-"+alarmLowKind+"-"):
		return alarmLowKind
	}

	return ""
}

// deleteTargetTrackingAlarms removes the managed alarms of the policies with the given ARNs. Must not hold b.mu.
func (b *InMemoryBackend) deleteTargetTrackingAlarms(policyARNs ...string) {
	cw, ok := b.cloudWatchBackend()
	if !ok {
		return
	}

	for _, policyARN := range policyARNs {
		var names []string
		for _, a := range targetTrackingAlarms(cw, &ScalingPolicy{ARN: policyARN}) {
			names = append(names, a.AlarmName)
		}

		if len(names) > 0 {
			_ = cw.DeleteAlarms(names)
		}
	}
}
